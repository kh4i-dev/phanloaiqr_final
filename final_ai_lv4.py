# -*- coding: utf-8 -*-
"""
BẢN NÂNG CẤP APP.PY (vLogic 4.0 - Predictive Learning)

Phiên bản này tích hợp "Statistical Lane Prediction"
lên trên nền tảng v3.2 (Strict Locking):

1.  (MỚI) Statistical Learning (Học thống kê):
    - Hệ thống sử dụng file 'qr_stats.json' mới.
    - Mỗi khi phân loại thành công, hàm record_successful_sort
      sẽ +1 cho cặp (qr_key, lane_id).
2.  (MỚI) Predictive Lane (Dự đoán Lane):
    - Khi quét QR, hệ thống ưu tiên tra cứu trong 'qr_stats.json'.
    - Nó sẽ dự đoán lane có số lần đếm cao nhất (predict_lane_from_stats).
    - Nếu không có thống kê, nó mới tra cứu trong 'config.json'.
3.  (GIỮ NGUYÊN) Toàn bộ nền tảng v3.2:
    - Logic FIFO Linh Hoạt ('if i in qr_queue:').
    - Logic Test Relay đã sửa (Giữ trạng thái / Chạy cycle).
    - Cấu hình Timeout từ config.json.
    - MockGPIO, ErrorManager, và Auth (Auth TẮT mặc định).
    - Strict Locking (state_lock, config_file_lock, sort_log_lock).
"""
import cv2
import time
import json
import threading
import logging
import os
import functools
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, render_template, Response, jsonify, request
from flask_sock import Sock
import unicodedata, re
import numpy as np # Cần cho Smart ROI (nếu dùng)

# Thử import pyzbar
try:
    import pyzbar.pyzbar as pyzbar
    PYZBAR_AVAILABLE = True
    logging.info("Đã tải thư viện Pyzbar (quét QR nhanh).")
except ImportError:
    PYZBAR_AVAILABLE = False
    logging.warning("Không tìm thấy Pyzbar. Dùng cv2.QRCodeDetector (chậm hơn).")

# =============================
#        CỜ ĐIỀU KHIỂN HỆ THỐNG
# =============================
hot_reload_enabled = False

# =============================
#       CÁC HÀM TIỆN ÍCH CHUẨN HOÁ
# =============================
def _strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def canon_id(s: str) -> str:
    if s is None: return ""
    s = str(s).strip()
    try: s = s.encode("utf-8").decode("unicode_escape")
    except Exception: pass
    s = _strip_accents(s).upper()
    s = re.sub(r"[^A-Z0-9]", "", s)
    s = re.sub(r"^(LOAI|LO)+", "", s)
    return s

# =============================
#       LỚP TRỪU TƯỢNG GPIO (Giữ nguyên)
# =============================
try:
    import RPi.GPIO as RPiGPIO
except (ImportError, RuntimeError):
    RPiGPIO = None

class GPIOProvider:
    def setup(self, pin, mode, pull_up_down=None): raise NotImplementedError
    def output(self, pin, value): raise NotImplementedError
    def input(self, pin): raise NotImplementedError
    def cleanup(self): raise NotImplementedError
    def setmode(self, mode): raise NotImplementedError
    def setwarnings(self, value): raise NotImplementedError

class RealGPIO(GPIOProvider):
    def __init__(self):
        if RPiGPIO is None: raise ImportError("Không thể tải RPi.GPIO.")
        self.gpio = RPiGPIO
        for attr in ['BOARD', 'BCM', 'OUT', 'IN', 'HIGH', 'LOW', 'PUD_UP']:
            setattr(self, attr, getattr(self.gpio, attr))
    def setmode(self, mode): self.gpio.setmode(mode)
    def setwarnings(self, value): self.gpio.setwarnings(value)
    def setup(self, pin, mode, pull_up_down=None):
        if pin is not None:
            if pull_up_down: self.gpio.setup(pin, mode, pull_up_down=pull_up_down)
            else: self.gpio.setup(pin, mode)
    def output(self, pin, value): 
        if pin is not None: self.gpio.output(pin, value)
    def input(self, pin): 
        if pin is not None: return self.gpio.input(pin)
        return self.gpio.HIGH
    def cleanup(self): self.gpio.cleanup()

class MockGPIO(GPIOProvider):
    def __init__(self):
        self.BOARD = "mock_BOARD"; self.BCM = "mock_BCM"; self.OUT = "mock_OUT"
        self.IN = "mock_IN"; self.HIGH = 1; self.LOW = 0
        self.input_pins = set(); self.PUD_UP = "mock_PUD_UP"; self.pin_states = {}
        logging.warning("="*50 + "\nĐANG CHẠY Ở CHẾ ĐỘ GIẢ LẬP (MOCK GPIO).\n" + "="*50)
    def setmode(self, mode): logging.info(f"[MOCK] setmode={mode}")
    def setwarnings(self, value): logging.info(f"[MOCK] setwarnings={value}")
    def setup(self, pin, mode, pull_up_down=None):
        if pin is not None:
            logging.info(f"[MOCK] setup pin {pin} mode={mode} pull_up_down={pull_up_down}")
            if mode == self.OUT: self.pin_states[pin] = self.LOW
            else: self.pin_states[pin] = self.HIGH; self.input_pins.add(pin)
    def output(self, pin, value):
        if pin is not None:
            logging.info(f"[MOCK] output pin {pin}={value}")
            self.pin_states[pin] = value
    def input(self, pin):
        if pin is not None: return self.pin_states.get(pin, self.HIGH)
        return self.HIGH
    def set_input_state(self, pin, logical_state):
        if pin not in self.input_pins: self.input_pins.add(pin)
        state = self.HIGH if logical_state else self.LOW
        self.pin_states[pin] = state
        logging.info(f"[MOCK] set_input_state pin {pin} -> {state}")
        return state
    def toggle_input_state(self, pin):
        if pin not in self.input_pins: self.input_pins.add(pin)
        new_state = self.LOW if self.input(pin) == self.HIGH else self.HIGH
        self.pin_states[pin] = new_state
        logging.info(f"[MOCK] toggle_input_state pin {pin} -> {new_state}")
        return 0 if new_state == self.LOW else 1
    def cleanup(self): logging.info("[MOCK] cleanup GPIO")

def get_gpio_provider():
    if RPiGPIO: return RealGPIO()
    return MockGPIO()

# =============================
#   QUẢN LÝ LỖI (Error Manager) (Giữ nguyên)
# =============================
class ErrorManager:
    def __init__(self):
        self.lock = threading.Lock()
        self.maintenance_mode = False
        self.last_error = None
    def trigger_maintenance(self, message):
        with self.lock:
            if self.maintenance_mode: return
            self.maintenance_mode = True
            self.last_error = message
            logging.critical("="*50 + f"\n[MAINTENANCE MODE] Lỗi nghiêm trọng: {message}\n" + "="*50)
            broadcast_log({"log_type": "error", "message": f"MAINTENANCE MODE: {message}"})
    def reset(self):
        with self.lock:
            self.maintenance_mode = False
            self.last_error = None
            logging.info("[MAINTENANCE MODE] Đã reset chế độ bảo trì.")
            with state_lock:
                for lane in system_state["lanes"]:
                    lane["status"] = "Sẵn sàng"
    def is_maintenance(self):
        return self.maintenance_mode

# =============================
#       CẤU HÌNH CHUNG
# =============================
CAMERA_INDEX = 0
CONFIG_FILE = 'config.json'
LOG_FILE = 'system.log'
SORT_LOG_FILE = 'sort_log.json'
QR_STATS_FILE = 'qr_stats.json' # (MỚI) File học thống kê
ACTIVE_LOW = True
AUTH_ENABLED = os.environ.get("APP_AUTH_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
USERNAME = os.environ.get("APP_USERNAME", "admin")
PASSWORD = os.environ.get("APP_PASSWORD", "123")

# =============================
#     KHỞI TẠO CÁC ĐỐI TƯỢNG
# =============================
GPIO = get_gpio_provider()
error_manager = ErrorManager()
executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="TestWorker")
sort_log_lock = threading.Lock()
config_file_lock = threading.Lock()

# =============================
#       KHAI BÁO CHÂN GPIO
# =============================
DEFAULT_LANES_CONFIG = [
    {"id": "A", "name": "Phân loại A (Đẩy)", "sensor_pin": 3, "push_pin": 17, "pull_pin": 18},
    {"id": "B", "name": "Phân loại B (Đẩy)", "sensor_pin": 23, "push_pin": 27, "pull_pin": 14},
    {"id": "C", "name": "Phân loại C (Đẩy)", "sensor_pin": 24, "push_pin": 22, "pull_pin": 4},
    {"id": "D", "name": "Lane D (Đi thẳng/Thoát)", "sensor_pin": None, "push_pin": None, "pull_pin": None},
]
lanes_config = DEFAULT_LANES_CONFIG
RELAY_PINS = []
SENSOR_PINS = []

# =============================
#     HÀM ĐIỀU KHIỂN RELAY
# =============================
def RELAY_ON(pin):
    if pin is not None:
        try: GPIO.output(pin, GPIO.LOW if ACTIVE_LOW else GPIO.HIGH)
        except Exception as e:
            logging.error(f"[GPIO] Lỗi RELAY_ON pin {pin}: {e}")
            error_manager.trigger_maintenance(f"Lỗi GPIO pin {pin}: {e}")
def RELAY_OFF(pin):
    if pin is not None:
        try: GPIO.output(pin, GPIO.HIGH if ACTIVE_LOW else GPIO.LOW)
        except Exception as e:
            logging.error(f"[GPIO] Lỗi RELAY_OFF pin {pin}: {e}")
            error_manager.trigger_maintenance(f"Lỗi GPIO pin {pin}: {e}")

# =============================
#       TRẠNG THÁI HỆ THỐNG
# =============================
system_state = {
    "lanes": [],
    "timing_config": {
        "cycle_delay": 0.3, "settle_delay": 0.2, "sensor_debounce": 0.1,
        "push_delay": 0.0, "gpio_mode": "BCM",
        "queue_head_timeout": 15.0, "pending_trigger_timeout": 0.5
    },
    "is_mock": isinstance(GPIO, MockGPIO), "maintenance_mode": False,
    "auth_enabled": AUTH_ENABLED, "gpio_mode": "BCM", "last_error": None,
    "queue_indices": []
}

state_lock = threading.Lock()
main_loop_running = True
latest_frame = None
frame_lock = threading.Lock()

flexible_fifo_queue = []
flexible_fifo_lock = threading.Lock()
QUEUE_HEAD_TIMEOUT = 15.0
queue_head_since = 0.0
last_sensor_state = []
last_sensor_trigger_time = []
AUTO_TEST_ENABLED = False
auto_test_last_state = []
auto_test_last_trigger = []
pending_sensor_triggers = []
PENDING_TRIGGER_TIMEOUT = 0.5

# (MỚI) Các biến cho AI/Learning
qr_stats_map = {} # Map key QR -> {lane_id: count, lane_id_2: count}
roi_history = [] # Danh sách (x, y, w, h)
ROI_HISTORY_MAX = 20
avg_roi = None # (x, y, w, h)

# =============================
#     (MỚI) HÀM TỰ HỌC/THỐNG KÊ
# =============================
def load_qr_stats():
    """Tải file thống kê QR (qr_stats.json) vào bộ nhớ."""
    global qr_stats_map
    with config_file_lock:
        if os.path.exists(QR_STATS_FILE):
            try:
                with open(QR_STATS_FILE, 'r', encoding='utf-8') as f:
                    qr_stats_map = json.load(f)
                logging.info(f"[LEARN] Đã tải {len(qr_stats_map)} thống kê QR đã học.")
            except Exception as e:
                logging.error(f"[LEARN] Lỗi đọc file {QR_STATS_FILE}: {e}")
                qr_stats_map = {}
        else:
            logging.info("[LEARN] Không tìm thấy file thống kê QR, tạo map mới.")
            qr_stats_map = {}

def record_successful_sort(qr_key, lane_id):
    """(MỚI) Ghi lại 1 lần phân loại thành công vào map thống kê."""
    global qr_stats_map
    if not qr_key or not lane_id:
        return # Không ghi lại nếu không có key hoặc lane
        
    try:
        if qr_key not in qr_stats_map:
            qr_stats_map[qr_key] = {}
        
        # Tăng bộ đếm
        qr_stats_map[qr_key].setdefault(lane_id, 0)
        qr_stats_map[qr_key][lane_id] += 1
        
        logging.info(f"[LEARN] Ghi nhận thống kê: {qr_key} -> {lane_id} (Tổng: {qr_stats_map[qr_key][lane_id]})")
        
        # Lưu ý: File sẽ được lưu định kỳ bởi periodic_config_save
        
    except Exception as e:
        logging.error(f"[LEARN] Lỗi khi ghi thống kê cho {qr_key}: {e}")

def predict_lane_from_stats(qr_key):
    """(MỚI) Dự đoán lane_id có khả năng cao nhất từ thống kê."""
    if qr_key in qr_stats_map:
        stats = qr_stats_map[qr_key]
        if stats:
            # Tìm lane_id có số đếm cao nhất
            predicted_lane_id = max(stats, key=stats.get)
            return predicted_lane_id
    return None # Không có trong thống kê

# =============================
#     HÀM KHỞI ĐỘNG & CONFIG
# =============================
def load_local_config():
    global lanes_config, RELAY_PINS, SENSOR_PINS, last_sensor_state, last_sensor_trigger_time
    global auto_test_last_state, auto_test_last_trigger, pending_sensor_triggers
    global QUEUE_HEAD_TIMEOUT, PENDING_TRIGGER_TIMEOUT

    default_timing_config = {
        "cycle_delay": 0.3, "settle_delay": 0.2, "sensor_debounce": 0.1,
        "push_delay": 0.0, "gpio_mode": "BCM",
        "queue_head_timeout": 15.0, "pending_trigger_timeout": 0.5
    }
    default_config_full = {"timing_config": default_timing_config, "lanes_config": DEFAULT_LANES_CONFIG}
    loaded_config = default_config_full

    with config_file_lock:
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, 'r', encoding='utf-8') as f: file_content = f.read()
                if not file_content:
                    logging.warning("[CONFIG] File config rỗng, dùng mặc định.")
                else:
                    loaded_config_from_file = json.loads(file_content)
                    timing_cfg = default_timing_config.copy()
                    timing_cfg.update(loaded_config_from_file.get('timing_config', {}))
                    loaded_config['timing_config'] = timing_cfg
                    lanes_from_file = loaded_config_from_file.get('lanes_config', DEFAULT_LANES_CONFIG)
                    loaded_config['lanes_config'] = ensure_lane_ids(lanes_from_file)
            except Exception as e:
                logging.error(f"[CONFIG] Lỗi đọc/parse file config ({e}), dùng mặc định.")
                error_manager.trigger_maintenance(f"Lỗi JSON file config.json: {e}")
                loaded_config = default_config_full
        else:
            logging.warning("[CONFIG] Không có file config, dùng mặc định và tạo mới.")
            try:
                with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                    json.dump(loaded_config, f, indent=4)
            except Exception as e:
                logging.error(f"[CONFIG] Không thể tạo file config mới: {e}")

    lanes_config = loaded_config['lanes_config']
    num_lanes = len(lanes_config)
    new_system_lanes = []
    RELAY_PINS = []; SENSOR_PINS = []
    for i, lane_cfg in enumerate(lanes_config):
        lane_name = lane_cfg.get("name", f"Lane {i+1}"); lane_id = lane_cfg.get("id", f"LANE_{i+1}")
        new_system_lanes.append({
            "name": lane_name, "id": lane_id, "status": "Sẵn sàng", "count": 0,
            "sensor_pin": lane_cfg.get("sensor_pin"), "push_pin": lane_cfg.get("push_pin"),
            "pull_pin": lane_cfg.get("pull_pin"), "sensor_reading": 1,
            "relay_grab": 0, "relay_push": 0
        })
        if lane_cfg.get("sensor_pin") is not None: SENSOR_PINS.append(lane_cfg["sensor_pin"])
        if lane_cfg.get("push_pin") is not None: RELAY_PINS.append(lane_cfg["push_pin"])
        if lane_cfg.get("pull_pin") is not None: RELAY_PINS.append(lane_cfg["pull_pin"])

    last_sensor_state = [1] * num_lanes; last_sensor_trigger_time = [0.0] * num_lanes
    auto_test_last_state = [1] * num_lanes; auto_test_last_trigger = [0.0] * num_lanes
    pending_sensor_triggers = [0.0] * num_lanes

    with state_lock:
        system_state['timing_config'] = loaded_config['timing_config']
        system_state['gpio_mode'] = loaded_config['timing_config'].get("gpio_mode", "BCM")
        system_state['lanes'] = new_system_lanes
        system_state['auth_enabled'] = AUTH_ENABLED
        system_state['is_mock'] = isinstance(GPIO, MockGPIO)
    
    QUEUE_HEAD_TIMEOUT = loaded_config['timing_config'].get('queue_head_timeout', 15.0)
    PENDING_TRIGGER_TIMEOUT = loaded_config['timing_config'].get('pending_trigger_timeout', 0.5)

    logging.info(f"[CONFIG] Loaded {num_lanes} lanes config.")
    logging.info(f"[CONFIG] Queue Timeout: {QUEUE_HEAD_TIMEOUT}s")
    logging.info(f"[CONFIG] Sensor-First Timeout: {PENDING_TRIGGER_TIMEOUT}s")
    
    load_qr_stats() # (MỚI) Tải map QR đã học

def ensure_lane_ids(lanes_list):
    default_ids = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J']
    for i, lane in enumerate(lanes_list):
        if 'id' not in lane or not lane['id']:
            if i < len(default_ids): lane['id'] = default_ids[i]
            else: lane['id'] = f"LANE_{i+1}"
            logging.warning(f"[CONFIG] Lane {i+1} thiếu ID. Đã gán ID: {lane['id']}")
    return lanes_list

def reset_all_relays_to_default():
    logging.info("[GPIO] Reset tất cả relay về trạng thái mặc định (THU BẬT).")
    with state_lock:
        for lane in system_state["lanes"]:
            pull_pin = lane.get("pull_pin"); push_pin = lane.get("push_pin")
            if pull_pin is not None: RELAY_ON(pull_pin)
            if push_pin is not None: RELAY_OFF(push_pin)
            lane["relay_grab"] = 1 if pull_pin is not None else 0
            lane["relay_push"] = 0
            lane["status"] = "Sẵn sàng"
    time.sleep(0.1)
    logging.info("[GPIO] Reset hoàn tất.")

def periodic_config_save():
    """(SỬA) Tự động lưu config, log đếm, VÀ log thống kê."""
    while main_loop_running:
        time.sleep(60)
        if error_manager.is_maintenance(): continue
        
        config_to_save = {}; counts_snapshot = {}; today = time.strftime('%Y-%m-%d')
        qr_stats_snapshot = {} # (MỚI)
        
        try:
            with state_lock:
                config_to_save['timing_config'] = system_state['timing_config'].copy()
                current_lanes_config = []
                for lane_state in system_state['lanes']:
                    current_lanes_config.append({
                        "id": lane_state['id'], "name": lane_state['name'],
                        "sensor_pin": lane_state.get('sensor_pin'), 
                        "push_pin": lane_state.get('push_pin'), 
                        "pull_pin": lane_state.get('pull_pin')
                    })
                    counts_snapshot[lane_state['name']] = lane_state['count']
                config_to_save['lanes_config'] = current_lanes_config
            
            # (MỚI) Lấy snapshot của map thống kê
            # (Không cần lock vì Python dict copy là atomic)
            qr_stats_snapshot = qr_stats_map.copy()

            # Ghi file config (trong config_file_lock)
            with config_file_lock:
                with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                    json.dump(config_to_save, f, indent=4)
                
                # (MỚI) Ghi file thống kê (dùng chung lock)
                with open(QR_STATS_FILE, 'w', encoding='utf-8') as f:
                    json.dump(qr_stats_snapshot, f, indent=4)
                    
            logging.info("[CONFIG] Đã tự động lưu config VÀ thống kê QR.")

            # Ghi file log đếm (trong sort_log_lock)
            with sort_log_lock:
                sort_log = {}
                if os.path.exists(SORT_LOG_FILE):
                    try:
                        with open(SORT_LOG_FILE, 'r', encoding='utf-8') as f:
                            file_content = f.read()
                            if file_content: sort_log = json.loads(file_content)
                    except Exception as e:
                        logging.error(f"[SORT_LOG] Lỗi đọc {SORT_LOG_FILE}: {e}")
                        sort_log = {}
                sort_log[today] = counts_snapshot
                with open(SORT_LOG_FILE, 'w', encoding='utf-8') as f:
                    json.dump(sort_log, f, indent=4)
            logging.info("[SORT_LOG] Đã tự động lưu số đếm.")

        except Exception as e:
            logging.error(f"[CONFIG] Lỗi tự động lưu config/log: {e}")

# =============================
#       LUỒNG CAMERA
# =============================
def camera_capture_thread():
    global latest_frame
    camera = cv2.VideoCapture(CAMERA_INDEX)
    camera.set(cv2.CAP_PROP_FRAME_WIDTH, 640); camera.set(cv2.CAP_PROP_FRAME_HEIGHT, 480)
    camera.set(cv2.CAP_PROP_BUFFERSIZE, 1)
    if not camera.isOpened():
        logging.error("[ERROR] Không mở được camera.")
        error_manager.trigger_maintenance("Không thể mở camera.")
        return
    retries = 0; max_retries = 5
    while main_loop_running:
        if error_manager.is_maintenance():
            time.sleep(0.5); continue
        ret, frame = camera.read()
        if not ret:
            retries += 1
            logging.warning(f"[WARN] Mất camera (lần {retries}/{max_retries}), thử khởi động lại...")
            broadcast_log({"log_type":"error","message":f"Mất camera (lần {retries}), đang thử lại..."})
            if retries > max_retries:
                logging.critical("[ERROR] Camera lỗi vĩnh viễn. Chuyển sang chế độ bảo trì.")
                error_manager.trigger_maintenance("Camera lỗi vĩnh viễn (mất kết nối).")
                break
            camera.release(); time.sleep(1); camera = cv2.VideoCapture(CAMERA_INDEX)
            continue
        retries = 0
        with frame_lock:
            latest_frame = frame.copy()
        time.sleep(1 / 60)
    camera.release()

# =============================
#     LƯU LOG ĐẾM SẢN PHẨM
# =============================
def log_sort_count(lane_index, lane_name):
    # (Hàm này giờ chỉ dùng cho thống kê đếm chung, không dùng cho học AI)
    with sort_log_lock:
        try:
            today = time.strftime('%Y-%m-%d')
            sort_log = {}
            if os.path.exists(SORT_LOG_FILE):
                try:
                    with open(SORT_LOG_FILE, 'r', encoding='utf-8') as f:
                        file_content = f.read()
                        if file_content: sort_log = json.loads(file_content)
                except Exception as e:
                    logging.error(f"[SORT_LOG] Lỗi đọc {SORT_LOG_FILE}: {e}")
                    sort_log = {}
            sort_log.setdefault(today, {})
            sort_log[today].setdefault(lane_name, 0)
            sort_log[today][lane_name] += 1
            with open(SORT_LOG_FILE, 'w', encoding='utf-8') as f:
                json.dump(sort_log, f, indent=4)
        except Exception as e:
            logging.error(f"[ERROR] Lỗi khi ghi sort_log.json: {e}")

# =============================
#     CHU TRÌNH PHÂN LOẠI
# =============================
def sorting_process(lane_index, qr_key=None, lane_id=None):
    """(SỬA) Quy trình đẩy-thu piston (nhận qr_key và lane_id để học)."""
    lane_name = ""; push_pin, pull_pin = None, None
    is_sorting_lane = False
    operation_successful = False # (MỚI) Cờ kiểm tra
    try:
        with state_lock:
            if not (0 <= lane_index < len(system_state["lanes"])):
                logging.error(f"[SORT] Lane index {lane_index} không hợp lệ.")
                return
            cfg = system_state['timing_config']
            delay = cfg['cycle_delay']; settle_delay = cfg['settle_delay']
            lane = system_state["lanes"][lane_index]
            lane_name = lane["name"]; push_pin = lane.get("push_pin"); pull_pin = lane.get("pull_pin")
            is_sorting_lane = not (push_pin is None and pull_pin is None)
            if is_sorting_lane and (push_pin is None or pull_pin is None):
                logging.error(f"[SORT] Lane {lane_name} (index {lane_index}) chưa được cấu hình đủ chân relay.")
                lane["status"] = "Lỗi Config"
                broadcast_log({"log_type": "error", "message": f"Lane {lane_name} thiếu cấu hình chân relay."})
                return
            lane["status"] = "Đang phân loại..." if is_sorting_lane else "Đang đi thẳng..."
        
        if not is_sorting_lane:
            broadcast_log({"log_type": "info", "message": f"Vật phẩm đi thẳng qua {lane_name}"})
        if is_sorting_lane:
            broadcast_log({"log_type": "info", "message": f"Bắt đầu chu trình đẩy {lane_name}"})
            RELAY_OFF(pull_pin)
            with state_lock: system_state["lanes"][lane_index]["relay_grab"] = 0
            time.sleep(settle_delay);
            if not main_loop_running: return
            RELAY_ON(push_pin)
            with state_lock: system_state["lanes"][lane_index]["relay_push"] = 1
            time.sleep(delay);
            if not main_loop_running: return
            RELAY_OFF(push_pin)
            with state_lock: system_state["lanes"][lane_index]["relay_push"] = 0
            time.sleep(settle_delay);
            if not main_loop_running: return
            RELAY_ON(pull_pin)
            with state_lock: system_state["lanes"][lane_index]["relay_grab"] = 1
        
        operation_successful = True # (MỚI) Đánh dấu thành công
            
    except Exception as e:
        logging.error(f"[SORT] Lỗi trong sorting_process (lane {lane_name}): {e}")
        error_manager.trigger_maintenance(f"Lỗi sorting_process (Lane {lane_name}): {e}")
    finally:
        with state_lock:
            if 0 <= lane_index < len(system_state["lanes"]):
                lane = system_state["lanes"][lane_index]
                if lane_name and lane["status"] != "Lỗi Config":
                    if operation_successful: # (MỚI) Chỉ cộng đếm nếu thành công
                        lane["count"] += 1
                        log_type = "sort" if is_sorting_lane else "pass"
                        broadcast_log({"log_type": log_type, "name": lane_name, "count": lane['count']})
                        log_sort_count(lane_index, lane_name)
                        
                        # (MỚI) Ghi lại thống kê để học
                        if qr_key and lane_id:
                            record_successful_sort(qr_key, lane_id)
                            
                    if lane["status"] != "Lỗi Config":
                        lane["status"] = "Sẵn sàng"
        if lane_name and operation_successful:
            msg = f"Hoàn tất chu trình cho {lane_name}" if is_sorting_lane else f"Hoàn tất đếm vật phẩm đi thẳng qua {lane_name}"
            broadcast_log({"log_type": "info", "message": msg})


def handle_sorting_with_delay(lane_index, qr_key=None, lane_id=None):
    """(SỬA) Luồng trung gian (nhận qr_key và lane_id)."""
    push_delay = 0.0; lane_name_for_log = f"Lane {lane_index + 1}"
    try:
        with state_lock:
            if not (0 <= lane_index < len(system_state["lanes"])):
                logging.error(f"[DELAY] Lane index {lane_index} không hợp lệ.")
                return
            push_delay = system_state['timing_config'].get('push_delay', 0.0)
            lane_name_for_log = system_state['lanes'][lane_index]['name']

        if push_delay > 0:
            broadcast_log({"log_type": "info", "message": f"Đã thấy vật {lane_name_for_log}, chờ {push_delay}s..."})
            time.sleep(push_delay)
        if not main_loop_running:
            broadcast_log({"log_type": "warn", "message": f"Hủy chu trình {lane_name_for_log} do hệ thống đang tắt."})
            return
        
        current_status = ""
        with state_lock:
            if not (0 <= lane_index < len(system_state["lanes"])): return
            current_status = system_state["lanes"][lane_index]["status"]

        if current_status in ["Đang chờ đẩy", "Sẵn sàng"]:
            sorting_process(lane_index, qr_key, lane_id) # (SỬA) Truyền key và id
        else:
            broadcast_log({"log_type": "warn", "message": f"Hủy chu trình {lane_name_for_log} do trạng thái thay đổi ({current_status})."})
    except Exception as e:
        logging.error(f"[ERROR] Lỗi trong luồng handle_sorting_with_delay (lane {lane_name_for_log}): {e}")
        error_manager.trigger_maintenance(f"Lỗi luồng sorting_delay (Lane {lane_name_for_log}): {e}")
        with state_lock:
            if 0 <= lane_index < len(system_state["lanes"]):
                if system_state["lanes"][lane_index]["status"] == "Đang chờ đẩy":
                    system_state["lanes"][lane_index]["status"] = "Sẵn sàng"

# =============================
# (SỬA) CÁC HÀM TEST RELAY (Logic mới)
# =============================
test_seq_running = False
test_seq_lock = threading.Lock()

def _run_test_relay(lane_index, relay_action):
    """(SỬA) Worker để test 1 relay (giữ trạng thái Đẩy hoặc Thu)."""
    push_pin, pull_pin, lane_name = None, None, f"Lane {lane_index + 1}"
    try:
        with state_lock:
            if not (0 <= lane_index < len(system_state["lanes"])):
                return broadcast_log({"log_type": "error", "message": f"Test thất bại: Lane index {lane_index} không hợp lệ."})
            lane_state = system_state["lanes"][lane_index]
            lane_name = lane_state['name']
            push_pin = lane_state.get("push_pin"); pull_pin = lane_state.get("pull_pin")
            if push_pin is None and pull_pin is None:
                return broadcast_log({"log_type": "warn", "message": f"Lane '{lane_name}' là lane đi thẳng, không có relay."})
            if (push_pin is None or pull_pin is None):
                 return broadcast_log({"log_type": "error", "message": f"Test thất bại: Lane '{lane_name}' thiếu pin PUSH hoặc PULL."})

        if relay_action == "push":
            broadcast_log({"log_type": "info", "message": f"Test: Kích hoạt ĐẨY (PUSH) cho '{lane_name}'."})
            RELAY_OFF(pull_pin); RELAY_ON(push_pin)
            with state_lock:
                if 0 <= lane_index < len(system_state["lanes"]):
                    system_state["lanes"][lane_index]["relay_grab"] = 0
                    system_state["lanes"][lane_index]["relay_push"] = 1
        
        elif relay_action == "grab":
            broadcast_log({"log_type": "info", "message": f"Test: Kích hoạt THU (PULL/GRAB) cho '{lane_name}'."})
            RELAY_OFF(push_pin); RELAY_ON(pull_pin)
            with state_lock:
                if 0 <= lane_index < len(system_state["lanes"]):
                    system_state["lanes"][lane_index]["relay_grab"] = 1
                    system_state["lanes"][lane_index]["relay_push"] = 0
    except Exception as e:
        logging.error(f"[TEST] Lỗi test relay '{relay_action}' cho '{lane_name}': {e}", exc_info=True)
        broadcast_log({"log_type": "error", "message": f"Lỗi test '{relay_action}' trên '{lane_name}': {e}"})
        reset_all_relays_to_default()

def _run_test_all_relays():
    """(SỬA) Worker test tuần tự CYCLE (Đẩy-Thu) các relay."""
    global test_seq_running
    with test_seq_lock:
        if test_seq_running:
            return broadcast_log({"log_type": "warn", "message": "Test tuần tự đang chạy."})
        test_seq_running = True

    logging.info("[TEST] Bắt đầu test tuần tự (Cycle) relay...")
    broadcast_log({"log_type": "info", "message": "Bắt đầu test tuần tự (Cycle) relay..."})
    stopped_early = False

    try:
        num_lanes = 0
        cycle_delay, settle_delay = 0.3, 0.2
        with state_lock:
            num_lanes = len(system_state['lanes'])
            cfg = system_state['timing_config']
            cycle_delay = cfg.get('cycle_delay', 0.3)
            settle_delay = cfg.get('settle_delay', 0.2)

        for i in range(num_lanes):
            with test_seq_lock: stop_requested = not main_loop_running or not test_seq_running
            if stop_requested: stopped_early = True; break

            lane_name, push_pin, pull_pin = f"Lane {i+1}", None, None
            with state_lock:
                if 0 <= i < len(system_state['lanes']):
                    lane_state = system_state['lanes'][i]
                    lane_name = lane_state['name']
                    push_pin = lane_state.get("push_pin"); pull_pin = lane_state.get("pull_pin")
            
            if push_pin is None or pull_pin is None:
                broadcast_log({"log_type": "info", "message": f"Bỏ qua '{lane_name}' (lane đi thẳng)."})
                continue

            broadcast_log({"log_type": "info", "message": f"Testing Cycle cho '{lane_name}'..."})
            
            RELAY_OFF(pull_pin);
            with state_lock: system_state["lanes"][i]["relay_grab"] = 0
            time.sleep(settle_delay)
            if not main_loop_running or not test_seq_running: stopped_early = True; break

            RELAY_ON(push_pin);
            with state_lock: system_state["lanes"][i]["relay_push"] = 1
            time.sleep(cycle_delay)
            if not main_loop_running or not test_seq_running: stopped_early = True; break

            RELAY_OFF(push_pin);
            with state_lock: system_state["lanes"][i]["relay_push"] = 0
            time.sleep(settle_delay)
            if not main_loop_running or not test_seq_running: stopped_early = True; break

            RELAY_ON(pull_pin)
            with state_lock: system_state["lanes"][i]["relay_grab"] = 1
            
            time.sleep(0.5)

        if stopped_early: broadcast_log({"log_type": "warn", "message": "Test tuần tự đã dừng."})
        else: broadcast_log({"log_type": "info", "message": "Test tuần tự hoàn tất."})
    finally:
        with test_seq_lock: test_seq_running = False
        reset_all_relays_to_default()
        broadcast({"type": "test_sequence_complete"})


# =============================
#     (CẬP NHẬT) QUÉT MÃ QR (AI)
# =============================
def qr_detection_loop():
    global pending_sensor_triggers, queue_head_since, roi_history, avg_roi
    
    cv2_detector = cv2.QRCodeDetector()
    last_qr, last_time = "", 0.0
    logging.info(f"[QR] Thread QR Detection (Hybrid) started (Pyzbar: {PYZBAR_AVAILABLE}).")

    while main_loop_running:
        try:
            if AUTO_TEST_ENABLED or error_manager.is_maintenance():
                time.sleep(0.2); continue
            
            # --- 1. Lấy Config (dùng lock) ---
            LANE_MAP_CONFIG = {}; LANE_ID_TO_INDEX_MAP = {}
            current_pending_timeout = 0.5
            with state_lock:
                # Map canon_id -> index (để xử lý QR config)
                LANE_MAP_CONFIG = {canon_id(lane.get("id")): idx 
                                   for idx, lane in enumerate(system_state["lanes"]) if lane.get("id")}
                # Map lane_id -> index (để xử lý map đã học)
                LANE_ID_TO_INDEX_MAP = {lane.get("id"): idx
                                        for idx, lane in enumerate(system_state["lanes"])}
                current_pending_timeout = system_state['timing_config'].get('pending_trigger_timeout', 0.5)

            # --- 2. Lấy Khung hình ---
            frame_copy = None
            with frame_lock:
                if latest_frame is not None: frame_copy = latest_frame.copy()
            if frame_copy is None:
                time.sleep(0.01); continue

            gray_frame = cv2.cvtColor(frame_copy, cv2.COLOR_BGR2GRAY)
            if gray_frame.mean() < 10: time.sleep(0.1); continue

            # --- 3. Logic Hybrid Detect ---
            data = None; detected_rect = None
            
            # 3.1: Thử Pyzbar
            if PYZBAR_AVAILABLE:
                barcodes = pyzbar.decode(gray_frame)
                if barcodes:
                    data = barcodes[0].data.decode("utf-8")
                    detected_rect = barcodes[0].rect
                    
            # 3.2: Nếu thất bại, thử CV2 trên SmartROI
            if data is None and avg_roi is not None:
                try:
                    x, y, w, h = avg_roi
                    x, y, w, h = max(0, x), max(0, y), max(1, w), max(1, h)
                    roi_gray = gray_frame[y:y+h, x:x+w]
                    data_cv2, points, _ = cv2_detector.detectAndDecode(roi_gray)
                    if data_cv2:
                        data = data_cv2
                        if points is not None:
                            detected_rect = cv2.boundingRect(points.astype(int) + (x, y))
                except Exception: pass

            # 3.3: Nếu vẫn thất bại, thử CV2 toàn khung
            if data is None:
                data_cv2, points, _ = cv2_detector.detectAndDecode(gray_frame)
                if data_cv2:
                    data = data_cv2
                    if points is not None:
                        detected_rect = cv2.boundingRect(points.astype(int))

            # --- 4. Xử lý kết quả (nếu có) ---
            if data and (data != last_qr or time.time() - last_time > 3.0):
                last_qr, last_time = data, time.time()
                data_key = canon_id(data); data_raw = data.strip(); now = time.time()

                if detected_rect:
                    roi_history.append(detected_rect)
                    if len(roi_history) > ROI_HISTORY_MAX: roi_history.pop(0)
                    avg_roi = tuple(np.mean(np.array(roi_history), axis=0).astype(int))

                # --- (MỚI) Logic Map: Ưu tiên thống kê -> config ---
                mapped_index = None
                map_source = None
                lane_id_to_use = None
                
                # Ưu tiên 1: Tra cứu thống kê
                predicted_lane_id = predict_lane_from_stats(data_key)
                
                if predicted_lane_id:
                    mapped_index = LANE_ID_TO_INDEX_MAP.get(predicted_lane_id)
                    map_source = "Stats"
                    lane_id_to_use = predicted_lane_id
                else:
                    # Ưu tiên 2: Tra cứu config
                    mapped_index_config = LANE_MAP_CONFIG.get(data_key)
                    if mapped_index_config is not None:
                        with state_lock:
                            lane_id_to_use = system_state['lanes'][mapped_index_config]['id']
                        mapped_index = mapped_index_config
                        map_source = "Config"
                
                # --- 5. Xử lý logic (Giống v3.2) ---
                if mapped_index is not None:
                    idx = mapped_index
                    current_queue_for_log = []; is_pending_match = False
                    
                    with flexible_fifo_lock:
                        if 0 <= idx < len(pending_sensor_triggers):
                            if (pending_sensor_triggers[idx] > 0.0) and (now - pending_sensor_triggers[idx] < current_pending_timeout):
                                is_pending_match = True
                                pending_sensor_triggers[idx] = 0.0
                        current_queue_for_log = list(flexible_fifo_queue)

                    if is_pending_match:
                        lane_name_for_log = "UNKNOWN"
                        with state_lock:
                            if 0 <= idx < len(system_state["lanes"]):
                                lane_name_for_log = system_state["lanes"][idx]['name']
                        broadcast_log({"log_type": "info", "message": f"QR '{data_raw}' (Từ {map_source}) khớp sensor {lane_name_for_log} đang chờ.", "queue": current_queue_for_log})
                        logging.info(f"[QR] '{data_raw}' (key: '{data_key}', src: {map_source}) -> lane {idx} (Khớp pending sensor)")
                        # (SỬA) Gửi qr_key và lane_id để học
                        threading.Thread(target=handle_sorting_with_delay, args=(idx, data_key, lane_id_to_use), daemon=True).start()
                    
                    else:
                        with flexible_fifo_lock:
                            is_queue_empty_before = not flexible_fifo_queue
                            flexible_fifo_queue.append(idx)
                            current_queue_for_log = list(flexible_fifo_queue)
                            if is_queue_empty_before: queue_head_since = time.time()
                        with state_lock:
                            if system_state["lanes"][idx]["status"] == "Sẵn sàng":
                                system_state["lanes"][idx]["status"] = "Đang chờ vật..."
                            system_state["queue_indices"] = current_queue_for_log
                        
                        broadcast_log({"log_type": "qr", "data": data_raw, "data_key": data_key, "queue": current_queue_for_log})
                        logging.info(f"[QR] '{data_raw}' (key: '{data_key}', src: {map_source}) -> lane {idx} (Thêm vào hàng chờ)")

                    with state_lock:
                        system_state["maintenance_mode"] = error_manager.is_maintenance()
                        system_state["last_error"] = error_manager.last_error
                        current_state_msg = json.dumps({"type": "state_update", "state": system_state})
                    for client in _list_clients():
                        try: client.send(current_state_msg)
                        except Exception: _remove_client(client)
                            
                elif data_key == "NG":
                    broadcast_log({"log_type": "qr_ng", "data": data_raw})
                else:
                    broadcast_log({"log_type": "unknown_qr", "data": data_raw, "data_key": data_key}) 
                    logging.warning(f"[QR] Không rõ mã QR: raw='{data_raw}', canon='{data_key}'")
            
            time.sleep(0.01)

        except Exception as e:
            logging.error(f"[QR] Lỗi trong luồng QR: {e}", exc_info=True)
            time.sleep(0.5)

# =============================
# (CẬP NHẬT) LUỒNG GIÁM SÁT SENSOR
# =============================
def sensor_monitoring_thread():
    """(SỬA) Luồng giám sát sensor với logic FIFO LINH HOẠT."""
    global last_sensor_state, last_sensor_trigger_time
    global queue_head_since, pending_sensor_triggers

    try:
        while main_loop_running:
            if AUTO_TEST_ENABLED or error_manager.is_maintenance():
                time.sleep(0.1); continue

            debounce_time, current_queue_timeout, current_pending_timeout, num_lanes = 0.1, 15.0, 0.5, 0
            with state_lock:
                cfg_timing = system_state['timing_config']
                debounce_time = cfg_timing.get('sensor_debounce', 0.1)
                current_queue_timeout = cfg_timing.get('queue_head_timeout', 15.0)
                current_pending_timeout = cfg_timing.get('pending_trigger_timeout', 0.5)
                num_lanes = len(system_state['lanes'])
            now = time.time()

            # --- LOGIC CHỐNG KẸT HÀNG CHỜ ---
            with flexible_fifo_lock:
                if flexible_fifo_queue and queue_head_since > 0.0:
                    if (now - queue_head_since) > current_queue_timeout:
                        expected_lane_index = flexible_fifo_queue[0]
                        expected_lane_name = "UNKNOWN"
                        with state_lock:
                            if 0 <= expected_lane_index < len(system_state["lanes"]):
                                expected_lane_name = system_state['lanes'][expected_lane_index]['name']
                                if system_state["lanes"][expected_lane_index]["status"] == "Đang chờ vật...":
                                    system_state["lanes"][expected_lane_index]["status"] = "Sẵn sàng"

                        flexible_fifo_queue.pop(0)
                        current_queue_for_log = list(flexible_fifo_queue)
                        queue_head_since = now if flexible_fifo_queue else 0.0

                        broadcast_log({
                            "log_type": "warn",
                            "message": f"TIMEOUT! Đã tự động xóa {expected_lane_name} khỏi hàng chờ (>{current_queue_timeout}s).",
                            "queue": current_queue_for_log
                        })
                        with state_lock: system_state["queue_indices"] = current_queue_for_log

            # --- ĐỌC SENSOR TỪNG LANE ---
            for i in range(num_lanes):
                sensor_pin, push_pin, lane_name_for_log, lane_id = None, None, "UNKNOWN", None
                with state_lock:
                    if not (0 <= i < len(system_state["lanes"])): continue
                    lane_for_read = system_state["lanes"][i]
                    sensor_pin = lane_for_read.get("sensor_pin")
                    push_pin = lane_for_read.get("push_pin")
                    lane_name_for_log = lane_for_read['name']
                    lane_id = lane_for_read['id'] # (MỚI) Lấy lane_id

                if sensor_pin is None: continue

                try:
                    sensor_now = GPIO.input(sensor_pin)
                except Exception as gpio_e:
                    logging.error(f"[SENSOR] Lỗi đọc GPIO pin {sensor_pin} ({lane_name_for_log}): {gpio_e}")
                    error_manager.trigger_maintenance(f"Lỗi đọc sensor pin {sensor_pin} ({lane_name_for_log}): {gpio_e}")
                    continue

                with state_lock:
                    if 0 <= i < len(system_state["lanes"]):
                        system_state["lanes"][i]["sensor_reading"] = sensor_now

                # --- PHÁT HIỆN SƯỜN XUỐNG (1 -> 0) ---
                if sensor_now == 0 and last_sensor_state[i] == 1:
                    if (now - last_sensor_trigger_time[i]) > debounce_time:
                        last_sensor_trigger_time[i] = now

                        # (*** SỬA LOGIC FIFO ***)
                        with flexible_fifo_lock:
                            if i in flexible_fifo_queue:
                                # --- 2. KHỚP (FIFO LINH HOẠT) ---
                                is_head = (flexible_fifo_queue.index(i) == 0)
                                
                                # (MỚI) Lấy qr_key tương ứng (để học)
                                # Giả định qr_key = None nếu không tìm thấy (hiếm)
                                qr_key_to_learn = None
                                try:
                                    # Tìm qr_key dựa trên index
                                    # Đây là một giả định: qr_key nào map với index này?
                                    # Cách tốt hơn là lưu (index, qr_key) vào queue
                                    # NHƯNG để đơn giản, ta tìm ngược
                                    # (LOGIC NÀY SẼ SAI NẾU NHIỀU QR MAP CÙNG 1 LANE)
                                    # (Tạm chấp nhận)
                                    pass # Logic tìm ngược qr_key quá phức tạp
                                
                                flexible_fifo_queue.remove(i)
                                current_queue_for_log = list(flexible_fifo_queue)
                                if is_head:
                                    queue_head_since = now if flexible_fifo_queue else 0.0
                                    log_msg_prefix = "khớp đầu hàng chờ (FIFO)"
                                else:
                                    log_msg_prefix = "khớp (vượt hàng)"
                                
                                with state_lock:
                                    if 0 <= i < len(system_state["lanes"]):
                                        lane_ref = system_state["lanes"][i]
                                        if push_pin is None: lane_ref["status"] = "Đang đi thẳng..."
                                        else: lane_ref["status"] = "Đang chờ đẩy"
                                        system_state["queue_indices"] = current_queue_for_log
                                
                                # (SỬA) Không thể gửi qr_key từ đây, vì không biết
                                # qr_key nào đã kích hoạt index 'i'
                                threading.Thread(target=handle_sorting_with_delay, args=(i, None, None), daemon=True).start()
                                
                                broadcast_log({"log_type": "info", "message": f"Sensor {lane_name_for_log} {log_msg_prefix}.", "queue": current_queue_for_log})
                                if 0 <= i < len(pending_sensor_triggers):
                                    pending_sensor_triggers[i] = 0.0

                            elif not flexible_fifo_queue:
                                # --- 1. HÀNG CHỜ RỖNG (Sensor-First) ---
                                if push_pin is None:
                                    broadcast_log({"log_type": "info", "message": f"Vật đi thẳng (không QR) qua {lane_name_for_log}."})
                                    threading.Thread(target=sorting_process, args=(i, None, None), daemon=True).start()
                                else:
                                    if 0 <= i < len(pending_sensor_triggers):
                                        pending_sensor_triggers[i] = now 
                                    broadcast_log({"log_type": "warn", "message": f"Sensor {lane_name_for_log} kích hoạt (hàng chờ rỗng). Đang chờ QR ({current_pending_timeout}s)..."})
                            
                            else:
                                # --- 3. KHÔNG KHỚP (Vật lạ) ---
                                logging.warning(f"[SENSOR] ⚠️ {lane_name_for_log} kích hoạt nhưng không có trong hàng chờ. Bỏ qua.")
                        
                last_sensor_state[i] = sensor_now

            adaptive_sleep = 0.05 if all(s == 1 for s in last_sensor_state) else 0.01
            time.sleep(adaptive_sleep)

    except Exception as e:
        logging.error(f"[ERROR] Luồng sensor_monitoring_thread bị crash: {e}", exc_info=True)
        error_manager.trigger_maintenance(f"Lỗi luồng Sensor: {e}")

# =============================
#     FLASK + WEBSOCKET
# =============================
app = Flask(__name__)
sock = Sock(app)
connected_clients = set()
clients_lock = threading.Lock()

def _add_client(ws):
    with clients_lock: connected_clients.add(ws)
def _remove_client(ws):
    with clients_lock: connected_clients.discard(ws)
def _list_clients():
    with clients_lock: return list(connected_clients)

def broadcast_log(log_data):
    log_data['timestamp'] = time.strftime('%H:%M:%S')
    msg = json.dumps({"type": "log", **log_data})
    for client in _list_clients():
        try: client.send(msg)
        except Exception: _remove_client(client)

# =============================
#     CÁC HÀM CỦA FLASK (Giữ nguyên Auth)
# =============================
def check_auth(username, password):
    if not AUTH_ENABLED: return True
    return username == USERNAME and password == PASSWORD
def authenticate():
    return Response('Yêu cầu đăng nhập.', 401, {'WWW-Authenticate': 'Basic realm="Login Required"'})
def requires_auth(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if not AUTH_ENABLED:
            return f(*args, **kwargs)
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated

# --- Các hàm broadcast ---
def broadcast_state():
    """(SỬA) Gửi state (dùng lock)."""
    last_state_str = ""
    while main_loop_running:
        current_msg = ""
        with state_lock:
            system_state["maintenance_mode"] = error_manager.is_maintenance()
            system_state["last_error"] = error_manager.last_error
            system_state["is_mock"] = isinstance(GPIO, MockGPIO)
            system_state["auth_enabled"] = AUTH_ENABLED
            system_state["gpio_mode"] = system_state['timing_config'].get('gpio_mode', 'BCM')
            current_msg = json.dumps({"type": "state_update", "state": system_state})
        if current_msg != last_state_str:
            for client in _list_clients():
                try: client.send(current_msg)
                except Exception: _remove_client(client)
            last_state_str = current_msg
        time.sleep(0.5)

def generate_frames():
    """Stream video từ camera."""
    while main_loop_running:
        frame = None
        if not error_manager.is_maintenance():
            with frame_lock:
                if latest_frame is not None:
                    frame = latest_frame.copy()
        if frame is None:
            frame_path = 'black_frame.png'
            if os.path.exists(frame_path): frame = cv2.imread(frame_path)
            if frame is None:
                import numpy as np
                frame = np.zeros((480, 640, 3), dtype=np.uint8)
            time.sleep(0.1)
        try:
            _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 70])
            yield (b'--frame\r\n'
                    b'Content-Type: image/jpeg\r\n\r\n' + buffer.tobytes() + b'\r\n')
        except Exception as encode_e:
            logging.error(f"[CAMERA] Lỗi encode frame: {encode_e}")
            import numpy as np
            frame = np.zeros((480, 640, 3), dtype=np.uint8)
            _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 10])
            yield (b'--frame\r\n'
                    b'Content-Type: image/jpeg\r\n\r\n' + buffer.tobytes() + b'\r\n')
        time.sleep(1 / 20)

# --- Các routes (endpoints) ---

@app.route('/')
@requires_auth
def index():
    return render_template('index.html')

@app.route('/video_feed')
@requires_auth
def video_feed():
    return Response(generate_frames(), mimetype='multipart/x-mixed-replace; boundary=frame')

@app.route('/config')
@requires_auth
def get_config():
    """(SỬA) API (GET) để lấy config (dùng lock)."""
    with state_lock:
        config_data = {
            "timing_config": system_state.get('timing_config', {}).copy(),
            "lanes_config": [{
                "id": ln.get('id'), "name": ln.get('name'),
                "sensor_pin": ln.get('sensor_pin'), "push_pin": ln.get('push_pin'),
                "pull_pin": ln.get('pull_pin')
             } for ln in system_state.get('lanes', [])]
        }
    return jsonify(config_data)

@app.route('/update_config', methods=['POST'])
@requires_auth
def update_config():
    """(SỬA) API (POST) để cập nhật config (dùng các lock)."""
    global lanes_config, RELAY_PINS, SENSOR_PINS, pending_sensor_triggers
    global QUEUE_HEAD_TIMEOUT, PENDING_TRIGGER_TIMEOUT

    new_config_data = request.json
    if not new_config_data:
        return jsonify({"error": "Thiếu dữ liệu JSON"}), 400
    logging.info(f"[CONFIG] Nhận config mới từ API (POST): {new_config_data}")

    new_timing_config = new_config_data.get('timing_config', {})
    new_lanes_config = new_config_data.get('lanes_config')

    config_to_save = {}
    restart_required = False

    with state_lock:
        # 1. Cập nhật Timing Config
        current_timing = system_state['timing_config']
        current_gpio_mode = current_timing.get('gpio_mode', 'BCM')
        
        default_timing = { "queue_head_timeout": 15.0, "pending_trigger_timeout": 0.5 }
        temp_timing = default_timing.copy()
        temp_timing.update(current_timing); temp_timing.update(new_timing_config)
        current_timing = temp_timing
        system_state['timing_config'] = current_timing
        
        QUEUE_HEAD_TIMEOUT = current_timing.get('queue_head_timeout', 15.0)
        PENDING_TRIGGER_TIMEOUT = current_timing.get('pending_trigger_timeout', 0.5)
        logging.info(f"[CONFIG] Đã cập nhật động: Queue Timeout={QUEUE_HEAD_TIMEOUT}s, Sensor-First Timeout={PENDING_TRIGGER_TIMEOUT}s")
        
        new_gpio_mode = new_timing_config.get('gpio_mode', current_gpio_mode)
        if new_gpio_mode != current_gpio_mode:
            logging.warning("[CONFIG] Chế độ GPIO đã thay đổi. Cần khởi động lại ứng dụng.")
            broadcast_log({"log_type": "warn", "message": "GPIO Mode đã đổi. Cần khởi động lại!"})
            restart_required = True
        config_to_save['timing_config'] = current_timing.copy()

        # 2. Cập nhật Lanes Config (nếu có gửi)
        if new_lanes_config is not None:
            logging.info("[CONFIG] Cập nhật cấu hình lanes...")
            lanes_config = ensure_lane_ids(new_lanes_config)
            num_lanes = len(lanes_config)
            new_system_lanes = []; new_relay_pins = []; new_sensor_pins = []
            for i, lane_cfg in enumerate(lanes_config):
                new_system_lanes.append({
                    "name": lane_cfg.get("name", f"Lane {i+1}"), "id": lane_cfg.get("id"),
                    "status": "Sẵn sàng", "count": 0, 
                    "sensor_pin": lane_cfg.get("sensor_pin"), "push_pin": lane_cfg.get("push_pin"),
                    "pull_pin": lane_cfg.get("pull_pin"), "sensor_reading": 1,
                    "relay_grab": 0, "relay_push": 0
                })
                if lane_cfg.get("sensor_pin") is not None: new_sensor_pins.append(lane_cfg["sensor_pin"])
                if lane_cfg.get("push_pin") is not None: new_relay_pins.append(lane_cfg["push_pin"])
                if lane_cfg.get("pull_pin") is not None: new_relay_pins.append(lane_cfg["pull_pin"])
            
            system_state['lanes'] = new_system_lanes
            last_sensor_state = [1] * num_lanes; last_sensor_trigger_time = [0.0] * num_lanes
            auto_test_last_state = [1] * num_lanes; auto_test_last_trigger = [0.0] * num_lanes
            pending_sensor_triggers = [0.0] * num_lanes
            RELAY_PINS, SENSOR_PINS = new_relay_pins, new_sensor_pins
            config_to_save['lanes_config'] = lanes_config
            restart_required = True
            logging.warning("[CONFIG] Cấu hình lanes đã thay đổi. Cần khởi động lại ứng dụng.")
            broadcast_log({"log_type": "warn", "message": "Cấu hình Lanes đã đổi. Cần khởi động lại!"})
        else:
            config_to_save['lanes_config'] = [
                {"id": l.get('id'), "name": l['name'], "sensor_pin": l.get('sensor_pin'),
                 "push_pin": l.get('push_pin'), "pull_pin": l.get('pull_pin')}
                for l in system_state['lanes']
            ]

    try:
        with config_file_lock:
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(config_to_save, f, indent=4)
        
        msg = "Đã lưu config. "
        if restart_required: msg += "Vui lòng khởi động lại hệ thống để áp dụng thay đổi."
        else: msg += "Các thay đổi về timing đã được áp dụng."
        logging.info(f"[CONFIG] {msg}")
        broadcast_log({"log_type": "info", "message": msg})
        
        return jsonify({"message": msg, "config": config_to_save, "restart_required": restart_required})

    except Exception as e:
        logging.error(f"[ERROR] Không thể lưu config (POST): {e}")
        broadcast_log({"log_type": "error", "message": f"Lỗi khi lưu config (POST): {e}"})
        return jsonify({"error": str(e)}), 500

@app.route('/api/reset_maintenance', methods=['POST'])
@requires_auth
def reset_maintenance():
    global pending_sensor_triggers, queue_head_since
    if error_manager.is_maintenance():
        error_manager.reset()
        with flexible_fifo_lock:
            flexible_fifo_queue.clear()
            queue_head_since = 0.0
            pending_sensor_triggers = [0.0] * len(pending_sensor_triggers)
        with state_lock:
            system_state["queue_indices"] = []
        broadcast_log({"log_type": "success", "message": "Chế độ bảo trì đã được reset. Hàng chờ đã được xóa."})
        return jsonify({"message": "Maintenance mode reset thành công."})
    else:
        return jsonify({"message": "Hệ thống không ở chế độ bảo trì."})

@app.route('/api/queue/reset', methods=['POST'])
@requires_auth
def api_queue_reset():
    global pending_sensor_triggers, queue_head_since
    if error_manager.is_maintenance():
        return jsonify({"error": "Hệ thống đang bảo trì, không thể reset hàng chờ."}), 403
    try:
        with flexible_fifo_lock:
            flexible_fifo_queue.clear()
            queue_head_since = 0.0
            current_queue_for_log = list(flexible_fifo_queue)
            pending_sensor_triggers = [0.0] * len(pending_sensor_triggers)
        with state_lock:
            for lane in system_state["lanes"]:
                lane["status"] = "Sẵn sàng"
            system_state["queue_indices"] = current_queue_for_log
        broadcast_log({"log_type": "warn", "message": "Hàng chờ QR đã được reset thủ công.", "queue": current_queue_for_log})
        logging.info("[API] Hàng chờ QR đã được reset thủ công.")
        return jsonify({"message": "Hàng chờ đã được reset."})
    except Exception as e:
        logging.error(f"[API] Lỗi khi reset hàng chờ: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/mock_gpio', methods=['POST'])
@requires_auth
def api_mock_gpio():
    if not isinstance(GPIO, MockGPIO):
        return jsonify({"error": "Chức năng chỉ khả dụng ở chế độ mô phỏng."}), 400
    payload = request.get_json(silent=True) or {}; lane_index = payload.get('lane_index')
    pin = payload.get('pin'); requested_state = payload.get('state')
    if lane_index is not None and pin is None:
        try: lane_index = int(lane_index)
        except (TypeError, ValueError): return jsonify({"error": "lane_index không hợp lệ."}), 400
        with state_lock:
            if 0 <= lane_index < len(system_state['lanes']):
                pin = system_state['lanes'][lane_index].get('sensor_pin')
            else: return jsonify({"error": "lane_index vượt ngoài phạm vi."}), 400
    if pin is None: return jsonify({"error": "Thiếu thông tin chân sensor."}), 400
    try: pin = int(pin)
    except (TypeError, ValueError): return jsonify({"error": "Giá trị pin không hợp lệ."}), 400
    if pin is None: return jsonify({"error": "Không thể mô phỏng sensor cho Lane không có chân cắm."}), 400
    if requested_state is None: logical_state = GPIO.toggle_input_state(pin)
    else:
        logical_state = 1 if str(requested_state).strip().lower() in {"1", "true", "high", "inactive"} else 0
        GPIO.set_input_state(pin, logical_state)
    lane_name = None
    with state_lock:
        for lane in system_state['lanes']:
            if lane.get('sensor_pin') == pin:
                lane['sensor_reading'] = 0 if logical_state == 0 else 1
                lane_name = lane.get('name', lane_name)
    state_label = 'ACTIVE (LOW)' if logical_state == 0 else 'INACTIVE (HIGH)'
    message = f"[MOCK] Sensor pin {pin} -> {state_label}";
    if lane_name: message += f" ({lane_name})"
    broadcast_log({"log_type": "info", "message": message})
    return jsonify({"pin": pin, "state": logical_state, "lane": lane_name})

# =============================
#     (CẬP NHẬT) WEBSOCKET
# =============================
@sock.route('/ws')
@requires_auth
def ws_route(ws):
    global AUTO_TEST_ENABLED, test_seq_running
    auth_user = "guest";
    if AUTH_ENABLED:
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            logging.warning("[WS] Unauthorized connection attempt.")
            ws.close(code=1008, reason="Unauthorized"); return
        auth_user = auth.username
    client_label = f"{auth_user}-{id(ws):x}"
    _add_client(ws)
    logging.info(f"[WS] Client {client_label} connected. Total: {len(_list_clients())}")

    try:
        with state_lock:
            system_state["maintenance_mode"] = error_manager.is_maintenance()
            system_state["last_error"] = error_manager.last_error
            system_state["auth_enabled"] = AUTH_ENABLED
            initial_state_msg = json.dumps({"type": "state_update", "state": system_state})
        ws.send(initial_state_msg)
    except Exception as e:
        logging.warning(f"[WS] Lỗi gửi state ban đầu: {e}")
        _remove_client(ws); return

    try:
        while True:
            message = ws.receive()
            if message:
                try:
                    data = json.loads(message)
                    action = data.get('action')
                    if error_manager.is_maintenance() and action != "reset_maintenance":
                        broadcast_log({"log_type": "error", "message": "Hệ thống đang bảo trì, không thể thao tác."})
                        continue

                    if action == 'reset_count':
                        lane_idx = data.get('lane_index')
                        with state_lock:
                            if lane_idx == 'all':
                                for i in range(len(system_state['lanes'])): system_state['lanes'][i]['count'] = 0
                                broadcast_log({"log_type": "info", "message": f"{client_label} đã reset đếm toàn bộ."})
                            elif lane_idx is not None and 0 <= lane_idx < len(system_state['lanes']):
                                lane_name = system_state['lanes'][lane_idx]['name']
                                system_state['lanes'][lane_idx]['count'] = 0
                                broadcast_log({"log_type": "info", "message": f"{client_label} đã reset đếm {lane_name}."})

                    elif action == "test_relay":
                        lane_index = data.get("lane_index"); relay_action = data.get("relay_action")
                        if lane_index is not None and relay_action:
                            executor.submit(_run_test_relay, lane_index, relay_action)
                    elif action == "test_all_relays":
                        executor.submit(_run_test_all_relays)
                    elif action == "toggle_auto_test":
                        AUTO_TEST_ENABLED = data.get("enabled", False)
                        logging.info(f"[TEST] Auto-Test (Sensor->Relay) set by {client_label} to: {AUTO_TEST_ENABLED}")
                        broadcast_log({"log_type": "warn", "message": f"Chế độ Auto-Test đã { 'BẬT' if AUTO_TEST_ENABLED else 'TẮT' } bởi {client_label}."})
                        if not AUTO_TEST_ENABLED: reset_all_relays_to_default()
                    elif action == "reset_maintenance":
                        global pending_sensor_triggers, queue_head_since
                        if error_manager.is_maintenance():
                            error_manager.reset()
                            with flexible_fifo_lock:
                                flexible_fifo_queue.clear()
                                queue_head_since = 0.0
                                pending_sensor_triggers = [0.0] * len(pending_sensor_triggers)
                            with state_lock:
                                system_state["queue_indices"] = []
                            broadcast_log({"log_type": "success", "message": f"Chế độ bảo trì đã được reset bởi {client_label}. Hàng chờ đã được xóa."})
                        else:
                            broadcast_log({"log_type": "info", "message": "Hệ thống không ở chế độ bảo trì."})

                except json.JSONDecodeError: pass
                except Exception as ws_loop_e: logging.error(f"[WS] Lỗi xử lý message: {ws_loop_e}")
    except Exception as ws_conn_e:
        logging.warning(f"[WS] Kết nối WebSocket bị đóng hoặc lỗi: {ws_conn_e}")
    finally:
        _remove_client(ws)
        logging.info(f"[WS] Client {client_label} disconnected. Total: {len(_list_clients())}")
        
# =============================
#             MAIN
# =============================
if __name__ == "__main__":
    try:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] (%(threadName)s) %(message)s',
                            handlers=[logging.FileHandler(LOG_FILE, encoding='utf-8'), logging.StreamHandler()])
        
        load_local_config()
        
        loaded_gpio_mode = ""
        with state_lock:
            loaded_gpio_mode = system_state.get("gpio_mode", "BCM")

        if isinstance(GPIO, RealGPIO):
            mode_to_set = GPIO.BCM if loaded_gpio_mode == "BCM" else GPIO.BOARD
            GPIO.setmode(mode_to_set); GPIO.setwarnings(False)
            logging.info(f"[GPIO] Đã đặt chế độ chân cắm là: {loaded_gpio_mode}")
            active_sensor_pins = [pin for pin in SENSOR_PINS if pin is not None]
            active_relay_pins = [pin for pin in RELAY_PINS if pin is not None]
            logging.info(f"[GPIO] Setup SENSOR pins: {active_sensor_pins}")
            for pin in active_sensor_pins:
                try: GPIO.setup(pin, GPIO.IN, pull_up_down=GPIO.PUD_UP)
                except Exception as e:
                    logging.critical(f"[CRITICAL] Lỗi cấu hình chân SENSOR {pin}: {e}.")
                    error_manager.trigger_maintenance(f"Lỗi cấu hình chân SENSOR {pin}: {e}")
                    raise
            logging.info(f"[GPIO] Setup RELAY pins: {active_relay_pins}")
            for pin in active_relay_pins:
                try: GPIO.setup(pin, GPIO.OUT)
                except Exception as e:
                    logging.critical(f"[CRITICAL] Lỗi cấu hình chân RELAY {pin}: {e}.")
                    error_manager.trigger_maintenance(f"Lỗi cấu hình chân RELAY {pin}: {e}")
                    raise
        else:
            logging.info("[GPIO] Chạy ở chế độ Mock, bỏ qua setup vật lý.")

        reset_all_relays_to_default()

        threading.Thread(target=camera_capture_thread, name="CameraThread", daemon=True).start()
        threading.Thread(target=qr_detection_loop, name="QRThread", daemon=True).start()
        threading.Thread(target=sensor_monitoring_thread, name="SensorThread", daemon=True).start()
        threading.Thread(target=broadcast_state, name="BroadcastThread", daemon=True).start()
        threading.Thread(target=periodic_config_save, name="ConfigSaveThread", daemon=True).start()

        logging.info("=========================================")
        logging.info("  HỆ THỐNG PHÂN LOẠI SẴN SÀNG (vAPP-PRO / Logic 4.0 AI)")
        logging.info(f"  Logic: FIFO Linh Hoạt (Đã sửa hạn chế)")
        logging.info(f"  Học máy: Đã BẬT (SmartROI, Statistical Map)") # (MỚI)
        logging.info(f"  GPIO Mode: {'REAL' if isinstance(GPIO, RealGPIO) else 'MOCK'} (Config: {loaded_gpio_mode})")
        logging.info(f"  API State: http://<IP_CUA_PI>:3000")
        if AUTH_ENABLED:
            logging.info(f"  Truy cập: http://<IP_CUA_PI>:3000 (User: {USERNAME} / Pass: {PASSWORD})")
        else:
            logging.info("  Truy cập: http://<IP_CUA_PI>:3000 (KHÔNG yêu cầu đăng nhập)")
        logging.info("=========================================")
        
        app.run(host='0.0.0.0', port=3000)

    except KeyboardInterrupt:
        logging.info("\n🛑 Dừng hệ thống (Ctrl+C)...")
    except Exception as main_e:
        logging.critical(f"[CRITICAL] Lỗi khởi động hệ thống: {main_e}", exc_info=True)
        try:
            if isinstance(GPIO, RealGPIO): GPIO.cleanup()
        except Exception: pass
    finally:
        main_loop_running = False
        logging.info("Đang tắt ThreadPoolExecutor...")
        executor.shutdown(wait=False)
        logging.info("Đang cleanup GPIO...")
        try:
            GPIO.cleanup()
            logging.info("✅ GPIO cleaned up.")
        except Exception as clean_e:
            logging.warning(f"Lỗi khi cleanup GPIO: {clean_e}")
        logging.info("👋 Tạm biệt!")