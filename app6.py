# -*- coding: utf-8 -*-
import cv2
import time
import json
import threading
import logging
import os
import functools
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, render_template, Response, jsonify, request
from datetime import datetime # (MỚI) Thêm datetime để log theo giờ

# =============================
#      LỚP TRỪU TƯỢNG GPIO
# =============================
try:
    import RPi.GPIO as RPiGPIO
except (ImportError, RuntimeError):
    RPiGPIO = None

class GPIOProvider:
    """Lớp trừu tượng (Abstract Class) để tương tác GPIO."""
    def setup(self, pin, mode, pull_up_down=None): raise NotImplementedError
    def output(self, pin, value): raise NotImplementedError
    def input(self, pin): raise NotImplementedError
    def cleanup(self): raise NotImplementedError
    def setmode(self, mode): raise NotImplementedError
    def setwarnings(self, value): raise NotImplementedError

class RealGPIO(GPIOProvider):
    """Triển khai GPIO thật (chạy trên Raspberry Pi)."""
    def __init__(self):
        if RPiGPIO is None: raise ImportError("Không thể tải RPi.GPIO.")
        self.gpio = RPiGPIO
        # Gán các hằng số từ RPiGPIO vào instance
        for attr in ['BOARD', 'BCM', 'OUT', 'IN', 'HIGH', 'LOW', 'PUD_UP']:
            setattr(self, attr, getattr(self.gpio, attr))
    def setmode(self, mode): self.gpio.setmode(mode)
    def setwarnings(self, value): self.gpio.setwarnings(value)
    def setup(self, pin, mode, pull_up_down=None):
        if pull_up_down: self.gpio.setup(pin, mode, pull_up_down=pull_up_down)
        else: self.gpio.setup(pin, mode)
    def output(self, pin, value): self.gpio.output(pin, value)
    def input(self, pin): return self.gpio.input(pin)
    def cleanup(self): self.gpio.cleanup()

# (MỚI) Thêm biến global để điều khiển Mock Sensor
mock_pin_override = {} # { pin: (target_value, expiry_time) }
mock_pin_override_lock = threading.Lock()

class MockGPIO(GPIOProvider):
    """Triển khai GPIO giả lập (Mock) để test trên PC."""
    def __init__(self):
        # Gán các giá trị giả lập
        for attr, val in [('BOARD', "mock_BOARD"), ('BCM', "mock_BCM"), ('OUT', "mock_OUT"),
                          ('IN', "mock_IN"), ('HIGH', 1), ('LOW', 0), ('PUD_UP', "mock_PUD_UP")]:
            setattr(self, attr, val)
        self.pin_states = {}
        logging.warning("="*50 + "\nĐANG CHẠY Ở CHẾ ĐỘ GIẢ LẬP (MOCK GPIO).\n" + "="*50)

    def setmode(self, mode): logging.info(f"[MOCK] setmode={mode}")
    def setwarnings(self, value): logging.info(f"[MOCK] setwarnings={value}")
    def setup(self, pin, mode, pull_up_down=None):
        pin_name = PIN_TO_NAME_MAP.get(pin, pin) # Lấy tên pin nếu có
        logging.info(f"[MOCK] setup pin {pin_name} ({pin}) mode={mode} pull_up_down={pull_up_down}")
        if mode == self.OUT: self.pin_states[pin] = self.LOW
        else: self.pin_states[pin] = self.HIGH
    def output(self, pin, value):
        pin_name = PIN_TO_NAME_MAP.get(pin, pin)
        value_str = "HIGH" if value == self.HIGH else "LOW"
        logging.info(f"[MOCK] output pin {pin_name} ({pin}) = {value_str} ({value})")
        self.pin_states[pin] = value
    def input(self, pin):
        pin_name = PIN_TO_NAME_MAP.get(pin, pin)
        current_time = time.time()
        # Kiểm tra override trước
        with mock_pin_override_lock:
            if pin in mock_pin_override:
                target_value, expiry_time = mock_pin_override[pin]
                if current_time < expiry_time:
                    # logging.debug(f"[MOCK] input pin {pin_name} ({pin}) -> OVERRIDE {target_value}")
                    return target_value
                else:
                    # Hết hạn override, xóa đi
                    del mock_pin_override[pin]
                    logging.info(f"[MOCK] Override cho pin {pin_name} ({pin}) đã hết hạn.")

        # Trả về giá trị mặc định (luôn là HIGH/1 cho sensor)
        val = self.pin_states.get(pin, self.HIGH)
        # logging.debug(f"[MOCK] input pin {pin_name} ({pin}) -> {val}")
        return val
    def cleanup(self): logging.info("[MOCK] cleanup GPIO")

def get_gpio_provider():
    """Tự động chọn RealGPIO nếu có thư viện, ngược lại chọn MockGPIO."""
    if RPiGPIO: return RealGPIO()
    return MockGPIO()

# =============================
#  QUẢN LÝ LỖI (Error Manager)
# =============================
class ErrorManager:
    """Quản lý trạng thái lỗi/bảo trì của hệ thống."""
    def __init__(self):
        self.lock = threading.Lock()
        self.maintenance_mode = False
        self.last_error = None

    def trigger_maintenance(self, message):
        """Kích hoạt chế độ bảo trì."""
        with self.lock:
            if self.maintenance_mode: return
            self.maintenance_mode = True
            self.last_error = message
            logging.critical("="*50 + f"\n[MAINTENANCE MODE] Lỗi nghiêm trọng: {message}\n" +
                             "Hệ thống đã dừng hoạt động. Yêu cầu kiểm tra.\n" + "="*50)
            # (MỚI) Gửi thông báo bảo trì riêng biệt
            broadcast({"type": "maintenance_update", "enabled": True, "reason": message})

    def reset(self):
        """Reset lại trạng thái (khi admin yêu cầu)."""
        with self.lock:
            if not self.maintenance_mode: return # Không cần reset nếu đang không bảo trì
            self.maintenance_mode = False
            self.last_error = None
            logging.info("[MAINTENANCE MODE] Đã reset chế độ bảo trì.")
            # (MỚI) Gửi thông báo hết bảo trì
            broadcast({"type": "maintenance_update", "enabled": False})

    def is_maintenance(self): return self.maintenance_mode

# =============================
#         CẤU HÌNH CHUNG
# =============================
CAMERA_INDEX = 0
CONFIG_FILE = 'config.json'
LOG_FILE = 'system.log'
SORT_LOG_FILE = 'sort_log.json'
ACTIVE_LOW = True
USERNAME = "admin"
PASSWORD = "123"

# =============================
#      KHỞI TẠO CÁC ĐỐI TƯỢNG
# =============================
GPIO = get_gpio_provider()
error_manager = ErrorManager()
executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="TestWorker")
sort_log_lock = threading.Lock()
# (MỚI) Thêm cờ và lock cho test tuần tự
test_sequence_running = False
test_sequence_lock = threading.Lock()


# =============================
#  MAP PIN <-> TÊN (ĐỂ LOGGING)
# =============================
# Tạo map ngược từ Tên -> Số hiệu (sẽ dùng để tạo map Số hiệu -> Tên)
_PIN_NAME_TO_NUMBER = {
    "P1_PUSH": 17, "P1_PULL": 18,
    "P2_PUSH": 27, "P2_PULL": 14,
    "P3_PUSH": 22, "P3_PULL": 4,
    "SENSOR1": 3, "SENSOR2": 23, "SENSOR3": 24,
}
# (MỚI) Bản đồ Số hiệu -> Tên để log dễ đọc hơn
PIN_TO_NAME_MAP = {v: k for k, v in _PIN_NAME_TO_NUMBER.items()}

# =============================
#         KHAI BÁO CHÂN GPIO
# =============================
# Định nghĩa mặc định (sẽ bị ghi đè bởi config)
DEFAULT_LANES_CONFIG = [
    {"name": "Loại 1", "sensor_pin": _PIN_NAME_TO_NUMBER["SENSOR1"], "push_pin": _PIN_NAME_TO_NUMBER["P1_PUSH"], "pull_pin": _PIN_NAME_TO_NUMBER["P1_PULL"]},
    {"name": "Loại 2", "sensor_pin": _PIN_NAME_TO_NUMBER["SENSOR2"], "push_pin": _PIN_NAME_TO_NUMBER["P2_PUSH"], "pull_pin": _PIN_NAME_TO_NUMBER["P2_PULL"]},
    {"name": "Loại 3", "sensor_pin": _PIN_NAME_TO_NUMBER["SENSOR3"], "push_pin": _PIN_NAME_TO_NUMBER["P3_PUSH"], "pull_pin": _PIN_NAME_TO_NUMBER["P3_PULL"]},
]
lanes_config = DEFAULT_LANES_CONFIG
RELAY_PINS = []
SENSOR_PINS = []

# =============================
#       HÀM ĐIỀU KHIỂN RELAY
# =============================
def RELAY_ON(pin):
    pin_name = PIN_TO_NAME_MAP.get(pin, pin)
    logging.debug(f"[GPIO] Bật relay {pin_name} ({pin})")
    GPIO.output(pin, GPIO.LOW if ACTIVE_LOW else GPIO.HIGH)

def RELAY_OFF(pin):
    pin_name = PIN_TO_NAME_MAP.get(pin, pin)
    logging.debug(f"[GPIO] Tắt relay {pin_name} ({pin})")
    GPIO.output(pin, GPIO.HIGH if ACTIVE_LOW else GPIO.LOW)

# =============================
#        TRẠNG THÁI HỆ THỐNG
# =============================
system_state = { "lanes": [], "timing_config": {}, "is_mock": isinstance(GPIO, MockGPIO),
                 "maintenance_mode": False, "gpio_mode": "BCM", "last_error": None }
state_lock = threading.Lock()
main_loop_running = True
latest_frame = None
frame_lock = threading.Lock()
last_sensor_state, last_sensor_trigger_time = [], []
AUTO_TEST_ENABLED = False
auto_test_last_state, auto_test_last_trigger = [], []

# =============================
#      HÀM KHỞI ĐỘNG & CONFIG
# =============================
def load_local_config():
    """Tải cấu hình từ config.json, bao gồm cả timing và lanes."""
    global lanes_config, RELAY_PINS, SENSOR_PINS
    global last_sensor_state, last_sensor_trigger_time, auto_test_last_state, auto_test_last_trigger

    # Định nghĩa cấu hình mặc định ở đây để dễ quản lý
    default_timing_config = {"cycle_delay": 0.3, "settle_delay": 0.2, "sensor_debounce": 0.1,
                             "push_delay": 0.0, "gpio_mode": "BCM"}
    default_config_full = {"timing_config": default_timing_config, "lanes_config": DEFAULT_LANES_CONFIG}
    loaded_config = default_config_full

    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f: # Thêm encoding
                file_content = f.read()
                if file_content:
                    loaded_config_from_file = json.loads(file_content)
                    timing_cfg = default_timing_config.copy()
                    timing_cfg.update(loaded_config_from_file.get('timing_config', {}))
                    loaded_config['timing_config'] = timing_cfg
                    loaded_config['lanes_config'] = loaded_config_from_file.get('lanes_config', DEFAULT_LANES_CONFIG)
                else: logging.warning("[CONFIG] File config rỗng, dùng mặc định.")
        except json.JSONDecodeError as e:
            logging.error(f"[CONFIG] Lỗi đọc JSON ({e}), dùng mặc định.")
            error_manager.trigger_maintenance(f"Lỗi file config.json (JSON invalid): {e}")
            loaded_config = default_config_full
        except Exception as e:
            logging.error(f"[CONFIG] Lỗi đọc file config khác ({e}), dùng mặc định.")
            error_manager.trigger_maintenance(f"Lỗi file config.json: {e}")
            loaded_config = default_config_full
    else:
        logging.warning("[CONFIG] Không có file config, dùng mặc định và tạo mới.")
        try:
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f: # Thêm encoding
                json.dump(loaded_config, f, indent=4, ensure_ascii=False)
        except Exception as e: logging.error(f"[CONFIG] Không thể tạo file config mới: {e}")

    # Cập nhật global config, state, và danh sách pins
    lanes_config = loaded_config['lanes_config']
    num_lanes = len(lanes_config)
    new_system_lanes, RELAY_PINS, SENSOR_PINS = [], [], []
    for i, lane_cfg in enumerate(lanes_config):
        # Xác thực kiểu dữ liệu cơ bản cho pins
        sensor_pin = int(lane_cfg["sensor_pin"]) if lane_cfg.get("sensor_pin") is not None else None
        push_pin = int(lane_cfg["push_pin"]) if lane_cfg.get("push_pin") is not None else None
        pull_pin = int(lane_cfg["pull_pin"]) if lane_cfg.get("pull_pin") is not None else None

        new_system_lanes.append({"name": lane_cfg.get("name", f"Lane {i+1}"), "status": "Sẵn sàng", "count": 0,
                                 "sensor_pin": sensor_pin, "push_pin": push_pin, "pull_pin": pull_pin,
                                 "sensor_reading": 1, "relay_grab": 0, "relay_push": 0})
        if sensor_pin is not None: SENSOR_PINS.append(sensor_pin)
        if push_pin is not None: RELAY_PINS.append(push_pin)
        if pull_pin is not None: RELAY_PINS.append(pull_pin)

    # Khởi tạo lại các biến state dựa trên số lanes mới
    last_sensor_state = [1] * num_lanes; last_sensor_trigger_time = [0.0] * num_lanes
    auto_test_last_state = [1] * num_lanes; auto_test_last_trigger = [0.0] * num_lanes

    with state_lock:
        system_state['timing_config'] = loaded_config['timing_config']
        system_state['gpio_mode'] = loaded_config['timing_config'].get("gpio_mode", "BCM")
        system_state['lanes'] = new_system_lanes
    logging.info(f"[CONFIG] Loaded {num_lanes} lanes config.")
    logging.info(f"[CONFIG] Loaded timing config: {system_state['timing_config']}")

def reset_all_relays_to_default():
    """Reset tất cả relay về trạng thái an toàn (THU BẬT, ĐẨY TẮT)."""
    logging.info("[GPIO] Reset tất cả relay về trạng thái mặc định (THU BẬT).")
    with state_lock:
        for lane in system_state["lanes"]:
            pull_pin, push_pin = lane.get("pull_pin"), lane.get("push_pin")
            if pull_pin is not None: RELAY_ON(pull_pin)
            if push_pin is not None: RELAY_OFF(push_pin)
            lane["relay_grab"] = 1 if pull_pin is not None else 0
            lane["relay_push"] = 0
            lane["status"] = "Sẵn sàng"
    time.sleep(0.1)
    logging.info("[GPIO] Reset hoàn tất.")

def periodic_config_save():
    """Tự động lưu config mỗi 60s."""
    while main_loop_running:
        time.sleep(60)
        if error_manager.is_maintenance(): continue
        try:
            config_to_save = {}
            with state_lock:
                config_to_save['timing_config'] = system_state['timing_config'].copy()
                current_lanes_config = [{"name": ln['name'], "sensor_pin": ln['sensor_pin'],
                                         "push_pin": ln['push_pin'], "pull_pin": ln['pull_pin']}
                                        for ln in system_state['lanes']]
                config_to_save['lanes_config'] = current_lanes_config
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f: # Thêm encoding
                json.dump(config_to_save, f, indent=4, ensure_ascii=False)
            logging.info("[CONFIG] Đã tự động lưu config.")
        except Exception as e: logging.error(f"[CONFIG] Lỗi tự động lưu config: {e}")

# =============================
#         LUỒNG CAMERA
# =============================
def camera_capture_thread():
    """Luồng đọc camera và cập nhật latest_frame."""
    global latest_frame
    camera = None
    try:
        camera = cv2.VideoCapture(CAMERA_INDEX)
        camera.set(cv2.CAP_PROP_FRAME_WIDTH, 640)
        camera.set(cv2.CAP_PROP_FRAME_HEIGHT, 480)
        camera.set(cv2.CAP_PROP_BUFFERSIZE, 1)

        if not camera.isOpened():
            error_manager.trigger_maintenance("Không thể mở camera.")
            return

        retries = 0
        max_retries = 5

        while main_loop_running:
            if error_manager.is_maintenance(): time.sleep(0.5); continue

            ret, frame = camera.read()
            if not ret:
                retries += 1
                logging.warning(f"[WARN] Mất camera (lần {retries}/{max_retries}), thử khởi động lại...")
                broadcast_log({"log_type":"error","message":f"Mất camera (lần {retries}), đang thử lại..."})

                if retries > max_retries:
                    error_manager.trigger_maintenance("Camera lỗi vĩnh viễn (mất kết nối).")
                    break

                if camera: camera.release()
                time.sleep(1)
                camera = cv2.VideoCapture(CAMERA_INDEX)
                # Cài đặt lại các thuộc tính sau khi mở lại
                if camera.isOpened():
                     camera.set(cv2.CAP_PROP_FRAME_WIDTH, 640)
                     camera.set(cv2.CAP_PROP_FRAME_HEIGHT, 480)
                     camera.set(cv2.CAP_PROP_BUFFERSIZE, 1)
                else:
                     logging.error("[CAMERA] Không thể mở lại camera sau khi mất kết nối.")
                     time.sleep(2) # Chờ lâu hơn trước khi thử lại
                continue

            retries = 0
            with frame_lock: latest_frame = frame.copy()
            time.sleep(1 / 30)

    except Exception as e:
         logging.error(f"[CAMERA] Luồng camera bị crash: {e}", exc_info=True)
         error_manager.trigger_maintenance(f"Lỗi nghiêm trọng luồng camera: {e}")
    finally:
         if camera: camera.release()

# =============================
#       LƯU LOG ĐẾM SẢN PHẨM
# =============================
def log_sort_count(lane_index, lane_name):
    """Ghi lại số lượng đếm vào file JSON theo giờ (an toàn)."""
    with sort_log_lock:
        try:
            now = datetime.now()
            today = now.strftime('%Y-%m-%d')
            hour = now.strftime('%H') # Lấy giờ hiện tại (00-23)

            sort_log = {}
            if os.path.exists(SORT_LOG_FILE):
                try:
                    with open(SORT_LOG_FILE, 'r', encoding='utf-8') as f: # Thêm encoding
                        file_content = f.read()
                        if file_content: sort_log = json.loads(file_content)
                except json.JSONDecodeError:
                     logging.error(f"[SORT_LOG] Lỗi đọc JSON {SORT_LOG_FILE}, file hỏng.")
                     backup_name = f"{SORT_LOG_FILE}.{time.strftime('%Y%m%d_%H%M%S')}.bak"
                     try: os.rename(SORT_LOG_FILE, backup_name); logging.warning(f"[SORT_LOG] Backup file lỗi -> {backup_name}")
                     except Exception as re: logging.error(f"[SORT_LOG] Không thể backup: {re}")
                     sort_log = {}
                except Exception as e: logging.error(f"[SORT_LOG] Lỗi đọc file khác: {e}"); sort_log = {}

            # (MỚI) Cấu trúc theo giờ: { "2025-10-30": { "09": { "Loại 1": 5, "Loại 2": 2 }, "10": { ... } } }
            sort_log.setdefault(today, {}).setdefault(hour, {}).setdefault(lane_name, 0)
            sort_log[today][hour][lane_name] += 1

            with open(SORT_LOG_FILE, 'w', encoding='utf-8') as f: # Thêm encoding
                json.dump(sort_log, f, indent=2, ensure_ascii=False) # indent=2 cho gọn

        except Exception as e: logging.error(f"[ERROR] Lỗi ghi sort_log.json: {e}")

# =============================
#       CHU TRÌNH PHÂN LOẠI
# =============================
def sorting_process(lane_index):
    """Quy trình đẩy-thu piston."""
    lane_name, push_pin, pull_pin = "", None, None
    try:
        with state_lock:
            if not (0 <= lane_index < len(system_state["lanes"])): return logging.error(f"[SORT] Invalid lane index {lane_index}")
            lane = system_state["lanes"][lane_index]
            lane_name, push_pin, pull_pin = lane["name"], lane.get("push_pin"), lane.get("pull_pin")
            if not push_pin or not pull_pin:
                logging.error(f"[SORT] Lane {lane_name} thiếu config pin relay.")
                lane["status"] = "Lỗi Config"; broadcast_log({"log_type": "error", "message": f"Lane {lane_name} thiếu config pin."})
                return
            lane["status"] = "Đang phân loại..."
            cfg = system_state['timing_config']
            delay, settle_delay = cfg['cycle_delay'], cfg['settle_delay']

        broadcast_log({"log_type": "info", "message": f"Bắt đầu đẩy {lane_name}"})
        RELAY_OFF(pull_pin); time.sleep(settle_delay);
        with state_lock: system_state["lanes"][lane_index]["relay_grab"] = 0
        if not main_loop_running: return

        RELAY_ON(push_pin); time.sleep(delay);
        with state_lock: system_state["lanes"][lane_index]["relay_push"] = 1
        if not main_loop_running: return

        RELAY_OFF(push_pin); time.sleep(settle_delay);
        with state_lock: system_state["lanes"][lane_index]["relay_push"] = 0
        if not main_loop_running: return

        RELAY_ON(pull_pin)
        with state_lock: system_state["lanes"][lane_index]["relay_grab"] = 1

    except Exception as e:
         logging.error(f"[SORT] Lỗi sorting_process ({lane_name}): {e}", exc_info=True)
         error_manager.trigger_maintenance(f"Lỗi sorting_process ({lane_name}): {e}")
    finally:
        with state_lock:
             if 0 <= lane_index < len(system_state["lanes"]):
                 lane = system_state["lanes"][lane_index]
                 if lane_name and lane["status"] != "Lỗi Config":
                     lane["count"] += 1; broadcast_log({"log_type": "sort", "name": lane_name, "count": lane['count']})
                     log_sort_count(lane_index, lane_name)
                 if lane["status"] != "Lỗi Config": lane["status"] = "Sẵn sàng"
        if lane_name: broadcast_log({"log_type": "info", "message": f"Hoàn tất chu trình {lane_name}"})

def handle_sorting_with_delay(lane_index):
    """Luồng trung gian chờ push_delay."""
    push_delay, lane_name_for_log = 0.0, f"Lane {lane_index + 1}"
    try:
        with state_lock:
            if not (0 <= lane_index < len(system_state["lanes"])): return logging.error(f"[DELAY] Invalid lane index {lane_index}")
            push_delay = system_state['timing_config'].get('push_delay', 0.0)
            lane_name_for_log = system_state['lanes'][lane_index]['name']

        if push_delay > 0:
            broadcast_log({"log_type": "info", "message": f"Thấy vật {lane_name_for_log}, chờ {push_delay}s..."})
            time.sleep(push_delay)

        if not main_loop_running: return broadcast_log({"log_type": "warn", "message": f"Hủy chu trình {lane_name_for_log} do hệ thống tắt."})

        with state_lock:
            if not (0 <= lane_index < len(system_state["lanes"])): return
            current_status = system_state["lanes"][lane_index]["status"]

        if current_status == "Đang chờ đẩy": sorting_process(lane_index)
        else: broadcast_log({"log_type": "warn", "message": f"Hủy chu trình {lane_name_for_log} do trạng thái thay đổi."})

    except Exception as e:
        logging.error(f"[ERROR] Lỗi handle_sorting_with_delay ({lane_name_for_log}): {e}", exc_info=True)
        error_manager.trigger_maintenance(f"Lỗi sorting_delay ({lane_name_for_log}): {e}")
        with state_lock:
             if 0 <= lane_index < len(system_state["lanes"]) and system_state["lanes"][lane_index]["status"] == "Đang chờ đẩy":
                  system_state["lanes"][lane_index]["status"] = "Sẵn sàng"
                  broadcast_log({"log_type": "error", "message": f"Lỗi delay, reset {lane_name_for_log}"})

# =============================
#       QUÉT MÃ QR TỰ ĐỘNG
# =============================
def qr_detection_loop():
    """Luồng quét mã QR."""
    detector = cv2.QRCodeDetector(); last_qr, last_time = "", 0.0; LANE_MAP = {}
    try:
        with state_lock: LANE_MAP = {lane["name"].upper().replace(" ", ""): i for i, lane in enumerate(system_state["lanes"])}
        logging.info(f"[QR] Lane map: {LANE_MAP}")
    except Exception as e: logging.error(f"[QR] Lỗi tạo Lane Map: {e}")
    while main_loop_running:
        try:
            if AUTO_TEST_ENABLED or error_manager.is_maintenance(): time.sleep(0.2); continue
            frame, gray = None, None
            with frame_lock:
                if latest_frame is not None: frame = latest_frame.copy()
            if frame is None: time.sleep(0.1); continue
            if frame.shape[0] > 0 and frame.shape[1] > 0:
                 gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
                 if gray.mean() < 15: time.sleep(0.1); continue
            else: time.sleep(0.1); continue
            data, _, _ = detector.detectAndDecode(gray)
            if data and (data != last_qr or time.time() - last_time > 3.0):
                last_qr, last_time = data, time.time(); data_upper = data.strip().upper().replace(" ", "")
                logging.info(f"[QR] Detected: {data_upper}")
                if data_upper in LANE_MAP:
                    idx = LANE_MAP[data_upper]
                    with state_lock:
                         if 0 <= idx < len(system_state["lanes"]) and system_state["lanes"][idx]["status"] == "Sẵn sàng":
                             broadcast_log({"log_type": "qr", "data": data}); system_state["lanes"][idx]["status"] = "Đang chờ vật..."
                elif data_upper == "NG": broadcast_log({"log_type": "qr_ng", "data": data})
                else: broadcast_log({"log_type": "unknown_qr", "data": data})
            time.sleep(0.1)
        except cv2.error as cv_e: logging.warning(f"[QR] Lỗi OpenCV: {cv_e}"); time.sleep(0.2)
        except Exception as e: logging.error(f"[QR] Lỗi loop: {e}", exc_info=True); time.sleep(0.5)

# =============================
#      LUỒNG GIÁM SÁT SENSOR
# =============================
def sensor_monitoring_thread():
    """Luồng giám sát sensor."""
    global last_sensor_state, last_sensor_trigger_time
    try:
        while main_loop_running:
            if AUTO_TEST_ENABLED or error_manager.is_maintenance(): time.sleep(0.1); continue
            with state_lock: debounce = system_state['timing_config']['sensor_debounce']; num = len(system_state['lanes'])
            now = time.time()
            for i in range(num):
                with state_lock:
                    if not (0 <= i < len(system_state["lanes"])): continue
                    lane = system_state["lanes"][i]; pin = lane.get("sensor_pin"); name = lane.get('name', f'L{i+1}'); status = lane["status"]
                if not pin: continue
                try: current = GPIO.input(pin)
                except Exception as e: logging.error(f"[SENSOR] Lỗi đọc GPIO {pin} ({name}): {e}"); error_manager.trigger_maintenance(f"Lỗi sensor {pin} ({name}): {e}"); continue
                with state_lock:
                     if 0 <= i < len(system_state["lanes"]): system_state["lanes"][i]["sensor_reading"] = current
                if current == 0 and last_sensor_state[i] == 1 and (now - last_sensor_trigger_time[i]) > debounce:
                    last_sensor_trigger_time[i] = now
                    if status == "Đang chờ vật...":
                        with state_lock:
                            if 0 <= i < len(system_state["lanes"]): system_state["lanes"][i]["status"] = "Đang chờ đẩy"
                        threading.Thread(target=handle_sorting_with_delay, args=(i,), daemon=True, name=f"Delay_{i}").start()
                    else: broadcast_log({"log_type": "warn", "message": f"Sensor {name} kích hoạt ngoài dự kiến."})
                last_sensor_state[i] = current
            sleep = 0.05 if all(s == 1 for s in last_sensor_state) else 0.01; time.sleep(sleep)
    except Exception as e: logging.error(f"[SENSOR] Crash: {e}", exc_info=True); error_manager.trigger_maintenance(f"Lỗi luồng Sensor: {e}")

# =============================
#        FLASK + WEBSOCKET
# =============================
app = Flask(__name__); from flask_sock import Sock; sock = Sock(app); connected_clients = set()
def broadcast(data):
    msg = json.dumps(data)
    for client in list(connected_clients):
        try: client.send(msg)
        except Exception: connected_clients.discard(client)
def broadcast_log(log_data): log_data['timestamp'] = datetime.now().strftime('%H:%M:%S'); broadcast({"type": "log", **log_data})

# =============================
#      CÁC HÀM XỬ LÝ TEST (🧪)
# =============================
def _run_test_relay(lane_index, relay_action):
    """Worker test 1 relay."""
    pin, key, name = None, None, f"L{lane_index + 1}"
    try:
        with state_lock:
            if not (0 <= lane_index < len(system_state["lanes"])): return broadcast_log({"log_type": "error", "message": f"Test fail: Invalid index {lane_index}."})
            lane = system_state["lanes"][lane_index]; name = lane['name']
            pin = lane.get("pull_pin") if relay_action == "grab" else lane.get("push_pin")
            key = "relay_grab" if relay_action == "grab" else "relay_push"
            if not pin: return broadcast_log({"log_type": "error", "message": f"Test fail: Lane {name} thiếu pin {relay_action}."})
        RELAY_ON(pin); with state_lock: system_state["lanes"][lane_index][key] = 1; time.sleep(0.5)
        # (MỚI) Kiểm tra cờ dừng ở đây nữa
        with test_sequence_lock:
            if not main_loop_running or not test_sequence_running:
                 logging.debug(f"[TEST] Dừng test relay {name} ({relay_action}) do cờ.")
                 # Tắt relay trước khi thoát
                 RELAY_OFF(pin);
                 with state_lock: system_state["lanes"][lane_index][key] = 0
                 return # Thoát sớm

        RELAY_OFF(pin); with state_lock: system_state["lanes"][lane_index][key] = 0
        broadcast_log({"log_type": "info", "message": f"Test {relay_action} {name} OK"})
    except Exception as e: logging.error(f"[TEST] Lỗi ({name}): {e}", exc_info=True); broadcast_log({"log_type": "error", "message": f"Lỗi test {name}: {e}"})

def _run_test_all_relays():
    """Worker test tuần tự các relay."""
    global test_sequence_running
    # Đặt cờ báo đang chạy test tuần tự
    with test_sequence_lock:
        if test_sequence_running:
            logging.warning("[TEST] Test tuần tự đang chạy, bỏ qua yêu cầu mới.")
            broadcast_log({"log_type": "warn", "message":"Test tuần tự đang chạy, vui lòng chờ."})
            return
        test_sequence_running = True

    logging.info("[TEST] Bắt đầu test tuần tự...")
    broadcast_log({"log_type": "info", "message":"Bắt đầu test tuần tự 6 relay..."})
    stopped_early = False # Cờ báo đã dừng sớm
    try:
        with state_lock: num_lanes = len(system_state['lanes'])
        for i in range(num_lanes):
            # Kiểm tra cờ dừng thường xuyên
            with test_sequence_lock:
                if not main_loop_running or not test_sequence_running:
                    stopped_early = True; break # Dừng vòng lặp
            with state_lock: name = system_state['lanes'][i]['name'] if 0 <= i < len(system_state['lanes']) else f"L{i+1}"
            broadcast_log({"log_type": "info", "message": f"Test THU {name}..."}); _run_test_relay(i, "grab"); time.sleep(0.5)

            with test_sequence_lock: # Kiểm tra lại
                if not main_loop_running or not test_sequence_running: stopped_early = True; break
            broadcast_log({"log_type": "info", "message": f"Test ĐẨY {name}..."}); _run_test_relay(i, "push"); time.sleep(0.5)

        # Log và broadcast dựa trên cờ stopped_early
        if stopped_early:
            logging.info("[TEST] Test tuần tự bị dừng.")
            broadcast_log({"log_type": "warn", "message":"Test tuần tự đã bị dừng."})
        else:
            logging.info("[TEST] Hoàn tất test tuần tự.")
            broadcast_log({"log_type": "info", "message":"Hoàn tất test tuần tự."})
    finally:
        # Reset cờ khi kết thúc
        with test_sequence_lock:
            test_sequence_running = False
        # Gửi thông báo để UI bật lại nút
        broadcast({"type": "test_sequence_complete"})


def _auto_test_cycle_worker(lane_index):
    """Worker cho chu trình Auto-Test."""
    name = f"L{lane_index + 1}"
    try:
        with state_lock:
            if 0 <= lane_index < len(system_state['lanes']): name = system_state['lanes'][lane_index]['name']
        broadcast_log({"log_type": "warn", "message": f"AUTO-TEST: Đẩy {name}"}); _run_test_relay(lane_index, "push")
        time.sleep(0.3)
        if not main_loop_running: return
        broadcast_log({"log_type": "warn", "message": f"AUTO-TEST: Thu {name}"}); _run_test_relay(lane_index, "grab")
    except Exception as e: logging.error(f"[TEST] Lỗi auto worker ({name}): {e}", exc_info=True)

def auto_test_loop():
    """Luồng riêng cho auto-test."""
    global AUTO_TEST_ENABLED, auto_test_last_state, auto_test_last_trigger
    logging.info("[TEST] Luồng Auto-Test đã khởi động.")
    try:
        while main_loop_running:
            if error_manager.is_maintenance():
                if AUTO_TEST_ENABLED: AUTO_TEST_ENABLED = False; logging.warning("[TEST] Tắt Auto-Test do lỗi."); broadcast_log({"log_type": "error", "message": "Tắt Auto-Test do bảo trì."})
                time.sleep(0.2); continue
            with state_lock: num = len(system_state['lanes'])
            if AUTO_TEST_ENABLED:
                now = time.time()
                for i in range(num):
                    with state_lock:
                        if not (0 <= i < len(system_state["lanes"])): continue
                        pin = system_state["lanes"][i].get("sensor_pin")
                    if not pin: continue
                    try: current = GPIO.input(pin)
                    except Exception as e: logging.error(f"[AUTO-TEST] Lỗi đọc GPIO {pin} ({i+1}): {e}"); error_manager.trigger_maintenance(f"Lỗi sensor {pin} (Auto-Test): {e}"); continue
                    with state_lock:
                         if 0 <= i < len(system_state["lanes"]): system_state["lanes"][i]["sensor_reading"] = current
                    if current == 0 and auto_test_last_state[i] == 1 and (now - auto_test_last_trigger[i]) > 1.0:
                        auto_test_last_trigger[i] = now
                        with state_lock: name = system_state['lanes'][i]['name'] if 0 <= i < len(system_state['lanes']) else f"L{i+1}"
                        broadcast_log({"log_type": "warn", "message": f"AUTO-TEST: Sensor {name} phát hiện!"})
                        executor.submit(_auto_test_cycle_worker, i)
                    auto_test_last_state[i] = current
                time.sleep(0.02)
            else:
                auto_test_last_state = [1] * num; auto_test_last_trigger = [0.0] * num; time.sleep(0.2)
    except Exception as e: logging.error(f"[AUTO-TEST] Crash: {e}", exc_info=True); error_manager.trigger_maintenance(f"Lỗi luồng Auto-Test: {e}")

def mock_trigger_pin_ws(pin, value, duration):
    """Kích hoạt override cho mock pin."""
    name = PIN_TO_NAME_MAP.get(pin, pin); val_str = "HIGH" if value == GPIO.HIGH else "LOW"
    logging.info(f"[MOCK] Trigger pin {name} ({pin}) = {val_str} ({value}) / {duration}s")
    with mock_pin_override_lock: mock_pin_override[pin] = (value, time.time() + duration)
    broadcast_log({"log_type": "info", "message": f"Mock: Đặt {name} = {val_str} / {duration}s"})

# =============================
#     CÁC HÀM CỦA FLASK (TIẾP)
# =============================
def check_auth(username, password): return username == USERNAME and password == PASSWORD
def authenticate(): return Response('Yêu cầu đăng nhập.', 401, {'WWW-Authenticate': 'Basic realm="Login Required"'})
def requires_auth(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password): return authenticate()
        return f(*args, **kwargs)
    return decorated

def broadcast_state():
    """Gửi state cho client."""
    last = "";
    while main_loop_running:
        current = ""
        with state_lock:
            system_state["maintenance_mode"] = error_manager.is_maintenance()
            system_state["last_error"] = error_manager.last_error
            system_state["gpio_mode"] = system_state['timing_config'].get('gpio_mode', 'BCM')
            try: current = json.dumps({"type": "state_update", "state": system_state})
            except TypeError as e: logging.error(f"Lỗi JSON dump state: {e}"); continue
        if current != last: broadcast(json.loads(current)); last = current
        time.sleep(0.5)

def generate_frames():
    """Stream video."""
    black = 'black_frame.png'; black_img = cv2.imread(black) if os.path.exists(black) else None
    if black_img is None: import numpy as np; black_img = np.zeros((480, 640, 3), dtype=np.uint8); logging.warning("[CAM] black_frame.png not found.")
    while main_loop_running:
        frame = None
        if not error_manager.is_maintenance():
            with frame_lock:
                if latest_frame is not None: frame = latest_frame.copy()
        current = frame if frame is not None else black_img
        try:
            ok, buf = cv2.imencode('.jpg', current, [cv2.IMWRITE_JPEG_QUALITY, 70])
            if ok: yield (b'--frame\r\nContent-Type: image/jpeg\r\n\r\n' + buf.tobytes() + b'\r\n')
            else: logging.warning("[CAM] Lỗi imencode.")
        except Exception as e: logging.error(f"[CAM] Lỗi encode: {e}", exc_info=True)
        time.sleep(1 / 20)

@app.route('/'); @requires_auth
def index(): return render_template('index.html')
@app.route('/video_feed'); @requires_auth
def video_feed(): return Response(generate_frames(), mimetype='multipart/x-mixed-replace; boundary=frame')
@app.route('/config'); @requires_auth
def get_config():
    with state_lock: cfg = {"timing_config": system_state.get('timing_config', {}), "lanes_config": [{"name": ln.get('name'), "sensor_pin": ln.get('sensor_pin'), "push_pin": ln.get('push_pin'), "pull_pin": ln.get('pull_pin')} for ln in system_state.get('lanes', [])]}
    return jsonify(cfg)
@app.route('/update_config', methods=['POST']); @requires_auth
def update_config():
    """API POST cập nhật config."""
    global lanes_config, RELAY_PINS, SENSOR_PINS, last_sensor_state, last_sensor_trigger_time, auto_test_last_state, auto_test_last_trigger
    data = request.json; user = request.authorization.username
    if not data: return jsonify({"error": "Thiếu JSON"}), 400
    logging.info(f"[CONFIG] Nhận config mới (POST) từ {user}: {data}")
    timing, lanes = data.get('timing_config', {}), data.get('lanes_config')
    cfg_save, restart = {}, False
    with state_lock:
        curr_t = system_state['timing_config']; curr_m = curr_t.get('gpio_mode', 'BCM')
        curr_t.update(timing); new_m = curr_t.get('gpio_mode', 'BCM')
        if new_m != curr_m: logging.warning("[CONFIG] GPIO Mode đổi. Cần restart!"); broadcast_log({"log_type": "warn", "message": "GPIO Mode đổi. Cần restart!"}); restart = True; curr_t['gpio_mode'] = curr_m
        system_state['gpio_mode'] = curr_t['gpio_mode']; cfg_save['timing_config'] = curr_t.copy()
        if isinstance(lanes, list):
             logging.info("[CONFIG] Cập nhật lanes..."); lanes_config = lanes; num = len(lanes_config)
             new_l, new_r, new_s = [], [], []
             for i, cfg in enumerate(lanes):
                 s, p, pl = (int(cfg[k]) if cfg.get(k) is not None else None for k in ["sensor_pin", "push_pin", "pull_pin"])
                 new_l.append({"name": cfg.get("name", f"L{i+1}"), "status": "Sẵn sàng", "count": 0, "sensor_pin": s, "push_pin": p, "pull_pin": pl, "sensor_reading": 1, "relay_grab": 0, "relay_push": 0})
                 if s is not None: new_s.append(s);
                 if p is not None: new_r.append(p)
                 if pl is not None: new_r.append(pl)
             system_state['lanes'] = new_l
             last_sensor_state = [1]*num; last_sensor_trigger_time = [0.0]*num; auto_test_last_state = [1]*num; auto_test_last_trigger = [0.0]*num
             RELAY_PINS, SENSOR_PINS = new_r, new_s; cfg_save['lanes_config'] = lanes_config; restart = True
             logging.warning("[CONFIG] Lanes config đổi. Cần restart!"); broadcast_log({"log_type": "warn", "message": "Lanes config đổi. Cần restart!"})
        else: cfg_save['lanes_config'] = [{"name":ln['name'], "sensor_pin":ln['sensor_pin'], "push_pin":ln['push_pin'], "pull_pin":ln['pull_pin']} for ln in system_state['lanes']]
    try:
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f: json.dump(cfg_save, f, indent=4, ensure_ascii=False)
        msg = "Lưu config OK." + (" Cần restart!" if restart else ""); log_t = "warn" if restart else "success"; broadcast_log({"log_type": log_t, "message": msg})
        return jsonify({"message": msg, "config": cfg_save, "restart_required": restart})
    except Exception as e: logging.error(f"[CONFIG] Lỗi lưu (POST): {e}", exc_info=True); broadcast_log({"log_type": "error", "message": f"Lỗi lưu config: {e}"}); return jsonify({"error": str(e)}), 500

@app.route('/api/state'); @requires_auth
def api_state(): with state_lock: return jsonify(system_state)
@app.route('/api/sort_log'); @requires_auth
def api_sort_log():
    summary = {}
    with sort_log_lock:
        try:
            data = {};
            if os.path.exists(SORT_LOG_FILE):
                 with open(SORT_LOG_FILE, 'r', encoding='utf-8') as f: content = f.read();
                 if content: data = json.loads(content)
            for date, hourly in data.items():
                 summary[date] = {}
                 for hour, lanes in hourly.items():
                     for name, count in lanes.items(): summary[date][name] = summary[date].get(name, 0) + count
            return jsonify(summary)
        except Exception as e: logging.error(f"[API] Lỗi đọc sort_log: {e}", exc_info=True); return jsonify({"error": str(e)}), 500
@app.route('/api/reset_maintenance', methods=['POST']); @requires_auth
def reset_maintenance():
    user = request.authorization.username
    if error_manager.is_maintenance(): error_manager.reset(); broadcast_log({"log_type": "success", "message": f"Bảo trì reset bởi {user}."}); return jsonify({"message": "OK"})
    else: return jsonify({"message": "Not in maintenance."})

@sock.route('/ws'); @requires_auth
def ws_route(ws):
    """WebSocket route."""
    global AUTO_TEST_ENABLED, test_sequence_running # Khai báo để sửa đổi
    auth = request.authorization; user = auth.username if auth else "Unknown"
    if not auth or not check_auth(auth.username, auth.password): logging.warning(f"[WS] Unauthorized."); ws.close(code=1008); return
    connected_clients.add(ws); logging.info(f"[WS] Client {user} connected. Total: {len(connected_clients)}")
    try: # Gửi state ban đầu
        with state_lock: state = system_state; state["maintenance_mode"] = error_manager.is_maintenance(); state["last_error"] = error_manager.last_error; initial = json.dumps({"type": "state_update", "state": state})
        ws.send(initial)
    except Exception as e: logging.warning(f"[WS] Lỗi gửi state ban đầu: {e}"); connected_clients.discard(ws); return
    try: # Lắng nghe message
        while True:
            msg = ws.receive();
            if not msg: break
            try:
                data = json.loads(msg); action = data.get('action')
                if error_manager.is_maintenance() and action not in ["reset_maintenance", "stop_tests"]: # (MỚI) Cho phép stop_tests khi bảo trì
                     broadcast_log({"log_type": "error", "message": "Hệ thống đang bảo trì."}); continue

                if action == 'reset_count':
                    idx = data.get('lane_index')
                    with state_lock: num = len(system_state['lanes'])
                    if idx == 'all':
                        with state_lock:
                             for i in range(num): system_state['lanes'][i]['count'] = 0
                        broadcast_log({"log_type": "info", "message": f"{user} reset đếm toàn bộ."})
                    elif isinstance(idx, int) and 0 <= idx < num:
                        with state_lock: name = system_state['lanes'][idx]['name']; system_state['lanes'][idx]['count'] = 0
                        broadcast_log({"log_type": "info", "message": f"{user} reset đếm {name}."})
                elif action == "test_relay": idx, act = data.get("lane_index"), data.get("relay_action"); executor.submit(_run_test_relay, idx, act)
                elif action == "test_all_relays":
                    # (MỚI) Kiểm tra trước khi submit
                    with test_sequence_lock:
                        if test_sequence_running: broadcast_log({"log_type":"warn", "message":"Test tuần tự đang chạy."})
                        else: executor.submit(_run_test_all_relays) # Chỉ chạy nếu không có test khác
                elif action == "toggle_auto_test":
                    AUTO_TEST_ENABLED = data.get("enabled", False); logging.info(f"[TEST] Auto-Test by {user}: {AUTO_TEST_ENABLED}")
                    broadcast_log({"log_type": "warn", "message": f"Auto-Test đã { 'BẬT' if AUTO_TEST_ENABLED else 'TẮT' } bởi {user}."});
                    if not AUTO_TEST_ENABLED: reset_all_relays_to_default()
                elif action == "reset_maintenance":
                     if error_manager.is_maintenance(): error_manager.reset(); broadcast_log({"log_type": "success", "message": f"Bảo trì reset bởi {user}."})
                     else: broadcast_log({"log_type": "info", "message": "Không ở chế độ bảo trì."})
                elif action == "mock_trigger_pin" and isinstance(GPIO, MockGPIO): pin, val, dur = data.get("pin"), data.get("value"), data.get("duration", 0.5); threading.Thread(target=mock_trigger_pin_ws, args=(pin, val, dur), daemon=True).start()
                # (MỚI) Xử lý dừng test tuần tự
                elif action == "stop_tests":
                    with test_sequence_lock:
                        if test_sequence_running:
                             test_sequence_running = False # Đặt cờ dừng
                             logging.info(f"[TEST] Nhận lệnh dừng test tuần tự từ {user}.")
                             broadcast_log({"log_type": "warn", "message": f"Lệnh dừng test đã được gửi bởi {user}."})
                        else:
                             broadcast_log({"log_type": "info", "message":"Không có test tuần tự nào đang chạy."})


            except json.JSONDecodeError: logging.warning(f"[WS] Invalid JSON từ {user}")
            except Exception as e: logging.error(f"[WS] Lỗi xử lý message từ {user}: {e}", exc_info=True)
    except Exception as e: logging.warning(f"[WS] Kết nối với {user} lỗi/đóng: {e}")
    finally: connected_clients.discard(ws); logging.info(f"[WS] Client {user} disconnected. Total: {len(connected_clients)}")

# =============================
#               MAIN
# =============================
if __name__ == "__main__":
    try:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] (%(threadName)s) %(message)s',
                            handlers=[logging.FileHandler(LOG_FILE, encoding='utf-8'), logging.StreamHandler()])
        load_local_config()
        with state_lock: mode = system_state.get("gpio_mode", "BCM")
        if isinstance(GPIO, RealGPIO):
             GPIO.setmode(GPIO.BCM if mode == "BCM" else GPIO.BOARD); GPIO.setwarnings(False); logging.info(f"[GPIO] Mode: {mode}")
             logging.info(f"[GPIO] Setup SENSORs: {SENSOR_PINS}");
             for pin in SENSOR_PINS: GPIO.setup(pin, GPIO.IN, pull_up_down=GPIO.PUD_UP)
             logging.info(f"[GPIO] Setup RELAYs: {RELAY_PINS}");
             for pin in RELAY_PINS: GPIO.setup(pin, GPIO.OUT)
        else: logging.info("[GPIO] Mock mode, skipping setup.")
        reset_all_relays_to_default()
        threads = [ threading.Thread(target=f, name=n, daemon=True) for f, n in [ (camera_capture_thread, "Camera"), (qr_detection_loop, "QR"), (sensor_monitoring_thread, "Sensor"), (broadcast_state, "Broadcast"), (auto_test_loop, "AutoTest"), (periodic_config_save, "ConfigSave") ]]
        for t in threads: t.start()
        logging.info("="*55 + "\n  HỆ THỐNG PHÂN LOẠI SẴN SÀNG (v2.0 - Final + StopTest)\n" + f"  GPIO: {'REAL' if isinstance(GPIO, RealGPIO) else 'MOCK'} ({mode})\n" + f"  Log: {LOG_FILE}, SortLog: {SORT_LOG_FILE}\n" + f"  API: http://<IP>:5000/api/state\n" + f"  Web: http://<IP>:5000 (User: {USERNAME} / Pass: {PASSWORD})\n" + "="*55)
        try: from waitress import serve; serve(app, host='0.0.0.0', port=5000, threads=8)
        except ImportError: logging.warning("Waitress not installed, using Flask dev server."); app.run(host='0.0.0.0', port=5000)
    except KeyboardInterrupt: logging.info("\n🛑 Dừng hệ thống...")
    except Exception as e: logging.critical(f"[CRITICAL] Lỗi khởi động: {e}", exc_info=True)
    finally:
        main_loop_running = False; logging.info("Đang tắt ThreadPool..."); executor.shutdown(wait=False)
        logging.info("Đang cleanup GPIO...")
        try: GPIO.cleanup(); logging.info("✅ GPIO cleaned up.")
        except Exception as e: logging.warning(f"Lỗi cleanup GPIO: {e}")
        logging.info("👋 Tạm biệt!")

