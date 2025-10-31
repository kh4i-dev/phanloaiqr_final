# -*- coding: utf-8 -*-
"""
BẢN NÂNG CẤP TỔNG HỢP (FINAL + LOGIC V3)
Phiên bản này kết hợp:
- Nền tảng production-ready của `app_final.py` (Waitress, ErrorManager, GPIO Abstraction).
- Logic phân loại vượt trội của `3.py` (FIFO nghiêm ngặt + Sensor-First).
- Tối ưu hóa tốc độ quét QR (pyzbar + ROI).
- Tối ưu hóa I/O (ghi log đếm mỗi 60s).
- Toàn bộ chú thích và log bằng Tiếng Việt.

"""
from flask_sock import Sock
import cv2
import time
import json
import threading
import logging
import os
import functools
import unicodedata  # Thêm từ 3.py
import re          # Thêm từ 3.py
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, render_template, Response, jsonify, request
from datetime import datetime

# Thử import pyzbar để tối ưu QR
try:
    import pyzbar.pyzbar as pyzbar
    PYZBAR_AVAILABLE = True
except ImportError:
    PYZBAR_AVAILABLE = False
    logging.warning("Thư viện 'pyzbar' không tìm thấy. Sử dụng 'cv2.QRCodeDetector' chậm hơn.")

# Thử import Waitress (cho production)
try:
    from waitress import serve
except ImportError:
    serve = None  # Dùng server dev của Flask nếu không có Waitress

# =============================
#      TRỪU TƯỢNG HÓA GPIO
# =============================
try:
    # Thử import thư viện RPi.GPIO thật
    import RPi.GPIO as RPiGPIO
except (ImportError, RuntimeError):
    RPiGPIO = None  # Đặt là None nếu thất bại (chạy trên PC)

class GPIOProvider:
    """Lớp trừu tượng (base class) để tương tác GPIO."""
    def setup(self, pin, mode, pull_up_down=None): raise NotImplementedError
    def output(self, pin, value): raise NotImplementedError
    def input(self, pin): raise NotImplementedError
    def cleanup(self): raise NotImplementedError
    def setmode(self, mode): raise NotImplementedError
    def setwarnings(self, value): raise NotImplementedError

class RealGPIO(GPIOProvider):
    """Triển khai dùng thư viện RPi.GPIO thật."""
    def __init__(self):
        if RPiGPIO is None: raise ImportError("Không thể tải thư viện RPi.GPIO. Bạn có đang chạy trên Raspberry Pi?")
        self.gpio = RPiGPIO
        # Gán các hằng số từ thư viện
        for attr in ['BOARD', 'BCM', 'OUT', 'IN', 'HIGH', 'LOW', 'PUD_UP']:
            setattr(self, attr, getattr(self.gpio, attr))

    def setmode(self, mode): self.gpio.setmode(mode)
    def setwarnings(self, value): self.gpio.setwarnings(value)

    def setup(self, pin, mode, pull_up_down=None):
        try:
            if pull_up_down: self.gpio.setup(pin, mode, pull_up_down=pull_up_down)
            else: self.gpio.setup(pin, mode)
            logging.debug(f"[GPIO] Setup pin {pin} OK.")
        except Exception as e:
            logging.error(f"[GPIO] Lỗi setup pin {pin}: {e}", exc_info=True)
            raise RuntimeError(f"Lỗi setup pin {pin}") from e

    def output(self, pin, value): self.gpio.output(pin, value)
    def input(self, pin): return self.gpio.input(pin)
    def cleanup(self): self.gpio.cleanup()

# --- Triển khai Mock GPIO ---
mock_pin_override = {}  # Dict toàn cục để ghi đè trạng thái pin input (cho test) {pin: (target_value, expiry_time)}
mock_pin_override_lock = threading.Lock()

class MockGPIO(GPIOProvider):
    """Triển khai GPIO giả lập (Mock) để test trên PC."""
    def __init__(self):
        # Gán hằng số giả lập
        for attr, val in [('BOARD', "mock_BOARD"), ('BCM', "mock_BCM"), ('OUT', "mock_OUT"),
                          ('IN', "mock_IN"), ('HIGH', 1), ('LOW', 0), ('PUD_UP', "mock_PUD_UP")]:
            setattr(self, attr, val)
        self.pin_states = {}  # Lưu trạng thái giả lập của các pin
        logging.warning("="*50 + "\nĐANG CHẠY Ở CHẾ ĐỘ GIẢ LẬP (MOCK GPIO).\n" + "="*50)

    def setmode(self, mode): logging.info(f"[MOCK] Đặt chế độ GPIO: {mode}")
    def setwarnings(self, value): logging.info(f"[MOCK] Đặt cảnh báo: {value}")

    def setup(self, pin, mode, pull_up_down=None):
        logging.info(f"[MOCK] Setup pin {pin} mode={mode} pull_up_down={pull_up_down}")
        if mode == self.OUT: self.pin_states[pin] = self.LOW  # Output mặc định là LOW
        else: self.pin_states[pin] = self.HIGH  # Input mặc định là HIGH (giả lập pull-up)

    def output(self, pin, value):
        value_str = "HIGH" if value == self.HIGH else "LOW"
        logging.info(f"[MOCK] Output pin {pin} = {value_str}({value})")
        self.pin_states[pin] = value

    def input(self, pin):
        current_time = time.time()
        # Kiểm tra xem có lệnh ghi đè tạm thời cho pin này không
        with mock_pin_override_lock:
            if pin in mock_pin_override:
                target_value, expiry_time = mock_pin_override[pin]
                if current_time < expiry_time:
                    return target_value
                else:
                    # Ghi đè đã hết hạn, xóa nó đi
                    del mock_pin_override[pin]
                    logging.info(f"[MOCK] Lệnh ghi đè cho pin {pin} đã hết hạn.")
        # Trả về trạng thái mặc định/cuối cùng (HIGH cho sensor)
        return self.pin_states.get(pin, self.HIGH)

    def cleanup(self): logging.info("[MOCK] Dọn dẹp GPIO")

def get_gpio_provider():
    """Hàm factory để chọn đúng nhà cung cấp GPIO."""
    if RPiGPIO:
        logging.info("Phát hiện thư viện RPi.GPIO. Sử dụng RealGPIO.")
        return RealGPIO()
    else:
        logging.info("Không tìm thấy RPi.GPIO. Sử dụng MockGPIO.")
        return MockGPIO()

# =============================
#    CÁC HÀM TIỆN ÍCH (TỪ 3.PY)
# =============================
def _strip_accents(s: str) -> str:
    """Bỏ dấu tiếng Việt."""
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def canon_id(s: str) -> str:
    """
    Chuẩn hoá ID/QR về dạng so khớp (dùng cho logic):
    - Bỏ dấu, Uppercase
    - Bỏ mọi ký tự không phải A-Z/0-9
    - Bỏ tiền tố LOAI / LO ở ĐẦU chuỗi
    """
    if s is None: return ""
    s = str(s).strip()
    try: s = s.encode("utf-8").decode("unicode_escape")  # Xử lý escape codes (ví dụ: '\u1ea0')
    except Exception: pass
    s = _strip_accents(s).upper()
    s = re.sub(r"[^A-Z0-9]", "", s)  # Chỉ giữ lại A-Z, 0-9
    s = re.sub(r"^(LOAI|LO)+", "", s) # Bỏ prefix LOAI/LO
    return s

# =============================
#      QUẢN LÝ LỖI
# =============================
class ErrorManager:
    """Quản lý trạng thái lỗi/bảo trì của hệ thống."""
    def __init__(self):
        self.lock = threading.Lock()
        self.maintenance = False
        self.error = None

    def trigger_maintenance(self, msg):
        """Kích hoạt chế độ bảo trì do lỗi nghiêm trọng."""
        with self.lock:
            if self.maintenance: return  # Đã ở chế độ bảo trì
            self.maintenance = True
            self.error = msg
            logging.critical("="*50 + f"\n[CHẾ ĐỘ BẢO TRÌ] Lý do: {msg}\n" +
                             "Hệ thống đã dừng. Cần can thiệp thủ công.\n" + "="*50)
            broadcast({"type": "maintenance_update", "enabled": True, "reason": msg})

    def reset(self):
        """Reset lại chế độ bảo trì (thường do người dùng kích hoạt)."""
        with self.lock:
            if not self.maintenance: return  # Không ở chế độ bảo trì
            self.maintenance = False
            self.error = None
            logging.info("[RESET BẢO TRÌ]")
            broadcast({"type": "maintenance_update", "enabled": False})

    def is_maintenance(self):
        """Kiểm tra hệ thống có đang bảo trì không."""
        return self.maintenance

# =============================
#      CẤU HÌNH & KHỞI TẠO TOÀN CỤC
# =============================
# --- Hằng số ---
CAMERA_INDEX = 0
CONFIG_FILE = 'config.json'
LOG_FILE = 'system.log'
SORT_LOG_FILE = 'sort_log.json'
ACTIVE_LOW = True  # Relay kích hoạt bằng mức LOW
USERNAME = os.environ.get("APP_USERNAME", "admin")
PASSWORD = os.environ.get("APP_PASSWORD", "123")

# --- Đối tượng toàn cục ---
GPIO = get_gpio_provider()
error_manager = ErrorManager()
executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix="Worker")  # Pool luồng
sort_log_lock = threading.Lock()  # Lock cho file sort_log.json
test_seq_running = False  # Cờ cho trạng thái test tuần tự
test_seq_lock = threading.Lock()  # Lock cho cờ test tuần tự

# --- Cấu hình Mặc định (Sẽ bị ghi đè bởi config.json) ---
DEFAULT_LANES_CFG = [
    {"id": "A", "name": "Phân loại A (Đẩy)", "sensor_pin": 3, "push_pin": 17, "pull_pin": 18},
    {"id": "B", "name": "Phân loại B (Đẩy)", "sensor_pin": 23, "push_pin": 27, "pull_pin": 14},
    {"id": "C", "name": "Phân loại C (Đẩy)", "sensor_pin": 24, "push_pin": 22, "pull_pin": 4},
    {"id": "D", "name": "Lane D (Đi thẳng/Thoát)", "sensor_pin": 25, "push_pin": None, "pull_pin": None},
]
DEFAULT_TIMING_CFG = {
    "cycle_delay": 0.3, "settle_delay": 0.2, "sensor_debounce": 0.1,
    "push_delay": 0.0, "gpio_mode": "BCM"
}
DEFAULT_QR_CFG = {
    "use_roi": False, "roi_x": 0, "roi_y": 0, "roi_w": 0, "roi_h": 0
}

# --- Biến toàn cục (Trạng thái & Điều khiển) ---
lanes_config = DEFAULT_LANES_CFG  # Cấu hình lane hiện tại (từ file)
qr_config = DEFAULT_QR_CFG        # Cấu hình QR/ROI hiện tại (từ file)
RELAY_PINS = []  # Danh sách các chân relay đang dùng
SENSOR_PINS = []  # Danh sách các chân sensor đang dùng

# Từ điển (dict) trạng thái trung tâm
system_state = {
    "lanes": [],            # Được điền từ lanes_config khi load
    "timing_config": {},    # Được điền từ file hoặc mặc định
    "is_mock": isinstance(GPIO, MockGPIO),
    "maintenance_mode": False,
    "gpio_mode": "BCM",     # Chế độ GPIO *đang chạy*
    "last_error": None,
    "queue_indices": []     # (Từ 3.py) Hàng chờ (dùng index)
}
state_lock = threading.Lock()  # Lock khi truy cập/sửa system_state
main_running = True  # Cờ để báo các luồng dừng lại
latest_frame = None  # Giữ khung hình mới nhất từ camera
frame_lock = threading.Lock()  # Lock khi truy cập latest_frame

# (Từ 3.py) Biến logic hàng chờ & Sensor-First
qr_queue = []  # Hàng chờ (lưu index của lane)
queue_lock = threading.Lock()
QUEUE_HEAD_TIMEOUT = 15.0  # (Tăng lên 15s)
queue_head_since = 0.0
pending_sensor_triggers = []  # Mảng lưu thời điểm sensor chờ QR
PENDING_TRIGGER_TIMEOUT = 0.5  # Thời gian sensor chờ QR (0.5s)

# Biến trạng thái chống nhiễu sensor
last_s_state, last_s_trig = [], []
# Biến trạng thái auto-test
AUTO_TEST = False
auto_s_state, auto_s_trig = [], []

# =============================
#       HÀM ĐIỀU KHIỂN RELAY
# =============================
def RELAY_ON(pin):
    """Bật relay (kích hoạt). Xử lý lỗi nếu có."""
    if pin is None: return logging.warning("[GPIO] Lệnh RELAY_ON gọi tới pin None")
    try:
        GPIO.output(pin, GPIO.LOW if ACTIVE_LOW else GPIO.HIGH)
    except Exception as e:
        logging.error(f"[GPIO] Lỗi kích hoạt relay pin {pin}: {e}", exc_info=True)
        error_manager.trigger_maintenance(f"Lỗi kích hoạt relay {pin}: {e}")

def RELAY_OFF(pin):
    """Tắt relay (ngừng kích hoạt). Xử lý lỗi nếu có."""
    if pin is None: return logging.warning("[GPIO] Lệnh RELAY_OFF gọi tới pin None")
    try:
        GPIO.output(pin, GPIO.HIGH if ACTIVE_LOW else GPIO.LOW)
    except Exception as e:
        logging.error(f"[GPIO] Lỗi tắt relay pin {pin}: {e}", exc_info=True)
        error_manager.trigger_maintenance(f"Lỗi tắt relay {pin}: {e}")

# =============================
#      LOAD/SAVE CẤU HÌNH & KHỞI TẠO
# =============================

def ensure_lane_ids(lanes_list):
    """(Từ 3.py) Đảm bảo mỗi lane có một ID cố định."""
    default_ids = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']
    for i, lane in enumerate(lanes_list):
        if 'id' not in lane or not lane['id']:
            lane['id'] = default_ids[i] if i < len(default_ids) else f"LANE_{i+1}"
            logging.warning(f"[CONFIG] Lane {i+1} thiếu ID, gán ID mặc định: {lane['id']}")
    return lanes_list

def load_config():
    """Tải cấu hình timing, lanes, và QR từ JSON."""
    global lanes_config, qr_config, RELAY_PINS, SENSOR_PINS
    global last_s_state, last_s_trig, auto_s_state, auto_s_trig
    global pending_sensor_triggers  # (Từ 3.py)

    loaded_cfg = {
        "timing_config": DEFAULT_TIMING_CFG.copy(),
        "lanes_config": [l.copy() for l in DEFAULT_LANES_CFG],
        "qr_config": DEFAULT_QR_CFG.copy()
    }

    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f: content = f.read()
            if content:
                file_cfg = json.loads(content)
                # Merge: Cấu hình trong file sẽ ghi đè lên mặc định
                loaded_cfg["timing_config"].update(file_cfg.get('timing_config', {}))
                loaded_cfg["qr_config"].update(file_cfg.get('qr_config', {}))
                
                lanes_from_file = file_cfg.get('lanes_config')
                if isinstance(lanes_from_file, list):
                    loaded_cfg["lanes_config"] = lanes_from_file
                
                # (Từ 3.py) Đảm bảo các lane có ID
                loaded_cfg["lanes_config"] = ensure_lane_ids(loaded_cfg["lanes_config"])

            else: logging.warning(f"[CONFIG] File {CONFIG_FILE} rỗng, dùng mặc định.")
        except Exception as e:
            logging.error(f"[CONFIG] Lỗi đọc {CONFIG_FILE}: {e}. Dùng mặc định.", exc_info=True)
            error_manager.trigger_maintenance(f"Lỗi file {CONFIG_FILE}: {e}")
            # Reset về mặc định nếu file lỗi
            loaded_cfg = {
                "timing_config": DEFAULT_TIMING_CFG.copy(),
                "lanes_config": ensure_lane_ids([l.copy() for l in DEFAULT_LANES_CFG]),
                "qr_config": DEFAULT_QR_CFG.copy()
            }
    else:
        logging.warning(f"[CONFIG] Không tìm thấy {CONFIG_FILE}, tạo file mới với cấu hình mặc định.")
        try:
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(loaded_cfg, f, indent=4, ensure_ascii=False)
        except Exception as e: logging.error(f"[CONFIG] Lỗi tạo file {CONFIG_FILE}: {e}")

    # Cập nhật biến toàn cục và system_state
    lanes_config = loaded_cfg['lanes_config']
    qr_config = loaded_cfg['qr_config']
    num_lanes = len(lanes_config)
    
    new_lanes_state = []
    RELAY_PINS.clear(); SENSOR_PINS.clear()

    for i, cfg in enumerate(lanes_config):
        s_pin = int(cfg["sensor_pin"]) if cfg.get("sensor_pin") is not None else None
        p_pin = int(cfg["push_pin"]) if cfg.get("push_pin") is not None else None
        pl_pin = int(cfg["pull_pin"]) if cfg.get("pull_pin") is not None else None

        new_lanes_state.append({
            "name": cfg.get("name", f"Lane {i+1}"),
            "id": cfg.get("id", f"ID_{i+1}"), # Lấy ID từ config (đã được ensure_lane_ids)
            "status": "Sẵn sàng", "count": 0,
            "sensor_pin": s_pin, "push_pin": p_pin, "pull_pin": pl_pin,
            "sensor_reading": 1, "relay_grab": 0, "relay_push": 0
        })
        if s_pin is not None: SENSOR_PINS.append(s_pin)
        if p_pin is not None: RELAY_PINS.append(p_pin)
        if pl_pin is not None: RELAY_PINS.append(pl_pin)

    # Khởi tạo các mảng trạng thái dựa trên số lane
    last_s_state = [1] * num_lanes; last_s_trig = [0.0] * num_lanes
    auto_s_state = [1] * num_lanes; auto_s_trig = [0.0] * num_lanes
    pending_sensor_triggers = [0.0] * num_lanes  # (Từ 3.py)

    # Cập nhật trạng thái trung tâm
    with state_lock:
        system_state['timing_config'] = loaded_cfg['timing_config']
        system_state['gpio_mode'] = loaded_cfg['timing_config'].get("gpio_mode", "BCM")
        system_state['lanes'] = new_lanes_state
        system_state['maintenance_mode'] = error_manager.is_maintenance()
        system_state['last_error'] = error_manager.error

    logging.info(f"[CONFIG] Đã tải cấu hình cho {num_lanes} lanes.")
    logging.info(f"[CONFIG] Cấu hình Timing: {system_state['timing_config']}")
    logging.info(f"[CONFIG] Cấu hình QR: {qr_config}")

def reset_relays():
    """Reset tất cả relay về trạng thái an toàn (Thu BẬT, Đẩy TẮT)."""
    logging.info("[GPIO] Reset tất cả relay về trạng thái mặc định (Thu BẬT, Đẩy TẮT)...")
    try:
        with state_lock:
            for lane in system_state["lanes"]:
                pull_pin, push_pin = lane.get("pull_pin"), lane.get("push_pin")
                if pull_pin is not None: RELAY_ON(pull_pin)
                if push_pin is not None: RELAY_OFF(push_pin)
                # Cập nhật state
                lane.update({"relay_grab": (1 if pull_pin is not None else 0),
                             "relay_push": 0,
                             "status": "Sẵn sàng" if lane["status"] != "Lỗi Config" else "Lỗi Config"})
        time.sleep(0.1)  # Chờ relay ổn định
        logging.info("[GPIO] Reset relay hoàn tất.")
    except Exception as e:
        logging.error(f"[GPIO] Lỗi khi reset relay: {e}", exc_info=True)
        error_manager.trigger_maintenance(f"Lỗi reset relay: {e}")

def save_state_periodically():
    """Lưu config và log đếm định kỳ (TỐI ƯU HÓA I/O)."""
    while main_running:
        time.sleep(60)  # Lưu mỗi 60 giây
        if error_manager.is_maintenance(): continue

        config_snapshot = {}
        counts_snapshot = {}
        today = datetime.now().strftime('%Y-%m-%d')

        try:
            # --- 1. Lấy snapshot trạng thái (trong lock) ---
            with state_lock:
                config_snapshot['timing_config'] = system_state['timing_config'].copy()
                config_snapshot['qr_config'] = qr_config.copy() # Lưu cả config QR
                
                current_lanes_config = []
                for lane_state in system_state['lanes']:
                    current_lanes_config.append({
                        "id": lane_state['id'], "name": lane_state['name'],
                        "sensor_pin": lane_state.get('sensor_pin'),
                        "push_pin": lane_state.get('push_pin'),
                        "pull_pin": lane_state.get('pull_pin')
                    })
                    # Lấy số đếm hiện tại trong RAM
                    counts_snapshot[lane_state['name']] = lane_state['count']
                
                config_snapshot['lanes_config'] = current_lanes_config

            # --- 2. Lưu file Config (ngoài lock) ---
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(config_snapshot, f, indent=4, ensure_ascii=False)
            logging.debug("[CONFIG] Đã tự động lưu config.")

            # --- 3. Lưu file Sort Log (ngoài lock, dùng lock riêng) ---
            with sort_log_lock:
                sort_log_data = {}
                if os.path.exists(SORT_LOG_FILE):
                    try:
                        with open(SORT_LOG_FILE, 'r', encoding='utf-8') as f:
                            sort_log_data = json.load(f)
                    except Exception:
                        sort_log_data = {}  # Tạo mới nếu file hỏng
                
                # Ghi đè số đếm của ngày hôm nay bằng snapshot RAM
                # (Logic này giả định app chạy 24/7. Nếu app khởi động lại,
                # nó sẽ bắt đầu đếm từ 0. Để cộng dồn, cần logic phức tạp hơn)
                sort_log_data[today] = counts_snapshot
                
                with open(SORT_LOG_FILE, 'w', encoding='utf-8') as f:
                    json.dump(sort_log_data, f, indent=4, ensure_ascii=False)
            logging.debug(f"[SORT_LOG] Đã tự động lưu số đếm vào {SORT_LOG_FILE}.")

        except Exception as e:
            logging.error(f"[SAVE] Lỗi khi tự động lưu state: {e}")

# =============================
#         LUỒNG CAMERA
# =============================
def run_camera():
    """Luồng chạy camera, xử lý lỗi và kết nối lại."""
    global latest_frame
    camera = None
    try:
        logging.info("[CAMERA] Khởi tạo camera...")
        camera = cv2.VideoCapture(CAMERA_INDEX)
        props = {cv2.CAP_PROP_FRAME_WIDTH: 640, cv2.CAP_PROP_FRAME_HEIGHT: 480, cv2.CAP_PROP_BUFFERSIZE: 1}
        for prop, value in props.items(): camera.set(prop, value)

        if not camera.isOpened():
            error_manager.trigger_maintenance("Không thể mở camera.")
            return

        logging.info("[CAMERA] Camera sẵn sàng.")
        retries, max_retries = 0, 5

        while main_running:
            if error_manager.is_maintenance(): time.sleep(0.5); continue

            ret, frame = camera.read()
            if not ret:
                retries += 1
                logging.warning(f"[CAMERA] Mất khung hình (Lần {retries}/{max_retries}). Đang thử lại...")
                broadcast_log({"log_type":"error", "message":f"Mất camera (Lần {retries})..."})

                if retries > max_retries:
                    error_manager.trigger_maintenance("Camera lỗi vĩnh viễn (mất kết nối).")
                    break

                if camera: camera.release()
                time.sleep(1)
                camera = cv2.VideoCapture(CAMERA_INDEX)
                if camera.isOpened():
                    for prop, value in props.items(): camera.set(prop, value)
                    logging.info("[CAMERA] Đã kết nối lại camera.")
                else:
                    logging.error("[CAMERA] Không thể mở lại camera.")
                    time.sleep(2)
                continue

            retries = 0  # Reset khi thành công
            with frame_lock:
                latest_frame = frame.copy()
            time.sleep(1 / 60)  # Tăng tốc độ chụp (60 FPS) để QR đọc nhanh hơn

    except Exception as e:
        logging.error(f"[CAMERA] Luồng camera bị crash: {e}", exc_info=True)
        error_manager.trigger_maintenance(f"Lỗi camera nghiêm trọng: {e}")
    finally:
        if camera: camera.release()
        logging.info("[CAMERA] Đã giải phóng camera.")

# =============================
#       LOGIC CHU TRÌNH PHÂN LOẠI (TỪ 3.PY)
# =============================
def sorting_process(lane_index):
    """(Từ 3.py) Quy trình đẩy-thu piston."""
    lane_name = ""
    push_pin, pull_pin = None, None
    is_sorting_lane = False
    operation_successful = False

    try:
        with state_lock:
            if not (0 <= lane_index < len(system_state["lanes"])):
                logging.error(f"[SORT] Lane index {lane_index} không hợp lệ.")
                return

            cfg = system_state['timing_config']
            delay = cfg['cycle_delay']
            settle_delay = cfg['settle_delay']
            lane = system_state["lanes"][lane_index]
            lane_name = lane["name"]
            push_pin = lane.get("push_pin")
            pull_pin = lane.get("pull_pin")
            
            # Kiểm tra xem đây là lane đẩy hay lane đi thẳng
            is_sorting_lane = not (push_pin is None or pull_pin is None)

            lane["status"] = "Đang phân loại..." if is_sorting_lane else "Đang đi thẳng..."

        if not is_sorting_lane:
            broadcast_log({"log_type": "info", "message": f"Vật phẩm đi thẳng qua {lane_name}"})
        else:
            broadcast_log({"log_type": "info", "message": f"Bắt đầu chu trình đẩy {lane_name}"})
            # 1. Nhả Grab (Pull OFF)
            RELAY_OFF(pull_pin)
            with state_lock: system_state["lanes"][lane_index]["relay_grab"] = 0
            time.sleep(settle_delay)
            if not main_running: return

            # 2. Kích hoạt Push (Push ON)
            RELAY_ON(push_pin)
            with state_lock: system_state["lanes"][lane_index]["relay_push"] = 1
            time.sleep(delay)
            if not main_running: return

            # 3. Tắt Push (Push OFF)
            RELAY_OFF(push_pin)
            with state_lock: system_state["lanes"][lane_index]["relay_push"] = 0
            time.sleep(settle_delay)
            if not main_running: return

            # 4. Kích hoạt Grab (Pull ON)
            RELAY_ON(pull_pin)
            with state_lock: system_state["lanes"][lane_index]["relay_grab"] = 1
        
        operation_successful = True # Đánh dấu thành công

    except Exception as e:
        logging.error(f"[SORT] Lỗi trong sorting_process (lane {lane_name}): {e}")
        error_manager.trigger_maintenance(f"Lỗi sorting_process (Lane {lane_name}): {e}")
    finally:
        with state_lock:
            if 0 <= lane_index < len(system_state["lanes"]):
                lane = system_state["lanes"][lane_index]
                if lane_name and lane["status"] != "Lỗi Config":
                    # (TỐI ƯU I/O) Chỉ tăng count trong RAM
                    if operation_successful:
                        lane["count"] += 1
                        log_type = "sort" if is_sorting_lane else "pass"
                        broadcast_log({"log_type": log_type, "name": lane_name, "count": lane['count']})
                        # (XÓA) Không gọi save_sort_log() ở đây
                    
                    if lane["status"] != "Lỗi Config":
                        lane["status"] = "Sẵn sàng"

        if lane_name and operation_successful:
            msg = f"Hoàn tất chu trình cho {lane_name}" if is_sorting_lane else f"Hoàn tất đếm vật phẩm {lane_name}"
            broadcast_log({"log_type": "info", "message": msg})


def handle_sorting_with_delay(lane_index):
    """(Từ 3.py) Luồng trung gian, chờ push_delay rồi mới gọi sorting_process."""
    push_delay = 0.0
    lane_name_for_log = f"Lane {lane_index + 1}"
    initial_status = "UNKNOWN"

    try:
        with state_lock:
            if not (0 <= lane_index < len(system_state["lanes"])):
                logging.error(f"[DELAY] Lane index {lane_index} không hợp lệ.")
                return
            push_delay = system_state['timing_config'].get('push_delay', 0.0)
            lane = system_state['lanes'][lane_index]
            lane_name_for_log = lane['name']
            initial_status = lane["status"] # Lưu trạng thái lúc bắt đầu

        if push_delay > 0:
            broadcast_log({"log_type": "info", "message": f"Đã thấy vật {lane_name_for_log}, chờ {push_delay}s..."})
            time.sleep(push_delay)

        if not main_running:
            broadcast_log({"log_type": "warn", "message": f"Hủy chu trình {lane_name_for_log} do hệ thống đang tắt."})
            return

        should_run_sort = False
        with state_lock:
            if not (0 <= lane_index < len(system_state["lanes"])): return
            current_status = system_state["lanes"][lane_index]["status"]
            
            # (Logic Sensor-First) Trạng thái có thể là "Đang chờ đẩy" (QR-first) 
            # hoặc "Sẵn sàng" (Sensor-first)
            if current_status in ["Đang chờ đẩy", "Sẵn sàng"]:
                should_run_sort = True
            elif initial_status == "Đang chờ đẩy":
                # Trạng thái đã bị thay đổi trong lúc chờ delay
                broadcast_log({"log_type": "warn", "message": f"Hủy chu trình {lane_name_for_log} do trạng thái thay đổi ({current_status})."})
                system_state["lanes"][lane_index]["status"] = "Sẵn sàng" # Reset

        if should_run_sort:
            sorting_process(lane_index)

    except Exception as e:
        logging.error(f"[ERROR] Lỗi trong luồng handle_sorting_with_delay (lane {lane_name_for_log}): {e}")
        error_manager.trigger_maintenance(f"Lỗi luồng sorting_delay (Lane {lane_name_for_log}): {e}")
        with state_lock:
            if 0 <= lane_index < len(system_state["lanes"]):
                if system_state["lanes"][lane_index]["status"] == "Đang chờ đẩy":
                    system_state["lanes"][lane_index]["status"] = "Sẵn sàng"
                    broadcast_log({"log_type": "error", "message": f"Lỗi delay, reset {lane_name_for_log}"})


# =============================
#       QUÉT MÃ QR (TỐI ƯU + LOGIC 3.PY)
# =============================
def qr_detection_loop():
    """Luồng quét QR (dùng PYZBAR + ROI) với logic Sensor-First."""
    global pending_sensor_triggers, queue_head_since
    detector = None
    if not PYZBAR_AVAILABLE:
        detector = cv2.QRCodeDetector() # Dùng cv2 nếu không có pyzbar
        
    last_qr, last_time = "", 0.0
    logging.info(f"[QR] Luồng QR bắt đầu (Sử dụng: {'Pyzbar' if PYZBAR_AVAILABLE else 'cv2.QRCodeDetector'}).")

    while main_running:
        try:
            if AUTO_TEST or error_manager.is_maintenance():
                time.sleep(0.2); continue
            
            # 🔄 Tạo lại LANE_MAP động theo config hiện tại
            with state_lock:
                # Map ID cố định đã chuẩn hóa (A, B, C...) sang Index (0, 1, 2...)
                LANE_MAP = {canon_id(lane.get("id")): idx 
                            for idx, lane in enumerate(system_state["lanes"]) if lane.get("id")}
                # Lấy cấu hình ROI (để có thể thay đổi động)
                cfg = qr_config 
                use_roi = cfg.get("use_roi", False)
                x, y = cfg.get("roi_x", 0), cfg.get("roi_y", 0)
                w, h = cfg.get("roi_w", 0), cfg.get("roi_h", 0)

            frame_copy = None
            with frame_lock:
                if latest_frame is not None: frame_copy = latest_frame.copy()

            if frame_copy is None:
                time.sleep(0.01); continue

            gray_frame = cv2.cvtColor(frame_copy, cv2.COLOR_BGR2GRAY)
            if gray_frame.mean() < 10:
                time.sleep(0.1); continue

            # (TỐI ƯU HÓA) Cắt ROI
            scan_image = gray_frame
            if use_roi and w > 0 and h > 0:
                y_end = min(y + h, gray_frame.shape[0])
                x_end = min(x + w, gray_frame.shape[1])
                scan_image = gray_frame[y:y_end, x:x_end]

            data = None
            if PYZBAR_AVAILABLE:
                barcodes = pyzbar.decode(scan_image)
                if barcodes:
                    data = barcodes[0].data.decode("utf-8")
            else:
                # Dùng cv2 nếu pyzbar không có
                data_cv2, _, _ = detector.detectAndDecode(gray_frame)
                if data_cv2: data = data_cv2

            # --- Xử lý QR tìm thấy (Logic từ 3.py) ---
            if data and (data != last_qr or time.time() - last_time > 3.0):
                last_qr, last_time = data, time.time()
                data_key = canon_id(data)
                data_raw = data.strip()
                now = time.time()

                if data_key in LANE_MAP:
                    idx = LANE_MAP[data_key]
                    current_queue_for_log = []
                    
                    is_pending_match = False
                    with queue_lock:
                        if 0 <= idx < len(pending_sensor_triggers):
                            if (pending_sensor_triggers[idx] > 0.0) and (now - pending_sensor_triggers[idx] < PENDING_TRIGGER_TIMEOUT):
                                is_pending_match = True
                                pending_sensor_triggers[idx] = 0.0 # Xóa cờ chờ
                        current_queue_for_log = list(qr_queue)

                    if is_pending_match:
                        # TRƯỜNG HỢP 1: Sensor đã kích hoạt TRƯỚC.
                        lane_name_for_log = system_state["lanes"][idx]['name']
                        broadcast_log({
                            "log_type": "info",
                            "message": f"QR '{data_raw}' khớp với sensor {lane_name_for_log} đang chờ.",
                            "queue": current_queue_for_log
                        })
                        logging.info(f"[QR] '{data_raw}' (key: '{data_key}') -> lane {idx} (Khớp pending sensor)")
                        executor.submit(handle_sorting_with_delay, idx) # Chạy xử lý
                    
                    else:
                        # TRƯỜNG HỢP 2: Bình thường. QR tới trước.
                        with queue_lock:
                            is_queue_empty_before = not qr_queue
                            qr_queue.append(idx)
                            current_queue_for_log = list(qr_queue)
                            if is_queue_empty_before: queue_head_since = time.time()

                        with state_lock:
                            if 0 <= idx < len(system_state["lanes"]):
                                if system_state["lanes"][idx]["status"] == "Sẵn sàng":
                                    system_state["lanes"][idx]["status"] = "Đang chờ vật..."
                        
                        system_state["queue_indices"] = current_queue_for_log
                        broadcast_log({"log_type": "qr", "data": data_raw, "data_key": data_key, "queue": current_queue_for_log})
                        logging.info(f"[QR] '{data_raw}' (key: '{data_key}') -> lane {idx} (Thêm vào hàng chờ)")

                    # Đẩy state update ngay lập tức
                    with state_lock:
                        state_msg = json.dumps({"type": "state_update", "state": system_state})
                    for client in _list_clients():
                        try: client.send(state_msg)
                        except Exception: _remove_client(client)
                            
                elif data_key == "NG":
                    broadcast_log({"log_type": "qr_ng", "data": data_raw})
                else:
                    broadcast_log({"log_type": "unknown_qr", "data": data_raw, "data_key": data_key}) 
                    logging.warning(f"[QR] Không rõ mã QR: raw='{data_raw}', key='{data_key}', map={list(LANE_MAP.keys())}")
            
            time.sleep(0.01) # Quét nhanh

        except Exception as e:
            logging.error(f"[QR] Lỗi trong luồng QR: {e}", exc_info=True)
            time.sleep(0.5)

# =============================
#      GIÁM SÁT SENSOR (LOGIC 3.PY)
# =============================
def sensor_monitoring_thread():
    """(Từ 3.py) Luồng giám sát sensor với logic FIFO nghiêm ngặt + Sensor-First."""
    global last_s_state, last_s_trig
    global queue_head_since, pending_sensor_triggers

    try:
        while main_running:
            if AUTO_TEST or error_manager.is_maintenance():
                time.sleep(0.1); continue

            with state_lock:
                debounce_time = system_state['timing_config']['sensor_debounce']
                num_lanes = len(system_state['lanes'])
            now = time.time()

            # --- LOGIC CHỐNG KẸT HÀNG CHỜ ---
            with queue_lock:
                if qr_queue and queue_head_since > 0.0:
                    if (now - queue_head_since) > QUEUE_HEAD_TIMEOUT:
                        expected_lane_index = qr_queue[0]
                        expected_lane_name = system_state['lanes'][expected_lane_index]['name']
                        if system_state["lanes"][expected_lane_index]["status"] == "Đang chờ vật...":
                            system_state["lanes"][expected_lane_index]["status"] = "Sẵn sàng"

                        qr_queue.pop(0)
                        current_queue_for_log = list(qr_queue)
                        queue_head_since = now if qr_queue else 0.0

                        broadcast_log({
                            "log_type": "warn",
                            "message": f"TIMEOUT! Tự động xóa {expected_lane_name} khỏi hàng chờ (>{QUEUE_HEAD_TIMEOUT}s).",
                            "queue": current_queue_for_log
                        })
                        with state_lock: system_state["queue_indices"] = current_queue_for_log

            # --- ĐỌC SENSOR TỪNG LANE ---
            for i in range(num_lanes):
                with state_lock:
                    if not (0 <= i < len(system_state["lanes"])): continue
                    lane_for_read = system_state["lanes"][i]
                    sensor_pin = lane_for_read.get("sensor_pin")
                    push_pin = lane_for_read.get("push_pin")
                    lane_name_for_log = lane_for_read['name']

                if sensor_pin is None: continue

                try:
                    sensor_now = GPIO.input(sensor_pin)
                except Exception as gpio_e:
                    logging.error(f"[SENSOR] Lỗi đọc GPIO pin {sensor_pin} ({lane_name_for_log}): {gpio_e}")
                    error_manager.trigger_maintenance(f"Lỗi đọc sensor {lane_name_for_log}: {gpio_e}")
                    continue

                with state_lock:
                    if 0 <= i < len(system_state["lanes"]):
                        system_state["lanes"][i]["sensor_reading"] = sensor_now

                # --- PHÁT HIỆN SƯỜN XUỐNG (1 -> 0) ---
                if sensor_now == 0 and last_s_state[i] == 1:
                    if (now - last_s_trig[i]) > debounce_time:
                        last_s_trig[i] = now

                        with queue_lock:
                            if not qr_queue:
                                # --- 1. HÀNG CHỜ RỖNG ---
                                if push_pin is None: # Lane đi thẳng
                                    broadcast_log({"log_type": "info", "message": f"Vật đi thẳng (không QR) qua {lane_name_for_log}."})
                                    executor.submit(sorting_process, i)
                                else: # Lane đẩy
                                    if 0 <= i < len(pending_sensor_triggers):
                                        pending_sensor_triggers[i] = now 
                                    broadcast_log({"log_type": "warn", "message": f"Sensor {lane_name_for_log} kích hoạt (hàng chờ rỗng). Đang chờ QR ({PENDING_TRIGGER_TIMEOUT}s)..."})

                            elif i == qr_queue[0]:
                                # --- 2. KHỚP ĐẦU HÀNG CHỜ (FIFO) ---
                                qr_queue.pop(0)
                                current_queue_for_log = list(qr_queue)
                                queue_head_since = now if qr_queue else 0.0
                                
                                with state_lock:
                                    if 0 <= i < len(system_state["lanes"]):
                                        system_state["lanes"][i]["status"] = "Đang chờ đẩy" if push_pin is not None else "Đang đi thẳng..."
                                        system_state["queue_indices"] = current_queue_for_log

                                executor.submit(handle_sorting_with_delay, i)
                                broadcast_log({"log_type": "info", "message": f"Sensor {lane_name_for_log} khớp đầu hàng chờ (FIFO).", "queue": current_queue_for_log})
                                if 0 <= i < len(pending_sensor_triggers):
                                    pending_sensor_triggers[i] = 0.0 # Xóa cờ chờ (nếu có)

                            else:
                                # --- 3. KHÔNG KHỚP ĐẦU HÀNG CHỜ (Pass-over) ---
                                # Vật đi ngang qua sensor khác (không phải sensor đầu hàng chờ)
                                # Bỏ qua, không báo lỗi.
                                logging.debug(f"[SENSOR] Bỏ qua trigger {lane_name_for_log} (đang chờ lane {qr_queue[0]}).")
                        
                last_s_state[i] = sensor_now

            adaptive_sleep = 0.05 if all(s == 1 for s in last_s_state) else 0.01
            time.sleep(adaptive_sleep)

    except Exception as e:
        logging.error(f"[ERROR] Luồng sensor_monitoring_thread bị crash: {e}", exc_info=True)
        error_manager.trigger_maintenance(f"Lỗi luồng Sensor: {e}")


# =============================
#        FLASK & WEBSOCKET
# =============================
app = Flask(__name__)
sock = Sock(app)
clients = set()
clients_lock = threading.Lock()

def _add_client(ws):
    with clients_lock: clients.add(ws)

def _remove_client(ws):
    with clients_lock: clients.discard(ws)

def _list_clients():
    with clients_lock: return list(clients)

def broadcast(data):
    """Gửi dữ liệu JSON tới tất cả client."""
    msg = json.dumps(data)
    disconnected = set()
    for client in _list_clients():
        try:
            client.send(msg)
        except Exception:
            disconnected.add(client)
    if disconnected:
        with clients_lock: clients.difference_update(disconnected)

def broadcast_log(log_data):
    """Định dạng và gửi 1 tin nhắn log."""
    log_data['timestamp'] = datetime.now().strftime('%H:%M:%S')
    broadcast({"type": "log", **log_data})

# =============================
#      CÁC HÀM TEST (TỪ APP_FINAL)
# =============================
def run_test_relay_worker(lane_index, relay_action):
    """(Giữ từ app_final) Worker để test 1 relay."""
    pin, state_key, lane_name = None, None, f"Lane {lane_index + 1}"
    try:
        with state_lock:
            if not (0 <= lane_index < len(system_state["lanes"])):
                return broadcast_log({"log_type": "error", "message": f"Test thất bại: Lane index {lane_index} không hợp lệ."})
            lane_state = system_state["lanes"][lane_index]
            lane_name = lane_state['name']
            pin = lane_state.get("pull_pin") if relay_action == "grab" else lane_state.get("push_pin")
            state_key = "relay_grab" if relay_action == "grab" else "relay_push"
            if pin is None:
                return broadcast_log({"log_type": "error", "message": f"Test thất bại: Lane '{lane_name}' thiếu pin '{relay_action}'."})

        RELAY_ON(pin)
        with state_lock:
             if 0 <= lane_index < len(system_state["lanes"]): system_state["lanes"][lane_index][state_key] = 1
        time.sleep(0.5)
        if not main_running: return

        RELAY_OFF(pin)
        with state_lock:
             if 0 <= lane_index < len(system_state["lanes"]): system_state["lanes"][lane_index][state_key] = 0

        broadcast_log({"log_type": "info", "message": f"Test '{relay_action}' trên '{lane_name}' thành công."})
    except Exception as e:
        logging.error(f"[TEST] Lỗi test relay '{relay_action}' cho '{lane_name}': {e}", exc_info=True)
        broadcast_log({"log_type": "error", "message": f"Lỗi test '{relay_action}' trên '{lane_name}': {e}"})

def run_test_all_relays_worker():
    """(Giữ từ app_final) Worker test tuần tự các relay."""
    global test_seq_running
    with test_seq_lock:
        if test_seq_running:
            return broadcast_log({"log_type": "warn", "message": "Test tuần tự đang chạy."})
        test_seq_running = True

    logging.info("[TEST] Bắt đầu test tuần tự relay...")
    broadcast_log({"log_type": "info", "message": "Bắt đầu test tuần tự relay..."})
    stopped_early = False

    try:
        num_lanes = 0
        with state_lock: num_lanes = len(system_state['lanes'])

        for i in range(num_lanes):
            with test_seq_lock: stop_requested = not main_running or not test_seq_running
            if stop_requested: stopped_early = True; break

            lane_name = f"Lane {i+1}"
            with state_lock:
                 if 0 <= i < len(system_state['lanes']): lane_name = system_state['lanes'][i]['name']

            broadcast_log({"log_type": "info", "message": f"Testing Thu (Pull) cho '{lane_name}'..."})
            run_test_relay_worker(i, "grab")
            time.sleep(0.5)

            with test_seq_lock: stop_requested = not main_running or not test_seq_running
            if stop_requested: stopped_early = True; break

            broadcast_log({"log_type": "info", "message": f"Testing Đẩy (Push) cho '{lane_name}'..."})
            run_test_relay_worker(i, "push")
            time.sleep(0.5)

        if stopped_early:
            broadcast_log({"log_type": "warn", "message": "Test tuần tự đã dừng."})
        else:
            broadcast_log({"log_type": "info", "message": "Test tuần tự hoàn tất."})
    finally:
        with test_seq_lock: test_seq_running = False
        broadcast({"type": "test_sequence_complete"})


def run_auto_test_cycle_worker(lane_index):
    """(Giữ từ app_final) Worker cho 1 chu trình Auto-Test."""
    lane_name = f"Lane {lane_index + 1}"
    try:
        with state_lock:
            if 0 <= lane_index < len(system_state['lanes']): lane_name = system_state['lanes'][lane_index]['name']
        
        # (SỬA) Dùng hàm sorting_process để test (thực tế hơn)
        broadcast_log({"log_type": "warn", "message": f"AUTO-TEST: Kích hoạt chu trình cho '{lane_name}'"})
        sorting_process(lane_index)
        
    except Exception as e:
        logging.error(f"[TEST] Lỗi trong auto-test worker cho '{lane_name}': {e}", exc_info=True)

def run_auto_test_monitor():
    """(Giữ từ app_final) Luồng giám sát sensor cho Auto-Test."""
    global AUTO_TEST, auto_s_state, auto_s_trig
    logging.info("[TEST] Luồng giám sát Auto-Test đã bắt đầu.")
    try:
        while main_running:
            if error_manager.is_maintenance():
                if AUTO_TEST:
                    AUTO_TEST = False
                    logging.warning("[TEST] Auto-Test bị tắt do bảo trì.")
                    broadcast_log({"log_type": "error", "message": "Auto-Test bị tắt do bảo trì."})
                time.sleep(0.2); continue

            lanes_info_auto = []
            num_lanes = 0
            with state_lock:
                num_lanes = len(system_state['lanes'])
                for i in range(num_lanes):
                     if 0 <= i < len(system_state["lanes"]):
                         lane_state = system_state["lanes"][i]
                         lanes_info_auto.append({
                             "index": i, "pin": lane_state.get("sensor_pin"),
                             "name": lane_state.get('name')
                         })

            if AUTO_TEST:
                current_time = time.time()
                new_auto_readings = auto_s_state[:]

                for sensor_info in lanes_info_auto:
                    index, pin, name = sensor_info["index"], sensor_info["pin"], sensor_info["name"]
                    if pin is None: continue

                    try:
                        current_reading = GPIO.input(pin)
                    except Exception as gpio_err:
                        error_manager.trigger_maintenance(f"Lỗi đọc sensor Auto-Test ({name}): {gpio_err}")
                        continue

                    with state_lock:
                        if 0 <= index < len(system_state["lanes"]):
                            system_state["lanes"][index]["sensor_reading"] = current_reading

                    if current_reading == 0 and auto_s_state[index] == 1:
                        if (current_time - auto_s_trig[index]) > 1.0: # Chống nhiễu 1s
                            auto_s_trig[index] = current_time
                            broadcast_log({"log_type": "warn", "message": f"AUTO-TEST: Sensor '{name}' phát hiện!"})
                            executor.submit(run_auto_test_cycle_worker, index)

                    new_auto_readings[index] = current_reading

                auto_s_state = new_auto_readings
                time.sleep(0.02)
            else:
                auto_s_state = [1] * num_lanes
                auto_s_trig = [0.0] * num_lanes
                time.sleep(0.2)
    except Exception as e:
         logging.error(f"[AUTO-TEST] Luồng giám sát Auto-Test bị crash: {e}", exc_info=True)
         error_manager.trigger_maintenance(f"Lỗi luồng Auto-Test: {e}")

def trigger_mock_pin(pin, value, duration):
    """(Giữ từ app_final) Worker để kích hoạt Mock pin."""
    if not isinstance(pin, int) or not isinstance(value, int) or not isinstance(duration, (int, float)) or duration <= 0:
        return logging.error(f"[MOCK] Lệnh trigger không hợp lệ: pin={pin}, value={value}, duration={duration}")

    value_str = "HIGH" if value == GPIO.HIGH else "LOW"
    logging.info(f"[MOCK] Kích hoạt pin {pin} về {value_str} trong {duration}s")
    with mock_pin_override_lock:
        mock_pin_override[pin] = (value, time.time() + duration)
    broadcast_log({"log_type": "info", "message": f"Mock: Pin {pin} đặt về {value_str} trong {duration}s"})

# =============================
#      FLASK ROUTES & AUTH
# =============================
def check_auth(username, password):
    """Kiểm tra username và password."""
    return username == USERNAME and password == PASSWORD

def auth_fail_response():
    """Trả về 401 Unauthorized."""
    return Response('Yêu cầu đăng nhập.', 401, {'WWW-Authenticate': 'Basic realm="Login Required"'})

def require_auth(f):
    """Decorator yêu cầu Basic Auth."""
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return auth_fail_response()
        return f(*args, **kwargs)
    return decorated

def broadcast_current_state():
    """Luồng gửi state định kỳ cho client."""
    last_broadcast_state_json = ""
    while main_running:
        current_state_snapshot = {}
        current_state_json = ""
        with state_lock:
            current_state_snapshot = json.loads(json.dumps(system_state))

        current_state_snapshot["maintenance_mode"] = error_manager.is_maintenance()
        current_state_snapshot["last_error"] = error_manager.error
        current_state_snapshot["gpio_mode"] = current_state_snapshot.get('timing_config',{}).get('gpio_mode','BCM')

        try:
            current_state_json = json.dumps({"type": "state_update", "state": current_state_snapshot})
        except TypeError as e:
            logging.error(f"Lỗi serialize state: {e}"); time.sleep(1); continue

        if current_state_json != last_broadcast_state_json:
            try:
                broadcast(json.loads(current_state_json))
                last_broadcast_state_json = current_state_json
            except json.JSONDecodeError: pass
        time.sleep(0.5)

def stream_frames():
    """Generator stream video."""
    placeholder_path = 'black_frame.png'
    placeholder_img = None
    if os.path.exists(placeholder_path): placeholder_img = cv2.imread(placeholder_path)
    if placeholder_img is None:
        import numpy as np
        placeholder_img = np.zeros((480, 640, 3), dtype=np.uint8)
        logging.warning(f"[CAMERA] Không tìm thấy ảnh '{placeholder_path}'. Dùng khung hình đen.")

    while main_running:
        frame_to_stream = None
        if not error_manager.is_maintenance():
            with frame_lock:
                if latest_frame is not None:
                    frame_to_stream = latest_frame.copy()
        current_frame = frame_to_stream if frame_to_stream is not None else placeholder_img

        try:
            is_success, buffer = cv2.imencode('.jpg', current_frame, [cv2.IMWRITE_JPEG_QUALITY, 70])
            if is_success:
                yield (b'--frame\r\nContent-Type: image/jpeg\r\n\r\n' + buffer.tobytes() + b'\r\n')
            else:
                logging.warning("[CAMERA] Lỗi encode khung hình.")
        except Exception as encode_err:
            logging.error(f"[CAMERA] Lỗi encode khung hình: {encode_err}", exc_info=True)
        time.sleep(1 / 20)  # Stream 20 FPS

# --- Flask Routes ---
@app.route('/')
@require_auth
def route_index():
    """Serve trang HTML chính."""
    # (SỬA) Dùng index.html (giao diện của 3.py)
    return render_template('index.html')

@app.route('/video_feed')
@require_auth
def route_video_feed():
    """Stream video camera."""
    return Response(stream_frames(), mimetype='multipart/x-mixed-replace; boundary=frame')

# (SỬA) Tách route /config thành 2 hàm: GET /config và POST /update_config
@app.route('/config', methods=['GET'])
@require_auth
def route_get_config():
    """API để GET config."""
    with state_lock:
        config_snapshot = {
            "timing_config": system_state.get('timing_config', {}).copy(),
            "lanes_config": [
                {"id": l.get('id'), "name": l.get('name'), "sensor_pin": l.get('sensor_pin'),
                 "push_pin": l.get('push_pin'), "pull_pin": l.get('pull_pin')}
                for l in system_state.get('lanes', [])
            ],
            "qr_config": qr_config.copy() # (MỚI) Trả về config QR
        }
    return jsonify(config_snapshot)

@app.route('/update_config', methods=['POST'])
@require_auth
def route_update_config():
    """API để POST config."""
    global lanes_config, qr_config, RELAY_PINS, SENSOR_PINS
    global last_s_state, last_s_trig, auto_s_state, auto_s_trig
    global pending_sensor_triggers # (Từ 3.py)
    
    user = request.authorization.username
    
    # --- XỬ LÝ POST ---
    data = request.json
    if not data: return jsonify({"error": "Thiếu dữ liệu JSON"}), 400
    logging.info(f"[CONFIG] Nhận config update từ {user}: {data}")

    timing_update = data.get('timing_config', {})
    lanes_update = data.get('lanes_config')
    qr_update = data.get('qr_config') # (MỚI) Nhận config QR
    
    config_to_save = {}
    restart_needed = False

    with state_lock:
        # 1. Cập nhật Timing
        current_timing = system_state['timing_config']
        current_gpio_mode = current_timing.get('gpio_mode', 'BCM')
        current_timing.update(timing_update)
        new_gpio_mode = current_timing.get('gpio_mode', 'BCM')

        if new_gpio_mode != current_gpio_mode:
            logging.warning("[CONFIG] Chế độ GPIO thay đổi. Cần khởi động lại!")
            broadcast_log({"log_type": "warn", "message": "Chế độ GPIO thay đổi. Cần khởi động lại!"})
            restart_needed = True
            system_state['gpio_mode'] = current_gpio_mode # Giữ mode cũ đang chạy
        else:
            system_state['gpio_mode'] = new_gpio_mode
        config_to_save['timing_config'] = current_timing.copy()

        # 2. Cập nhật QR Config (MỚI)
        if qr_update is not None:
            qr_config.update(qr_update)
            logging.info(f"[CONFIG] Cấu hình QR được cập nhật: {qr_config}")
        config_to_save['qr_config'] = qr_config.copy()

        # 3. Cập nhật Lanes Config
        if isinstance(lanes_update, list):
             logging.info("[CONFIG] Cập nhật cấu hình lanes...")
             lanes_config = ensure_lane_ids(lanes_update) # (Từ 3.py)
             num_lanes = len(lanes_config)
             new_lanes_state, new_relay_pins, new_sensor_pins = [], [], []
             
             for i, cfg in enumerate(lanes_config):
                 s_pin = int(cfg["sensor_pin"]) if cfg.get("sensor_pin") is not None else None
                 p_pin = int(cfg["push_pin"]) if cfg.get("push_pin") is not None else None
                 pl_pin = int(cfg["pull_pin"]) if cfg.get("pull_pin") is not None else None
                 new_lanes_state.append({
                     "id": cfg.get("id"), "name": cfg.get("name", f"Lane {i+1}"), 
                     "status": "Sẵn sàng", "count": 0,
                     "sensor_pin": s_pin, "push_pin": p_pin, "pull_pin": pl_pin,
                     "sensor_reading": 1, "relay_grab": 0, "relay_push": 0})
                 if s_pin is not None: new_sensor_pins.append(s_pin)
                 if p_pin is not None: new_relay_pins.append(p_pin)
                 if pl_pin is not None: new_relay_pins.append(pl_pin)

             system_state['lanes'] = new_lanes_state
             # Reset các mảng state phụ thuộc
             last_s_state = [1] * num_lanes; last_s_trig = [0.0] * num_lanes
             auto_s_state = [1] * num_lanes; auto_s_trig = [0.0] * num_lanes
             pending_sensor_triggers = [0.0] * num_lanes # (Từ 3.py)
             RELAY_PINS, SENSOR_PINS = new_relay_pins, new_sensor_pins
             
             config_to_save['lanes_config'] = lanes_config
             restart_needed = True
             logging.warning("[CONFIG] Cấu hình lanes thay đổi. Cần khởi động lại!")
             broadcast_log({"log_type": "warn", "message": "Cấu hình lanes thay đổi. Cần khởi động lại!"})
        else:
            config_to_save['lanes_config'] = [
                {"id": l['id'], "name": l['name'], "sensor_pin": l['sensor_pin'],
                 "push_pin": l['push_pin'], "pull_pin": l['pull_pin']}
                for l in system_state['lanes']
            ]
            
    # 4. Lưu file (ngoài lock)
    try:
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(config_to_save, f, indent=4, ensure_ascii=False)
        msg = "Đã lưu cấu hình." + (" Yêu cầu khởi động lại!" if restart_needed else "")
        log_type = "warn" if restart_needed else "success"
        broadcast_log({"log_type": log_type, "message": msg})
        return jsonify({"message": msg, "config": config_to_save, "restart_required": restart_needed})
    except Exception as e:
        logging.error(f"[CONFIG] Lỗi lưu config POST: {e}", exc_info=True)
        broadcast_log({"log_type": "error", "message": f"Lỗi lưu config: {e}"})
        return jsonify({"error": f"Lỗi lưu config: {e}"}), 500

@app.route('/api/sort_log')
@require_auth
def route_api_sort_log():
    """API lấy log đếm (đã được tối ưu I/O)."""
    daily_summary = {}
    with sort_log_lock:
        try:
            full_data = {}
            if os.path.exists(SORT_LOG_FILE):
                with open(SORT_LOG_FILE, 'r', encoding='utf-8') as f:
                    content = f.read()
                    if content: full_data = json.loads(content)
            
            # (SỬA) Logic đọc log đã thay đổi (chỉ còn theo ngày)
            daily_summary = full_data 

            return jsonify(daily_summary)
        except Exception as e:
            logging.error(f"[API] Lỗi đọc sort log: {e}", exc_info=True)
            return jsonify({"error": f"Lỗi xử lý sort log: {e}"}), 500

@app.route('/api/reset_maintenance', methods=['POST'])
@require_auth
def route_reset_maintenance():
    """API reset chế độ bảo trì."""
    global pending_sensor_triggers, queue_head_since # (Từ 3.py)
    user = request.authorization.username
    if error_manager.is_maintenance():
        error_manager.reset()
        # (LOGIC TỪ 3.PY) Reset cả hàng chờ khi reset bảo trì
        with queue_lock:
            qr_queue.clear()
            queue_head_since = 0.0
            pending_sensor_triggers = [0.0] * len(pending_sensor_triggers)
        with state_lock:
            system_state["queue_indices"] = []
        
        broadcast_log({"log_type": "success", "message": f"Reset bảo trì bởi {user}. Hàng chờ đã xóa."})
        return jsonify({"message": "Đã reset chế độ bảo trì."})
    else:
        return jsonify({"message": "Hệ thống không ở chế độ bảo trì."})

# (MỚI) API reset hàng chờ (từ 3.py)
@app.route('/api/queue/reset', methods=['POST'])
@require_auth
def api_queue_reset():
    """API (POST) để xóa hàng chờ QR."""
    global pending_sensor_triggers, queue_head_since
    if error_manager.is_maintenance():
        return jsonify({"error": "Hệ thống đang bảo trì."}), 403

    try:
        with queue_lock:
            qr_queue.clear()
            queue_head_since = 0.0
            current_queue_for_log = list(qr_queue)
            pending_sensor_triggers = [0.0] * len(pending_sensor_triggers)

        with state_lock:
            for lane in system_state["lanes"]:
                lane["status"] = "Sẵn sàng"
            system_state["queue_indices"] = current_queue_for_log

        broadcast_log({"log_type": "warn", "message": "Hàng chờ QR đã được reset thủ công.", "queue": current_queue_for_log})
        logging.info("[API] Hàng chờ QR đã được reset thủ công.")
        return jsonify({"message": "Hàng chờ đã được reset."})
    except Exception as e:
        logging.error(f"[API] Lỗi reset hàng chờ: {e}")
        return jsonify({"error": str(e)}), 500

# --- WebSocket Route ---
@sock.route('/ws')
@require_auth # (SỬA) Dùng decorator cho WebSocket
def route_ws(ws):
    """Xử lý kết nối WebSocket."""
    global AUTO_TEST, test_seq_running
    global pending_sensor_triggers, queue_head_since # (Từ 3.py)

    user = request.authorization.username
    _add_client(ws)
    logging.info(f"[WS] Client '{user}' đã kết nối. Tổng: {len(_list_clients())}")

    try:
        # Gửi state ban đầu
        with state_lock:
            initial_state_snapshot = json.loads(json.dumps(system_state))
        initial_state_snapshot["maintenance_mode"] = error_manager.is_maintenance()
        initial_state_snapshot["last_error"] = error_manager.error
        ws.send(json.dumps({"type": "state_update", "state": initial_state_snapshot}))
    except Exception as e:
        logging.warning(f"[WS] Lỗi gửi state ban đầu cho '{user}': {e}")
        _remove_client(ws)
        return

    try:
        while True:
            message = ws.receive()
            if message is None: break

            try:
                data = json.loads(message)
                action = data.get('action')
                logging.debug(f"[WS] Nhận action '{action}' từ '{user}'")

                if error_manager.is_maintenance() and action not in ["reset_maintenance", "stop_tests"]:
                     broadcast_log({"log_type": "error", "message": "Hành động bị chặn: Hệ thống đang bảo trì."})
                     continue

                if action == 'reset_count':
                    idx = data.get('lane_index')
                    num_lanes = 0
                    with state_lock: num_lanes = len(system_state['lanes'])
                    if idx == 'all':
                        with state_lock:
                            for i in range(num_lanes): system_state['lanes'][i]['count'] = 0
                        broadcast_log({"log_type": "info", "message": f"Reset toàn bộ số đếm bởi '{user}'."})
                    elif isinstance(idx, int) and 0 <= idx < num_lanes:
                        with state_lock:
                            name = system_state['lanes'][idx]['name']
                            system_state['lanes'][idx]['count'] = 0
                        broadcast_log({"log_type": "info", "message": f"Reset đếm '{name}' bởi '{user}'."})

                elif action == "test_relay":
                    idx, act = data.get("lane_index"), data.get("relay_action")
                    if idx is not None and act in ["grab", "push"]:
                        executor.submit(run_test_relay_worker, idx, act)

                elif action == "test_all_relays":
                    executor.submit(run_test_all_relays_worker)

                elif action == "toggle_auto_test":
                    AUTO_TEST = data.get("enabled", False)
                    logging.info(f"[TEST] Auto-Test đặt thành {AUTO_TEST} bởi '{user}'.")
                    status_msg = "BẬT" if AUTO_TEST else "TẮT"
                    broadcast_log({"log_type": "warn", "message": f"Chế độ Auto-Test đã {status_msg} (bởi '{user}')."})
                    if not AUTO_TEST: reset_relays()

                elif action == "reset_maintenance":
                    if error_manager.is_maintenance():
                        error_manager.reset()
                        with queue_lock:
                            qr_queue.clear()
                            queue_head_since = 0.0
                            pending_sensor_triggers = [0.0] * len(pending_sensor_triggers)
                        with state_lock: system_state["queue_indices"] = []
                        broadcast_log({"log_type": "success", "message": f"Reset bảo trì bởi '{user}'. Hàng chờ đã xóa."})
                    else:
                        broadcast_log({"log_type": "info", "message": "Hệ thống không ở chế độ bảo trì."})

                elif action == "mock_trigger_pin" and isinstance(GPIO, MockGPIO):
                    pin, val, dur = data.get("pin"), data.get("value"), data.get("duration", 0.5)
                    if pin is not None and val is not None and isinstance(dur, (int, float)) and dur > 0:
                        executor.submit(trigger_mock_pin, pin, val, dur)
                    else: logging.warning(f"[MOCK] Lệnh trigger WS không hợp lệ: {data}")

                elif action == "stop_tests":
                    with test_seq_lock:
                        if test_seq_running:
                            test_seq_running = False
                            broadcast_log({"log_type": "warn", "message": f"Lệnh dừng test bởi '{user}'."})
                        else:
                            broadcast_log({"log_type": "info", "message": "Không có test tuần tự nào đang chạy."})

            except json.JSONDecodeError:
                logging.warning(f"[WS] Nhận JSON không hợp lệ từ '{user}': {message[:100]}...")
            except Exception as loop_err:
                logging.error(f"[WS] Lỗi xử lý message từ '{user}': {loop_err}", exc_info=True)

    except Exception as conn_err:
        if "close" not in str(conn_err).lower():
            logging.warning(f"[WS] Kết nối WebSocket lỗi/đóng cho '{user}': {conn_err}")
    finally:
        _remove_client(ws)
        logging.info(f"[WS] Client '{user}' đã ngắt kết nối. Tổng: {len(_list_clients())}")

# =============================
#         MAIN EXECUTION
# =============================
if __name__ == "__main__":
    threads = {} # Dict để giữ các luồng
    try:
        # --- 1. Cài đặt Logging ---
        log_format = '%(asctime)s [%(levelname)s] (%(threadName)s) %(message)s'
        logging.basicConfig(level=logging.INFO, format=log_format,
                            handlers=[logging.FileHandler(LOG_FILE, encoding='utf-8'),
                                      logging.StreamHandler()])
        logging.info("--- HỆ THỐNG ĐANG KHỞI ĐỘNG ---")

        # --- 2. Tải Cấu hình ---
        load_config()
        with state_lock: current_gpio_mode = system_state.get("gpio_mode", "BCM")

        # --- 3. Khởi tạo GPIO (Quan trọng) ---
        if isinstance(GPIO, RealGPIO):
            try:
                GPIO.setmode(GPIO.BCM if current_gpio_mode == "BCM" else GPIO.BOARD)
                GPIO.setwarnings(False)
                logging.info(f"[GPIO] Chế độ đã đặt: {current_gpio_mode}")
                logging.info(f"[GPIO] Cài đặt chân SENSOR: {SENSOR_PINS}")
                for pin in SENSOR_PINS: GPIO.setup(pin, GPIO.IN, pull_up_down=GPIO.PUD_UP)
                logging.info(f"[GPIO] Cài đặt chân RELAY: {RELAY_PINS}")
                for pin in RELAY_PINS: GPIO.setup(pin, GPIO.OUT)
                logging.info("[GPIO] Cài đặt chân vật lý hoàn tất.")
            except Exception as setup_err:
                 logging.critical(f"[CRITICAL] Cài đặt GPIO thất bại: {setup_err}", exc_info=True)
                 error_manager.trigger_maintenance(f"Lỗi cài đặt GPIO: {setup_err}")
        else: logging.info("[GPIO] Chế độ Mock, bỏ qua cài đặt chân vật lý.")

        # --- 4. Reset Relay ---
        if not error_manager.is_maintenance():
            reset_relays()

        # --- 5. Khởi động các luồng nền ---
        thread_targets = {
            "Camera": run_camera, 
            "QRScanner": qr_detection_loop,          # (Từ 3.py + Tối ưu)
            "SensorMon": sensor_monitoring_thread,   # (Từ 3.py)
            "StateBcast": broadcast_current_state,
            "AutoTestMon": run_auto_test_monitor,
            "ConfigSave": save_state_periodically    # (Tối ưu I/O)
        }
        for name, func in thread_targets.items():
            t = threading.Thread(target=func, name=name, daemon=True)
            t.start()
            threads[name] = t
        logging.info(f"Đã khởi động {len(threads)} luồng nền.")

        # --- 6. Thông báo sẵn sàng ---
        logging.info("="*55 + "\n HỆ THỐNG PHÂN LOẠI SẴN SÀNG (vFinal-Logic_v3)\n" +
                     f" GPIO Mode: {'REAL' if isinstance(GPIO, RealGPIO) else 'MOCK'} (Config: {current_gpio_mode})\n" +
                     f" Log File: {LOG_FILE}, Sort Log: {SORT_LOG_FILE}\n" +
                     f" Truy cập: http://<IP>:3000 (User: {USERNAME} / Pass: ***)\n" + "="*55)

        # --- 7. Chạy Web Server ---
        host = '0.0.0.0'
        port = 3000 # (Đổi sang port 3000 như 3.py)
        if serve:
            logging.info(f"Khởi động máy chủ Waitress trên {host}:{port}")
            serve(app, host=host, port=port, threads=8)
        else:
            logging.warning("Không tìm thấy Waitress. Dùng server dev của Flask (Không khuyến nghị cho production).")
            app.run(host=host, port=port, debug=False) # Tắt debug mode

    except KeyboardInterrupt:
        logging.info("\n--- HỆ THỐNG ĐANG TẮT (Ctrl+C) ---")
    except Exception as startup_err:
        logging.critical(f"[CRITICAL] Khởi động hệ thống thất bại: {startup_err}", exc_info=True)
        try:
            if isinstance(GPIO, RealGPIO): GPIO.cleanup()
        except Exception: pass
    finally:
        # --- Dọn dẹp ---
        main_running = False
        logging.info("Đang dừng các luồng nền...")
        executor.shutdown(wait=False)
        logging.info("Đang dọn dẹp GPIO...")
        try:
            GPIO.cleanup()
            logging.info("Dọn dẹp GPIO thành công.")
        except Exception as cleanup_err:
            logging.warning(f"Lỗi khi dọn dẹp GPIO: {cleanup_err}")
        logging.info("--- HỆ THỐNG ĐÃ TẮT HOÀN TOÀN ---")

