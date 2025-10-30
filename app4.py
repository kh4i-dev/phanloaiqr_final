# -*- coding: utf-8 -*-
import cv2
import time
import json
import threading
import logging
import os
import functools # (SỬA LỖI) Thêm thư viện functools
# (SỬA LỖI 1) Thêm thư viện ThreadPoolExecutor
from concurrent.futures import ThreadPoolExecutor
# (SỬA LỖI 1) Thêm 'jsonify' và 'Response', 'request'
from flask import Flask, render_template, Response, jsonify, request

# (MỚI) Thêm các lớp (class) để Mock GPIO
# =============================
#      LỚP TRỪU TƯỢNG GPIO
# =============================
try:
    # Thử import thư viện thật của Pi
    import RPi.GPIO as RPiGPIO
except (ImportError, RuntimeError):
    # Nếu thất bại (chạy trên Windows/Mac), dùng None
    RPiGPIO = None

class GPIOProvider:
    """Lớp trừu tượng (Abstract Class) để tương tác GPIO."""
    def setup(self, pin, mode, pull_up_down=None):
        raise NotImplementedError
    def output(self, pin, value):
        raise NotImplementedError
    def input(self, pin):
        raise NotImplementedError
    def cleanup(self):
        raise NotImplementedError
    def setmode(self, mode):
        raise NotImplementedError
    def setwarnings(self, value):
        raise NotImplementedError

class RealGPIO(GPIOProvider):
    """Triển khai GPIO thật (chạy trên Raspberry Pi)."""
    def __init__(self):
        if RPiGPIO is None:
            raise ImportError("Không thể tải thư viện RPi.GPIO. Bạn đang chạy trên Pi?")
        self.gpio = RPiGPIO
        self.BOARD = self.gpio.BOARD
        self.BCM = self.gpio.BCM
        self.OUT = self.gpio.OUT
        self.IN = self.gpio.IN
        self.HIGH = self.gpio.HIGH
        self.LOW = self.gpio.LOW
        self.PUD_UP = self.gpio.PUD_UP

    def setmode(self, mode): self.gpio.setmode(mode)
    def setwarnings(self, value): self.gpio.setwarnings(value)
    def setup(self, pin, mode, pull_up_down=None):
        if pull_up_down:
            self.gpio.setup(pin, mode, pull_up_down=pull_up_down)
        else:
            self.gpio.setup(pin, mode)
    def output(self, pin, value): self.gpio.output(pin, value)
    def input(self, pin): return self.gpio.input(pin)
    def cleanup(self): self.gpio.cleanup()

class MockGPIO(GPIOProvider):
    """Triển khai GPIO giả lập (Mock) để test trên PC."""
    def __init__(self):
        self.BOARD = "mock_BOARD"
        self.BCM = "mock_BCM"
        self.OUT = "mock_OUT"
        self.IN = "mock_IN"
        self.HIGH = 1
        self.LOW = 0
        self.PUD_UP = "mock_PUD_UP"
        self.pin_states = {} # Giả lập trạng thái pin
        self.input_pins = set()
        logging.warning("="*50)
        logging.warning("KHÔNG TÌM THẤY RPi.GPIO! ĐANG CHẠY Ở CHẾ ĐỘ GIẢ LẬP (MOCK).")
        logging.warning("="*50)

    def setmode(self, mode): logging.info(f"[MOCK] setmode={mode}")
    def setwarnings(self, value): logging.info(f"[MOCK] setwarnings={value}")
    def setup(self, pin, mode, pull_up_down=None):
        logging.info(f"[MOCK] setup pin {pin} mode={mode} pull_up_down={pull_up_down}")
        if mode == self.OUT:
            self.pin_states[pin] = self.LOW # Mặc định là LOW
        else: # IN
            self.pin_states[pin] = self.HIGH # Mặc định là HIGH (do PUD_UP)
            self.input_pins.add(pin)
    def output(self, pin, value):
        logging.info(f"[MOCK] output pin {pin}={value}")
        self.pin_states[pin] = value
    def input(self, pin):
        # Giả lập sensor luôn ở trạng thái 1 (không có vật)
        val = self.pin_states.get(pin, self.HIGH)
        # logging.info(f"[MOCK] input pin {pin} -> {val}")
        return val
    def set_input_state(self, pin, logical_state):
        """Đặt trạng thái chân input (0 hoặc 1)."""
        if pin not in self.input_pins:
            logging.info(f"[MOCK] Tự động thêm chân input {pin}.")
            self.input_pins.add(pin)
        state = self.HIGH if logical_state else self.LOW
        self.pin_states[pin] = state
        logging.info(f"[MOCK] set_input_state pin {pin} -> {state}")
        return state
    def toggle_input_state(self, pin):
        """Đảo trạng thái chân input và trả về giá trị mới (0/1)."""
        if pin not in self.input_pins:
            self.input_pins.add(pin)
        current = self.input(pin)
        new_state = self.LOW if current == self.HIGH else self.HIGH
        self.pin_states[pin] = new_state
        logging.info(f"[MOCK] toggle_input_state pin {pin} -> {new_state}")
        return 0 if new_state == self.LOW else 1
    def cleanup(self): logging.info("[MOCK] cleanup GPIO")

def get_gpio_provider():
    """Tự động chọn RealGPIO nếu có thư viện, ngược lại chọn MockGPIO."""
    if RPiGPIO:
        return RealGPIO()
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
            if self.maintenance_mode: # Đã ở chế độ bảo trì rồi
                return
            self.maintenance_mode = True
            self.last_error = message
            logging.critical("="*50)
            logging.critical(f"[MAINTENANCE MODE] Lỗi nghiêm trọng: {message}")
            logging.critical("Hệ thống đã dừng hoạt động. Yêu cầu kiểm tra.")
            logging.critical("="*50)
            # Gửi log cho client ngay lập tức
            broadcast_log({"log_type": "error", "message": f"MAINTENANCE MODE: {message}"})

    def reset(self):
        """Reset lại trạng thái (khi admin yêu cầu)."""
        with self.lock:
            self.maintenance_mode = False
            self.last_error = None
            logging.info("[MAINTENANCE MODE] Đã reset chế độ bảo trì.")

    def is_maintenance(self):
        """Kiểm tra xem hệ thống có đang bảo trì không."""
        return self.maintenance_mode

# =============================
#         CẤU HÌNH CHUNG
# =============================
CAMERA_INDEX = 0
CONFIG_FILE = 'config.json'
LOG_FILE = 'system.log'
SORT_LOG_FILE = 'sort_log.json'
ACTIVE_LOW = True

# (MỚI) Thông tin đăng nhập (có thể tắt hoàn toàn qua biến môi trường)
AUTH_ENABLED = os.environ.get("APP_AUTH_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
USERNAME = os.environ.get("APP_USERNAME", "admin")
PASSWORD = os.environ.get("APP_PASSWORD", "123")

# =============================
#      KHỞI TẠO CÁC ĐỐI TƯỢNG
# =============================
GPIO = get_gpio_provider()
error_manager = ErrorManager()

# (SỬA LỖI 1) Khởi tạo ThreadPoolExecutor (giới hạn 3 luồng test)
executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="TestWorker")
# (SỬA LỖI 3) Thêm lock cho file sort_log.json
sort_log_lock = threading.Lock()

# =============================
#         KHAI BÁO CHÂN GPIO
# =============================
# (CHUYỂN) Không gọi GPIO.setmode() ở đây nữa, chuyển vào __main__

# Định nghĩa mặc định (sẽ bị ghi đè bởi config)
DEFAULT_LANES_CONFIG = [
    {"name": "Loại 1", "sensor_pin": 3, "push_pin": 17, "pull_pin": 18},
    {"name": "Loại 2", "sensor_pin": 23, "push_pin": 27, "pull_pin": 14},
    {"name": "Loại 3", "sensor_pin": 24, "push_pin": 22, "pull_pin": 4},
]
lanes_config = DEFAULT_LANES_CONFIG # Sẽ được cập nhật từ file config

# Tạo danh sách chân từ config (sau khi load)
RELAY_PINS = []
SENSOR_PINS = []

# =============================
#       HÀM ĐIỀU KHIỂN RELAY
# =============================
def RELAY_ON(pin):
    """Bật relay (kích hoạt)."""
    GPIO.output(pin, GPIO.LOW if ACTIVE_LOW else GPIO.HIGH)

def RELAY_OFF(pin):
    """Tắt relay (ngừng kích hoạt)."""
    GPIO.output(pin, GPIO.HIGH if ACTIVE_LOW else GPIO.LOW)

# =============================
#        TRẠNG THÁI HỆ THỐNG
# =============================
# Cấu trúc system_state ban đầu, lanes sẽ được tạo động từ config
system_state = {
    "lanes": [], # Sẽ được tạo động từ lanes_config
    "timing_config": {
        "cycle_delay": 0.3,
        "settle_delay": 0.2,
        "sensor_debounce": 0.1,
        "push_delay": 0.0,
        "gpio_mode": "BCM"
    },
    "is_mock": isinstance(GPIO, MockGPIO),
    "auth_enabled": AUTH_ENABLED,
    "maintenance_mode": False,
    "gpio_mode": "BCM",
    "last_error": None # (MỚI) Thêm last_error vào state
}

# Các biến global cho threading
state_lock = threading.Lock()
main_loop_running = True
latest_frame = None
frame_lock = threading.Lock()

# Biến cho việc chống nhiễu (debounce) sensor
last_sensor_state = [] # Sẽ được khởi tạo dựa trên số lanes
last_sensor_trigger_time = [] # Sẽ được khởi tạo

# Biến toàn cục cho chức năng Test
AUTO_TEST_ENABLED = False
auto_test_last_state = [] # Sẽ được khởi tạo
auto_test_last_trigger = [] # Sẽ được khởi tạo


# =============================
#      HÀM KHỞI ĐỘNG & CONFIG
# =============================
def load_local_config():
    """Tải cấu hình từ config.json, bao gồm cả timing và lanes."""
    global lanes_config, RELAY_PINS, SENSOR_PINS
    global last_sensor_state, last_sensor_trigger_time
    global auto_test_last_state, auto_test_last_trigger

    default_timing_config = {
        "cycle_delay": 0.3,
        "settle_delay": 0.2,
        "sensor_debounce": 0.1,
        "push_delay": 0.0,
        "gpio_mode": "BCM"
    }
    # (MỚI) Thêm config mặc định cho lanes
    default_config_full = {
        "timing_config": default_timing_config,
        "lanes_config": DEFAULT_LANES_CONFIG # Lấy từ biến global
    }

    loaded_config = default_config_full # Bắt đầu với mặc định

    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                file_content = f.read()
                if not file_content:
                    logging.warning("[CONFIG] File config rỗng, dùng mặc định.")
                else:
                    loaded_config_from_file = json.loads(file_content)
                    # Merge timing config
                    timing_cfg = default_timing_config.copy()
                    timing_cfg.update(loaded_config_from_file.get('timing_config', {}))
                    loaded_config['timing_config'] = timing_cfg
                    # Merge lanes config
                    loaded_config['lanes_config'] = loaded_config_from_file.get('lanes_config', DEFAULT_LANES_CONFIG)

        except Exception as e:
            logging.error(f"[CONFIG] Lỗi đọc file config ({e}), dùng mặc định.")
            error_manager.trigger_maintenance(f"Lỗi file config.json: {e}")
            loaded_config = default_config_full # Reset về mặc định nếu lỗi
    else:
        logging.warning("[CONFIG] Không có file config, dùng mặc định và tạo mới.")
        try:
             with open(CONFIG_FILE, 'w') as f:
                json.dump(loaded_config, f, indent=4) # Lưu config đầy đủ
        except Exception as e:
             logging.error(f"[CONFIG] Không thể tạo file config mới: {e}")

    # Cập nhật global config và state
    lanes_config = loaded_config['lanes_config']
    num_lanes = len(lanes_config)

    # Tạo lại system_state["lanes"] dựa trên config mới
    new_system_lanes = []
    RELAY_PINS = []
    SENSOR_PINS = []
    for i, lane_cfg in enumerate(lanes_config):
        new_system_lanes.append({
            "name": lane_cfg.get("name", f"Lane {i+1}"),
            "status": "Sẵn sàng",
            "count": 0,
            "sensor_pin": lane_cfg.get("sensor_pin"),
            "push_pin": lane_cfg.get("push_pin"),
            "pull_pin": lane_cfg.get("pull_pin"),
            "sensor_reading": 1,
            "relay_grab": 0,
            "relay_push": 0
        })
        # Thêm pin vào danh sách để setup
        if lane_cfg.get("sensor_pin"): SENSOR_PINS.append(lane_cfg["sensor_pin"])
        if lane_cfg.get("push_pin"): RELAY_PINS.append(lane_cfg["push_pin"])
        if lane_cfg.get("pull_pin"): RELAY_PINS.append(lane_cfg["pull_pin"])

    # Khởi tạo các biến state dựa trên số lanes
    last_sensor_state = [1] * num_lanes
    last_sensor_trigger_time = [0.0] * num_lanes
    auto_test_last_state = [1] * num_lanes
    auto_test_last_trigger = [0.0] * num_lanes

    with state_lock:
        system_state['timing_config'] = loaded_config['timing_config']
        system_state['gpio_mode'] = loaded_config['timing_config'].get("gpio_mode", "BCM")
        system_state['lanes'] = new_system_lanes # Ghi đè lanes cũ
        system_state['auth_enabled'] = AUTH_ENABLED
        system_state['is_mock'] = isinstance(GPIO, MockGPIO)
    logging.info(f"[CONFIG] Loaded {num_lanes} lanes config.")
    logging.info(f"[CONFIG] Loaded timing config: {system_state['timing_config']}")

def reset_all_relays_to_default():
    """Reset tất cả relay về trạng thái an toàn (THU BẬT, ĐẨY TẮT)."""
    logging.info("[GPIO] Reset tất cả relay về trạng thái mặc định (THU BẬT).")
    with state_lock:
        # Lặp qua state để đảm bảo dùng đúng pin đã load
        for lane in system_state["lanes"]:
            if lane.get("pull_pin"): RELAY_ON(lane["pull_pin"])
            if lane.get("push_pin"): RELAY_OFF(lane["push_pin"])
            # Cập nhật trạng thái
            lane["relay_grab"] = 1 if lane.get("pull_pin") else 0
            lane["relay_push"] = 0
            lane["status"] = "Sẵn sàng"
    time.sleep(0.1)
    logging.info("[GPIO] Reset hoàn tất.")

def periodic_config_save():
    """Tự động lưu config mỗi 60s (bao gồm cả timing và lanes)."""
    while main_loop_running:
        time.sleep(60)

        if error_manager.is_maintenance():
            continue

        try:
            config_to_save = {}
            with state_lock:
                config_to_save['timing_config'] = system_state['timing_config'].copy()
                # Lấy lanes_config hiện tại (có thể đã thay đổi)
                current_lanes_config = []
                for lane_state in system_state['lanes']:
                    current_lanes_config.append({
                        "name": lane_state['name'],
                        "sensor_pin": lane_state['sensor_pin'],
                        "push_pin": lane_state['push_pin'],
                        "pull_pin": lane_state['pull_pin']
                    })
                config_to_save['lanes_config'] = current_lanes_config

            with open(CONFIG_FILE, 'w') as f:
                json.dump(config_to_save, f, indent=4)
            logging.info("[CONFIG] Đã tự động lưu config (timing + lanes).")
        except Exception as e:
            logging.error(f"[CONFIG] Lỗi tự động lưu config: {e}")

# =============================
#         LUỒNG CAMERA
# =============================
def camera_capture_thread():
    global latest_frame
    camera = cv2.VideoCapture(CAMERA_INDEX)
    camera.set(cv2.CAP_PROP_FRAME_WIDTH, 640)
    camera.set(cv2.CAP_PROP_FRAME_HEIGHT, 480)
    camera.set(cv2.CAP_PROP_BUFFERSIZE, 1)

    if not camera.isOpened():
        logging.error("[ERROR] Không mở được camera.")
        error_manager.trigger_maintenance("Không thể mở camera.")
        return

    retries = 0
    max_retries = 5

    while main_loop_running:
        if error_manager.is_maintenance():
            time.sleep(0.5)
            continue

        ret, frame = camera.read()
        if not ret:
            retries += 1
            logging.warning(f"[WARN] Mất camera (lần {retries}/{max_retries}), thử khởi động lại...")
            broadcast_log({"log_type":"error","message":f"Mất camera (lần {retries}), đang thử lại..."})

            if retries > max_retries:
                logging.critical("[ERROR] Camera lỗi vĩnh viễn. Chuyển sang chế độ bảo trì.")
                error_manager.trigger_maintenance("Camera lỗi vĩnh viễn (mất kết nối).")
                break

            camera.release()
            time.sleep(1)
            camera = cv2.VideoCapture(CAMERA_INDEX)
            continue

        retries = 0

        with frame_lock:
            latest_frame = frame.copy()
        time.sleep(1 / 30)
    camera.release()

# =============================
#       LƯU LOG ĐẾM SẢN PHẨM
# =============================
def log_sort_count(lane_index, lane_name):
    """Ghi lại số lượng đếm vào file JSON theo ngày (an toàn)."""
    with sort_log_lock:
        try:
            today = time.strftime('%Y-%m-%d')

            sort_log = {}
            if os.path.exists(SORT_LOG_FILE):
                try:
                    with open(SORT_LOG_FILE, 'r') as f:
                        file_content = f.read()
                        if file_content:
                            sort_log = json.loads(file_content)
                except json.JSONDecodeError:
                     logging.error(f"[SORT_LOG] Lỗi đọc file {SORT_LOG_FILE}, file có thể bị hỏng.")
                     # Tạo backup và bắt đầu lại file mới
                     backup_name = f"{SORT_LOG_FILE}.{time.strftime('%Y%m%d_%H%M%S')}.bak"
                     try:
                         os.rename(SORT_LOG_FILE, backup_name)
                         logging.warning(f"[SORT_LOG] Đã backup file lỗi thành {backup_name}")
                     except Exception as re:
                          logging.error(f"[SORT_LOG] Không thể backup file lỗi: {re}")
                     sort_log = {} # Bắt đầu lại
                except Exception as e:
                     logging.error(f"[SORT_LOG] Lỗi không xác định khi đọc file: {e}")
                     sort_log = {}

            sort_log.setdefault(today, {})
            sort_log[today].setdefault(lane_name, 0)

            sort_log[today][lane_name] += 1

            with open(SORT_LOG_FILE, 'w') as f:
                json.dump(sort_log, f, indent=4)

        except Exception as e:
            logging.error(f"[ERROR] Lỗi khi ghi sort_log.json: {e}")

# =============================
#       CHU TRÌNH PHÂN LOẠI
# =============================
def sorting_process(lane_index):
    """Quy trình đẩy-thu piston (chạy trên 1 luồng riêng)."""
    lane_name = ""
    push_pin, pull_pin = None, None # Khởi tạo
    try:
        with state_lock:
            # Đảm bảo index hợp lệ
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

            # Kiểm tra xem pin có được định nghĩa không
            if not push_pin or not pull_pin:
                logging.error(f"[SORT] Lane {lane_name} (index {lane_index}) chưa được cấu hình đủ chân relay.")
                lane["status"] = "Lỗi Config"
                broadcast_log({"log_type": "error", "message": f"Lane {lane_name} thiếu cấu hình chân relay."})
                return # Không chạy nếu thiếu pin

            lane["status"] = "Đang phân loại..."

        broadcast_log({"log_type": "info", "message": f"Bắt đầu chu trình đẩy {lane_name}"})

        # --- Chu trình ---
        RELAY_OFF(pull_pin)
        with state_lock: system_state["lanes"][lane_index]["relay_grab"] = 0
        time.sleep(settle_delay)
        if not main_loop_running: return

        RELAY_ON(push_pin)
        with state_lock: system_state["lanes"][lane_index]["relay_push"] = 1
        time.sleep(delay)
        if not main_loop_running: return

        RELAY_OFF(push_pin)
        with state_lock: system_state["lanes"][lane_index]["relay_push"] = 0
        time.sleep(settle_delay)
        if not main_loop_running: return

        RELAY_ON(pull_pin)
        with state_lock: system_state["lanes"][lane_index]["relay_grab"] = 1

    except Exception as e:
         logging.error(f"[SORT] Lỗi trong sorting_process (lane {lane_name}): {e}")
         error_manager.trigger_maintenance(f"Lỗi sorting_process (Lane {lane_name}): {e}")
    finally:
        # Đảm bảo luôn reset trạng thái, ngay cả khi pin bị thiếu
        with state_lock:
             if 0 <= lane_index < len(system_state["lanes"]):
                 lane = system_state["lanes"][lane_index]
                 # Chỉ tăng count và log nếu chu trình chạy (có lane_name) và không phải lỗi config
                 if lane_name and lane["status"] != "Lỗi Config":
                     lane["count"] += 1
                     broadcast_log({"log_type": "sort", "name": lane_name, "count": lane['count']})
                     # Ghi log chỉ khi thành công
                     log_sort_count(lane_index, lane_name)
                 
                 # Reset trạng thái về Sẵn sàng (hoặc giữ Lỗi Config)
                 if lane["status"] != "Lỗi Config":
                     lane["status"] = "Sẵn sàng"

        if lane_name: # Chỉ log nếu lane_name đã được gán
            broadcast_log({"log_type": "info", "message": f"Hoàn tất chu trình cho {lane_name}"})


def handle_sorting_with_delay(lane_index):
    """Luồng trung gian, chờ push_delay rồi mới gọi sorting_process."""
    push_delay = 0.0
    lane_name_for_log = f"Lane {lane_index + 1}"

    try:
        with state_lock:
             # Đảm bảo index hợp lệ
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

        with state_lock:
            # Kiểm tra index lại lần nữa trước khi truy cập
            if not (0 <= lane_index < len(system_state["lanes"])): return
            current_status = system_state["lanes"][lane_index]["status"]

        if current_status == "Đang chờ đẩy":
             sorting_process(lane_index)
        else:
             broadcast_log({"log_type": "warn", "message": f"Hủy chu trình {lane_name_for_log} do trạng thái thay đổi."})

    except Exception as e:
        logging.error(f"[ERROR] Lỗi trong luồng handle_sorting_with_delay (lane {lane_name_for_log}): {e}")
        error_manager.trigger_maintenance(f"Lỗi luồng sorting_delay (Lane {lane_name_for_log}): {e}")
        with state_lock:
             # Kiểm tra index trước khi reset
             if 0 <= lane_index < len(system_state["lanes"]):
                 if system_state["lanes"][lane_index]["status"] == "Đang chờ đẩy":
                      system_state["lanes"][lane_index]["status"] = "Sẵn sàng"
                      broadcast_log({"log_type": "error", "message": f"Lỗi delay, reset {lane_name_for_log}"})


# =============================
#       QUÉT MÃ QR TỰ ĐỘNG
# =============================
def qr_detection_loop():
    detector = cv2.QRCodeDetector()
    last_qr, last_time = "", 0.0
    # (SỬA) Tạo LANE_MAP động từ system_state
    LANE_MAP = {}
    with state_lock:
        for i, lane in enumerate(system_state["lanes"]):
            # Chuyển tên lane thành chữ hoa, bỏ dấu cách để làm key
            key = lane["name"].upper().replace(" ", "")
            LANE_MAP[key] = i
    logging.info(f"[QR] Lane map đã tạo: {LANE_MAP}")

    while main_loop_running:
        if AUTO_TEST_ENABLED or error_manager.is_maintenance():
            time.sleep(0.2)
            continue

        frame_copy = None
        with frame_lock:
            if latest_frame is not None:
                frame_copy = latest_frame.copy()

        if frame_copy is None:
            time.sleep(0.1)
            continue

        try:
            gray_frame = cv2.cvtColor(frame_copy, cv2.COLOR_BGR2GRAY)
            if gray_frame.mean() < 10:
                time.sleep(0.1)
                continue
        except Exception:
            pass

        try:
            data, _, _ = detector.detectAndDecode(frame_copy)
        except cv2.error:
            data = None
            time.sleep(0.1)
            continue

        if data and (data != last_qr or time.time() - last_time > 3.0):
            last_qr, last_time = data, time.time()
            data_upper = data.strip().upper().replace(" ", "") # Chuẩn hóa key

            if data_upper in LANE_MAP:
                idx = LANE_MAP[data_upper]
                # Kiểm tra index hợp lệ
                with state_lock:
                     if 0 <= idx < len(system_state["lanes"]):
                         if system_state["lanes"][idx]["status"] == "Sẵn sàng":
                             broadcast_log({"log_type": "qr", "data": data_upper})
                             system_state["lanes"][idx]["status"] = "Đang chờ vật..."
            elif data_upper == "NG":
                broadcast_log({"log_type": "qr_ng", "data": data_upper})
            else:
                broadcast_log({"log_type": "unknown_qr", "data": data_upper})

        time.sleep(0.1)

# =============================
#      LUỒNG GIÁM SÁT SENSOR
# =============================
def sensor_monitoring_thread():
    global last_sensor_state, last_sensor_trigger_time

    try:
        while main_loop_running:
            if AUTO_TEST_ENABLED or error_manager.is_maintenance():
                time.sleep(0.1)
                continue

            with state_lock:
                debounce_time = system_state['timing_config']['sensor_debounce']
                num_lanes = len(system_state['lanes'])
            now = time.time()

            for i in range(num_lanes):
                with state_lock:
                    # Kiểm tra index trước khi truy cập
                    if not (0 <= i < len(system_state["lanes"])): continue
                    lane = system_state["lanes"][i]
                    sensor_pin = lane.get("sensor_pin")
                    current_status = lane["status"]

                # Bỏ qua nếu lane không có sensor pin
                if not sensor_pin: continue

                try:
                    sensor_now = GPIO.input(sensor_pin)
                except Exception as gpio_e:
                     logging.error(f"[SENSOR] Lỗi đọc GPIO pin {sensor_pin} (Lane {lane.get('name', i+1)}): {gpio_e}")
                     error_manager.trigger_maintenance(f"Lỗi đọc sensor pin {sensor_pin} (Lane {lane.get('name', i+1)}): {gpio_e}")
                     continue # Bỏ qua lần đọc này

                with state_lock:
                     # Kiểm tra index lại
                     if 0 <= i < len(system_state["lanes"]):
                         system_state["lanes"][i]["sensor_reading"] = sensor_now

                # Phát hiện sườn xuống (1 -> 0)
                if sensor_now == 0 and last_sensor_state[i] == 1:
                    # Chống nhiễu (debounce)
                    if (now - last_sensor_trigger_time[i]) > debounce_time:
                        last_sensor_trigger_time[i] = now

                        # Chỉ kích hoạt nếu trạng thái là "Đang chờ vật"
                        if current_status == "Đang chờ vật...":
                            with state_lock:
                                # Kiểm tra index lại
                                if 0 <= i < len(system_state["lanes"]):
                                     system_state["lanes"][i]["status"] = "Đang chờ đẩy"

                            threading.Thread(target=handle_sorting_with_delay, args=(i,), daemon=True).start()
                        else:
                            broadcast_log({"log_type": "warn", "message": f"Sensor {lane.get('name', i+1)} bị kích hoạt ngoài dự kiến."})

                last_sensor_state[i] = sensor_now

            adaptive_sleep = 0.05 if all(s == 1 for s in last_sensor_state) else 0.01
            time.sleep(adaptive_sleep)

    except Exception as e:
        logging.error(f"[ERROR] Luồng sensor_monitoring_thread bị crash: {e}")
        error_manager.trigger_maintenance(f"Lỗi luồng Sensor: {e}")


# =============================
#        FLASK + WEBSOCKET
# =============================
app = Flask(__name__)
from flask_sock import Sock
sock = Sock(app)
connected_clients = set()
clients_lock = threading.Lock()

def _add_client(ws):
    with clients_lock:
        connected_clients.add(ws)

def _remove_client(ws):
    with clients_lock:
        connected_clients.discard(ws)

def _list_clients():
    with clients_lock:
        return list(connected_clients)

def broadcast_log(log_data):
    """Gửi 1 tin nhắn log cụ thể cho client."""
    log_data['timestamp'] = time.strftime('%H:%M:%S')
    msg = json.dumps({"type": "log", **log_data})
    for client in _list_clients():
        try:
            client.send(msg)
        except Exception:
            _remove_client(client)

# =============================
#      CÁC HÀM XỬ LÝ TEST (🧪)
# =============================

def _run_test_relay(lane_index, relay_action):
    """Hàm worker (chạy trong ThreadPool) để test 1 relay."""
    pin_to_test = None
    state_key_to_update = None
    lane_name = f"Lane {lane_index + 1}"

    try:
        with state_lock:
            # Kiểm tra index
            if not (0 <= lane_index < len(system_state["lanes"])):
                 logging.error(f"[TEST] Lane index {lane_index} không hợp lệ.")
                 broadcast_log({"log_type": "error", "message": f"Test thất bại: Lane index {lane_index} không hợp lệ."})
                 return

            lane = system_state["lanes"][lane_index]
            lane_name = lane['name']
            if relay_action == "grab":
                pin_to_test = lane.get("pull_pin")
                state_key_to_update = "relay_grab"
            else: # "push"
                pin_to_test = lane.get("push_pin")
                state_key_to_update = "relay_push"

            # Kiểm tra pin
            if not pin_to_test:
                logging.error(f"[TEST] Lane {lane_name} thiếu cấu hình pin {relay_action}.")
                broadcast_log({"log_type": "error", "message": f"Test thất bại: Lane {lane_name} thiếu pin {relay_action}."})
                return

        RELAY_ON(pin_to_test)
        with state_lock:
             if 0 <= lane_index < len(system_state["lanes"]):
                 system_state["lanes"][lane_index][state_key_to_update] = 1

        time.sleep(0.5)
        if not main_loop_running: return

        RELAY_OFF(pin_to_test)
        with state_lock:
             if 0 <= lane_index < len(system_state["lanes"]):
                 system_state["lanes"][lane_index][state_key_to_update] = 0

        broadcast_log({"log_type": "info", "message": f"Test {relay_action} {lane_name} OK"})

    except Exception as e:
        logging.error(f"[ERROR] Lỗi test relay {lane_name}: {e}")
        broadcast_log({"log_type": "error", "message": f"Lỗi test relay {lane_name}: {e}"})

def _run_test_all_relays():
    """Hàm worker (chạy trong ThreadPool) để test tuần tự các relay."""
    logging.info("[TEST] Bắt đầu test tuần tự các relay...")
    with state_lock:
        num_lanes = len(system_state['lanes'])

    for i in range(num_lanes):
        if not main_loop_running: break
        with state_lock: # Lấy tên lane an toàn
            lane_name = system_state['lanes'][i]['name'] if 0 <= i < len(system_state['lanes']) else f"Lane {i+1}"
        broadcast_log({"log_type": "info", "message": f"Test THU (grab) {lane_name}..."})
        _run_test_relay(i, "grab")
        time.sleep(0.5)

        if not main_loop_running: break
        broadcast_log({"log_type": "info", "message": f"Test ĐẨY (push) {lane_name}..."})
        _run_test_relay(i, "push")
        time.sleep(0.5)
    logging.info("[TEST] Hoàn tất test tuần tự.")
    broadcast_log({"log_type": "info", "message": "Hoàn tất test tuần tự các relay."})

def _auto_test_cycle_worker(lane_index):
    """Hàm worker (chạy trong ThreadPool) cho chu trình Đẩy -> Thu (Auto-Test)."""
    lane_name = f"Lane {lane_index + 1}"
    try:
        with state_lock:
            if 0 <= lane_index < len(system_state['lanes']):
                lane_name = system_state['lanes'][lane_index]['name']

        broadcast_log({"log_type": "warn", "message": f"AUTO-TEST: Đẩy {lane_name}"})
        _run_test_relay(lane_index, "push")
        time.sleep(0.3)
        if not main_loop_running: return

        broadcast_log({"log_type": "warn", "message": f"AUTO-TEST: Thu {lane_name}"})
        _run_test_relay(lane_index, "grab")
    except Exception as e:
         logging.error(f"[ERROR] Lỗi auto_test_cycle_worker ({lane_name}): {e}")

def auto_test_loop():
    """Luồng riêng cho auto-test: Sensor sáng -> Đẩy rồi Thu."""
    global AUTO_TEST_ENABLED, auto_test_last_state, auto_test_last_trigger

    logging.info("[TEST] Luồng Auto-Test (sensor->relay) đã khởi động.")

    try:
        while main_loop_running:
            if error_manager.is_maintenance():
                if AUTO_TEST_ENABLED:
                    AUTO_TEST_ENABLED = False
                    logging.warning("[TEST] Tự động tắt Auto-Test do có lỗi hệ thống.")
                    broadcast_log({"log_type": "error", "message": "Tự động tắt Auto-Test do hệ thống đang bảo trì."})
                time.sleep(0.2)
                continue

            with state_lock: # Lấy số lanes hiện tại
                 num_lanes = len(system_state['lanes'])

            if AUTO_TEST_ENABLED:
                now = time.time()
                for i in range(num_lanes):
                    with state_lock:
                        # Kiểm tra index
                        if not (0 <= i < len(system_state["lanes"])): continue
                        sensor_pin = system_state["lanes"][i].get("sensor_pin")

                    if not sensor_pin: continue # Bỏ qua lane thiếu sensor pin

                    try:
                        sensor_now = GPIO.input(sensor_pin)
                    except Exception as gpio_e:
                        logging.error(f"[AUTO-TEST] Lỗi đọc GPIO pin {sensor_pin} (Lane {i+1}): {gpio_e}")
                        error_manager.trigger_maintenance(f"Lỗi đọc sensor pin {sensor_pin} (Auto-Test): {gpio_e}")
                        continue

                    with state_lock:
                         if 0 <= i < len(system_state["lanes"]):
                             system_state["lanes"][i]["sensor_reading"] = sensor_now

                    if sensor_now == 0 and auto_test_last_state[i] == 1:
                        if (now - auto_test_last_trigger[i]) > 1.0:
                            auto_test_last_trigger[i] = now

                            with state_lock: # Lấy tên lane an toàn
                                lane_name = system_state['lanes'][i]['name'] if 0 <= i < len(system_state['lanes']) else f"Lane {i+1}"
                            broadcast_log({"log_type": "warn", "message": f"AUTO-TEST: Sensor {lane_name} phát hiện!"})
                            executor.submit(_auto_test_cycle_worker, i)

                    auto_test_last_state[i] = sensor_now
                time.sleep(0.02)
            else:
                auto_test_last_state = [1] * num_lanes
                auto_test_last_trigger = [0.0] * num_lanes
                time.sleep(0.2)
    except Exception as e:
         logging.error(f"[ERROR] Luồng auto_test_loop bị crash: {e}")
         error_manager.trigger_maintenance(f"Lỗi luồng Auto-Test: {e}")


# =============================
#     CÁC HÀM CỦA FLASK (TIẾP)
# =============================
def check_auth(username, password):
    """Kiểm tra username và password."""
    if not AUTH_ENABLED:
        return True
    return username == USERNAME and password == PASSWORD

def authenticate():
    """Gửi phản hồi 401 (Yêu cầu đăng nhập)."""
    return Response(
        'Yêu cầu đăng nhập.', 401,
        {'WWW-Authenticate': 'Basic realm="Login Required"'})

def requires_auth(f):
    """Decorator để yêu cầu đăng nhập cho một route."""
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
    """Gửi state cho client, chỉ khi state thay đổi."""
    last_state_str = ""

    while main_loop_running:
        current_msg = ""
        with state_lock:
            # Cập nhật trạng thái bảo trì và lỗi cuối
            system_state["maintenance_mode"] = error_manager.is_maintenance()
            system_state["last_error"] = error_manager.last_error
            system_state["is_mock"] = isinstance(GPIO, MockGPIO)
            system_state["auth_enabled"] = AUTH_ENABLED
            # Cập nhật chế độ gpio từ timing_config
            system_state["gpio_mode"] = system_state['timing_config'].get('gpio_mode', 'BCM')
            current_msg = json.dumps({"type": "state_update", "state": system_state})

        if current_msg != last_state_str:
            for client in _list_clients():
                try:
                    client.send(current_msg)
                except Exception:
                    # Gỡ client lỗi ra khỏi danh sách
                    _remove_client(client)
            last_state_str = current_msg

        time.sleep(0.5)

def generate_frames():
    """Stream video từ camera."""
    while main_loop_running:
        frame = None
        # Chỉ stream nếu không bảo trì
        if not error_manager.is_maintenance():
            with frame_lock:
                if latest_frame is not None:
                    frame = latest_frame.copy()

        if frame is None:
            # Nếu đang bảo trì hoặc không có frame, gửi frame đen
            frame = cv2.imread('black_frame.png') # Tạo 1 file ảnh đen 640x480
            if frame is None: # Nếu đọc file lỗi, tạo frame đen bằng numpy
                 import numpy as np
                 frame = np.zeros((480, 640, 3), dtype=np.uint8)
            time.sleep(0.1)
            # Không cần continue, vẫn gửi frame đen

        try:
            _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 70])
            yield (b'--frame\r\n'
                   b'Content-Type: image/jpeg\r\n\r\n' + buffer.tobytes() + b'\r\n')
        except Exception as encode_e:
             logging.error(f"[CAMERA] Lỗi encode frame: {encode_e}")
             # Gửi frame đen nếu encode lỗi
             import numpy as np
             frame = np.zeros((480, 640, 3), dtype=np.uint8)
             _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 10]) # Chất lượng thấp
             yield (b'--frame\r\n'
                    b'Content-Type: image/jpeg\r\n\r\n' + buffer.tobytes() + b'\r\n')

        time.sleep(1 / 20) # Giữ 20 FPS

# --- Các routes (endpoints) ---

@app.route('/')
@requires_auth
def index():
    """Trang chủ (dashboard)."""
    return render_template('index4v2.html')

@app.route('/video_feed')
@requires_auth
def video_feed():
    """Nguồn cấp video."""
    return Response(generate_frames(), mimetype='multipart/x-mixed-replace; boundary=frame')

@app.route('/config')
@requires_auth
def get_config():
    """API (GET) để lấy config hiện tại (timing + lanes)."""
    with state_lock:
        # Trả về cả timing và lanes config
        config_data = {
            "timing_config": system_state.get('timing_config', {}),
             # (SỬA) Lấy lanes_config từ state thay vì global
            "lanes_config": [{
                "name": ln.get('name'),
                "sensor_pin": ln.get('sensor_pin'),
                "push_pin": ln.get('push_pin'),
                "pull_pin": ln.get('pull_pin')
             } for ln in system_state.get('lanes', [])]
        }
    return jsonify(config_data)

@app.route('/update_config', methods=['POST'])
@requires_auth
def update_config():
    """API (POST) để cập nhật config (timing + lanes)."""
    global lanes_config, RELAY_PINS, SENSOR_PINS # Khai báo để có thể sửa đổi

    new_config_data = request.json
    if not new_config_data:
        return jsonify({"error": "Thiếu dữ liệu JSON"}), 400

    logging.info(f"[CONFIG] Nhận config mới từ API (POST): {new_config_data}")

    # Tách riêng timing và lanes config
    new_timing_config = new_config_data.get('timing_config', {})
    new_lanes_config = new_config_data.get('lanes_config') # Có thể là None nếu không gửi

    # --- Cập nhật state và lưu file ---
    config_to_save = {}
    restart_required = False # Cờ báo cần restart nếu đổi GPIO mode hoặc lanes

    with state_lock:
        # 1. Cập nhật Timing Config
        current_timing = system_state['timing_config']
        current_gpio_mode = current_timing.get('gpio_mode', 'BCM')
        current_timing.update(new_timing_config)
        new_gpio_mode = current_timing.get('gpio_mode', 'BCM')

        # Kiểm tra nếu GPIO mode thay đổi
        if new_gpio_mode != current_gpio_mode:
             logging.warning("[CONFIG] Chế độ GPIO đã thay đổi. Cần khởi động lại ứng dụng để áp dụng.")
             broadcast_log({"log_type": "warn", "message": "GPIO Mode đã đổi. Cần khởi động lại!"})
             restart_required = True
             # Vẫn lưu nhưng giữ nguyên gpio_mode cũ trong state đang chạy
             system_state['gpio_mode'] = current_gpio_mode
             current_timing['gpio_mode'] = current_gpio_mode
        else:
             system_state['gpio_mode'] = new_gpio_mode # Cập nhật state nếu không đổi

        config_to_save['timing_config'] = current_timing.copy()

        # 2. Cập nhật Lanes Config (nếu có gửi)
        if new_lanes_config is not None:
             logging.info("[CONFIG] Cập nhật cấu hình lanes...")
             lanes_config = new_lanes_config # Cập nhật global config
             num_lanes = len(lanes_config)
             # Tạo lại system_state["lanes"]
             new_system_lanes = []
             new_relay_pins = []
             new_sensor_pins = []
             for i, lane_cfg in enumerate(lanes_config):
                new_system_lanes.append({
                    "name": lane_cfg.get("name", f"Lane {i+1}"),
                    "status": "Sẵn sàng", "count": 0,
                    "sensor_pin": lane_cfg.get("sensor_pin"),
                    "push_pin": lane_cfg.get("push_pin"),
                    "pull_pin": lane_cfg.get("pull_pin"),
                    "sensor_reading": 1, "relay_grab": 0, "relay_push": 0
                })
                # Cập nhật danh sách pin mới
                if lane_cfg.get("sensor_pin"): new_sensor_pins.append(lane_cfg["sensor_pin"])
                if lane_cfg.get("push_pin"): new_relay_pins.append(lane_cfg["push_pin"])
                if lane_cfg.get("pull_pin"): new_relay_pins.append(lane_cfg["pull_pin"])

             # Cập nhật state lanes
             system_state['lanes'] = new_system_lanes
             # Cập nhật các biến phụ thuộc số lanes
             global last_sensor_state, last_sensor_trigger_time, auto_test_last_state, auto_test_last_trigger
             last_sensor_state = [1] * num_lanes
             last_sensor_trigger_time = [0.0] * num_lanes
             auto_test_last_state = [1] * num_lanes
             auto_test_last_trigger = [0.0] * num_lanes
             
             # Cập nhật danh sách pin global (sẽ dùng nếu restart)
             RELAY_PINS = new_relay_pins
             SENSOR_PINS = new_sensor_pins

             config_to_save['lanes_config'] = lanes_config # Thêm vào để lưu file
             restart_required = True # Thay đổi lanes cũng cần restart
             logging.warning("[CONFIG] Cấu hình lanes đã thay đổi. Cần khởi động lại ứng dụng.")
             broadcast_log({"log_type": "warn", "message": "Cấu hình Lanes đã đổi. Cần khởi động lại!"})
        else:
            # Nếu không gửi lanes_config mới, lấy cái cũ để lưu file
            current_lanes_cfg_for_save = []
            for lane_state in system_state['lanes']:
                 current_lanes_cfg_for_save.append({
                    "name": lane_state['name'],
                    "sensor_pin": lane_state['sensor_pin'],
                    "push_pin": lane_state['push_pin'],
                    "pull_pin": lane_state['pull_pin']
                 })
            config_to_save['lanes_config'] = current_lanes_cfg_for_save


    try:
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config_to_save, f, indent=4)
        msg = "Đã lưu config mới." + (" Cần khởi động lại!" if restart_required else "")
        broadcast_log({"log_type": "success" if not restart_required else "warn", "message": msg})
        return jsonify({"message": msg, "config": config_to_save, "restart_required": restart_required})
    except Exception as e:
        logging.error(f"[ERROR] Không thể lưu config (POST): {e}")
        broadcast_log({"log_type": "error", "message": f"Lỗi khi lưu config (POST): {e}"})
        return jsonify({"error": str(e)}), 500


@app.route('/api/state')
@requires_auth
def api_state():
    """API (GET) để lấy toàn bộ state."""
    with state_lock:
        return jsonify(system_state)

@app.route('/api/sort_log')
@requires_auth
def api_sort_log():
    """API (GET) để lấy lịch sử đếm (cho biểu đồ)."""
    with sort_log_lock:
        try:
            sort_data = {}
            if os.path.exists(SORT_LOG_FILE):
                 with open(SORT_LOG_FILE, 'r') as f:
                    file_content = f.read()
                    if file_content:
                        sort_data = json.loads(file_content)
            return jsonify(sort_data)
        except Exception as e:
            logging.error(f"[ERROR] Lỗi đọc sort_log.json cho API: {e}")
            return jsonify({"error": str(e)}), 500

# (MỚI) Thêm API để reset maintenance mode
@app.route('/api/reset_maintenance', methods=['POST'])
@requires_auth
def reset_maintenance():
    """API (POST) để reset chế độ bảo trì."""
    if error_manager.is_maintenance():
        error_manager.reset()
        broadcast_log({"log_type": "success", "message": "Chế độ bảo trì đã được reset từ UI."})
        return jsonify({"message": "Maintenance mode reset thành công."})
    else:
        return jsonify({"message": "Hệ thống không ở chế độ bảo trì."})


@app.route('/api/mock_gpio', methods=['POST'])
@requires_auth
def api_mock_gpio():
    """API (POST) để điều khiển sensor ở chế độ giả lập."""
    if not isinstance(GPIO, MockGPIO):
        return jsonify({"error": "Chức năng chỉ khả dụng ở chế độ mô phỏng."}), 400

    payload = request.get_json(silent=True) or {}
    lane_index = payload.get('lane_index')
    pin = payload.get('pin')
    requested_state = payload.get('state')

    if lane_index is not None and pin is None:
        try:
            lane_index = int(lane_index)
        except (TypeError, ValueError):
            return jsonify({"error": "lane_index không hợp lệ."}), 400
        with state_lock:
            if 0 <= lane_index < len(system_state['lanes']):
                pin = system_state['lanes'][lane_index].get('sensor_pin')
            else:
                return jsonify({"error": "lane_index vượt ngoài phạm vi."}), 400

    if pin is None:
        return jsonify({"error": "Thiếu thông tin chân sensor."}), 400

    try:
        pin = int(pin)
    except (TypeError, ValueError):
        return jsonify({"error": "Giá trị pin không hợp lệ."}), 400

    if requested_state is None:
        logical_state = GPIO.toggle_input_state(pin)
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
    message = f"[MOCK] Sensor pin {pin} -> {state_label}"
    if lane_name:
        message += f" ({lane_name})"
    broadcast_log({
        "log_type": "info",
        "message": message
    })
    return jsonify({"pin": pin, "state": logical_state, "lane": lane_name})


@sock.route('/ws')
@requires_auth
def ws_route(ws):
    """Kết nối WebSocket chính."""
    global AUTO_TEST_ENABLED

    # Kiểm tra xác thực một lần nữa (đề phòng)
    auth = request.authorization if AUTH_ENABLED else None
    if AUTH_ENABLED and (not auth or not check_auth(auth.username, auth.password)):
        logging.warning("[WS] Unauthorized connection attempt.")
        ws.close(code=1008, reason="Unauthorized") # Gửi mã lỗi 1008
        return

    client_label = auth.username if auth else f"guest-{id(ws):x}"
    _add_client(ws)
    logging.info(f"[WS] Client {client_label} connected. Total: {len(_list_clients())}")

    # 1. Gửi state ban đầu
    try:
        with state_lock:
            system_state["maintenance_mode"] = error_manager.is_maintenance()
            system_state["last_error"] = error_manager.last_error
            system_state["auth_enabled"] = AUTH_ENABLED
            initial_state_msg = json.dumps({"type": "state_update", "state": system_state})
        ws.send(initial_state_msg)
    except Exception as e:
        logging.warning(f"[WS] Lỗi gửi state ban đầu: {e}")
        _remove_client(ws)
        return

    # 2. Lắng nghe message
    try:
        while True:
            message = ws.receive()
            if message:
                try:
                    data = json.loads(message)
                    action = data.get('action')

                    if error_manager.is_maintenance() and action != "reset_maintenance": # Chỉ cho phép reset
                         broadcast_log({"log_type": "error", "message": "Hệ thống đang bảo trì, không thể thao tác."})
                         continue

                    # 1. Reset Count
                    if action == 'reset_count':
                        lane_idx = data.get('lane_index')
                        with state_lock:
                            if lane_idx == 'all':
                                for i in range(len(system_state['lanes'])):
                                    system_state['lanes'][i]['count'] = 0
                                broadcast_log({"log_type": "info", "message": f"{client_label} đã reset đếm toàn bộ."})
                            elif lane_idx is not None and 0 <= lane_idx < len(system_state['lanes']):
                                broadcast_log({"log_type": "info", "message": f"{client_label} đã reset đếm {system_state['lanes'][lane_idx]['name']}."})
                                system_state['lanes'][lane_idx]['count'] = 0

                    # 2. Cập nhật Config (Qua WebSocket - Dự phòng)
                    elif action == 'update_config':
                         # Nên dùng POST /update_config thay cho cái này
                         logging.warning("[CONFIG] Nhận lệnh update_config qua WebSocket (nên dùng POST).")
                         # (Logic giống hệt POST /update_config nhưng không trả về response)
                         # ... (bỏ qua để tránh lặp code) ...
                         pass

                    # 3. Test Relay Thủ Công
                    elif action == "test_relay":
                        lane_index = data.get("lane_index")
                        relay_action = data.get("relay_action")
                        if lane_index is not None and relay_action:
                            executor.submit(_run_test_relay, lane_index, relay_action)

                    # 4. Test Tuần Tự
                    elif action == "test_all_relays":
                        executor.submit(_run_test_all_relays)

                    # 5. Bật/Tắt Auto-Test
                    elif action == "toggle_auto_test":
                        AUTO_TEST_ENABLED = data.get("enabled", False)
                        logging.info(f"[TEST] Auto-Test (Sensor->Relay) set by {client_label} to: {AUTO_TEST_ENABLED}")
                        broadcast_log({"log_type": "warn", "message": f"Chế độ Auto-Test đã { 'BẬT' if AUTO_TEST_ENABLED else 'TẮT' } bởi {client_label}."})

                        if not AUTO_TEST_ENABLED:
                             reset_all_relays_to_default()

                    # (MỚI) 6. Reset Maintenance Mode
                    elif action == "reset_maintenance":
                         if error_manager.is_maintenance():
                             error_manager.reset()
                             broadcast_log({"log_type": "success", "message": f"Chế độ bảo trì đã được reset bởi {client_label}."})
                         else:
                             broadcast_log({"log_type": "info", "message": "Hệ thống không ở chế độ bảo trì."})


                except json.JSONDecodeError:
                    pass # Bỏ qua message lỗi
                except Exception as ws_loop_e: # Bắt lỗi chung trong vòng lặp
                     logging.error(f"[WS] Lỗi xử lý message: {ws_loop_e}")
                     # Không nên trigger maintenance ở đây, chỉ log lỗi

    except Exception as ws_conn_e: # Bắt lỗi kết nối WebSocket
         logging.warning(f"[WS] Kết nối WebSocket bị đóng hoặc lỗi: {ws_conn_e}")
    finally:
        _remove_client(ws)
        logging.info(f"[WS] Client {client_label} disconnected. Total: {len(_list_clients())}")

# =============================
#               MAIN
# =============================
if __name__ == "__main__":
    try:
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] (%(threadName)s) %(message)s',
            handlers=[
                logging.FileHandler(LOG_FILE, encoding='utf-8'), # Thêm encoding
                logging.StreamHandler()
            ]
        )

        load_local_config() # Load config (bao gồm cả lanes_config)

        # Lấy chế độ GPIO đã tải và set mode
        with state_lock:
            loaded_gpio_mode = system_state.get("gpio_mode", "BCM")

        # Chỉ set mode và setup nếu không phải Mock
        if isinstance(GPIO, RealGPIO):
             GPIO.setmode(GPIO.BCM if loaded_gpio_mode == "BCM" else GPIO.BOARD)
             GPIO.setwarnings(False)
             logging.info(f"[GPIO] Đã đặt chế độ chân cắm là: {loaded_gpio_mode}")

             # Setup các chân GPIO sau khi đã setmode
             logging.info(f"[GPIO] Setup SENSOR pins: {SENSOR_PINS}")
             for pin in SENSOR_PINS:
                 GPIO.setup(pin, GPIO.IN, pull_up_down=GPIO.PUD_UP)

             logging.info(f"[GPIO] Setup RELAY pins: {RELAY_PINS}")
             for pin in RELAY_PINS:
                 GPIO.setup(pin, GPIO.OUT)
        else:
             logging.info("[GPIO] Chạy ở chế độ Mock, bỏ qua setup vật lý.")


        reset_all_relays_to_default()

        # Khởi tạo các luồng (Thread)
        threading.Thread(target=camera_capture_thread, name="CameraThread", daemon=True).start()
        threading.Thread(target=qr_detection_loop, name="QRThread", daemon=True).start()
        threading.Thread(target=sensor_monitoring_thread, name="SensorThread", daemon=True).start()
        threading.Thread(target=broadcast_state, name="BroadcastThread", daemon=True).start()
        threading.Thread(target=auto_test_loop, name="AutoTestThread", daemon=True).start()
        threading.Thread(target=periodic_config_save, name="ConfigSaveThread", daemon=True).start()


        logging.info("=========================================")
        logging.info("  HỆ THỐNG PHÂN LOẠI SẴN SÀNG (PRODUCTION v1.3 - Full Config)")
        logging.info(f"  GPIO Mode: {'REAL' if isinstance(GPIO, RealGPIO) else 'MOCK'} (Config: {loaded_gpio_mode})")
        logging.info(f"  Log file: {LOG_FILE}")
        logging.info(f"  Sort log file: {SORT_LOG_FILE}")
        logging.info(f"  API State: http://<IP_CUA_PI>:5000/api/state")
        if AUTH_ENABLED:
            logging.info(f"  Truy cập: http://<IP_CUA_PI>:5000 (User: {USERNAME} / Pass: {PASSWORD})")
        else:
            logging.info("  Truy cập: http://<IP_CUA_PI>:5000 (không yêu cầu đăng nhập)")
        logging.info("=========================================")
        # Chạy Flask server
        app.run(host='0.0.0.0', port=5000)

    except KeyboardInterrupt:
        logging.info("\n🛑 Dừng hệ thống (Ctrl+C)...")
    except Exception as main_e: # Bắt lỗi khởi động
         logging.critical(f"[CRITICAL] Lỗi khởi động hệ thống: {main_e}", exc_info=True)
         # Cố gắng cleanup GPIO nếu có thể
         try:
             if isinstance(GPIO, RealGPIO): GPIO.cleanup()
         except Exception: pass
    finally:
        main_loop_running = False
        logging.info("Đang tắt ThreadPoolExecutor...")
        executor.shutdown(wait=False) # Không cần đợi lâu

        # Cố gắng cleanup GPIO một lần nữa
        logging.info("Đang cleanup GPIO...")
        try:
             GPIO.cleanup()
             logging.info("✅ GPIO cleaned up.")
        except Exception as clean_e:
             logging.warning(f"Lỗi khi cleanup GPIO: {clean_e}")

        logging.info("👋 Tạm biệt!")
