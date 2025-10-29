# -*- coding: utf-8 -*-
import cv2
import time
import json
import threading
import logging
import os
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
    def output(self, pin, value): 
        logging.info(f"[MOCK] output pin {pin}={value}")
        self.pin_states[pin] = value
    def input(self, pin): 
        # Giả lập sensor luôn ở trạng thái 1 (không có vật)
        val = self.pin_states.get(pin, self.HIGH)
        # logging.info(f"[MOCK] input pin {pin} -> {val}")
        return val
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

# (MỚI) Thông tin đăng nhập
USERNAME = "admin"
PASSWORD = "123" # Đổi mật khẩu này!

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
# Dùng GPIO.BCM (chuẩn) thay vì GPIO.BOARD
GPIO.setmode(GPIO.BCM)
GPIO.setwarnings(False)

# --- Relay (piston) ---
P1_PUSH = 17   # BOARD 11
P1_PULL = 18   # BOARD 12
P2_PUSH = 27   # BOARD 13
P2_PULL = 14   # BOARD 8
P3_PUSH = 22   # BOARD 15
P3_PULL = 4    # BOARD 7

# --- Sensor ---
SENSOR1 = 3    # BOARD 5
SENSOR2 = 23   # BOARD 16
SENSOR3 = 24   # BOARD 18

# Danh sách setup GPIO (chỉ dùng để setup ban đầu)
RELAY_PINS = [P1_PUSH, P1_PULL, P2_PUSH, P2_PULL, P3_PUSH, P3_PULL]
SENSOR_PINS = [SENSOR1, SENSOR2, SENSOR3]

# Setup chế độ GPIO
for pin in RELAY_PINS:
    GPIO.setup(pin, GPIO.OUT)
for pin in SENSOR_PINS:
    # Dùng PUD_UP (pull-up resistor) cho cảm biến
    GPIO.setup(pin, GPIO.IN, pull_up_down=GPIO.PUD_UP)

# =============================
#       HÀM ĐIỀU KHIỂN RELAY
# =============================
# Các hàm này giúp code dễ đọc hơn, không cần nhớ HIGH/LOW
def RELAY_ON(pin):
    """Bật relay (kích hoạt)."""
    GPIO.output(pin, GPIO.LOW if ACTIVE_LOW else GPIO.HIGH)

def RELAY_OFF(pin):
    """Tắt relay (ngừng kích hoạt)."""
    GPIO.output(pin, GPIO.HIGH if ACTIVE_LOW else GPIO.LOW)

# =============================
#        TRẠNG THÁI HỆ THỐNG
# =============================
# Đây là "Single Source of Truth" (Nguồn dữ liệu duy nhất) của hệ thống
system_state = {
    "lanes": [
        {
            "name": "Loại 1", "status": "Sẵn sàng", "count": 0,
            "sensor_pin": SENSOR1, "push_pin": P1_PUSH, "pull_pin": P1_PULL,
            "sensor_reading": 1, "relay_grab": 0, "relay_push": 0
        },
        {
            "name": "Loại 2", "status": "Sẵn sàng", "count": 0,
            "sensor_pin": SENSOR2, "push_pin": P2_PUSH, "pull_pin": P2_PULL,
            "sensor_reading": 1, "relay_grab": 0, "relay_push": 0
        },
        {
            "name": "Loại 3", "status": "Sẵn sàng", "count": 0,
            "sensor_pin": SENSOR3, "push_pin": P3_PUSH, "pull_pin": P3_PULL,
            "sensor_reading": 1, "relay_grab": 0, "relay_push": 0
        }
    ],
    "timing_config": {
        "cycle_delay": 0.3,   # Thời gian đẩy
        "settle_delay": 0.2,  # Thời gian chờ (giữa 2 hành động)
        "sensor_debounce": 0.1, # Thời gian chống nhiễu sensor
        "push_delay": 0.0     # Thời gian chờ từ lúc sensor thấy -> lúc đẩy
    },
    # (MỚI) Thêm 2 trạng thái này để UI biết
    "is_mock": isinstance(GPIO, MockGPIO), # Đang chạy giả lập?
    "maintenance_mode": False # Đang bảo trì?
}

# Các biến global cho threading
state_lock = threading.Lock() # Lock để bảo vệ system_state
main_loop_running = True # Cờ (flag) để báo các luồng dừng lại
latest_frame = None      # Khung hình camera mới nhất
frame_lock = threading.Lock() # Lock để bảo vệ latest_frame

# Biến cho việc chống nhiễu (debounce) sensor
last_sensor_state = [1, 1, 1]
last_sensor_trigger_time = [0.0, 0.0, 0.0]

# Biến toàn cục cho chức năng Test
AUTO_TEST_ENABLED = False
auto_test_last_state = [1, 1, 1]
auto_test_last_trigger = [0.0, 0.0, 0.0]


# =============================
#      HÀM KHỞI ĐỘNG & CONFIG
# =============================
def load_local_config():
    """Tải cấu hình từ config.json, nếu không có thì tạo mặc định."""
    default_config = {
        "cycle_delay": 0.3,
        "settle_delay": 0.2,
        "sensor_debounce": 0.1,
        "push_delay": 0.0
    }
    
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                file_content = f.read()
                if not file_content:
                    logging.warning("[CONFIG] File config rỗng, dùng mặc định.")
                    cfg_from_file = default_config
                else:
                    cfg_from_file = json.loads(file_content).get('timing_config', default_config)
                
                # Đảm bảo 4 key luôn tồn tại
                final_cfg = default_config.copy()
                final_cfg.update(cfg_from_file) 

                with state_lock:
                    system_state['timing_config'] = final_cfg
                logging.info(f"[CONFIG] Loaded config: {final_cfg}")
        except Exception as e:
            logging.error(f"[CONFIG] Lỗi đọc file config ({e}), dùng mặc định.")
            error_manager.trigger_maintenance(f"Lỗi file config.json: {e}")
            with state_lock:
                system_state['timing_config'] = default_config
    else:
        logging.warning("[CONFIG] Không có file config, dùng mặc định và tạo mới.")
        with state_lock:
            system_state['timing_config'] = default_config
        try:
             with open(CONFIG_FILE, 'w') as f:
                json.dump({"timing_config": default_config}, f, indent=4)
        except Exception as e:
             logging.error(f"[CONFIG] Không thể tạo file config mới: {e}")

def reset_all_relays_to_default():
    """Reset tất cả relay về trạng thái an toàn (THU BẬT, ĐẨY TẮT)."""
    logging.info("[GPIO] Reset tất cả relay về trạng thái mặc định (THU BẬT).")
    with state_lock:
        for lane in system_state["lanes"]:
            RELAY_ON(lane["pull_pin"])
            RELAY_OFF(lane["push_pin"])
            # Cập nhật trạng thái
            lane["relay_grab"] = 1
            lane["relay_push"] = 0
            lane["status"] = "Sẵn sàng"
    time.sleep(0.1) 
    logging.info("[GPIO] Reset hoàn tất.")

def periodic_config_save():
    """Tự động lưu config mỗi 60s."""
    while main_loop_running:
        time.sleep(60)
        
        if error_manager.is_maintenance():
            continue # Không lưu config nếu đang lỗi

        try:
            with state_lock:
                config_to_save = system_state['timing_config'].copy()
            
            with open(CONFIG_FILE, 'w') as f:
                json.dump({"timing_config": config_to_save}, f, indent=4)
            logging.info("[CONFIG] Đã tự động lưu config.")
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
        # (MỚI) Kích hoạt bảo trì
        error_manager.trigger_maintenance("Không thể mở camera.")
        return

    retries = 0
    max_retries = 5 # Thử lại 5 lần

    while main_loop_running:
        # (MỚI) Dừng nếu đang bảo trì
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
                # (MỚI) Kích hoạt bảo trì
                error_manager.trigger_maintenance("Camera lỗi vĩnh viễn (mất kết nối).")
                break # Dừng luồng camera

            camera.release()
            time.sleep(1)
            camera = cv2.VideoCapture(CAMERA_INDEX)
            continue
        
        retries = 0 # Reset bộ đếm nếu thành công

        with frame_lock:
            latest_frame = frame.copy()
        time.sleep(1 / 30) # 30 FPS
    camera.release()

# =============================
#       LƯU LOG ĐẾM SẢN PHẨM
# =============================
def log_sort_count(lane_index, lane_name):
    """Ghi lại số lượng đếm vào file JSON theo ngày (an toàn)."""
    # (SỬA LỖI 3) Dùng lock để bảo vệ file
    with sort_log_lock:
        try:
            today = time.strftime('%Y-%m-%d')
            
            sort_log = {}
            if os.path.exists(SORT_LOG_FILE):
                with open(SORT_LOG_FILE, 'r') as f:
                    file_content = f.read()
                    if file_content:
                        sort_log = json.loads(file_content)
                    else:
                        logging.warning("[SORT_LOG] File sort_log.json rỗng.")
            
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
    try:
        with state_lock:
            cfg = system_state['timing_config']
            delay = cfg['cycle_delay']
            settle_delay = cfg['settle_delay']
            
            lane = system_state["lanes"][lane_index]
            lane_name = lane["name"]
            push_pin = lane["push_pin"]
            pull_pin = lane["pull_pin"]
            
            lane["status"] = "Đang phân loại..."
        
        broadcast_log({"log_type": "info", "message": f"Bắt đầu chu trình đẩy {lane_name}"})

        # 1. Tắt relay THU
        RELAY_OFF(pull_pin)
        with state_lock: system_state["lanes"][lane_index]["relay_grab"] = 0
        time.sleep(settle_delay)
        if not main_loop_running: return

        # 2. Bật relay ĐẨY
        RELAY_ON(push_pin)
        with state_lock: system_state["lanes"][lane_index]["relay_push"] = 1
        time.sleep(delay)
        if not main_loop_running: return
        
        # 3. Tắt relay ĐẨY
        RELAY_OFF(push_pin)
        with state_lock: system_state["lanes"][lane_index]["relay_push"] = 0
        time.sleep(settle_delay)
        if not main_loop_running: return
        
        # 4. Bật relay THU (về vị trí cũ)
        RELAY_ON(pull_pin)
        with state_lock: system_state["lanes"][lane_index]["relay_grab"] = 1
        
    finally:
        # Dù lỗi hay không, luôn reset trạng thái về Sẵn sàng
        with state_lock:
            lane = system_state["lanes"][lane_index] 
            lane["status"] = "Sẵn sàng"
            lane["count"] += 1
            broadcast_log({"log_type": "sort", "name": lane_name, "count": lane['count']})
        
        # (SỬA LỖI 4) Di chuyển hàm log vào trong finally
        if lane_name: 
            log_sort_count(lane_index, lane_name)
            
        broadcast_log({"log_type": "info", "message": f"Hoàn tất chu trình cho {lane_name}"})

def handle_sorting_with_delay(lane_index):
    """Luồng trung gian, chờ push_delay rồi mới gọi sorting_process."""
    push_delay = 0.0
    lane_name_for_log = f"Lane {lane_index + 1}" 
    
    try:
        with state_lock:
            push_delay = system_state['timing_config'].get('push_delay', 0.0)
            lane_name_for_log = system_state['lanes'][lane_index]['name']
        
        if push_delay > 0:
            broadcast_log({"log_type": "info", "message": f"Đã thấy vật {lane_name_for_log}, chờ {push_delay}s..."})
            time.sleep(push_delay)
        
        if not main_loop_running:
             broadcast_log({"log_type": "warn", "message": f"Hủy chu trình {lane_name_for_log} do hệ thống đang tắt."})
             return

        with state_lock:
            current_status = system_state["lanes"][lane_index]["status"]
        
        # Chỉ chạy nếu trạng thái vẫn là "Đang chờ đẩy"
        if current_status == "Đang chờ đẩy":
             sorting_process(lane_index)
        else:
             broadcast_log({"log_type": "warn", "message": f"Hủy chu trình {lane_name_for_log} do trạng thái thay đổi."})
        
    except Exception as e:
        logging.error(f"[ERROR] Lỗi trong luồng handle_sorting_with_delay (lane {lane_name_for_log}): {e}")
        # (SỬA LỖI 2) Kích hoạt bảo trì nếu luồng này lỗi
        error_manager.trigger_maintenance(f"Lỗi luồng sorting_delay (Lane {lane_name_for_log}): {e}")
        with state_lock:
             if system_state["lanes"][lane_index]["status"] == "Đang chờ đẩy":
                  system_state["lanes"][lane_index]["status"] = "Sẵn sàng"
                  broadcast_log({"log_type": "error", "message": f"Lỗi delay, reset {lane_name_for_log}"})


# =============================
#       QUÉT MÃ QR TỰ ĐỘNG
# =============================
def qr_detection_loop():
    detector = cv2.QRCodeDetector()
    last_qr, last_time = "", 0.0
    LANE_MAP = {"LOAI1": 0, "LOAI2": 1, "LOAI3": 2}

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
            data_upper = data.strip().upper()
            
            if data_upper in LANE_MAP:
                idx = LANE_MAP[data_upper]
                with state_lock:
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
            now = time.time()

            for i in range(3):
                with state_lock:
                    lane = system_state["lanes"][i]
                    sensor_pin = lane["sensor_pin"]
                    current_status = lane["status"]

                sensor_now = GPIO.input(sensor_pin)
                
                with state_lock:
                    system_state["lanes"][i]["sensor_reading"] = sensor_now

                # Phát hiện sườn xuống (1 -> 0)
                if sensor_now == 0 and last_sensor_state[i] == 1:
                    # Chống nhiễu (debounce)
                    if (now - last_sensor_trigger_time[i]) > debounce_time:
                        last_sensor_trigger_time[i] = now
                        
                        # Chỉ kích hoạt nếu trạng thái là "Đang chờ vật"
                        if current_status == "Đang chờ vật...":
                            with state_lock:
                                system_state["lanes"][i]["status"] = "Đang chờ đẩy"
                            
                            threading.Thread(target=handle_sorting_with_delay, args=(i,), daemon=True).start()
                        else:
                            broadcast_log({"log_type": "warn", "message": f"Sensor {lane['name']} bị kích hoạt ngoài dự kiến."})
                
                last_sensor_state[i] = sensor_now
            
            adaptive_sleep = 0.05 if all(s == 1 for s in last_sensor_state) else 0.01
            time.sleep(adaptive_sleep)
            
    except Exception as e:
        logging.error(f"[ERROR] Luồng sensor_monitoring_thread bị crash: {e}")
        # (SỬA LỖI 2) Kích hoạt bảo trì nếu luồng này lỗi
        error_manager.trigger_maintenance(f"Lỗi luồng Sensor: {e}")


# =============================
#        FLASK + WEBSOCKET
# =============================
app = Flask(__name__)
sock = Sock(app)
connected_clients = set()

def broadcast_log(log_data):
    """Gửi 1 tin nhắn log cụ thể cho client."""
    log_data['timestamp'] = time.strftime('%H:%M:%S')
    msg = json.dumps({"type": "log", **log_data})
    for client in list(connected_clients):
        try:
            client.send(msg)
        except Exception:
            connected_clients.remove(client)

# =============================
#      CÁC HÀM XỬ LÝ TEST (🧪)
# =============================

def _run_test_relay(lane_index, relay_action):
    """Hàm worker (chạy trong ThreadPool) để test 1 relay."""
    pin_to_test = None
    state_key_to_update = None
    
    try:
        with state_lock:
            lane = system_state["lanes"][lane_index]
            if relay_action == "grab":
                pin_to_test = lane["pull_pin"]
                state_key_to_update = "relay_grab"
            else: # "push"
                pin_to_test = lane["push_pin"]
                state_key_to_update = "relay_push"

        RELAY_ON(pin_to_test)
        with state_lock:
            system_state["lanes"][lane_index][state_key_to_update] = 1
        
        time.sleep(0.5)
        if not main_loop_running: return

        RELAY_OFF(pin_to_test)
        with state_lock:
            system_state["lanes"][lane_index][state_key_to_update] = 0
            
        broadcast_log({"log_type": "info", "message": f"Test {relay_action} lane {lane_index+1} OK"})

    except Exception as e:
        logging.error(f"[ERROR] Lỗi test relay {lane_index+1}: {e}")
        broadcast_log({"log_type": "error", "message": f"Lỗi test relay {lane_index+1}: {e}")

def _run_test_all_relays():
    """Hàm worker (chạy trong ThreadPool) để test tuần tự 6 relay."""
    logging.info("[TEST] Bắt đầu test tuần tự 6 relay...")
    for i in range(3):
        if not main_loop_running: break
        broadcast_log({"log_type": "info", "message": f"Test THU (grab) Lane {i+1}..."})
        _run_test_relay(i, "grab") # Gọi hàm worker
        time.sleep(0.5)
        
        if not main_loop_running: break
        broadcast_log({"log_type": "info", "message": f"Test ĐẨY (push) Lane {i+1}..."})
        _run_test_relay(i, "push") # Gọi hàm worker
        time.sleep(0.5)
    logging.info("[TEST] Hoàn tất test tuần tự.")
    broadcast_log({"log_type": "info", "message": "Hoàn tất test tuần tự 6 relay."})

def _auto_test_cycle_worker(lane_index):
    """Hàm worker (chạy trong ThreadPool) cho chu trình Đẩy -> Thu (Auto-Test)."""
    try:
        broadcast_log({"log_type": "warn", "message": f"AUTO-TEST: Đẩy Lane {lane_index+1}"})
        _run_test_relay(lane_index, "push")
        time.sleep(0.3)
        if not main_loop_running: return
        
        broadcast_log({"log_type": "warn", "message": f"AUTO-TEST: Thu Lane {lane_index+1}"})
        _run_test_relay(lane_index, "grab")
    except Exception as e:
         logging.error(f"[ERROR] Lỗi auto_test_cycle_worker (lane {lane_index+1}): {e}")

def auto_test_loop():
    """Luồng riêng cho auto-test: Sensor sáng -> Đẩy rồi Thu."""
    global AUTO_TEST_ENABLED, auto_test_last_state, auto_test_last_trigger
    
    logging.info("[TEST] Luồng Auto-Test (sensor->relay) đã khởi động.")
    
    try:
        while main_loop_running:
            if error_manager.is_maintenance():
                # Tự động tắt nếu hệ thống vào chế độ bảo trì
                if AUTO_TEST_ENABLED:
                    AUTO_TEST_ENABLED = False
                    logging.warning("[TEST] Tự động tắt Auto-Test do có lỗi hệ thống.")
                    broadcast_log({"log_type": "error", "message": "Tự động tắt Auto-Test do hệ thống đang bảo trì."})
                time.sleep(0.2)
                continue

            if AUTO_TEST_ENABLED:
                now = time.time()
                for i in range(3):
                    with state_lock:
                        sensor_pin = system_state["lanes"][i]["sensor_pin"]
                    
                    sensor_now = GPIO.input(sensor_pin)
                    
                    with state_lock:
                        system_state["lanes"][i]["sensor_reading"] = sensor_now

                    if sensor_now == 0 and auto_test_last_state[i] == 1:
                        if (now - auto_test_last_trigger[i]) > 1.0: 
                            auto_test_last_trigger[i] = now
                            
                            broadcast_log({"log_type": "warn", "message": f"AUTO-TEST: Sensor {i+1} phát hiện!"})
                            # (SỬA LỖI 1) Đưa vào ThreadPoolExecutor
                            executor.submit(_auto_test_cycle_worker, i)
                    
                    auto_test_last_state[i] = sensor_now
                time.sleep(0.02)
            else:
                auto_test_last_state = [1, 1, 1]
                auto_test_last_trigger = [0.0, 0.0, 0.0]
                time.sleep(0.2)
    except Exception as e:
         logging.error(f"[ERROR] Luồng auto_test_loop bị crash: {e}")
         # (SỬA LỖI 2) Kích hoạt bảo trì nếu luồng này lỗi
         error_manager.trigger_maintenance(f"Lỗi luồng Auto-Test: {e}")


# =============================
#     CÁC HÀM CỦA FLASK (TIẾP)
# =============================

# (MỚI) Hàm kiểm tra Basic Auth
def check_auth(username, password):
    """Kiểm tra username và password."""
    return username == USERNAME and password == PASSWORD

def authenticate():
    """Gửi phản hồi 401 (Yêu cầu đăng nhập)."""
    return Response(
    'Yêu cầu đăng nhập.', 401,
    {'WWW-Authenticate': 'Basic realm="Login Required"'})

def requires_auth(f):
    """Decorator để yêu cầu đăng nhập cho một route."""
    def decorated(*args, **kwargs):
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
            # (MỚI) Cập nhật trạng thái bảo trì
            system_state["maintenance_mode"] = error_manager.is_maintenance()
            current_msg = json.dumps({"type": "state_update", "state": system_state})
        
        if current_msg != last_state_str:
            for client in list(connected_clients):
                try:
                    client.send(current_msg)
                except Exception:
                    connected_clients.remove(client) 
            last_state_str = current_msg
            
        time.sleep(0.5)

def generate_frames():
    """Stream video từ camera."""
    while main_loop_running:
        frame = None
        with frame_lock:
            if latest_frame is not None:
                frame = latest_frame.copy()
        
        if frame is None:
            time.sleep(0.1)
            continue
            
        _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 70])
        yield (b'--frame\r\n'
               b'Content-Type: image/jpeg\r\n\r\n' + buffer.tobytes() + b'\r\n')
        time.sleep(1 / 20)

# --- Các routes (endpoints) ---

@app.route('/')
@requires_auth # (MỚI) Yêu cầu đăng nhập
def index():
    """Trang chủ (dashboard)."""
    return render_template('index.html')

@app.route('/video_feed')
@requires_auth # (MỚI) Yêu cầu đăng nhập
def video_feed():
    """Nguồn cấp video."""
    return Response(generate_frames(), mimetype='multipart/x-mixed-replace; boundary=frame')

@app.route('/config')
@requires_auth # (MỚI) Yêu cầu đăng nhập
def get_config():
    """API (GET) để lấy config hiện tại."""
    with state_lock:
        cfg = system_state.get('timing_config', {})
    return jsonify(cfg)

@app.route('/api/state')
@requires_auth # (MỚI) Yêu cầu đăng nhập
def api_state():
    """API (GET) để lấy toàn bộ state."""
    with state_lock:
        return jsonify(system_state)

@app.route('/api/sort_log')
@requires_auth # (MỚI) Yêu cầu đăng nhập
def api_sort_log():
    """API (GET) để lấy lịch sử đếm (cho biểu đồ)."""
    # (SỬA LỖI 3) Dùng lock để đọc file an toàn
    with sort_log_lock:
        try:
            if os.path.exists(SORT_LOG_FILE):
                with open(SORT_LOG_FILE, 'r') as f:
                    file_content = f.read()
                    if file_content:
                        return jsonify(json.loads(file_content))
            return jsonify({}) # Trả về rỗng nếu không có file
        except Exception as e:
            logging.error(f"[ERROR] Lỗi đọc sort_log.json cho API: {e}")
            return jsonify({"error": str(e)}), 500

@sock.route('/ws')
@requires_auth # (MỚI) Yêu cầu đăng nhập cho WebSocket
def ws_route(ws):
    """Kết nối WebSocket chính."""
    global AUTO_TEST_ENABLED
    
    connected_clients.add(ws)
    logging.info(f"[WS] Client {request.authorization.username} connected. Total: {len(connected_clients)}")

    # 1. Gửi state ban đầu
    try:
        with state_lock:
            system_state["maintenance_mode"] = error_manager.is_maintenance()
            initial_state_msg = json.dumps({"type": "state_update", "state": system_state})
        ws.send(initial_state_msg)
    except Exception as e:
        logging.warning(f"[WS] Lỗi gửi state ban đầu: {e}")
        connected_clients.remove(ws)
        return

    # 2. Lắng nghe message
    try:
        while True:
            message = ws.receive()
            if message:
                try:
                    data = json.loads(message)
                    action = data.get('action')

                    # (MỚI) Không cho phép hành động nếu đang bảo trì
                    if error_manager.is_maintenance():
                         broadcast_log({"log_type": "error", "message": "Hệ thống đang bảo trì, không thể thao tác."})
                         continue # Bỏ qua tất cả các lệnh

                    # 1. Xử lý Reset Count
                    if action == 'reset_count':
                        lane_idx = data.get('lane_index') # Đổi sang lane_index
                        with state_lock:
                            if lane_idx == 'all':
                                for lane in system_state['lanes']:
                                    lane['count'] = 0
                                broadcast_log({"log_type": "info", "message": "Reset đếm toàn bộ."})
                            elif lane_idx is not None and 0 <= lane_idx <= 2:
                                broadcast_log({"log_type": "info", "message": f"Reset đếm {system_state['lanes'][lane_idx]['name']}."})
                                system_state['lanes'][lane_idx]['count'] = 0
                    
                    # 2. Xử lý Cập nhật Config
                    elif action == 'update_config':
                        new_config = data.get('config', {})
                        if new_config:
                            logging.info(f"[CONFIG] Nhận config mới từ UI: {new_config}")
                            config_to_save = {}
                            with state_lock:
                                system_state['timing_config'].update(new_config)
                                config_to_save = system_state['timing_config'].copy()
                            # Lưu file
                            try:
                                with open(CONFIG_FILE, 'w') as f:
                                    json.dump({"timing_config": config_to_save}, f, indent=4)
                                broadcast_log({"log_type": "info", "message": "Đã lưu config mới từ UI."})
                            except Exception as e:
                                logging.error(f"[ERROR] Không thể lưu config: {e}")
                                broadcast_log({"log_type": "error", "message": f"Lỗi khi lưu config: {e}"})

                    # 3. Xử lý Test Relay Thủ Công
                    elif action == "test_relay":
                        lane_index = data.get("lane_index")
                        relay_action = data.get("relay_action")
                        if lane_index is not None and relay_action:
                            # (SỬA LỖI 1) Đưa vào ThreadPoolExecutor
                            executor.submit(_run_test_relay, lane_index, relay_action)

                    # 4. Xử lý Test Tuần Tự
                    elif action == "test_all_relays":
                        # (SỬA LỖI 1) Đưa vào ThreadPoolExecutor
                        executor.submit(_run_test_all_relays)
                    
                    # 5. Xử lý Bật/Tắt Auto-Test
                    elif action == "toggle_auto_test":
                        AUTO_TEST_ENABLED = data.get("enabled", False)
                        logging.info(f"[TEST] Auto-Test (Sensor->Relay) set to: {AUTO_TEST_ENABLED}")
                        broadcast_log({"log_type": "warn", "message": f"Chế độ Auto-Test (Sensor->Relay) đã { 'BẬT' if AUTO_TEST_ENABLED else 'TẮT' }."})
                        
                        if not AUTO_TEST_ENABLED:
                             reset_all_relays_to_default()

                except json.JSONDecodeError:
                    pass
    finally:
        connected_clients.remove(ws)
        logging.info(f"[WS] Client {request.authorization.username} disconnected. Total: {len(connected_clients)}")

# =============================
#               MAIN
# =============================
if __name__ == "__main__":
    try:
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            # (MỚI) Thêm (threadName) vào format
            format='%(asctime)s [%(levelname)s] (%(threadName)s) %(message)s',
            handlers=[
                logging.FileHandler(LOG_FILE), 
                logging.StreamHandler()      
            ]
        )
        
        load_local_config()
        reset_all_relays_to_default()
        
        # Khởi tạo các luồng (Thread)
        threading.Thread(target=camera_capture_thread, name="CameraThread", daemon=True).start()
        threading.Thread(target=qr_detection_loop, name="QRThread", daemon=True).start()
        threading.Thread(target=sensor_monitoring_thread, name="SensorThread", daemon=True).start()
        threading.Thread(target=broadcast_state, name="BroadcastThread", daemon=True).start()
        threading.Thread(target=auto_test_loop, name="AutoTestThread", daemon=True).start()
        threading.Thread(target=periodic_config_save, name="ConfigSaveThread", daemon=True).start()


        logging.info("=========================================")
        logging.info("  HỆ THỐNG PHÂN LOẠI SẴN SÀNG (PRODUCTION v1.1)")
        logging.info(f"  GPIO Mode: {'REAL' if isinstance(GPIO, RealGPIO) else 'MOCK'}")
        logging.info(f"  Log file: {LOG_FILE}")
        logging.info(f"  Sort log file: {SORT_LOG_FILE}")
        logging.info(f"  API State: http://<IP_CUA_PI>:5000/api/state")
        logging.info(f"  Truy cập: http://<IP_CUA_PI>:5000 (User: {USERNAME} / Pass: {PASSWORD})")
        logging.info("=========================================")
        # Chạy Flask server
        app.run(host='0.0.0.0', port=5000)
        
    except KeyboardInterrupt:
        logging.info("\n🛑 Dừng hệ thống (Ctrl+C)...")
    finally:
        # Báo cho các luồng con dừng lại
        main_loop_running = False
        # (SỬA LỖI 1) Tắt ThreadPool an toàn
        logging.info("Đang tắt ThreadPoolExecutor...")
        executor.shutdown(wait=True)
        
        time.sleep(0.5)
        GPIO.cleanup()
        logging.info("✅ GPIO cleaned up. Tạm biệt!")

