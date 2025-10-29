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
from datetime import datetime

try:
    from waitress import serve
except ImportError:
    serve = None  # fallback dev server

# =====================================================
#                    LỚP TRỪU TƯỢNG GPIO
# =====================================================
try:
    import RPi.GPIO as RPiGPIO
except (ImportError, RuntimeError):
    RPiGPIO = None


class GPIOProvider:
    """Lớp trừu tượng GPIO."""
    def setup(self, pin, mode, pull_up_down=None): raise NotImplementedError
    def output(self, pin, value): raise NotImplementedError
    def input(self, pin): raise NotImplementedError
    def cleanup(self): raise NotImplementedError
    def setmode(self, mode): raise NotImplementedError
    def setwarnings(self, value): raise NotImplementedError


class RealGPIO(GPIOProvider):
    """GPIO thật."""
    def __init__(self):
        if RPiGPIO is None:
            raise ImportError("Không thể tải RPi.GPIO.")
        self.gpio = RPiGPIO
        for attr in ['BOARD', 'BCM', 'OUT', 'IN', 'HIGH', 'LOW', 'PUD_UP']:
            setattr(self, attr, getattr(self.gpio, attr))

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


# =====================================================
#                    MOCK GPIO (GIẢ LẬP)
# =====================================================
mock_pin_override = {}
mock_pin_override_lock = threading.Lock()


class MockGPIO(GPIOProvider):
    def __init__(self):
        for attr, val in [('BOARD', "mock_BOARD"), ('BCM', "mock_BCM"),
                          ('OUT', "mock_OUT"), ('IN', "mock_IN"),
                          ('HIGH', 1), ('LOW', 0), ('PUD_UP', "mock_PUD_UP")]:
            setattr(self, attr, val)
        self.pin_states = {}
        logging.warning("=" * 50 + "\nĐANG CHẠY Ở CHẾ ĐỘ GIẢ LẬP (MOCK GPIO)\n" + "=" * 50)

    def setmode(self, mode): logging.info(f"[MOCK] setmode={mode}")
    def setwarnings(self, value): logging.info(f"[MOCK] setwarnings={value}")

    def setup(self, pin, mode, pull_up_down=None):
        pin_name = PIN_TO_NAME_MAP.get(pin, pin)
        logging.info(f"[MOCK] setup {pin_name}({pin}) mode={mode}")
        self.pin_states[pin] = self.LOW if mode == self.OUT else self.HIGH

    def output(self, pin, value):
        pin_name = PIN_TO_NAME_MAP.get(pin, pin)
        val_str = "HIGH" if value == self.HIGH else "LOW"
        logging.info(f"[MOCK] output {pin_name}({pin}) = {val_str}")
        self.pin_states[pin] = value

    def input(self, pin):
        with mock_pin_override_lock:
            if pin in mock_pin_override:
                target, expiry = mock_pin_override[pin]
                if time.time() < expiry:
                    return target
                else:
                    del mock_pin_override[pin]
        return self.pin_states.get(pin, self.HIGH)

    def cleanup(self): logging.info("[MOCK] cleanup GPIO")


def get_gpio_provider():
    return RealGPIO() if RPiGPIO else MockGPIO()


# =====================================================
#                   QUẢN LÝ LỖI HỆ THỐNG
# =====================================================
class ErrorManager:
    def __init__(self):
        self.lock = threading.Lock()
        self.maintenance = False
        self.error = None

    def trigger_maintenance(self, msg):
        with self.lock:
            if self.maintenance:
                return
            self.maintenance = True
            self.error = msg
            logging.critical(f"[MAINTENANCE] {msg}")
            broadcast({"type": "maintenance_update", "enabled": True, "reason": msg})

    def reset(self):
        with self.lock:
            self.maintenance = False
            self.error = None
            broadcast({"type": "maintenance_update", "enabled": False})

    def is_maintenance(self):
        return self.maintenance


# =====================================================
#                   CẤU HÌNH KHỞI TẠO
# =====================================================
CAMERA_INDEX = 0
CONFIG_FILE = 'config.json'
SORT_LOG_FILE = 'sort_log.json'
LOG_FILE = 'system.log'
ACTIVE_LOW = True
USERNAME = "admin"
PASSWORD = "123"

_GPIO_MAP = {
    "P1+": 17, "P1-": 18,
    "P2+": 27, "P2-": 14,
    "P3+": 22, "P3-": 4,
    "S1": 3, "S2": 23, "S3": 24
}
PIN_TO_NAME_MAP = {v: k for k, v in _GPIO_MAP.items()}

DEFAULT_LANES_CFG = [
    {"name": f"Loại {i+1}",
     "sensor_pin": _GPIO_MAP[f"S{i+1}"],
     "push_pin": _GPIO_MAP[f"P{i+1}+"],
     "pull_pin": _GPIO_MAP[f"P{i+1}-"]}
    for i in range(3)
]
DEFAULT_TIMING_CFG = {
    "cycle_delay": 0.3,
    "settle_delay": 0.2,
    "sensor_debounce": 0.1,
    "push_delay": 0.0,
    "gpio_mode": "BCM"
}

# =====================================================
#               KHỞI TẠO BIẾN TOÀN CỤC
# =====================================================
GPIO = get_gpio_provider()
error_manager = ErrorManager()
executor = ThreadPoolExecutor(max_workers=3)
system_state = {
    "lanes": [],
    "timing_config": DEFAULT_TIMING_CFG.copy(),
    "gpio_mode": "BCM",
    "maintenance_mode": False,
    "last_error": None,
}
state_lock = threading.Lock()
main_running = True
latest_frame = None
frame_lock = threading.Lock()

# =====================================================
#                   FLASK APP
# =====================================================
app = Flask(__name__, template_folder='templates')
from flask_sock import Sock
sock = Sock(app)
clients = set()


def broadcast(data):
    msg = json.dumps(data)
    to_remove = set()
    for c in list(clients):
        try:
            c.send(msg)
        except Exception:
            to_remove.add(c)
    clients.difference_update(to_remove)


def broadcast_log(data):
    data["timestamp"] = datetime.now().strftime("%H:%M:%S")
    broadcast({"type": "log", **data})


# =====================================================
#                   AUTH MIDDLEWARE
# =====================================================
def check_auth(u, p):
    return u == USERNAME and p == PASSWORD


def auth_fail():
    return Response('Login Required.', 401, {'WWW-Authenticate': 'Basic realm="Login Required"'})


def require_auth(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return auth_fail()
        return f(*args, **kwargs)
    return decorated


# =====================================================
#                       ROUTES
# =====================================================
@app.route('/')
def route_index():
    return render_template('index6.html')


@app.route('/video_feed')
def route_video_feed():
    return Response(stream_frames(), mimetype='multipart/x-mixed-replace; boundary=frame')
# =====================================================
#                CẤU HÌNH, KHỞI ĐỘNG & GPIO
# =====================================================
def load_config():
    """Đọc hoặc tạo config.json"""
    global system_state
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                cfg = json.load(f)
            system_state['timing_config'] = cfg.get('timing_config', DEFAULT_TIMING_CFG.copy())
            system_state['lanes'] = cfg.get('lanes_config', DEFAULT_LANES_CFG.copy())
        else:
            logging.warning("[CFG] File không tồn tại, tạo mới.")
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump({
                    "timing_config": DEFAULT_TIMING_CFG,
                    "lanes_config": DEFAULT_LANES_CFG
                }, f, indent=4, ensure_ascii=False)
            system_state['timing_config'] = DEFAULT_TIMING_CFG.copy()
            system_state['lanes'] = DEFAULT_LANES_CFG.copy()
    except Exception as e:
        logging.error(f"[CFG] Lỗi đọc config: {e}", exc_info=True)
        system_state['timing_config'] = DEFAULT_TIMING_CFG.copy()
        system_state['lanes'] = DEFAULT_LANES_CFG.copy()


def RELAY_ON(pin):
    try:
        GPIO.output(pin, GPIO.LOW if ACTIVE_LOW else GPIO.HIGH)
    except Exception as e:
        logging.error(f"[GPIO] Lỗi bật pin {pin}: {e}")


def RELAY_OFF(pin):
    try:
        GPIO.output(pin, GPIO.HIGH if ACTIVE_LOW else GPIO.LOW)
    except Exception as e:
        logging.error(f"[GPIO] Lỗi tắt pin {pin}: {e}")


def reset_relays():
    """Đưa tất cả relay về trạng thái an toàn"""
    with state_lock:
        for lane in system_state['lanes']:
            p = lane.get('push_pin')
            pl = lane.get('pull_pin')
            if p: RELAY_OFF(p)
            if pl: RELAY_ON(pl)
    logging.info("[GPIO] Đã reset relay về an toàn")


# =====================================================
#                    LUỒNG CAMERA
# =====================================================
def stream_frames():
    """Trả về luồng MJPEG"""
    black = cv2.imread("black.png") if os.path.exists("black.png") else None
    if black is None:
        import numpy as np
        black = np.zeros((480, 640, 3), dtype=np.uint8)

    while main_running:
        frame = None
        with frame_lock:
            if latest_frame is not None:
                frame = latest_frame.copy()
        curr = frame if frame is not None else black

        try:
            ok, buf = cv2.imencode('.jpg', curr, [cv2.IMWRITE_JPEG_QUALITY, 70])
            if ok:
                yield (b'--frame\r\nContent-Type: image/jpeg\r\n\r\n' + buf.tobytes() + b'\r\n')
            else:
                logging.warning("[CAM] Encode thất bại.")
        except Exception as e:
            logging.error(f"[CAM] Lỗi encode: {e}", exc_info=True)

        time.sleep(1 / 20)


# =====================================================
#                    WEBSOCKET
# =====================================================
@sock.route('/ws')
def route_ws(ws):
    """Kênh WebSocket"""
    clients.add(ws)
    logging.info(f"[WS] Client connect ({len(clients)})")
    try:
        # Gửi state ban đầu
        with state_lock:
            initial = json.dumps({"type": "state_update", "state": system_state})
        ws.send(initial)

        while True:
            msg = ws.receive()
            if msg is None:
                break
            data = json.loads(msg)
            action = data.get('action')

            if action == 'reset':
                reset_relays()
                broadcast_log({"log_type": "warn", "message": "Đã reset relay"})
            elif action == 'toggle_mock':
                broadcast_log({"log_type": "info", "message": "Mock command nhận!"})
            else:
                broadcast_log({"log_type": "warn", "message": f"Không rõ lệnh: {action}"})
    except Exception as e:
        logging.warning(f"[WS] Lỗi WS: {e}")
    finally:
        clients.discard(ws)
        logging.info(f"[WS] Client rời ({len(clients)})")


# =====================================================
#                    HÀM CHÍNH MAIN
# =====================================================
if __name__ == "__main__":
    try:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] (%(threadName)s) %(message)s",
            handlers=[
                logging.FileHandler(LOG_FILE, encoding="utf-8"),
                logging.StreamHandler()
            ]
        )

        logging.info("=== KHỞI ĐỘNG HỆ THỐNG PHÂN LOẠI (v2.6 FIXED) ===")
        load_config()

        mode = system_state["timing_config"].get("gpio_mode", "BCM")

        # GPIO setup
        if isinstance(GPIO, RealGPIO):
            GPIO.setmode(GPIO.BCM if mode == "BCM" else GPIO.BOARD)
            GPIO.setwarnings(False)
            logging.info(f"[GPIO] Mode: {mode}")

            for lane in system_state["lanes"]:
                if lane.get("sensor_pin"):
                    GPIO.setup(lane["sensor_pin"], GPIO.IN, pull_up_down=GPIO.PUD_UP)
                if lane.get("push_pin"):
                    GPIO.setup(lane["push_pin"], GPIO.OUT)
                if lane.get("pull_pin"):
                    GPIO.setup(lane["pull_pin"], GPIO.OUT)
            reset_relays()
        else:
            logging.info("[GPIO] MOCK MODE, bỏ qua thiết lập thực.")

        # Thông tin khởi động
        logging.info(f"Web: http://<IP>:5000 (User: {USERNAME})")
        logging.info(f"API:  /api/state, /video_feed")
        logging.info(f"GPIO: {'REAL' if isinstance(GPIO, RealGPIO) else 'MOCK'} Mode={mode}")
        logging.info("==============================================")

        # Chạy server
        host = "0.0.0.0"
        port = 5000
        if serve:
            logging.info(f"Chạy Waitress tại {host}:{port}")
            serve(app, host=host, port=port, threads=8)
        else:
            logging.warning("Waitress chưa cài — dùng Flask dev server.")
            app.run(host=host, port=port)
    except KeyboardInterrupt:
        logging.info("🛑 Người dùng dừng hệ thống.")
    except Exception as e:
        logging.critical(f"[CRITICAL] Lỗi khởi động: {e}", exc_info=True)
    finally:
        main_running = False
        logging.info("Đang cleanup GPIO...")
        try:
            GPIO.cleanup()
            logging.info("✅ GPIO cleaned.")
        except Exception as e:
            logging.warning(f"Lỗi cleanup: {e}")
        logging.info("👋 Kết thúc chương trình.")
