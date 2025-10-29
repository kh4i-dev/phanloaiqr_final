# -*- coding: utf-8 -*-
import cv2
import time
import json
import threading
import RPi.GPIO as GPIO
import os
from flask import Flask, render_template, Response
from flask_sock import Sock

# =============================
#        CẤU HÌNH CHUNG
# =============================
CAMERA_INDEX = 0
CONFIG_FILE = 'config.json'
ACTIVE_LOW = True  # Nếu relay kích bằng mức LOW thì True, ngược lại False

# --- GPIO CONFIG ---
GPIO.setmode(GPIO.BOARD)
GPIO.setwarnings(False)

define_pins = {
    "P1_PUSH": 11,
    "P1_PULL": 12,
    "P2_PUSH": 13,
    "P2_PULL": 8,
    "P3_PUSH": 15,
    "P3_PULL": 7,
}

# Cặp (push, pull) cho mỗi lane
RELAY_PINS = {
    0: {'push': 11, 'pull': 12},
    1: {'push': 13, 'pull': 8},
    2: {'push': 15, 'pull': 7}
}
SENSOR_PINS = {0: 5, 1: 29, 2: 31}

# Hàm bật/tắt relay theo kiểu ACTIVE_LOW
def RELAY_ON(pin):
    GPIO.output(pin, GPIO.LOW if ACTIVE_LOW else GPIO.HIGH)
def RELAY_OFF(pin):
    GPIO.output(pin, GPIO.HIGH if ACTIVE_LOW else GPIO.LOW)

# Setup GPIO
for pins in RELAY_PINS.values():
    GPIO.setup(pins['push'], GPIO.OUT)
    GPIO.setup(pins['pull'], GPIO.OUT)
for pin in SENSOR_PINS.values():
    GPIO.setup(pin, GPIO.IN, pull_up_down=GPIO.PUD_UP)

# =============================
#      TRẠNG THÁI HỆ THỐNG
# =============================
system_state = {
    "lanes": [
        {"name": "Loại 1", "status": "Sẵn sàng", "count": 0, "sensor": 1, "relay_grab": 1, "relay_push": 1},
        {"name": "Loại 2", "status": "Sẵn sàng", "count": 0, "sensor": 1, "relay_grab": 1, "relay_push": 1},
        {"name": "Loại 3", "status": "Sẵn sàng", "count": 0, "sensor": 1, "relay_grab": 1, "relay_push": 1}
    ],
    "timing_config": {"cycle_delay": 0.3}
}

state_lock = threading.Lock()
main_loop_running = True
latest_frame = None
frame_lock = threading.Lock()
prev_sensor = [1, 1, 1]

# =============================
#     HÀM KHỞI ĐỘNG & CẤU HÌNH
# =============================
def load_local_config():
    default_delay = 0.3
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                cfg = json.load(f)
                delay = cfg.get('timing_config', {}).get('cycle_delay', default_delay)
                with state_lock:
                    system_state['timing_config']['cycle_delay'] = delay
                print(f"[CONFIG] Loaded: cycle_delay = {delay}")
        except Exception:
            print("[CONFIG] Lỗi đọc file config, dùng mặc định 0.3s")
    else:
        print("[CONFIG] Không có file config, dùng mặc định 0.3s")

def reset_all_relays_to_default():
    print("[GPIO] Reset tất cả relay về trạng thái mặc định (THU BẬT).")
    for lane_pins in RELAY_PINS.values():
        RELAY_ON(lane_pins['pull'])
        RELAY_OFF(lane_pins['push'])

# =============================
#        LUỒNG CAMERA
# =============================
def camera_capture_thread():
    global latest_frame
    camera = cv2.VideoCapture(CAMERA_INDEX)
    camera.set(cv2.CAP_PROP_FRAME_WIDTH, 640)
    camera.set(cv2.CAP_PROP_FRAME_HEIGHT, 480)
    camera.set(cv2.CAP_PROP_BUFFERSIZE, 1)

    if not camera.isOpened():
        print("[ERROR] Không mở được camera.")
        return

    while main_loop_running:
        ret, frame = camera.read()
        if not ret:
            print("[WARN] Mất camera, thử khởi động lại...")
            camera.release()
            time.sleep(1)
            camera = cv2.VideoCapture(CAMERA_INDEX)
            continue

        with frame_lock:
            latest_frame = frame.copy()
        time.sleep(1 / 30)
    camera.release()

# =============================
#   CHU TRÌNH PHÂN LOẠI
# =============================
def sorting_process(lane_index):
    with state_lock:
        delay = system_state['timing_config']['cycle_delay']
        lane_name = system_state["lanes"][lane_index]["name"]
        system_state["lanes"][lane_index]["status"] = "Đang phân loại..."
    broadcast_log({"log_type": "info", "message": f"Bắt đầu chu trình cho {lane_name}"})

    try:
        pull_pin = RELAY_PINS[lane_index]['pull']
        push_pin = RELAY_PINS[lane_index]['push']

        RELAY_OFF(pull_pin); time.sleep(0.2)   # Nhả THU
        RELAY_ON(push_pin); time.sleep(delay)  # ĐẨY
        RELAY_OFF(push_pin); time.sleep(0.2)   # Rút lại
        RELAY_ON(pull_pin)                     # Bật lại THU
    finally:
        with state_lock:
            lane = system_state["lanes"][lane_index]
            lane["status"] = "Sẵn sàng"
            lane["count"] += 1
            broadcast_log({"log_type": "sort", "name": lane_name, "count": lane['count']})
    broadcast_log({"log_type": "info", "message": f"Hoàn tất chu trình cho {lane_name}"})

# =============================
#     QUÉT MÃ QR TỰ ĐỘNG
# =============================
def qr_detection_loop():
    detector = cv2.QRCodeDetector()
    last_qr, last_time = "", 0
    LANE_MAP = {"LOAI1": 0, "LOAI2": 1, "LOAI3": 2}

    while main_loop_running:
        frame_copy = None
        with frame_lock:
            if latest_frame is not None:
                frame_copy = latest_frame.copy()
        if frame_copy is None:
            time.sleep(0.2)
            continue

        try:
            data, _, _ = detector.detectAndDecode(frame_copy)
        except cv2.error:
            data = None
            time.sleep(0.2)
            continue

        if data and (data != last_qr or time.time() - last_time > 3):
            last_qr, last_time = data, time.time()
            data_upper = data.strip().upper()
            if data_upper in LANE_MAP:
                idx = LANE_MAP[data_upper]
                with state_lock:
                    if system_state["lanes"][idx]["status"] == "Sẵn sàng":
                        broadcast_log({"log_type": "qr", "data": data_upper})
                        system_state["lanes"][idx]["status"] = "Đang chờ vật..."
                timeout = time.time() + 15
                while time.time() < timeout:
                    if GPIO.input(SENSOR_PINS[idx]) == 0:
                        threading.Thread(target=sorting_process, args=(idx,), daemon=True).start()
                        break
                    time.sleep(0.05)
                else:
                    with state_lock:
                        system_state["lanes"][idx]["status"] = "Sẵn sàng"
            elif data_upper == "NG":
                broadcast_log({"log_type": "qr_ng", "data": data_upper})
            else:
                broadcast_log({"log_type": "unknown_qr", "data": data_upper})
        time.sleep(0.25)

# =============================
#     FLASK + WEBSOCKET
# =============================
app = Flask(__name__)
sock = Sock(app)
connected_clients = set()

def broadcast_state():
    while main_loop_running:
        with state_lock:
            for i in range(3):
                sensor_now = GPIO.input(SENSOR_PINS[i])
                if sensor_now != prev_sensor[i]:
                    prev_sensor[i] = sensor_now
                    broadcast_log({"log_type": "sensor", "name": system_state['lanes'][i]['name'], "status": sensor_now})
                system_state['lanes'][i]['sensor'] = sensor_now
                system_state['lanes'][i]['relay_grab'] = 1 if GPIO.input(RELAY_PINS[i]['pull']) == GPIO.LOW else 0
                system_state['lanes'][i]['relay_push'] = 1 if GPIO.input(RELAY_PINS[i]['push']) == GPIO.LOW else 0

            msg = json.dumps({"type": "state_update", "state": system_state})
            for client in list(connected_clients):
                try:
                    client.send(msg)
                except:
                    connected_clients.remove(client)
        time.sleep(0.5)

def broadcast_log(log_data):
    log_data['timestamp'] = time.strftime('%H:%M:%S')
    msg = json.dumps({"type": "log", **log_data})
    for client in list(connected_clients):
        try:
            client.send(msg)
        except:
            connected_clients.remove(client)

def generate_frames():
    while main_loop_running:
        with frame_lock:
            if latest_frame is None:
                time.sleep(0.1)
                continue
            frame = latest_frame.copy()
        _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 70])
        yield (b'--frame\r\n'
               b'Content-Type: image/jpeg\r\n\r\n' + buffer.tobytes() + b'\r\n')
        time.sleep(1 / 20)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/video_feed')
def video_feed():
    return Response(generate_frames(), mimetype='multipart/x-mixed-replace; boundary=frame')

@sock.route('/ws')
def ws_route(ws):
    connected_clients.add(ws)
    try:
        while True:
            ws.receive()
    finally:
        connected_clients.remove(ws)

# =============================
#           MAIN
# =============================
if __name__ == "__main__":
    try:
        load_local_config()
        reset_all_relays_to_default()
        threading.Thread(target=camera_capture_thread, daemon=True).start()
        threading.Thread(target=qr_detection_loop, daemon=True).start()
        threading.Thread(target=broadcast_state, daemon=True).start()

        print("🌐 Chạy server tại http://0.0.0.0:5000")
        app.run(host='0.0.0.0', port=5000)
    except KeyboardInterrupt:
        print("\n🛑 Dừng hệ thống...")
    finally:
        main_loop_running = False
        time.sleep(0.5)
        GPIO.cleanup()
        print("✅ GPIO cleaned up.")
