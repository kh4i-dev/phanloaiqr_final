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
#         CẤU HÌNH CHUNG
# =============================
CAMERA_INDEX = 0
CONFIG_FILE = 'config.json'
ACTIVE_LOW = True  # Nếu relay kích bằng mức LOW thì True, ngược lại False

# =============================
#         KHAI BÁO CHÂN GPIO
# =============================
# Lưu ý: Dùng GPIO.BCM sẽ tốt hơn GPIO.BOARD vì nó nhất quán
# trên các model Pi khác nhau. Tuy nhiên, nếu bạn đã đi dây
# theo BOARD, hãy giữ nguyên.
GPIO.setmode(GPIO.BCM)
GPIO.setwarnings(False)

# --- Relay ---
P1_PUSH = 17   # BOARD 11
P1_PULL = 18   # BOARD 12
P2_PUSH = 27   # BOARD 13
P2_PULL = 14   # BOARD 8
P3_PUSH = 22   # BOARD 15
P3_PULL = 4    # BOARD 7

# --- Sensor ---git p
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
    GPIO.setup(pin, GPIO.IN, pull_up_down=GPIO.PUD_UP)

# =============================
#       HÀM ĐIỀU KHIỂN RELAY
# =============================
def RELAY_ON(pin):
    GPIO.output(pin, GPIO.LOW if ACTIVE_LOW else GPIO.HIGH)

def RELAY_OFF(pin):
    GPIO.output(pin, GPIO.HIGH if ACTIVE_LOW else GPIO.LOW)

# =============================
#        TRẠNG THÁI HỆ THỐNG
# =============================
# Đây là "Single Source of Truth" (Nguồn sự thật duy nhất)
# Mọi luồng khác sẽ đọc/ghi vào đây (với lock)
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
        "sensor_debounce": 0.1 # Thời gian chống nhiễu sensor
    }
}

# Các biến global cho threading
state_lock = threading.Lock()
main_loop_running = True
latest_frame = None
frame_lock = threading.Lock()

# Biến cho việc chống nhiễu (debounce) sensor
last_sensor_state = [1, 1, 1]
last_sensor_trigger_time = [0.0, 0.0, 0.0]

# =============================
#      HÀM KHỞI ĐỘNG & CONFIG
# =============================
def load_local_config():
    default_config = {
        "cycle_delay": 0.3,
        "settle_delay": 0.2,
        "sensor_debounce": 0.1
    }
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                cfg = json.load(f).get('timing_config', default_config)
                with state_lock:
                    system_state['timing_config'] = cfg
                print(f"[CONFIG] Loaded config: {cfg}")
        except Exception:
            print("[CONFIG] Lỗi đọc file config, dùng mặc định.")
            with state_lock:
                system_state['timing_config'] = default_config
    else:
        print("[CONFIG] Không có file config, dùng mặc định.")
        with state_lock:
            system_state['timing_config'] = default_config

def reset_all_relays_to_default():
    print("[GPIO] Reset tất cả relay về trạng thái mặc định (THU BẬT).")
    with state_lock:
        for lane in system_state["lanes"]:
            RELAY_ON(lane["pull_pin"])
            RELAY_OFF(lane["push_pin"])
            # Cập nhật trạng thái
            lane["relay_grab"] = 1
            lane["relay_push"] = 0
            lane["status"] = "Sẵn sàng"

# =============================
#         LUỒNG CAMERA
# =============================
def camera_capture_thread():
    global latest_frame
    camera = cv2.VideoCapture(CAMERA_INDEX)
    camera.set(cv2.CAP_PROP_FRAME_WIDTH, 640)
    camera.set(cv2.CAP_PROP_FRAME_HEIGHT, 480)
    camera.set(cv2.CAP_PROP_BUFFERSIZE, 1) # Rất quan trọng để giảm lag

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
        time.sleep(1 / 30) # 30 FPS
    camera.release()

# =============================
#       CHU TRÌNH PHÂN LOẠI
# =============================
def sorting_process(lane_index):
    # Lấy thông tin cấu hình 1 lần (bên ngoài try...finally)
    with state_lock:
        cfg = system_state['timing_config']
        delay = cfg['cycle_delay']
        settle_delay = cfg['settle_delay']
        
        lane = system_state["lanes"][lane_index]
        lane_name = lane["name"]
        push_pin = lane["push_pin"]
        pull_pin = lane["pull_pin"]
        
        lane["status"] = "Đang phân loại..."
    
    broadcast_log({"log_type": "info", "message": f"Bắt đầu chu trình cho {lane_name}"})

    try:
        # 1. Tắt relay THU
        RELAY_OFF(pull_pin)
        with state_lock: system_state["lanes"][lane_index]["relay_grab"] = 0
        time.sleep(settle_delay)
        
        # 2. Bật relay ĐẨY
        RELAY_ON(push_pin)
        with state_lock: system_state["lanes"][lane_index]["relay_push"] = 1
        time.sleep(delay)
        
        # 3. Tắt relay ĐẨY
        RELAY_OFF(push_pin)
        with state_lock: system_state["lanes"][lane_index]["relay_push"] = 0
        time.sleep(settle_delay)
        
        # 4. Bật relay THU (về vị trí cũ)
        RELAY_ON(pull_pin)
        with state_lock: system_state["lanes"][lane_index]["relay_grab"] = 1
        
    finally:
        # Dù lỗi hay không, luôn reset trạng thái
        with state_lock:
            lane = system_state["lanes"][lane_index] # Lấy lại
            lane["status"] = "Sẵn sàng"
            lane["count"] += 1
            broadcast_log({"log_type": "sort", "name": lane_name, "count": lane['count']})
        broadcast_log({"log_type": "info", "message": f"Hoàn tất chu trình cho {lane_name}"})

# =============================
#       QUÉT MÃ QR TỰ ĐỘNG
# =============================
def qr_detection_loop():
    detector = cv2.QRCodeDetector()
    last_qr, last_time = "", 0.0
    LANE_MAP = {"LOAI1": 0, "LOAI2": 1, "LOAI3": 2}

    while main_loop_running:
        frame_copy = None
        with frame_lock:
            if latest_frame is not None:
                frame_copy = latest_frame.copy()
        
        if frame_copy is None:
            time.sleep(0.1)
            continue

        try:
            data, _, _ = detector.detectAndDecode(frame_copy)
        except cv2.error:
            data = None
            time.sleep(0.1) # Chờ 1 chút nếu có lỗi decode
            continue

        # Chống lặp QR: chỉ xử lý nếu QR mới, hoặc đã qua 3s
        if data and (data != last_qr or time.time() - last_time > 3.0):
            last_qr, last_time = data, time.time()
            data_upper = data.strip().upper()
            
            if data_upper in LANE_MAP:
                idx = LANE_MAP[data_upper]
                with state_lock:
                    if system_state["lanes"][idx]["status"] == "Sẵn sàng":
                        broadcast_log({"log_type": "qr", "data": data_upper})
                        system_state["lanes"][idx]["status"] = "Đang chờ vật..."
                    # Nếu không "Sẵn sàng" (đang chạy, đang chờ), thì bỏ qua QR này
                    # Luồng này KHÔNG bị block
            elif data_upper == "NG":
                broadcast_log({"log_type": "qr_ng", "data": data_upper})
            else:
                broadcast_log({"log_type": "unknown_qr", "data": data_upper})
        
        time.sleep(0.1) # Quét QR 10 lần/giây

# =============================
#      LUỒNG GIÁM SÁT SENSOR
# =============================
def sensor_monitoring_thread():
    global last_sensor_state, last_sensor_trigger_time
    
    while main_loop_running:
        with state_lock:
            debounce_time = system_state['timing_config']['sensor_debounce']
        now = time.time()

        for i in range(3):
            with state_lock:
                lane = system_state["lanes"][i]
                sensor_pin = lane["sensor_pin"]
                current_status = lane["status"]

            # Đọc trạng thái sensor
            sensor_now = GPIO.input(sensor_pin)
            
            # Cập nhật trạng thái sensor vào state (để UI thấy)
            with state_lock:
                system_state["lanes"][i]["sensor_reading"] = sensor_now

            # Logic chống nhiễu (Debounce)
            # Chỉ xử lý khi có sườn xuống (1 -> 0)
            if sensor_now == 0 and last_sensor_state[i] == 1:
                # Đã qua thời gian chống nhiễu
                if (now - last_sensor_trigger_time[i]) > debounce_time:
                    last_sensor_trigger_time[i] = now
                    
                    # Kiểm tra xem có đang chờ vật không
                    if current_status == "Đang chờ vật...":
                        # Kích hoạt chu trình phân loại trong 1 luồng riêng
                        threading.Thread(target=sorting_process, args=(i,), daemon=True).start()
                    else:
                        # Sensor bị kích hoạt nhưng không có QR nào chờ
                        broadcast_log({"log_type": "warn", "message": f"Sensor {lane['name']} bị kích hoạt ngoài dự kiến."})
            
            # Lưu trạng thái sensor của lần lặp này
            last_sensor_state[i] = sensor_now
        
        time.sleep(0.02) # Tần suất quét sensor 50Hz

# =============================
#        FLASK + WEBSOCKET
# =============================
app = Flask(__name__)
sock = Sock(app)
connected_clients = set()

def broadcast_state():
    """Gửi toàn bộ system_state cho client. Luồng này không đọc GPIO."""
    while main_loop_running:
        with state_lock:
            # Chỉ đọc state, không cần đọc GPIO ở đây
            msg = json.dumps({"type": "state_update", "state": system_state})
        
        for client in list(connected_clients):
            try:
                client.send(msg)
            except Exception:
                connected_clients.remove(client) # Xóa client nếu bị lỗi
        time.sleep(0.5) # Gửi 2 lần/giây

def broadcast_log(log_data):
    """Gửi 1 tin nhắn log cụ thể cho client."""
    log_data['timestamp'] = time.strftime('%H:%M:%S')
    msg = json.dumps({"type": "log", **log_data})
    for client in list(connected_clients):
        try:
            client.send(msg)
        except Exception:
            connected_clients.remove(client)

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
        time.sleep(1 / 20) # Stream 20 FPS

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/video_feed')
def video_feed():
    return Response(generate_frames(), mimetype='multipart/x-mixed-replace; boundary=frame')

@sock.route('/ws')
def ws_route(ws):
    connected_clients.add(ws)
    print(f"[WS] Client connected. Total: {len(connected_clients)}")
    try:
        while True:
            # Nhận message (nếu có), ví dụ: reset count
            message = ws.receive()
            if message:
                try:
                    data = json.loads(message)
                    if data.get('action') == 'reset_count':
                        lane_idx = data.get('lane')
                        with state_lock:
                            if lane_idx == 'all':
                                for lane in system_state['lanes']:
                                    lane['count'] = 0
                                broadcast_log({"log_type": "info", "message": "Reset đếm toàn bộ."})
                            elif 0 <= lane_idx <= 2:
                                system_state['lanes'][lane_idx]['count'] = 0
                                broadcast_log({"log_type": "info", "message": f"Reset đếm {system_state['lanes'][lane_idx]['name']}."})
                except json.JSONDecodeError:
                    pass # Bỏ qua mgit essage không phải JSON
    finally:
        connected_clients.remove(ws)
        print(f"[WS] Client disconnected. Total: {len(connected_clients)}")

# =============================
#               MAIN
# =============================
if __name__ == "__main__":
    try:
        load_local_config()
        reset_all_relays_to_default()
        
        # Khởi tạo các luồng
        threading.Thread(target=camera_capture_thread, daemon=True).start()
        threading.Thread(target=qr_detection_loop, daemon=True).start()
        threading.Thread(target=sensor_monitoring_thread, daemon=True).start()
        threading.Thread(target=broadcast_state, daemon=True).start()

        print("=========================================")
        print("  HỆ THỐNG PHÂN LOẠI SẴN SÀNG")
        print("  Truy cập: http://<IP_CUA_PI>:5000")
        print("=========================================")
        app.run(host='0.0.0.0', port=5000)
        
    except KeyboardInterrupt:
        print("\n🛑 Dừng hệ thống (Ctrl+C)...")
    finally:
        main_loop_running = False
        time.sleep(0.5) # Chờ các luồng con dừng
        GPIO.cleanup()
        print("✅ GPIO cleaned up. Tạm biệt!")
