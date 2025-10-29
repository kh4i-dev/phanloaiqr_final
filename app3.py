# -*- coding: utf-8 -*-
import cv2
import time
import json
import threading
import RPi.GPIO as GPIO
import os
import logging # (CẢI TIẾN G) Thêm module logging
# (SỬA) Thêm 'jsonify' để xử lý route /config
from flask import Flask, render_template, Response, jsonify
from flask_sock import Sock

# =============================
#         CẤU HÌNH CHUNG
# =============================
CAMERA_INDEX = 0
CONFIG_FILE = 'config.json'
LOG_FILE = 'system.log' # (CẢI TIẾN G) Tên file log
SORT_LOG_FILE = 'sort_log.json' # (NÂNG CẤP 3) File lưu lịch sử đếm
ACTIVE_LOW = True  # Nếu relay kích bằng mức LOW thì True, ngược lại False

# =============================
#         KHAI BÁO CHÂN GPIO
# =============================
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

# (MỚI) Biến toàn cục cho chức năng Test
AUTO_TEST_ENABLED = False
auto_test_last_state = [1, 1, 1]
auto_test_last_trigger = [0.0, 0.0, 0.0]


# =============================
#      HÀM KHỞI ĐỘNG & CONFIG
# =============================
def load_local_config():
    default_config = {
        "cycle_delay": 0.3,
        "settle_delay": 0.2,
        "sensor_debounce": 0.1,
        "push_delay": 0.0
    }
    
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                # (SỬA) Xử lý file rỗng hoặc lỗi
                file_content = f.read()
                if not file_content:
                    logging.warning("[CONFIG] File config rỗng, dùng mặc định.")
                    cfg_from_file = default_config
                else:
                    cfg_from_file = json.loads(file_content).get('timing_config', default_config)

                final_cfg = default_config.copy()
                final_cfg.update(cfg_from_file) 

                with state_lock:
                    system_state['timing_config'] = final_cfg
                logging.info(f"[CONFIG] Loaded config: {final_cfg}")
        except Exception as e:
            logging.error(f"[CONFIG] Lỗi đọc file config ({e}), dùng mặc định.")
            with state_lock:
                system_state['timing_config'] = default_config
    else:
        logging.warning("[CONFIG] Không có file config, dùng mặc định.")
        with state_lock:
            system_state['timing_config'] = default_config

def reset_all_relays_to_default():
    logging.info("[GPIO] Reset tất cả relay về trạng thái mặc định (THU BẬT).")
    with state_lock:
        for lane in system_state["lanes"]:
            RELAY_ON(lane["pull_pin"])
            RELAY_OFF(lane["push_pin"])
            # Cập nhật trạng thái
            lane["relay_grab"] = 1
            lane["relay_push"] = 0
            lane["status"] = "Sẵn sàng"
    time.sleep(0.1) # (CẢI TIẾN MỤC 3) Chờ 0.1s cho relay ổn định
    logging.info("[GPIO] Reset hoàn tất.")


# (CẢI TIẾN D) Luồng tự động lưu config
def periodic_config_save():
    """Tự động lưu config mỗi 60s."""
    while main_loop_running:
        time.sleep(60)
        try:
            with state_lock:
                # Chỉ lưu timing_config
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
        broadcast_log({"log_type":"error","message":"Không mở được camera."})
        return

    # (NÂNG CẤP 1) Thêm watchdog cho camera
    retries = 0
    max_retries = 5

    while main_loop_running:
        ret, frame = camera.read()
        if not ret:
            retries += 1
            logging.warning(f"[WARN] Mất camera (lần {retries}/{max_retries}), thử khởi động lại...")
            broadcast_log({"log_type":"error","message":f"Mất camera (lần {retries}), đang thử lại..."})
            
            if retries > max_retries:
                logging.critical("[ERROR] Camera lỗi vĩnh viễn. Cần khởi động lại hệ thống.")
                broadcast_log({"log_type":"error","message":"Camera lỗi vĩnh viễn. Cần khởi động lại."})
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
#     (NÂNG CẤP 3) LƯU LOG ĐẾM
# =============================
def log_sort_count(lane_index, lane_name):
    """Ghi lại số lượng đếm vào file JSON theo ngày."""
    try:
        today = time.strftime('%Y-%m-%d')
        
        # Đọc file (nếu có)
        sort_log = {}
        if os.path.exists(SORT_LOG_FILE):
            with open(SORT_LOG_FILE, 'r') as f:
                sort_log = json.load(f)
        
        # Đảm bảo cấu trúc
        sort_log.setdefault(today, {})
        sort_log[today].setdefault(lane_name, 0)
        
        # Tăng và ghi đè
        sort_log[today][lane_name] += 1
        
        with open(SORT_LOG_FILE, 'w') as f:
            json.dump(sort_log, f, indent=4)
            
    except Exception as e:
        logging.error(f"[ERROR] Lỗi khi ghi sort_log.json: {e}")

# =============================
#       CHU TRÌNH PHÂN LOẠI
# =============================
def sorting_process(lane_index):
    lane_name = "" # Khởi tạo
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
        if not main_loop_running: return # (CẢI TIẾN C) Kiểm tra an toàn

        # 2. Bật relay ĐẨY
        RELAY_ON(push_pin)
        with state_lock: system_state["lanes"][lane_index]["relay_push"] = 1
        time.sleep(delay)
        if not main_loop_running: return # (CẢI TIẾN C) Kiểm tra an toàn
        
        # 3. Tắt relay ĐẨY
        RELAY_OFF(push_pin)
        with state_lock: system_state["lanes"][lane_index]["relay_push"] = 0
        time.sleep(settle_delay)
        if not main_loop_running: return # (CẢI TIẾN C) Kiểm tra an toàn
        
        # 4. Bật relay THU (về vị trí cũ)
        RELAY_ON(pull_pin)
        with state_lock: system_state["lanes"][lane_index]["relay_grab"] = 1
        
    finally:
        with state_lock:
            lane = system_state["lanes"][lane_index] 
            lane["status"] = "Sẵn sàng"
            lane["count"] += 1
            broadcast_log({"log_type": "sort", "name": lane_name, "count": lane['count']})
        
        # (NÂNG CẤP 3) Ghi log đếm
        if lane_name: # Đảm bảo lane_name đã được gán
            log_sort_count(lane_index, lane_name)
            
        broadcast_log({"log_type": "info", "message": f"Hoàn tất chu trình cho {lane_name}"})

# =============================
# HÀM TRUNG GIAN XỬ LÝ DELAY
# =============================
def handle_sorting_with_delay(lane_index):
    push_delay = 0.0
    lane_name_for_log = f"Lane {lane_index + 1}" 
    
    try:
        with state_lock:
            push_delay = system_state['timing_config'].get('push_delay', 0.0)
            lane_name_for_log = system_state['lanes'][lane_index]['name']
        
        if push_delay > 0:
            broadcast_log({"log_type": "info", "message": f"Đã thấy vật {lane_name_for_log}, chờ {push_delay}s..."})
            time.sleep(push_delay)
        
        # (CẢI TIẾN C) Kiểm tra an toàn trước khi chạy
        if not main_loop_running:
             broadcast_log({"log_type": "warn", "message": f"Hủy chu trình {lane_name_for_log} do hệ thống đang tắt."})
             return

        with state_lock:
            current_status = system_state["lanes"][lane_index]["status"]
        
        if current_status == "Đang chờ đẩy":
             sorting_process(lane_index)
        else:
             broadcast_log({"log_type": "warn", "message": f"Hủy chu trình {lane_name_for_log} do trạng thái thay đổi."})
        
    except Exception as e:
        logging.error(f"[ERROR] Lỗi trong luồng handle_sorting_with_delay (lane {lane_name_for_log}): {e}")
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
        # (CẢI TIẾN MỤC 3) Tạm nghỉ nếu đang bật Auto-Test
        if AUTO_TEST_ENABLED:
            time.sleep(0.2)
            continue
            
        frame_copy = None
        with frame_lock:
            if latest_frame is not None:
                frame_copy = latest_frame.copy()
        
        if frame_copy is None:
            time.sleep(0.1)
            continue
            
        # (NÂNG CẤP 4) Tối ưu QR, bỏ qua frame tối
        try:
            gray_frame = cv2.cvtColor(frame_copy, cv2.COLOR_BGR2GRAY)
            if gray_frame.mean() < 10: # Ngưỡng độ sáng (0-255)
                time.sleep(0.1) # Frame quá tối, bỏ qua
                continue
        except Exception:
            pass # Bỏ qua nếu lỗi convert

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
                    # Chỉ kích hoạt nếu lane Sẵn sàng
                    if system_state["lanes"][idx]["status"] == "Sẵn sàng":
                        broadcast_log({"log_type": "qr", "data": data_upper})
                        system_state["lanes"][idx]["status"] = "Đang chờ vật..."
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
    
    # (CẢI TIẾN MỤC 3) Bọc try/except để luồng không bị chết
    try:
        while main_loop_running:
            # (MỚI) Không giám sát sensor nếu đang bật auto-test
            if AUTO_TEST_ENABLED:
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

                if sensor_now == 0 and last_sensor_state[i] == 1:
                    if (now - last_sensor_trigger_time[i]) > debounce_time:
                        last_sensor_trigger_time[i] = now
                        
                        if current_status == "Đang chờ vật...":
                            with state_lock:
                                system_state["lanes"][i]["status"] = "Đang chờ đẩy"
                            
                            threading.Thread(target=handle_sorting_with_delay, args=(i,), daemon=True).start()
                        else:
                            broadcast_log({"log_type": "warn", "message": f"Sensor {lane['name']} bị kích hoạt ngoài dự kiến."})
                
                last_sensor_state[i] = sensor_now
            
            # (CẢI TIẾN A) Ngủ 0.01s nếu có vật, 0.05s nếu không (giảm tải CPU)
            adaptive_sleep = 0.05 if all(s == 1 for s in last_sensor_state) else 0.01
            time.sleep(adaptive_sleep)
            
    except Exception as e:
        logging.error(f"[ERROR] Luồng sensor_monitoring_thread bị crash: {e}")
        broadcast_log({"log_type": "error", "message": f"LỖI NGHIÊM TRỌNG: Luồng sensor bị dừng! {e}"})


# =============================
#        FLASK + WEBSOCKET
# =============================
app = Flask(__name__)
sock = Sock(app)
connected_clients = set()

# (MỚI) Đưa hàm broadcast_log lên trước để các hàm test có thể gọi
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
# (MỚI) CÁC HÀM XỬ LÝ TEST
# =============================
def handle_test_relay(lane_index, relay_action):
    """Kích hoạt 1 relay cụ thể (0.5s) để test."""
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

        # Kích hoạt relay
        RELAY_ON(pin_to_test)
        with state_lock:
            system_state["lanes"][lane_index][state_key_to_update] = 1
        
        time.sleep(0.5) # Giữ 0.5s
        # (CẢI TIẾN C) Kiểm tra an toàn
        if not main_loop_running: return

        # Tắt relay
        RELAY_OFF(pin_to_test)
        with state_lock:
            system_state["lanes"][lane_index][state_key_to_update] = 0
            
        broadcast_log({"log_type": "info", "message": f"Test {relay_action} lane {lane_index+1} OK"})

    except Exception as e:
        logging.error(f"[ERROR] Lỗi test relay {lane_index+1}: {e}")
        broadcast_log({"log_type": "error", "message": f"Lỗi test relay {lane_index+1}: {e}"})

def test_all_relays_thread():
    """Chạy test tuần tự 6 relay (chạy trên luồng riêng)."""
    logging.info("[TEST] Bắt đầu test tuần tự 6 relay...")
    for i in range(3):
        if not main_loop_running: break
        broadcast_log({"log_type": "info", "message": f"Test THU (grab) Lane {i+1}..."})
        handle_test_relay(i, "grab")
        time.sleep(0.5)
        
        if not main_loop_running: break
        broadcast_log({"log_type": "info", "message": f"Test ĐẨY (push) Lane {i+1}..."})
        handle_test_relay(i, "push")
        time.sleep(0.5)
    logging.info("[TEST] Hoàn tất test tuần tự.")
    broadcast_log({"log_type": "info", "message": "Hoàn tất test tuần tự 6 relay."})

# (NÂNG CẤP 7) Hàm worker cho auto-test (Đẩy -> Thu)
def auto_test_cycle_worker(lane_index):
    """Thực hiện 1 chu trình Đẩy -> Thu cho auto-test."""
    try:
        broadcast_log({"log_type": "warn", "message": f"AUTO-TEST: Đẩy Lane {lane_index+1}"})
        handle_test_relay(lane_index, "push")
        time.sleep(0.3) # Chờ 0.3s
        if not main_loop_running: return
        
        broadcast_log({"log_type": "warn", "message": f"AUTO-TEST: Thu Lane {lane_index+1}"})
        handle_test_relay(lane_index, "grab")
    except Exception as e:
         logging.error(f"[ERROR] Lỗi auto_test_cycle_worker (lane {lane_index+1}): {e}")

def auto_test_loop():
    """(NÂNG CẤP 7) Luồng riêng cho auto-test: Sensor sáng -> Đẩy rồi Thu."""
    global AUTO_TEST_ENABLED, auto_test_last_state, auto_test_last_trigger
    
    logging.info("[TEST] Luồng Auto-Test (sensor->relay) đã khởi động.")
    
    try:
        while main_loop_running:
            if AUTO_TEST_ENABLED:
                now = time.time()
                for i in range(3):
                    with state_lock:
                        sensor_pin = system_state["lanes"][i]["sensor_pin"]
                    
                    sensor_now = GPIO.input(sensor_pin)
                    
                    # Cập nhật UI
                    with state_lock:
                        system_state["lanes"][i]["sensor_reading"] = sensor_now

                    # Logic debounce cho auto-test (1s)
                    if sensor_now == 0 and auto_test_last_state[i] == 1:
                        if (now - auto_test_last_trigger[i]) > 1.0: # Chống nhiễu 1s
                            auto_test_last_trigger[i] = now
                            
                            broadcast_log({"log_type": "warn", "message": f"AUTO-TEST: Sensor {i+1} phát hiện!"})
                            # (NÂNG CẤP 7) Kích hoạt chu trình Đẩy -> Thu (chạy nền)
                            threading.Thread(target=auto_test_cycle_worker, args=(i,), daemon=True).start()
                    
                    auto_test_last_state[i] = sensor_now
                time.sleep(0.02) # Quét 50Hz
            else:
                # Khi không bật, reset trạng thái
                auto_test_last_state = [1, 1, 1]
                auto_test_last_trigger = [0.0, 0.0, 0.0]
                time.sleep(0.2) # Nghỉ 0.2s
    except Exception as e:
         logging.error(f"[ERROR] Luồng auto_test_loop bị crash: {e}")
         broadcast_log({"log_type": "error", "message": f"LỖI NGHIÊM TRỌNG: Luồng Auto-Test bị dừng! {e}"})


# =============================
#     CÁC HÀM CỦA FLASK (TIẾP)
# =============================

def broadcast_state():
    """(CẢI TIẾN F) Gửi state cho client, chỉ khi state thay đổi và giảm tần suất."""
    last_state_str = "" # Lưu state cuối dạng JSON
    
    while main_loop_running:
        current_msg = ""
        with state_lock:
            # Chuyển state sang JSON 1 lần duy nhất bên trong lock
            current_msg = json.dumps({"type": "state_update", "state": system_state})
        
        # Chỉ gửi khi state thật sự thay đổi
        if current_msg != last_state_str:
            for client in list(connected_clients):
                try:
                    client.send(current_msg)
                except Exception:
                    connected_clients.remove(client) 
            last_state_str = current_msg # Cập nhật state cuối
            
        time.sleep(0.5) # Giảm tần suất (gửi 2 lần/giây)

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
    return render_template('index3.html')

@app.route('/video_feed')
def video_feed():
    return Response(generate_frames(), mimetype='multipart/x-mixed-replace; boundary=frame')

# (SỬA) Route để web fetch config ban đầu, dùng jsonify
@app.route('/config')
def get_config():
    with state_lock:
        cfg = system_state.get('timing_config', {})
    # Dùng jsonify chuẩn của Flask
    return jsonify(cfg)

# (NÂNG CẤP 6) Thêm REST API để đọc state
@app.route('/api/state')
def api_state():
    with state_lock:
        # Trả về bản copy để tránh lỗi thread
        return jsonify(system_state)

@sock.route('/ws')
def ws_route(ws):
    global AUTO_TEST_ENABLED # (MỚI) Khai báo để có thể thay đổi
    
    connected_clients.add(ws)
    logging.info(f"[WS] Client connected. Total: {len(connected_clients)}")

    try:
        with state_lock:
            initial_state_msg = json.dumps({"type": "state_update", "state": system_state})
        ws.send(initial_state_msg)
    except Exception as e:
        logging.warning(f"[WS] Lỗi gửi state ban đầu: {e}")
        connected_clients.remove(ws)
        return

    try:
        while True:
            message = ws.receive()
            if message:
                try:
                    data = json.loads(message)
                    action = data.get('action')

                    # 1. Xử lý Reset Count
                    if action == 'reset_count':
                        lane_idx = data.get('lane')
                        with state_lock:
                            if lane_idx == 'all':
                                for lane in system_state['lanes']:
                                    lane['count'] = 0
                                broadcast_log({"log_type": "info", "message": "Reset đếm toàn bộ."})
                            elif 0 <= lane_idx <= 2:
                                system_state['lanes'][lane_idx]['count'] = 0
                                broadcast_log({"log_type": "info", "message": f"Reset đếm {system_state['lanes'][lane_idx]['name']}."})
                    
                    # 2. Xử lý Cập nhật Config
                    elif action == 'update_config':
                        new_config = data.get('config', {})
                        if new_config:
                            logging.info(f"[CONFIG] Nhận config mới từ UI: {new_config}")
                            config_to_save = {}
                            with state_lock:
                                system_state['timing_config'].update(new_config)
                                config_to_save = system_state['timing_config'].copy()

                            try:
                                with open(CONFIG_FILE, 'w') as f:
                                    json.dump({"timing_config": config_to_save}, f, indent=4)
                                broadcast_log({"log_type": "info", "message": "Đã lưu config mới từ UI."})
                            except Exception as e:
                                logging.error(f"[ERROR] Không thể lưu config: {e}")
                                broadcast_log({"log_type": "error", "message": f"Lỗi khi lưu config: {e}"})

                    # (MỚI) 3. Xử lý Test Relay Thủ Công
                    elif action == "test_relay":
                        # (SỬA LỖI MỤC 3) Đọc đúng key "lane_index"
                        lane_index = data.get("lane_index") # 0, 1, hoặc 2
                        relay_action = data.get("relay_action") # "grab" hoặc "push"
                        if lane_index is not None and relay_action:
                            # Chạy test trên luồng riêng để không block WebSocket
                            threading.Thread(target=handle_test_relay, args=(lane_index, relay_action), daemon=True).start()

                    # (MỚI) 4. Xử lý Test Tuần Tự
                    elif action == "test_all_relays":
                        # Chạy test trên luồng riêng
                        threading.Thread(target=test_all_relays_thread, daemon=True).start()
                    
                    # (MỚI) 5. Xử lý Bật/Tắt Auto-Test
                    elif action == "toggle_auto_test":
                        AUTO_TEST_ENABLED = data.get("enabled", False)
                        logging.info(f"[TEST] Auto-Test (Sensor->Relay) set to: {AUTO_TEST_ENABLED}")
                        broadcast_log({"log_type": "warn", "message": f"Chế độ Auto-Test (Sensor->Relay) đã { 'BẬT' if AUTO_TEST_ENABLED else 'TẮT' }."})
                        
                        # (MỚI) Nếu tắt auto-test, reset lại trạng thái các lane
                        if not AUTO_TEST_ENABLED:
                             reset_all_relays_to_default()

                except json.JSONDecodeError:
                    pass # Bỏ qua message không phải JSON
    finally:
        connected_clients.remove(ws)
        logging.info(f"[WS] Client disconnected. Total: {len(connected_clients)}")

# =============================
#               MAIN
# =============================
if __name__ == "__main__":
    try:
        # (CẢI TIẾN G) Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] (%(threadName)s) %(message)s',
            handlers=[
                logging.FileHandler(LOG_FILE), # Ghi ra file
                logging.StreamHandler()      # Ghi ra console
            ]
        )
        
        load_local_config()
        reset_all_relays_to_default()
        
        # Khởi tạo các luồng
        threading.Thread(target=camera_capture_thread, name="CameraThread", daemon=True).start()
        threading.Thread(target=qr_detection_loop, name="QRThread", daemon=True).start()
        threading.Thread(target=sensor_monitoring_thread, name="SensorThread", daemon=True).start()
        threading.Thread(target=broadcast_state, name="BroadcastThread", daemon=True).start()
        threading.Thread(target=auto_test_loop, name="AutoTestThread", daemon=True).start()
        threading.Thread(target=periodic_config_save, name="ConfigSaveThread", daemon=True).start()


        logging.info("=========================================")
        logging.info("  HỆ THỐNG PHÂN LOẠI SẴN SÀNG HOẠT ĐỘNG")
        logging.info(f"  Config: {system_state['timing_config']}")
        logging.info(f"  Log file: {LOG_FILE}")
        logging.info(f"  Sort log file: {SORT_LOG_FILE}") # (NÂNG CẤP 3)
        logging.info("  Truy cập: http://<IP_CUA_PI>:5000")
        logging.info("  API State: http://<IP_CUA_PI>:5000/api/state") # (NÂNG CẤP 6)
        logging.info("=========================================")
        app.run(host='0.0.0.0', port=5000)
        
    except KeyboardInterrupt:
        logging.info("\n🛑 Dừng hệ thống (Ctrl+C)...")
    finally:
        main_loop_running = False
        time.sleep(0.5) # Chờ các luồng con dừng
        GPIO.cleanup()
        logging.info("✅ GPIO cleaned up. Tạm biệt!")

