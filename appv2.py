# -*- coding: utf-8 -*-
import cv2
import time
import json
import threading
from flask_sock import Sock
import logging
import unicodedata, re

def _strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def canon_id(s: str) -> str:
    """Chuẩn hoá ID/QR để khớp ổn định."""
    if not s:
        return ""
    s = str(s).strip().upper()
    s = _strip_accents(s)
    s = re.sub(r"\bQR_?\b", "", s)
    s = re.sub(r"[^A-Z0-9]", "", s)
    s = re.sub(r"^(LOAI|LO)+", "", s)
    return s

import os
import functools
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, render_template, Response, jsonify, request
from datetime import datetime
try:
    # Use Waitress as the production server if available
    from waitress import serve
except ImportError:
    serve = None # Fallback to Flask's development server

# =============================
#      GPIO ABSTRACTION
# =============================
try:
    # Try importing the actual RPi.GPIO library
    import RPi.GPIO as RPiGPIO
except (ImportError, RuntimeError):
    RPiGPIO = None # Set to None if import fails (e.g., running on PC)

class GPIOProvider:
    """Abstract base class for GPIO interaction."""
    def setup(self, pin, mode, pull_up_down=None): raise NotImplementedError
    def output(self, pin, value): raise NotImplementedError
    def input(self, pin): raise NotImplementedError
    def cleanup(self): raise NotImplementedError
    def setmode(self, mode): raise NotImplementedError
    def setwarnings(self, value): raise NotImplementedError

class RealGPIO(GPIOProvider):
    """Implementation using the actual RPi.GPIO library."""
    def __init__(self):
        if RPiGPIO is None: raise ImportError("Cannot load RPi.GPIO library. Is it installed and are you on a Raspberry Pi?")
        self.gpio = RPiGPIO
        # Assign constants from the library
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
            # Log and re-raise critical setup errors
            logging.error(f"[GPIO] Error setting up pin {pin}: {e}", exc_info=True)
            raise RuntimeError(f"Error setting up pin {pin}") from e

    def output(self, pin, value): self.gpio.output(pin, value)
    def input(self, pin): return self.gpio.input(pin)
    def cleanup(self): self.gpio.cleanup()

# --- Mock GPIO Implementation ---
mock_pin_override = {} # Global dict to temporarily override mock input pin states {pin: (target_value, expiry_time)}
mock_pin_override_lock = threading.Lock()

class MockGPIO(GPIOProvider):
    """Mock implementation for testing on non-Pi environments."""
    def __init__(self):
        # Assign mock constants
        for attr, val in [('BOARD', "mock_BOARD"), ('BCM', "mock_BCM"), ('OUT', "mock_OUT"),
                          ('IN', "mock_IN"), ('HIGH', 1), ('LOW', 0), ('PUD_UP', "mock_PUD_UP")]:
            setattr(self, attr, val)
        self.pin_states = {} # Stores the simulated state of pins
        logging.warning("="*50 + "\nRUNNING IN MOCK GPIO MODE.\n" + "="*50)

    def setmode(self, mode): logging.info(f"[MOCK] Set GPIO mode: {mode}")
    def setwarnings(self, value): logging.info(f"[MOCK] Set warnings: {value}")

    def setup(self, pin, mode, pull_up_down=None):
        pin_name = PIN_TO_NAME_MAP.get(pin, pin) # Get symbolic name if available
        logging.info(f"[MOCK] Setup pin {pin_name}({pin}) mode={mode} pull_up_down={pull_up_down}")
        if mode == self.OUT: self.pin_states[pin] = self.LOW # Default output to LOW
        else: self.pin_states[pin] = self.HIGH # Default input to HIGH (simulating pull-up)

    def output(self, pin, value):
        pin_name = PIN_TO_NAME_MAP.get(pin, pin)
        value_str = "HIGH" if value == self.HIGH else "LOW"
        logging.info(f"[MOCK] Output pin {pin_name}({pin}) = {value_str}({value})")
        self.pin_states[pin] = value

    def input(self, pin):
        pin_name = PIN_TO_NAME_MAP.get(pin, pin)
        current_time = time.time()
        # Check if there's a temporary override for this pin
        with mock_pin_override_lock:
            if pin in mock_pin_override:
                target_value, expiry_time = mock_pin_override[pin]
                if current_time < expiry_time:
                    # logging.debug(f"[MOCK] Input pin {pin_name}({pin}) -> OVERRIDE {target_value}")
                    return target_value
                else:
                    # Override expired, remove it
                    del mock_pin_override[pin]
                    logging.info(f"[MOCK] Override for pin {pin_name}({pin}) expired.")
        # Return the default/last set state (HIGH for sensors by default)
        return self.pin_states.get(pin, self.HIGH)

    def cleanup(self): logging.info("[MOCK] Cleanup GPIO")

def get_gpio_provider():
    """Factory function to get the appropriate GPIO provider."""
    if RPiGPIO:
        logging.info("RPi.GPIO library detected. Using RealGPIO.")
        return RealGPIO()
    else:
        logging.info("RPi.GPIO library not found. Using MockGPIO.")
        return MockGPIO()

# =============================
#      ERROR MANAGER
# =============================
class ErrorManager:
    """Manages the system's maintenance mode state."""
    def __init__(self):
        self.lock = threading.Lock()
        self.maintenance = False
        self.error = None

    def trigger_maintenance(self, msg):
        """Activates maintenance mode due to a critical error."""
        with self.lock:
            if self.maintenance: return # Already in maintenance
            self.maintenance = True
            self.error = msg
            logging.critical("="*50 + f"\n[MAINTENANCE MODE ACTIVATED] Reason: {msg}\n" +
                             "System halted. Operator intervention required.\n" + "="*50)
            # Broadcast the maintenance status update immediately
            broadcast({"type": "maintenance_update", "enabled": True, "reason": msg})

    def reset(self):
        """Resets maintenance mode (usually triggered by operator)."""
        with self.lock:
            if not self.maintenance: return # Not in maintenance, nothing to reset
            self.maintenance = False
            self.error = None
            logging.info("[MAINTENANCE MODE RESET]")
            # Broadcast the status update
            broadcast({"type": "maintenance_update", "enabled": False})

    def is_maintenance(self):
        """Checks if the system is currently in maintenance mode."""
        return self.maintenance

# =============================
#      GLOBAL CONFIG & INIT
# =============================
# --- Constants ---
CAMERA_INDEX = 0
CONFIG_FILE = 'config.json'
LOG_FILE = 'system.log'
SORT_LOG_FILE = 'sort_log.json'
ACTIVE_LOW = True # Relays activated by LOW signal
USERNAME = os.environ.get("APP_USERNAME", "admin") # Get from env or use default
PASSWORD = os.environ.get("APP_PASSWORD", "123")   # Get from env or use default

# --- Global Objects ---
GPIO = get_gpio_provider() # Get Real or Mock GPIO instance
error_manager = ErrorManager() # Manage maintenance mode
executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix="Worker") # Thread pool for tests/tasks
sort_log_lock = threading.Lock() # Lock for sort log file access
test_seq_running = False # Flag for sequential test status
test_seq_lock = threading.Lock() # Lock for sequential test flag

# --- Pin Mapping (BCM mode numbers) ---
# Map symbolic names to BCM pin numbers
_PIN_MAP = {
    "P1+": 17, "P1-": 18, "S1": 3,  # Lane 1: Push, Pull, Sensor
    "P2+": 27, "P2-": 14, "S2": 23,  # Lane 2: Push, Pull, Sensor
    "P3+": 22, "P3-": 4,  "S3": 24,   # Lane 3: Push, Pull, Sensor
    "P4+": None, "P4-": None, "S4": None   # Lane 4: Push, Pull, Sensor
}
# Create reverse map (Number -> Name) for logging clarity
PIN_TO_NAME_MAP = {v: k for k, v in _PIN_MAP.items()}

# --- Default Config (will be overridden by config.json) ---
DEFAULT_LANES_CFG = [
    {"id": "A", "name": "Loại 1", "sensor_pin": _PIN_MAP["S1"], "push_pin": _PIN_MAP["P1+"], "pull_pin": _PIN_MAP["P1-"]},
    {"id": "B", "name": "Loại 2", "sensor_pin": _PIN_MAP["S2"], "push_pin": _PIN_MAP["P2+"], "pull_pin": _PIN_MAP["P2-"]},
    {"id": "C", "name": "Loại 3", "sensor_pin": _PIN_MAP["S3"], "push_pin": _PIN_MAP["P3+"], "pull_pin": _PIN_MAP["P3-"]},
    {"id": "D", "name": "Loại 4 - Đi thẳng", "sensor_pin": None, "push_pin": None, "pull_pin": None},
]

DEFAULT_TIMING_CFG = {
    "cycle_delay": 0.3, "settle_delay": 0.2, "sensor_debounce": 0.1,
    "push_delay": 0.0, "gpio_mode": "BCM"
}

# --- Global Variables (State & Control) ---
lanes_config = DEFAULT_LANES_CFG # Current lane configuration (from file)
RELAY_PINS = [] # List of all relay pins in use
SENSOR_PINS = [] # List of all sensor pins in use

# Central system state dictionary
system_state = {
    "lanes": [],            # Populated from lanes_config on load
    "timing_config": {},    # Populated from file or defaults
    "is_mock": isinstance(GPIO, MockGPIO),
    "maintenance_mode": False,
    "gpio_mode": "BCM",     # Reflects the mode *currently* in use
    "last_error": None,
    "queue_indices": []
}

state_lock = threading.Lock() # Lock for accessing/modifying system_state
main_running = True # Flag to signal threads to stop
latest_frame = None # Holds the latest frame from the camera
frame_lock = threading.Lock() # Lock for accessing latest_frame

# Sensor debouncing state variables
last_s_state, last_s_trig = [], [] # Populated based on number of lanes
# Auto-test state variables
AUTO_TEST = False
auto_s_state, auto_s_trig = [], [] # Populated based on number of lanes
# --- QR queue management ---
QUEUE_HEAD_TIMEOUT = 10.0 # Seconds before the head of the queue is considered stale
qr_queue = []
queue_lock = threading.Lock()
queue_head_since = 0.0


def clear_qr_queue(reason=None):
    """Clears the in-memory QR queue and updates shared state."""
    global queue_head_since
    with queue_lock:
        qr_queue.clear()
        queue_head_since = 0.0
        current_queue = []

    with state_lock:
        system_state["queue_indices"] = current_queue

    if reason:
        broadcast_log({"log_type": "warn", "message": reason, "queue": current_queue})
# =============================
#       RELAY CONTROL FUNCTIONS
# =============================
def RELAY_ON(pin):
    """Turns a relay ON (activates it). Handles potential GPIO errors."""
    if pin is None: return logging.warning("[GPIO] RELAY_ON called with None pin")
    name = PIN_TO_NAME_MAP.get(pin, pin)
    logging.debug(f"[GPIO] Activating relay {name}({pin})")
    try:
        GPIO.output(pin, GPIO.LOW if ACTIVE_LOW else GPIO.HIGH)
    except Exception as e:
        logging.error(f"[GPIO] Error activating relay {name}: {e}", exc_info=True)
        error_manager.trigger_maintenance(f"Error activating relay {name}: {e}")

def RELAY_OFF(pin):
    """Turns a relay OFF (deactivates it). Handles potential GPIO errors."""
    if pin is None: return logging.warning("[GPIO] RELAY_OFF called with None pin")
    name = PIN_TO_NAME_MAP.get(pin, pin)
    logging.debug(f"[GPIO] Deactivating relay {name}({pin})")
    try:
        GPIO.output(pin, GPIO.HIGH if ACTIVE_LOW else GPIO.LOW)
    except Exception as e:
        logging.error(f"[GPIO] Error deactivating relay {name}: {e}", exc_info=True)
        error_manager.trigger_maintenance(f"Error deactivating relay {name}: {e}")

# =============================
#      CONFIG LOAD/SAVE & INIT
# =============================
def load_config():
    """Loads timing and lane config from JSON, creates if not found."""
    global lanes_config, RELAY_PINS, SENSOR_PINS, last_s_state, last_s_trig, auto_s_state, auto_s_trig
    loaded = {"timing_config": DEFAULT_TIMING_CFG.copy(),
              "lanes_config": [l.copy() for l in DEFAULT_LANES_CFG]} # Start with deep copies of defaults

    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f: content = f.read()
            if content:
                file_cfg = json.loads(content)
                # Merge timing config (file overrides default)
                timing = DEFAULT_TIMING_CFG.copy()
                timing.update(file_cfg.get('timing_config', {}))
                loaded['timing_config'] = timing
                # Use lanes config from file or default if missing/invalid
                
                def ensure_lane_ids(lanes_list):
                    default_ids = ['A', 'B', 'C', 'D', 'E', 'F']
                    for i, lane in enumerate(lanes_list):
                        if 'id' not in lane or not lane['id']:
                            lane['id'] = default_ids[i] if i < len(default_ids) else f"LANE_{i+1}"
                    return lanes_list
                lanes_from_file = file_cfg.get('lanes_config', DEFAULT_LANES_CFG)
                loaded['lanes_config'] = lanes_from_file if isinstance(lanes_from_file, list) else [l.copy() for l in DEFAULT_LANES_CFG]
            else: logging.warning(f"[CONFIG] {CONFIG_FILE} is empty, using defaults.")
        except json.JSONDecodeError as e:
            logging.error(f"[CONFIG] Error decoding {CONFIG_FILE}: {e}. Using defaults.")
            error_manager.trigger_maintenance(f"Invalid JSON in {CONFIG_FILE}: {e}")
            loaded = {"timing_config": DEFAULT_TIMING_CFG.copy(), "lanes_config": [l.copy() for l in DEFAULT_LANES_CFG]}
        except Exception as e:
            logging.error(f"[CONFIG] Error reading {CONFIG_FILE}: {e}. Using defaults.")
            error_manager.trigger_maintenance(f"Error reading {CONFIG_FILE}: {e}")
            loaded = {"timing_config": DEFAULT_TIMING_CFG.copy(), "lanes_config": [l.copy() for l in DEFAULT_LANES_CFG]}
    else:
        logging.warning(f"[CONFIG] {CONFIG_FILE} not found, creating with defaults.")
        try:
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(loaded, f, indent=4, ensure_ascii=False)
        except Exception as e: logging.error(f"[CONFIG] Error creating {CONFIG_FILE}: {e}")

    # Update global variables and system state from loaded config
    lanes_config = loaded['lanes_config']
    num_lanes = len(lanes_config)
    new_lanes_state, RELAY_PINS, SENSOR_PINS = [], [], []
    RELAY_PINS.clear(); SENSOR_PINS.clear() # Clear previous pins

    for i, cfg in enumerate(lanes_config):
        # Safely convert pins to int, allowing None
        s_pin = int(cfg["sensor_pin"]) if cfg.get("sensor_pin") is not None else None
        p_pin = int(cfg["push_pin"]) if cfg.get("push_pin") is not None else None
        pl_pin = int(cfg["pull_pin"]) if cfg.get("pull_pin") is not None else None

        new_lanes_state.append({
            "name": cfg.get("name", f"Lane {i+1}"), "status": "OK", "count": 0,
            "sensor_pin": s_pin, "push_pin": p_pin, "pull_pin": pl_pin,
            "sensor_reading": 1, "relay_grab": 0, "relay_push": 0
        })
        # Add valid pins to lists for setup
        if s_pin is not None: SENSOR_PINS.append(s_pin)
        if p_pin is not None: RELAY_PINS.append(p_pin)
        if pl_pin is not None: RELAY_PINS.append(pl_pin)

    # Initialize state arrays based on the actual number of lanes
    last_s_state = [1] * num_lanes; last_s_trig = [0.0] * num_lanes
    auto_s_state = [1] * num_lanes; auto_s_trig = [0.0] * num_lanes

    # Update the central system state
    with state_lock:
        system_state['timing_config'] = loaded['timing_config']
        system_state['gpio_mode'] = loaded['timing_config'].get("gpio_mode", "BCM")
        system_state['lanes'] = new_lanes_state
        # Ensure maintenance status is reflected correctly
        system_state['maintenance_mode'] = error_manager.is_maintenance()
        system_state['last_error'] = error_manager.error
        system_state['queue_indices'] = []

    logging .info(f"[CONFIG] Config loaded successfully for {num_lanes} lanes.")
    logging.info(f"[CONFIG] Timing config: {system_state['timing_config']}")
    logging.info(f"[CONFIG] Relay pins: {RELAY_PINS}, Sensor pins: {SENSOR_PINS}")

    clear_qr_queue() # Reset queue whenever configuration is (re)loaded
def reset_relays():
    """Resets all relays to a safe default state (Pull ON, Push OFF)."""
    logging.info("[GPIO] Resetting all relays to default state (Pull ON, Push OFF)...")
    try:
        with state_lock:
            for lane in system_state["lanes"]:
                pull_pin, push_pin = lane.get("pull_pin"), lane.get("push_pin")
                if pull_pin is not None: RELAY_ON(pull_pin) # Turn Pull ON
                if push_pin is not None: RELAY_OFF(push_pin) # Turn Push OFF
                # Update state to reflect the physical state
                lane.update({"relay_grab": (1 if pull_pin is not None else 0),
                             "relay_push": 0,
                             "status": "OK" if lane["status"] != "ERR" else "ERR"}) # Keep ERR status if it exists
        time.sleep(0.1) # Short delay for relays to settle
        logging.info("[GPIO] Relays reset completed.")
        clear_qr_queue("Queue cleared due to relay reset.")
    except Exception as e:
        logging.error(f"[GPIO] Error during relay reset: {e}", exc_info=True)
        error_manager.trigger_maintenance(f"Error resetting relays: {e}")

def save_config_periodically():
    """Periodically saves the current configuration to the JSON file."""
    while main_running:
        time.sleep(60) # Save every 60 seconds
        if error_manager.is_maintenance(): continue # Don't save if in maintenance
        try:
            config_snapshot = {}
            with state_lock: # Get a snapshot of the config from the current state
                config_snapshot['timing_config'] = system_state['timing_config'].copy()
                config_snapshot['lanes_config'] = [
                    {"name": l['name'], "sensor_pin": l['sensor_pin'],
                     "push_pin": l['push_pin'], "pull_pin": l['pull_pin']}
                    for l in system_state['lanes']
                ]
            # Write the snapshot to the file
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(config_snapshot, f, indent=4, ensure_ascii=False)
            logging.debug("[CONFIG] Configuration auto-saved.")
        except Exception as e:
            logging.error(f"[CONFIG] Error during periodic config save: {e}")

# =============================
#         CAMERA THREAD
# =============================
def run_camera():
    """Thread function to continuously capture frames from the camera."""
    global latest_frame
    camera = None
    try:
        logging.info("[CAMERA] Initializing camera...")
        camera = cv2.VideoCapture(CAMERA_INDEX)
        # Configure camera properties
        props = {cv2.CAP_PROP_FRAME_WIDTH: 640, cv2.CAP_PROP_FRAME_HEIGHT: 480, cv2.CAP_PROP_BUFFERSIZE: 1}
        for prop, value in props.items(): camera.set(prop, value)

        if not camera.isOpened():
            error_manager.trigger_maintenance("Cannot open camera.")
            return

        logging.info("[CAMERA] Camera ready.")
        retries, max_retries = 0, 5

        while main_running:
            if error_manager.is_maintenance(): time.sleep(0.5); continue # Pause if in maintenance

            ret, frame = camera.read()
            if not ret: # Handle camera disconnection
                retries += 1
                logging.warning(f"[CAMERA] Failed to grab frame (Attempt {retries}/{max_retries}). Retrying...")
                broadcast_log({"log_type":"error", "message":f"Camera connection lost (Attempt {retries})..."})

                if retries > max_retries:
                    error_manager.trigger_maintenance("Camera failed permanently after multiple retries.")
                    break # Exit thread

                # Attempt to release and reopen the camera
                if camera: camera.release()
                time.sleep(1) # Wait before reopening
                camera = cv2.VideoCapture(CAMERA_INDEX)
                if camera.isOpened():
                    for prop, value in props.items(): camera.set(prop, value) # Reapply settings
                    logging.info("[CAMERA] Camera reconnected successfully.")
                else:
                    logging.error("[CAMERA] Failed to reopen camera.")
                    time.sleep(2) # Wait longer before next attempt
                continue # Skip frame processing for this iteration

            retries = 0 # Reset retry counter on successful frame capture
            # Update the global frame variable safely
            with frame_lock:
                latest_frame = frame.copy()
            time.sleep(1 / 30) # Aim for ~30 FPS capture rate

    except Exception as e:
        logging.error(f"[CAMERA] Camera thread crashed: {e}", exc_info=True)
        error_manager.trigger_maintenance(f"Critical camera error: {e}")
    finally:
        if camera: camera.release()
        logging.info("[CAMERA] Camera released.")

# =============================
#       SORT LOGGING
# =============================
def summarize_sort_log(hourly_data):
    """Aggregate hourly sort data into a per-day summary."""
    daily_summary = {}
    for date, hours in hourly_data.items():
        daily_summary[date] = {}
        for hour_values in hours.values():
            for lane_name, count in hour_values.items():
                daily_summary[date][lane_name] = daily_summary[date].get(lane_name, 0) + count
    return daily_summary
def save_sort_log(lane_index, lane_name):
    """Logs the sort count to a JSON file and broadcasts the aggregated summary."""
    summary_to_broadcast = None
    with sort_log_lock: # Ensure exclusive access to the log file
        try:
            now = datetime.now()
            today_str = now.strftime('%Y-%m-%d')
            hour_str = now.strftime('%H') # Hour (00-23)

            sort_log_data = {}
            if os.path.exists(SORT_LOG_FILE):
                try:
                    with open(SORT_LOG_FILE, 'r', encoding='utf-8') as f:
                        content = f.read()
                        if content: sort_log_data = json.loads(content)
                except json.JSONDecodeError: # Handle corrupted JSON file
                    logging.error(f"[SORT_LOG] Error decoding {SORT_LOG_FILE}. Backing up and starting new log.")
                    backup_filename = f"{SORT_LOG_FILE}.{time.strftime('%Y%m%d_%H%M%S')}.bak"
                    try:
                        os.rename(SORT_LOG_FILE, backup_filename)
                        logging.warning(f"[SORT_LOG] Corrupted log backed up to {backup_filename}")
                    except Exception as move_err:
                        logging.error(f"[SORT_LOG] Failed to back up corrupted log: {move_err}")
                    sort_log_data = {} # Start with empty data
                except Exception as read_err:
                    logging.error(f"[SORT_LOG] Error reading {SORT_LOG_FILE}: {read_err}")
                    sort_log_data = {} # Start with empty data on other read errors

            # Ensure the nested dictionary structure exists
            sort_log_data.setdefault(today_str, {}).setdefault(hour_str, {}).setdefault(lane_name, 0)
            # Increment the count for the specific lane in the current hour
            sort_log_data[today_str][hour_str][lane_name] += 1

            # Write the updated data back to the file
            with open(SORT_LOG_FILE, 'w', encoding='utf-8') as f:
                json.dump(sort_log_data, f, indent=2, ensure_ascii=False) # Use indent=2 for readability
            summary_to_broadcast = summarize_sort_log(sort_log_data)
        except Exception as e:
            logging.error(f"[SORT_LOG] Failed to write sort log: {e}")
        if summary_to_broadcast is not None:
            broadcast({"type": "sort_log_update", "summary": summary_to_broadcast})


# =============================
#       SORTING CYCLE LOGIC
# =============================
def run_sort_cycle(lane_index):
    """Executes the physical sorting cycle (pull off, push on, push off, pull on)."""
    lane_name, push_pin, pull_pin = "", None, None
    cycle_delay, settle_delay = 0.3, 0.2 # Default timings
    operation_successful = False # Flag to track successful completion

    try:
        # --- 1. Get config and update initial state (within lock) ---
        with state_lock:
            if not (0 <= lane_index < len(system_state["lanes"])):
                return logging.error(f"[SORT] Invalid lane index received: {lane_index}")
            lane_state = system_state["lanes"][lane_index]
            lane_name = lane_state["name"]
            push_pin = lane_state.get("push_pin")
            pull_pin = lane_state.get("pull_pin")

            # Check if pins are configured
            if push_pin is None or pull_pin is None:
                logging.error(f"[SORT] Lane '{lane_name}' (Index {lane_index}) is missing relay pin configuration.")
                lane_state["status"] = "ERR" # Set status to error
                broadcast_log({"log_type": "error", "message": f"Lane '{lane_name}' missing pin config."})
                return # Abort cycle

            # Set status to sorting and get timings
            lane_state["status"] = "SORTING"
            timing_cfg = system_state['timing_config']
            cycle_delay = timing_cfg['cycle_delay']
            settle_delay = timing_cfg['settle_delay']

            # Log start of cycle
            broadcast_log({"log_type": "info", "message": f"Starting sort cycle for '{lane_name}'"})

        # --- 2. Execute GPIO sequence (outside lock) ---
        RELAY_OFF(pull_pin) # Deactivate Pull relay
        time.sleep(settle_delay)
        with state_lock: # Update state inside lock for safety
             if 0 <= lane_index < len(system_state["lanes"]): system_state["lanes"][lane_index]["relay_grab"] = 0
        if not main_running: return # Check if system is shutting down

        RELAY_ON(push_pin) # Activate Push relay
        time.sleep(cycle_delay)
        with state_lock:
             if 0 <= lane_index < len(system_state["lanes"]): system_state["lanes"][lane_index]["relay_push"] = 1
        if not main_running: return

        RELAY_OFF(push_pin) # Deactivate Push relay
        time.sleep(settle_delay)
        with state_lock:
             if 0 <= lane_index < len(system_state["lanes"]): system_state["lanes"][lane_index]["relay_push"] = 0
        if not main_running: return

        RELAY_ON(pull_pin) # Reactivate Pull relay (return to default)
        with state_lock:
             if 0 <= lane_index < len(system_state["lanes"]): system_state["lanes"][lane_index]["relay_grab"] = 1

        operation_successful = True # Mark as successful if sequence completes

    except Exception as e:
        logging.error(f"[SORT] Error during sorting cycle for '{lane_name}': {e}", exc_info=True)
        error_manager.trigger_maintenance(f"Sorting cycle error ({lane_name}): {e}")
    finally:
        # --- 3. Update final state (within lock) ---
        with state_lock:
            # Check index validity again in case config changed during operation
            if 0 <= lane_index < len(system_state["lanes"]):
                lane_state = system_state["lanes"][lane_index]
                # Safely update final relay states based on expected end state
                lane_state["relay_grab"] = 1 if pull_pin is not None else 0 # Should be ON if pin exists
                lane_state["relay_push"] = 0 # Should always be OFF
                # Only increment count and log if the operation was successful and lane wasn't in error
                if operation_successful and lane_state["status"] != "ERR":
                    lane_state["count"] += 1
                    broadcast_log({"log_type": "sort", "name": lane_name, "count": lane_state['count']})
                    save_sort_log(lane_index, lane_name) # Save to hourly log

                # Reset status to OK unless it was already an error
                if lane_state["status"] != "ERR":
                    lane_state["status"] = "OK"

        if operation_successful and lane_name: # Log completion only if successful
            broadcast_log({"log_type": "info", "message": f"Sort cycle completed for '{lane_name}'"})

def run_delayed_sort(lane_index):
    """Handles the push delay before initiating the sort cycle."""
    delay = 0.0
    lane_name = f"Lane {lane_index + 1}" # Default name for logging if state access fails
    initial_status = "UNKNOWN"

    try:
        # --- 1. Get delay and lane name (within lock) ---
        with state_lock:
            if not (0 <= lane_index < len(system_state["lanes"])):
                return logging.error(f"[DELAY] Invalid lane index in run_delayed_sort: {lane_index}")
            timing_cfg = system_state['timing_config']
            delay = timing_cfg.get('push_delay', 0.0) # Safely get push_delay
            lane_state = system_state['lanes'][lane_index]
            lane_name = lane_state['name']
            initial_status = lane_state["status"] # Store the status when delay starts

        # --- 2. Wait for the delay (outside lock) ---
        if delay > 0:
            broadcast_log({"log_type": "info", "message": f"Object detected at '{lane_name}', waiting {delay}s..."})
            time.sleep(delay)

        # --- 3. Check shutdown status (outside lock) ---
        if not main_running:
            return broadcast_log({"log_type": "warn", "message": f"Sort cycle for '{lane_name}' cancelled due to shutdown."})

        # --- 4. Re-check status and trigger sort cycle (within lock) ---
        should_run_sort = False
        with state_lock:
            # Check index validity again
            if not (0 <= lane_index < len(system_state["lanes"])): return
            current_status = system_state["lanes"][lane_index]["status"]

            # Only proceed if the status is still 'WAIT_PUSH'
            if current_status == "WAIT_PUSH":
                should_run_sort = True
            # If status changed *after* delay started but *was* WAIT_PUSH initially, log cancellation and reset
            elif initial_status == "WAIT_PUSH":
                logging.warning(f"[DELAY] Sort cycle for '{lane_name}' cancelled. Status changed from WAIT_PUSH to {current_status} during delay.")
                system_state["lanes"][lane_index]["status"] = "OK" # Reset to OK
                broadcast_log({"log_type": "warn", "message": f"Sort for '{lane_name}' cancelled (status: {current_status})."})
            # Else: Status wasn't WAIT_PUSH initially, or changed to something else irrelevant

        # --- 5. Run sort cycle (outside lock) ---
        if should_run_sort:
            run_sort_cycle(lane_index) # Call the main sorting function

    except Exception as e:
        logging.error(f"[DELAY] Error in delay handler for '{lane_name}': {e}", exc_info=True)
        error_manager.trigger_maintenance(f"Delayed sort error ({lane_name}): {e}")
        # Attempt to reset status if it was stuck in WAIT_PUSH
        with state_lock:
             if 0 <= lane_index < len(system_state["lanes"]) and system_state["lanes"][lane_index]["status"] == "WAIT_PUSH":
                 system_state["lanes"][lane_index]["status"] = "OK"
                 broadcast_log({"log_type": "error", "message": f"Reset '{lane_name}' after delay error."})

# =============================
#       QR CODE SCANNING
# =============================
def run_qr_scan():
    """Thread function to detect QR codes from the camera feed."""
    global queue_head_since
    detector = cv2.QRCodeDetector()
    last_qr_code, last_detection_time = "", 0.0
    lane_map = {} # Map QR code content (UPPERCASE, no spaces) to lane index
    lane_map_needs_update = True # Flag to rebuild map if config changes

    while main_running:
        try:
            # Pause scanning if in auto-test or maintenance mode
            if AUTO_TEST or error_manager.is_maintenance():
                time.sleep(0.2); continue

            # --- Update Lane Map if necessary ---
            if lane_map_needs_update:
                try:
                    with state_lock:
                        # Build map: 'LOAI1' -> 0, 'LOAI2' -> 1, etc.
                        lane_map = {
                        canon_id(lane.get("id", lane["name"])): i
                        for i, lane in enumerate(system_state["lanes"])
                    }

                    logging.info(f"[QR] Lane map updated: {lane_map}")
                    lane_map_needs_update = False # Reset flag
                except Exception as map_err:
                    logging.error(f"[QR] Error updating lane map: {map_err}")
                    time.sleep(1) # Wait before retrying map update
                    continue

            # --- Get and Preprocess Frame ---
            frame_copy, gray_frame = None, None
            with frame_lock:
                if latest_frame is not None: frame_copy = latest_frame.copy()
            if frame_copy is None or frame_copy.size == 0: # Check if frame is valid
                time.sleep(0.1); continue

            # Convert to grayscale and check brightness (optimization)
            gray_frame = cv2.cvtColor(frame_copy, cv2.COLOR_BGR2GRAY)
            if gray_frame.mean() < 15: # Skip dark frames (threshold adjustable)
                time.sleep(0.1); continue

            # --- Detect and Decode QR Code ---
            data, _, _ = detector.detectAndDecode(gray_frame)
            current_time = time.time()

            # --- Process Detected QR Code ---
            # Check if QR is new or detected again after 3 seconds cooldown
            if data and (data != last_qr_code or current_time - last_detection_time > 3.0):
                last_qr_code, last_detection_time = data, current_time
                # Normalize QR data for matching
                qr_content_normalized = canon_id(data)
                logging.info(f"[QR] Detected QR: '{data}' (Normalized: '{qr_content_normalized}')")

                # Match QR content to a lane
                if qr_content_normalized in lane_map:
                    lane_index = lane_map[qr_content_normalized]
                    lane_name = f"Lane {lane_index + 1}"
                    lane_status = "UNKNOWN"
                    with state_lock:
                        # Check index validity and if lane is ready
                        if 0 <= lane_index < len(system_state["lanes"]):
                            lane_state = system_state["lanes"][lane_index]
                            lane_name = lane_state.get("name", lane_name)
                            lane_status = lane_state.get("status", "UNKNOWN")

                    if lane_status == "OK":
                        queue_snapshot = []
                        global queue_head_since
                        with queue_lock:
                            was_empty = not qr_queue
                            qr_queue.append(lane_index)
                            if was_empty:
                                queue_head_since = current_time
                            queue_snapshot = list(qr_queue)

                        with state_lock:
                            if 0 <= lane_index < len(system_state["lanes"]):
                                system_state["lanes"][lane_index]["status"] = "WAIT_OBJ"
                                system_state["queue_indices"] = queue_snapshot

                        broadcast_log({
                            "log_type": "qr",
                            "data": data,
                            "lane": lane_name,
                            "queue": queue_snapshot
                        })
                    else:
                        with queue_lock:
                            queue_snapshot = list(qr_queue)
                        broadcast_log({
                            "log_type": "warn",
                            "message": f"Lane '{lane_name}' not ready for QR '{data}'. Status: {lane_status}",
                            "queue": queue_snapshot
                        })
                # Handle specific 'NG' code
                elif qr_content_normalized == "NG":
                    broadcast_log({"log_type": "qr_ng", "data": data})
                # Handle unknown codes
                else:
                    broadcast_log({"log_type": "unknown_qr", "data": data})

            time.sleep(0.1) # Scan rate (e.g., 10 scans/sec)

        except cv2.error as cv_err: # Handle specific OpenCV errors
            # Common errors that might not be critical
            if "Invalid image size" in str(cv_err) or "Unsupported format" in str(cv_err):
                logging.warning(f"[QR] OpenCV frame processing warning: {cv_err}")
            else: # Log other OpenCV errors as errors
                logging.error(f"[QR] Unknown OpenCV error: {cv_err}", exc_info=True)
            time.sleep(0.2) # Wait a bit longer after an OpenCV error
        except Exception as e: # Handle generic errors
            logging.error(f"[QR] Error in QR detection loop: {e}", exc_info=True)
            # Avoid triggering maintenance for transient QR errors unless persistent
            time.sleep(0.5) # Wait longer after a generic error

# =============================
#      SENSOR MONITORING
# =============================
def run_sensor_monitor():
    """Thread function to monitor sensor states and trigger actions."""
    global last_s_state, last_s_trig,queue_head_since
    try:
        while main_running:
            # Pause monitoring if in auto-test or maintenance mode
            if AUTO_TEST or error_manager.is_maintenance():
                time.sleep(0.1); continue

            lanes_to_check = [] # List of sensor info to check
            debounce_interval = 0.1 # Default debounce time

            # --- 1. Get current config and lane info (brief lock) ---
            with state_lock:
                timing_cfg = system_state['timing_config']
                debounce_interval = timing_cfg.get('sensor_debounce', 0.1) # Safely get debounce time
                current_lanes_state = system_state['lanes'] # Get snapshot
                num_lanes = len(current_lanes_state)
                # Prepare list of sensors to read
                for i in range(num_lanes):
                    lane_state = current_lanes_state[i]
                    lanes_to_check.append({
                        "index": i,
                        "pin": lane_state.get("sensor_pin"),
                        "name": lane_state.get('name'),
                        "status": lane_state.get("status")
                    })

            current_time = time.time()
            new_sensor_readings = last_s_state[:] # Copy last readings
             # --- Queue timeout handling ---
            timed_out_lane = None
            queue_snapshot_after_timeout = None
            with queue_lock:
                if qr_queue and queue_head_since > 0.0:
                    if (current_time - queue_head_since) > QUEUE_HEAD_TIMEOUT:
                        timed_out_lane = qr_queue.pop(0)
                        queue_head_since = current_time if qr_queue else 0.0
                if timed_out_lane is not None or queue_snapshot_after_timeout is None:
                    queue_snapshot_after_timeout = list(qr_queue)

            if timed_out_lane is not None:
                lane_name = f"Lane {timed_out_lane + 1}"
                with state_lock:
                    if 0 <= timed_out_lane < len(system_state["lanes"]):
                        lane_state = system_state["lanes"][timed_out_lane]
                        lane_name = lane_state.get("name", lane_name)
                        if lane_state.get("status") in ("WAIT_OBJ", "WAIT_PUSH"):
                            lane_state["status"] = "OK"
                        system_state["queue_indices"] = queue_snapshot_after_timeout
                broadcast_log({
                    "log_type": "warn",
                    "message": f"Queue timeout: removed '{lane_name}' after {QUEUE_HEAD_TIMEOUT}s without sensor trigger.",
                    "queue": queue_snapshot_after_timeout
                })

            # --- 2. Read sensors and process events (outside lock) ---
            for sensor_info in lanes_to_check:
                index, pin, name, status = sensor_info["index"], sensor_info["pin"], sensor_info["name"], sensor_info["status"]
                # Skip if sensor pin is not configured for this lane
                if pin is None: continue

                # Read sensor input safely
                try:
                    current_reading = GPIO.input(pin) # 0 = Active (object present), 1 = Inactive
                except Exception as gpio_err:
                    logging.error(f"[SENSOR] GPIO read error on pin {pin} ('{name}'): {gpio_err}")
                    error_manager.trigger_maintenance(f"Sensor read error ({name}, pin {pin}): {gpio_err}")
                    continue # Skip processing this sensor on error

                # Update sensor reading in global state (needs lock)
                with state_lock:
                    # Double-check index validity before updating state
                    if 0 <= index < len(system_state["lanes"]):
                        system_state["lanes"][index]["sensor_reading"] = current_reading

                # --- Event detection: Falling edge (1 -> 0) ---
                # Check for transition from inactive (1) to active (0)
                if current_reading == 0 and last_s_state[index] == 1:
                    # Apply debounce: Check if enough time has passed since last trigger
                    if (current_time - last_s_trig[index]) > debounce_interval:
                        last_s_trig[index] = current_time # Record time of this valid trigger

                        # Check if the lane was waiting for an object
                        lane_name = name or f"Lane {index + 1}"

                        # Synchronize with queue to ensure triggers match expected order
                        queue_removed = False
                        removal_type = "none" # head, out_of_order, none
                        queue_snapshot_after = []
                        with queue_lock:
                            if qr_queue:
                                try:
                                    position = qr_queue.index(index)
                                except ValueError:
                                    position = -1

                                if position >= 0:
                                    qr_queue.pop(position)
                                    queue_head_since = current_time if qr_queue else 0.0
                                    queue_removed = True
                                    removal_type = "head" if position == 0 else "out_of_order"
                                queue_snapshot_after = list(qr_queue)
                            else:
                                queue_snapshot_after = []

                        if not queue_removed:
                            log_type = "warn" if not queue_snapshot_after else "error"
                            msg = (f"Sensor '{lane_name}' triggered while queue empty." if not queue_snapshot_after
                                   else f"Sensor '{lane_name}' triggered but lane not in queue (queue head {queue_snapshot_after[0]}).")
                            broadcast_log({"log_type": log_type, "message": msg, "queue": queue_snapshot_after})
                            new_sensor_readings[index] = current_reading
                            continue

                        should_start_delay_thread = False
                        lane_status_now = "UNKNOWN"
                        with state_lock:
                            if 0 <= index < len(system_state["lanes"]):
                                lane_state = system_state["lanes"][index]
                                lane_name = lane_state.get("name", lane_name)
                                lane_status_now = lane_state.get("status", "UNKNOWN")
                                system_state["queue_indices"] = queue_snapshot_after
                                if lane_state.get("status") == "WAIT_OBJ":
                                    lane_state["status"] = "WAIT_PUSH"
                                    lane_status_now = "WAIT_PUSH"
                                    should_start_delay_thread = True

                        if removal_type == "out_of_order":
                            broadcast_log({
                                "log_type": "warn",
                                "message": f"Sensor '{lane_name}' triggered out of queue order. Adjusting queue.",
                                "queue": queue_snapshot_after
                            })
                        else:
                            broadcast_log({
                                "log_type": "info",
                                "message": f"Sensor '{lane_name}' confirmed arrival. Queue advanced.",
                                "queue": queue_snapshot_after
                            })

                        if should_start_delay_thread:
                            executor.submit(run_delayed_sort, index) # Use thread pool
                        elif lane_status_now != "WAIT_PUSH":
                            broadcast_log({
                                "log_type": "warn",
                                "message": f"Sensor '{lane_name}' triggered but lane status is {lane_status_now}.",
                                "queue": queue_snapshot_after
                            })
                # Store the current reading for the next cycle's comparison
                new_sensor_readings[index] = current_reading

            # Update the global last sensor state array
            last_s_state = new_sensor_readings

            # --- 3. Adaptive sleep (outside lock) ---
            # Sleep longer if all sensors are inactive, shorter if any are active
            sleep_duration = 0.05 if all(s == 1 for s in last_s_state) else 0.01
            time.sleep(sleep_duration)

    except Exception as e:
        logging.error(f"[SENSOR] Sensor monitoring thread crashed: {e}", exc_info=True)
        error_manager.trigger_maintenance(f"Critical sensor thread error: {e}")

# =============================
#        FLASK & WEBSOCKET
# =============================
app = Flask(__name__)
sock = Sock(app)
clients = set() # Renamed from connected_clients for brevity
clients_lock = threading.Lock() # Lock specifically for the clients set

def broadcast(data):
    """Sends JSON data to all connected WebSocket clients safely."""
    msg = json.dumps(data)
    disconnected_clients = set()
    # Iterate over a copy of the set to allow safe removal during iteration
    with clients_lock:
        current_clients = clients.copy()

    for client in current_clients:
        try:
            client.send(msg)
        except Exception:
            # Mark client for removal if send fails
            disconnected_clients.add(client)

    # Remove disconnected clients from the original set
    if disconnected_clients:
        with clients_lock:
            clients.difference_update(disconnected_clients)
            logging.debug(f"[WS] Removed {len(disconnected_clients)} disconnected client(s).")

def broadcast_log(log_data):
    """Formats and broadcasts a log message."""
    log_data['timestamp'] = datetime.now().strftime('%H:%M:%S')
    broadcast({"type": "log", **log_data})

# =============================
#      TESTING FUNCTIONS (🧪)
# =============================
def run_test_relay_worker(lane_index, relay_action):
    """Worker function (for ThreadPool) to test a single relay."""
    pin, state_key, lane_name = None, None, f"Lane {lane_index + 1}"
    try:
        # --- Get pin and state key (brief lock) ---
        with state_lock:
            if not (0 <= lane_index < len(system_state["lanes"])):
                return broadcast_log({"log_type": "error", "message": f"Test failed: Invalid lane index {lane_index}."})
            lane_state = system_state["lanes"][lane_index]
            lane_name = lane_state['name']
            pin = lane_state.get("pull_pin") if relay_action == "grab" else lane_state.get("push_pin")
            state_key = "relay_grab" if relay_action == "grab" else "relay_push"
            if pin is None:
                return broadcast_log({"log_type": "error", "message": f"Test failed: Lane '{lane_name}' missing pin config for '{relay_action}'."})

        # --- Perform GPIO actions (outside lock) ---
        RELAY_ON(pin) # Turn relay ON
        with state_lock: # Update state to ON
             if 0 <= lane_index < len(system_state["lanes"]): system_state["lanes"][lane_index][state_key] = 1
        time.sleep(0.5) # Hold for 0.5 seconds
        if not main_running: return # Check for shutdown

        RELAY_OFF(pin) # Turn relay OFF
        with state_lock: # Update state to OFF
             if 0 <= lane_index < len(system_state["lanes"]): system_state["lanes"][lane_index][state_key] = 0

        broadcast_log({"log_type": "info", "message": f"Test '{relay_action}' on '{lane_name}' successful."})

    except Exception as e:
        logging.error(f"[TEST] Error testing relay '{relay_action}' for '{lane_name}': {e}", exc_info=True)
        broadcast_log({"log_type": "error", "message": f"Error testing '{relay_action}' on '{lane_name}': {e}"})

def run_test_all_relays_worker():
    """Worker function (for ThreadPool) to test all relays sequentially."""
    global test_seq_running
    # Ensure only one sequential test runs at a time
    with test_seq_lock:
        if test_seq_running:
            logging.warning("[TEST] Sequential test already in progress.")
            broadcast_log({"log_type": "warn", "message": "Sequential test is already running."})
            return
        test_seq_running = True

    logging.info("[TEST] Starting sequential relay test...")
    broadcast_log({"log_type": "info", "message": "Starting sequential relay test..."})
    stopped_early = False

    try:
        num_lanes = 0
        with state_lock: num_lanes = len(system_state['lanes'])

        for i in range(num_lanes):
            # Check if stopped or system shutting down before each action
            with test_seq_lock: stop_requested = not main_running or not test_seq_running
            if stop_requested: stopped_early = True; break

            # Get lane name safely
            lane_name = f"Lane {i+1}"
            with state_lock:
                 if 0 <= i < len(system_state['lanes']): lane_name = system_state['lanes'][i]['name']

            # Test Grab (Pull)
            broadcast_log({"log_type": "info", "message": f"Testing Grab (Pull) for '{lane_name}'..."})
            run_test_relay_worker(i, "grab") # This already handles state updates
            time.sleep(0.5)

            # Re-check stop condition
            with test_seq_lock: stop_requested = not main_running or not test_seq_running
            if stop_requested: stopped_early = True; break

            # Test Push
            broadcast_log({"log_type": "info", "message": f"Testing Push for '{lane_name}'..."})
            run_test_relay_worker(i, "push")
            time.sleep(0.5)

        # Log completion status
        if stopped_early:
            logging.info("[TEST] Sequential relay test stopped.")
            broadcast_log({"log_type": "warn", "message": "Sequential test stopped."})
        else:
            logging.info("[TEST] Sequential relay test completed.")
            broadcast_log({"log_type": "info", "message": "Sequential relay test completed."})

    finally: # Always reset the flag and notify UI
        with test_seq_lock: test_seq_running = False
        broadcast({"type": "test_sequence_complete"}) # Notify UI test is done


def run_auto_test_cycle_worker(lane_index):
    """Worker for a single Auto-Test cycle (Push -> Pull)."""
    lane_name = f"Lane {lane_index + 1}"
    try:
        with state_lock:
            if 0 <= lane_index < len(system_state['lanes']): lane_name = system_state['lanes'][lane_index]['name']

        broadcast_log({"log_type": "warn", "message": f"AUTO-TEST: Executing Push for '{lane_name}'"})
        run_test_relay_worker(lane_index, "push") # Use the existing test worker
        time.sleep(0.3) # Short delay between push and pull
        if not main_running: return # Check shutdown

        broadcast_log({"log_type": "warn", "message": f"AUTO-TEST: Executing Pull for '{lane_name}'"})
        run_test_relay_worker(lane_index, "grab")
    except Exception as e:
        logging.error(f"[TEST] Error during auto-test cycle worker for '{lane_name}': {e}", exc_info=True)

def run_auto_test_monitor():
    """Dedicated thread to monitor sensors and trigger auto-test cycles."""
    global AUTO_TEST, auto_s_state, auto_s_trig
    logging.info("[TEST] Auto-Test monitoring thread started.")
    try:
        while main_running:
            # --- Check Maintenance/Pause ---
            if error_manager.is_maintenance():
                if AUTO_TEST: # Automatically disable if maintenance occurs
                    AUTO_TEST = False
                    logging.warning("[TEST] Auto-Test disabled due to maintenance mode.")
                    broadcast_log({"log_type": "error", "message": "Auto-Test disabled due to maintenance."})
                time.sleep(0.2); continue

            # --- Get Lane Info (brief lock) ---
            lanes_info_auto = []
            num_lanes = 0
            queue_clear_reason = None
            with state_lock:
                num_lanes = len(system_state['lanes'])
                for i in range(num_lanes):
                     if 0 <= i < len(system_state["lanes"]):
                         lane_state = system_state["lanes"][i]
                         lanes_info_auto.append({
                             "index": i,
                             "pin": lane_state.get("sensor_pin"),
                             "name": lane_state.get('name')
                         })

            # --- Process Sensors if Auto-Test is ON ---
            if AUTO_TEST:
                current_time = time.time()
                new_auto_readings = auto_s_state[:] # Copy last readings

                for sensor_info in lanes_info_auto:
                    index, pin, name = sensor_info["index"], sensor_info["pin"], sensor_info["name"]
                    if pin is None: continue # Skip lanes without sensor

                    # Read sensor safely
                    try:
                        current_reading = GPIO.input(pin)
                    except Exception as gpio_err:
                        logging.error(f"[AUTO-TEST] GPIO read error on pin {pin} ('{name}'): {gpio_err}")
                        error_manager.trigger_maintenance(f"Auto-Test sensor read error ({name}, pin {pin}): {gpio_err}")
                        continue

                    # Update sensor reading in main state (needs lock)
                    with state_lock:
                        if 0 <= index < len(system_state["lanes"]):
                            system_state["lanes"][index]["sensor_reading"] = current_reading

                    # Detect falling edge (1 -> 0) with debounce
                    if current_reading == 0 and auto_s_state[index] == 1:
                        if (current_time - auto_s_trig[index]) > 1.0: # 1 second debounce for auto-test
                            auto_s_trig[index] = current_time
                            broadcast_log({"log_type": "warn", "message": f"AUTO-TEST: Sensor '{name}' triggered!"})
                            # Submit the Push->Pull cycle to the thread pool
                            executor.submit(run_auto_test_cycle_worker, index)

                    new_auto_readings[index] = current_reading

                auto_s_state = new_auto_readings # Update last readings
                time.sleep(0.02) # Fast scan when active
            else: # Reset state if Auto-Test is OFF
                auto_s_state = [1] * num_lanes
                auto_s_trig = [0.0] * num_lanes
                time.sleep(0.2) # Slower check when inactive

    except Exception as e:
         logging.error(f"[AUTO-TEST] Auto-Test monitoring thread crashed: {e}", exc_info=True)
         error_manager.trigger_maintenance(f"Critical Auto-Test thread error: {e}")

def trigger_mock_pin(pin, value, duration):
    """Worker function (for ThreadPool) to temporarily override a mock pin's input value."""
    # Validate inputs
    if not isinstance(pin, int) or not isinstance(value, int) or not isinstance(duration, (int, float)) or duration <= 0:
        return logging.error(f"[MOCK] Invalid trigger parameters: pin={pin}, value={value}, duration={duration}")

    pin_name = PIN_TO_NAME_MAP.get(pin, pin)
    value_str = "HIGH" if value == GPIO.HIGH else "LOW"
    logging.info(f"[MOCK] Triggering pin {pin_name}({pin}) to {value_str} for {duration}s")

    # Set the override with expiry time
    with mock_pin_override_lock:
        mock_pin_override[pin] = (value, time.time() + duration)

    broadcast_log({"log_type": "info", "message": f"Mock: Pin {pin_name} set to {value_str} for {duration}s"})

# =============================
#      FLASK ROUTES & AUTH
# =============================
# --- Authentication Helpers ---
def check_auth(username, password):
    """Validates username and password."""
    return username == USERNAME and password == PASSWORD

def auth_fail_response():
    """Returns a 401 Unauthorized response."""
    return Response('Authentication required.', 401, {'WWW-Authenticate': 'Basic realm="Login Required"'})

def require_auth(f):
    """Decorator to enforce Basic Authentication on a route."""
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return auth_fail_response()
        return f(*args, **kwargs)
    return decorated

# --- State Broadcasting Thread ---
def broadcast_current_state():
    """Periodically broadcasts the system state to clients if it has changed."""
    last_broadcast_state_json = ""
    while main_running:
        current_state_snapshot = {}
        current_state_json = ""
        # --- Create state snapshot (brief lock) ---
        with state_lock:
            # Create a deep copy to avoid modification during serialization
            # Using json loads/dumps is a simple way for deep copy here
            current_state_snapshot = json.loads(json.dumps(system_state))

        # --- Add dynamic status outside lock ---
        current_state_snapshot["maintenance_mode"] = error_manager.is_maintenance()
        current_state_snapshot["last_error"] = error_manager.error
        current_state_snapshot["gpio_mode"] = current_state_snapshot.get('timing_config',{}).get('gpio_mode','BCM')

        # --- Serialize and Broadcast if changed ---
        try:
            current_state_json = json.dumps({"type": "state_update", "state": current_state_snapshot})
        except TypeError as e:
            logging.error(f"Error serializing system state: {e}")
            time.sleep(1); continue # Wait and retry if serialization fails

        if current_state_json != last_broadcast_state_json:
            try:
                # Use the generic broadcast function which handles JSON internally
                broadcast(json.loads(current_state_json))
                last_broadcast_state_json = current_state_json # Update last sent state
            except json.JSONDecodeError: pass # Should not happen, but ignore if it does

        time.sleep(0.5) # Broadcast check interval

# --- Camera Feed Streaming ---
def stream_frames():
    """Generator function to stream camera frames or a placeholder."""
    # Load placeholder image once
    placeholder_path = 'black_frame.png'
    placeholder_img = None
    if os.path.exists(placeholder_path): placeholder_img = cv2.imread(placeholder_path)
    if placeholder_img is None:
        import numpy as np
        placeholder_img = np.zeros((480, 640, 3), dtype=np.uint8) # Create black frame if file not found
        logging.warning(f"[CAMERA] Placeholder image '{placeholder_path}' not found. Using black frame.")

    while main_running:
        frame_to_stream = None
        # Get latest frame if not in maintenance
        if not error_manager.is_maintenance():
            with frame_lock:
                if latest_frame is not None:
                    frame_to_stream = latest_frame.copy()

        # Use placeholder if no frame or in maintenance
        current_frame = frame_to_stream if frame_to_stream is not None else placeholder_img

        # Encode frame as JPEG
        try:
            is_success, buffer = cv2.imencode('.jpg', current_frame, [cv2.IMWRITE_JPEG_QUALITY, 70])
            if is_success:
                # Yield frame in multipart format
                yield (b'--frame\r\nContent-Type: image/jpeg\r\n\r\n' + buffer.tobytes() + b'\r\n')
            else:
                logging.warning("[CAMERA] Failed to encode frame.")
        except Exception as encode_err:
            logging.error(f"[CAMERA] Error encoding frame: {encode_err}", exc_info=True)

        time.sleep(1 / 20) # Target streaming FPS

# --- Flask Routes ---
@app.route('/')
@require_auth
def route_index():
    """Serves the main HTML page."""
    return render_template('index_final.html') # Use the final HTML file

@app.route('/video_feed')
@require_auth
def route_video_feed():
    """Provides the camera video stream."""
    return Response(stream_frames(), mimetype='multipart/x-mixed-replace; boundary=frame')

@app.route('/config', methods=['GET'])
@require_auth
def route_get_config():
    """API endpoint to GET the current configuration (timing + lanes)."""
    with state_lock:
        config_snapshot = {
            "timing_config": system_state.get('timing_config', {}).copy(),
            "lanes_config": [
             {"id": l['id'], "name": l['name'], "sensor_pin": l['sensor_pin'],
            "push_pin": l['push_pin'], "pull_pin": l['pull_pin']}
            for l in system_state['lanes']
            ]}
    return jsonify(config_snapshot)

@app.route('/config', methods=['POST']) # Combined GET and POST
@require_auth
def route_update_config():
    """API endpoint to POST updates to the configuration."""
    global lanes_config, RELAY_PINS, SENSOR_PINS, last_s_state, last_s_trig, auto_s_state, auto_s_trig, lane_map_needs_update
    data = request.json
    user = request.authorization.username
    if not data: return jsonify({"error": "Missing JSON payload"}), 400
    logging.info(f"[CONFIG] Received config update via POST from {user}: {data}")

    timing_update = data.get('timing_config', {})
    lanes_update = data.get('lanes_config') # Expects a list or None/missing
    config_to_save = {}
    restart_needed = False
    queue_clear_reason = None

    with state_lock:
        # --- 1. Update Timing Config ---
        current_timing = system_state['timing_config']
        current_gpio_mode = current_timing.get('gpio_mode', 'BCM')
        # Update current timing dict with received values
        current_timing.update(timing_update)
        new_gpio_mode = current_timing.get('gpio_mode', 'BCM')

        # Check if GPIO mode changed - requires restart
        if new_gpio_mode != current_gpio_mode:
            logging.warning("[CONFIG] GPIO mode changed. Application restart required to apply.")
            broadcast_log({"log_type": "warn", "message": "GPIO mode changed. Restart required!"})
            restart_needed = True
            # Keep the *running* mode as the old one, but save the new one
            current_timing['gpio_mode'] = new_gpio_mode # Save new mode
            system_state['gpio_mode'] = current_gpio_mode # Keep running old mode
        else:
            system_state['gpio_mode'] = new_gpio_mode # Update running mode if no change

        config_to_save['timing_config'] = current_timing.copy()

        # --- 2. Update Lanes Config (if provided) ---
        if isinstance(lanes_update, list):
            logging.info("[CONFIG] Updating lanes configuration...")
            lanes_config = lanes_update # Update the global variable
            num_lanes = len(lanes_config)
            new_lanes_state, new_relay_pins, new_sensor_pins = [], [], []
            # Rebuild lane state and pin lists based on new config
            for i, cfg in enumerate(lanes_config):
                s_pin = int(cfg["sensor_pin"]) if cfg.get("sensor_pin") is not None else None
                p_pin = int(cfg["push_pin"]) if cfg.get("push_pin") is not None else None
                pl_pin = int(cfg["pull_pin"]) if cfg.get("pull_pin") is not None else None
                new_lanes_state.append({
                    "name": cfg.get("name", f"Lane {i+1}"), "status": "OK", "count": 0,
                    "sensor_pin": s_pin, "push_pin": p_pin, "pull_pin": pl_pin,
                    "sensor_reading": 1, "relay_grab": 0, "relay_push": 0})
                if s_pin is not None:
                    new_sensor_pins.append(s_pin)
                if p_pin is not None:
                    new_relay_pins.append(p_pin)
                if pl_pin is not None:
                    new_relay_pins.append(pl_pin)

            # Update central state
            system_state['lanes'] = new_lanes_state
            # Reset dependent state arrays
            last_s_state = [1] * num_lanes
            last_s_trig = [0.0] * num_lanes
            auto_s_state = [1] * num_lanes
            auto_s_trig = [0.0] * num_lanes
            RELAY_PINS, SENSOR_PINS = new_relay_pins, new_sensor_pins
            config_to_save['lanes_config'] = lanes_config # Add new lanes to save data
            lane_map_needs_update = True # Signal QR thread to rebuild map
            restart_needed = True # Changing lanes requires restart for GPIO setup
            logging.warning("[CONFIG] Lanes configuration changed. Application restart required.")
            broadcast_log({"log_type": "warn", "message": "Lanes configuration changed. Restart required!"})
            queue_clear_reason = "Queue cleared due to lane configuration change."
             
        else: # If lanes not in payload, use current lanes for saving
            config_to_save['lanes_config'] = [
                {"name": l['name'], "sensor_pin": l['sensor_pin'],
                 "push_pin": l['push_pin'], "pull_pin": l['pull_pin']}
                for l in system_state['lanes']
            ]
    # --- 3. Save to File (outside lock) ---
    try:
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(config_to_save, f, indent=4, ensure_ascii=False)
        msg = "Configuration saved successfully." + (" Restart required!" if restart_needed else "")
        log_type = "warn" if restart_needed else "success"
        broadcast_log({"log_type": log_type, "message": msg})
        # Return the saved config and restart status
        response_payload = {"message": msg, "config": config_to_save, "restart_required": restart_needed}
    except Exception as e:
        logging.error(f"[CONFIG] Failed to save config via POST: {e}", exc_info=True)
        broadcast_log({"log_type": "error", "message": f"Error saving config: {e}"})
        return jsonify({"error": f"Failed to save config: {e}"}), 500
    if queue_clear_reason:
        clear_qr_queue(queue_clear_reason)

    # Return the saved config and restart status
    return jsonify(response_payload)
@app.route('/api/state')
@require_auth
def route_api_state():
    """API endpoint to GET the current system state."""
    with state_lock:
        # Return a deep copy to prevent modification issues
        state_copy = json.loads(json.dumps(system_state))
    # Add dynamic statuses before returning
    state_copy["maintenance_mode"] = error_manager.is_maintenance()
    state_copy["last_error"] = error_manager.error
    return jsonify(state_copy)
@app.route('/api/queue/reset', methods=['POST'])
@require_auth
def route_api_queue_reset():
    """API endpoint to clear the pending QR queue."""
    user = request.authorization.username
    clear_qr_queue(f"Queue reset by {user} via API.")
    return jsonify({"message": "Queue cleared.", "queue": []})

@app.route('/api/sort_log')
@require_auth
def route_api_sort_log():
    """API endpoint to GET the sort log, summarized by day."""
   
    with sort_log_lock:
        try:
            hourly_data = {}
            if os.path.exists(SORT_LOG_FILE):
                with open(SORT_LOG_FILE, 'r', encoding='utf-8') as f:
                    content = f.read()
                    if content:
                        try: hourly_data = json.loads(content)
                        except json.JSONDecodeError as json_err:
                            logging.error(f"[API] Error decoding {SORT_LOG_FILE}: {json_err}")
                            return jsonify({"error": f"Corrupted sort log file: {json_err}"}), 500

            # Summarize hourly data into daily totals
            return jsonify(summarize_sort_log(hourly_data))
        except Exception as e:
            logging.error(f"[API] Error reading or summarizing sort log: {e}", exc_info=True)
            return jsonify({"error": f"Failed to process sort log: {e}"}), 500

@app.route('/api/reset_maintenance', methods=['POST'])
@require_auth
def route_reset_maintenance():
    """API endpoint to POST a request to reset maintenance mode."""
    user = request.authorization.username
    if error_manager.is_maintenance():
        error_manager.reset() # This broadcasts the update
        broadcast_log({"log_type": "success", "message": f"Maintenance mode reset by {user}."})
        return jsonify({"message": "Maintenance mode reset initiated."})
    else:
        return jsonify({"message": "System is not in maintenance mode."})

# --- WebSocket Route ---
@sock.route('/ws')
# @require_auth # Authentication should be handled within the WS handler for clarity
def route_ws(ws):
    """Handles WebSocket connections for real-time updates and commands."""
    global AUTO_TEST, test_seq_running, lane_map_needs_update

    # --- Authentication Check ---
    auth = request.authorization
    user = auth.username if auth else "Anonymous"
    if not auth or not check_auth(auth.username, auth.password):
        logging.warning(f"[WS] Unauthorized connection attempt from {request.remote_addr}.")
        ws.close(code=1008, reason="Authentication Required")
        return

    # --- Add Client ---
    with clients_lock: clients.add(ws)
    logging.info(f"[WS] Client '{user}' connected from {request.remote_addr}. Total clients: {len(clients)}")

    # --- Send Initial State ---
    try:
        initial_state_snapshot = {}
        with state_lock: initial_state_snapshot = json.loads(json.dumps(system_state))
        initial_state_snapshot["maintenance_mode"] = error_manager.is_maintenance()
        initial_state_snapshot["last_error"] = error_manager.error
        ws.send(json.dumps({"type": "state_update", "state": initial_state_snapshot}))
    except Exception as e:
        logging.warning(f"[WS] Error sending initial state to '{user}': {e}")
        with clients_lock: clients.discard(ws)
        return # Disconnect if initial send fails

    # --- Message Handling Loop ---
    try:
        while True:
            message = ws.receive()
            if message is None: break # Client disconnected gracefully

            try:
                data = json.loads(message)
                action = data.get('action')
                logging.debug(f"[WS] Received action '{action}' from '{user}'")

                # Block most actions if in maintenance mode
                if error_manager.is_maintenance() and action not in ["reset_maintenance", "stop_tests"]:
                     broadcast_log({"log_type": "error", "message": "Action blocked: System in maintenance mode."})
                     continue

                # --- Action Dispatch ---
                if action == 'reset_count':
                    idx = data.get('lane_index')
                    num_lanes = 0
                    with state_lock: num_lanes = len(system_state['lanes'])
                    if idx == 'all':
                        with state_lock:
                            for i in range(num_lanes): system_state['lanes'][i]['count'] = 0
                        broadcast_log({"log_type": "info", "message": f"All counts reset by '{user}'."})
                    elif isinstance(idx, int) and 0 <= idx < num_lanes:
                        with state_lock:
                            name = system_state['lanes'][idx]['name']
                            system_state['lanes'][idx]['count'] = 0
                        broadcast_log({"log_type": "info", "message": f"Count for '{name}' reset by '{user}'."})

                elif action == "test_relay":
                    idx, act = data.get("lane_index"), data.get("relay_action")
                    if idx is not None and act in ["grab", "push"]:
                        executor.submit(run_test_relay_worker, idx, act) # Use thread pool

                elif action == "test_all_relays":
                    executor.submit(run_test_all_relays_worker) # Use thread pool

                elif action == "toggle_auto_test":
                    AUTO_TEST = data.get("enabled", False)
                    logging.info(f"[TEST] Auto-Test mode set to {AUTO_TEST} by '{user}'.")
                    status_msg = "ENABLED" if AUTO_TEST else "DISABLED"
                    broadcast_log({"log_type": "warn", "message": f"Auto-Test mode {status_msg} by '{user}'."})
                    if AUTO_TEST:
                        clear_qr_queue("Queue cleared when Auto-Test mode enabled.")
                    else:
                        reset_relays() # Reset relays when turning off
                elif action == "reset_maintenance":
                    if error_manager.is_maintenance():
                        error_manager.reset() # Broadcasts update internally
                        broadcast_log({"log_type": "success", "message": f"Maintenance mode reset by '{user}'."})
                    else:
                        broadcast_log({"log_type": "info", "message": "System not in maintenance mode."})

                elif action == "mock_trigger_pin" and isinstance(GPIO, MockGPIO):
                    pin, val, dur = data.get("pin"), data.get("value"), data.get("duration", 0.5)
                    if pin is not None and val is not None and isinstance(dur, (int, float)) and dur > 0:
                        executor.submit(trigger_mock_pin, pin, val, dur) # Use thread pool
                    else: logging.warning(f"[MOCK] Invalid trigger command via WS: {data}")

                elif action == "stop_tests":
                    with test_seq_lock:
                        if test_seq_running:
                            test_seq_running = False # Set flag to stop
                            logging.info(f"[TEST] Sequential test stop requested by '{user}'.")
                            broadcast_log({"log_type": "warn", "message": f"Stop test command issued by '{user}'."})
                        else:
                            broadcast_log({"log_type": "info", "message": "No sequential test running to stop."})
                # else: logging.debug(f"[WS] Unknown action '{action}' from '{user}'")

            except json.JSONDecodeError:
                logging.warning(f"[WS] Received invalid JSON from '{user}': {message[:100]}...")
            except Exception as loop_err:
                logging.error(f"[WS] Error processing message from '{user}': {loop_err}", exc_info=True)

    except Exception as conn_err:
        # Log unexpected connection errors (like abrupt close)
        if "close" not in str(conn_err).lower(): # Avoid logging standard closes as errors
            logging.warning(f"[WS] WebSocket connection error/closed for '{user}': {conn_err}")
    finally:
        # --- Remove Client ---
        with clients_lock: clients.discard(ws)
        logging.info(f"[WS] Client '{user}' disconnected. Total clients: {len(clients)}")

# =============================
#         MAIN EXECUTION
# =============================
if __name__ == "__main__":
    try:
        # --- Setup Logging ---
        log_format = '%(asctime)s [%(levelname)s] (%(threadName)s) %(message)s'
        logging.basicConfig(level=logging.INFO, format=log_format,
                            handlers=[logging.FileHandler(LOG_FILE, encoding='utf-8'),
                                      logging.StreamHandler()])
        logging.info("--- SYSTEM STARTING ---")

        # --- Load Configuration ---
        load_config() # Loads timing and lanes config, populates state
        with state_lock: current_gpio_mode = system_state.get("gpio_mode", "BCM")

        # --- Initialize GPIO ---
        if isinstance(GPIO, RealGPIO):
            try: # Critical block: Setup GPIO pins
                GPIO.setmode(GPIO.BCM if current_gpio_mode == "BCM" else GPIO.BOARD)
                GPIO.setwarnings(False)
                logging.info(f"[GPIO] Mode set to: {current_gpio_mode}")
                logging.info(f"[GPIO] Setting up SENSOR pins: {SENSOR_PINS}")
                for pin in SENSOR_PINS: GPIO.setup(pin, GPIO.IN, pull_up_down=GPIO.PUD_UP)
                logging.info(f"[GPIO] Setting up RELAY pins: {RELAY_PINS}")
                for pin in RELAY_PINS: GPIO.setup(pin, GPIO.OUT)
                logging.info("[GPIO] Physical pin setup complete.")
            except Exception as setup_err:
                 logging.critical(f"[CRITICAL] GPIO setup failed: {setup_err}", exc_info=True)
                 error_manager.trigger_maintenance(f"GPIO setup failed: {setup_err}") # Enter maintenance if setup fails
        else: logging.info("[GPIO] Mock mode active, skipping physical pin setup.")

        # --- Reset Relays (if not in maintenance from setup error) ---
        if not error_manager.is_maintenance():
            reset_relays()

        # --- Start Background Threads ---
        thread_targets = {
            "Camera": run_camera, "QRScanner": run_qr_scan, "SensorMon": run_sensor_monitor,
            "StateBcast": broadcast_current_state, "AutoTestMon": run_auto_test_monitor,
            "ConfigSave": save_config_periodically
        }
        threads = { name: threading.Thread(target=func, name=name, daemon=True)
                    for name, func in thread_targets.items() }
        for t in threads.values(): t.start()
        logging.info(f"Started {len(threads)} background threads.")

        # --- System Ready Log ---
        logging.info("="*55 + "\n SORTING SYSTEM READY (v7 - Final)\n" +
                     f" GPIO Mode: {'REAL' if isinstance(GPIO, RealGPIO) else 'MOCK'} (Config: {current_gpio_mode})\n" +
                     f" Log File: {LOG_FILE}, Sort Log: {SORT_LOG_FILE}\n" +
                     f" Access: http://<IP>:5000 (User: {USERNAME} / Pass: ***)\n" + "="*55)

        # --- Start Web Server ---
        host = '0.0.0.0'
        port = 5000
        if serve:
            logging.info(f"Starting Waitress server on {host}:{port}")
            serve(app, host=host, port=port, threads=8) # Use Waitress in production
        else:
            logging.warning("Waitress not found. Falling back to Flask development server (NOT recommended for production).")
            app.run(host=host, port=port) # Use Flask dev server as fallback

    except KeyboardInterrupt:
        logging.info("\n--- SYSTEM SHUTTING DOWN (KeyboardInterrupt) ---")
    except ImportError as import_err:
        logging.critical(f"[CRITICAL] Missing required library: {import_err}.")
    except Exception as startup_err:
        logging.critical(f"[CRITICAL] System startup failed: {startup_err}", exc_info=True)
        # Attempt GPIO cleanup even on startup failure
        try:
            if isinstance(GPIO, RealGPIO): GPIO.cleanup()
        except Exception: pass
    finally:
        # --- Cleanup ---
        main_running = False # Signal threads to stop
        logging.info("Stopping background threads...")
        # Threads are daemons, will exit automatically. Explicit join is not strictly necessary but can be added.
        # for name, t in threads.items():
        #     if t.is_alive(): t.join(timeout=1.0)
        logging.info("Shutting down ThreadPoolExecutor...")
        executor.shutdown(wait=False) # Don't wait indefinitely for tasks
        logging.info("Cleaning up GPIO...")
        try:
            GPIO.cleanup()
            logging.info("GPIO cleanup successful.")
        except Exception as cleanup_err:
            logging.warning(f"Error during GPIO cleanup: {cleanup_err}")
        logging.info("--- SYSTEM SHUTDOWN COMPLETE ---")
