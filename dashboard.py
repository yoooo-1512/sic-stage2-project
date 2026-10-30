"""
Dasbor Monitor Gas LPG berbasis IoT (ESP32 + MQ-2 + MQTT) - Streamlit
Made with Love by templeOS | Samsung Innovation Campus (SIC) Stage 2 IoT
Catatan penting:
- Nilai "persentase" dari ESP32 adalah PEMETAAN ADC (0-4095 -> 0-100), bukan
  konsentrasi gas terkalibrasi (ppm / %LEL). Dashboard ini menyebutnya "Level sinyal sensor".
- Status (AMAN/WASPADA/BAHAYA) ditentukan oleh ESP32 dari ADC
  (WASPADA >= 1200, BAHAYA >= 2500).
Cara Jalankan:  streamlit run dashboard.py
"""

import hashlib
import uuid
import html
import inspect
import io
import json
import math
import os
import threading
import time
from collections import deque
from contextlib import nullcontext
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np
import pandas as pd
import paho.mqtt.client as mqtt
import plotly.graph_objects as go
import requests
import streamlit as st
st.set_page_config(page_title="GasWatch | Monitor LPG", page_icon="◉", layout="wide")
ROOT = Path(__file__).parent
CONFIG = ROOT / "config.json"
DATA_DIR = ROOT / "data"
COLLECTOR_SCHEMA_VERSION = "gaswatch-2.1"
FIXED_MQTT = {
    "mqtt_host": "broker.hivemq.com",
    "mqtt_port": 1883,
    "topic_percentage": "templeOS/gas/persentase",
    "topic_status": "templeOS/gas/status",
    "topic_adc": "templeOS/gas/adc",
}
FIXED_TELEGRAM_BOT_TOKEN = ""
ADC_MAX = 4095
ADC_WASPADA = 1200
ADC_BAHAYA = 2500
PCT_WASPADA = ADC_WASPADA / ADC_MAX * 100   # \~29.3 %
PCT_BAHAYA = ADC_BAHAYA / ADC_MAX * 100     # \~61.1 %
SAMPLE_INTERVAL = 1.0   # ESP32 mengirim tiap 1 detik
STALE_AFTER = 5         # detik tanpa data langsung -> dianggap usang
GAP_SECONDS = 10        # jeda > ini memutus satu periode status
PAIR_MAX_AGE = 5        # detik: umur maksimal persentase untuk dipasangkan dg status
MAX_ROWS = 50000        # batas baris CSV; dipangkas otomatis
TRIM_TO = 40000
TREND_EPS = 1.5         # poin %/menit; di bawah ini dianggap stabil
VALID_STATUSES = ("AMAN", "WASPADA", "BAHAYA")
STATUS_COLORS = {"AMAN": "#16a34a", "WASPADA": "#d97706", "BAHAYA": "#dc2626"}
CYAN = "#0891b2"
DEFAULTS = {
    "mqtt_host": "broker.hivemq.com",
    "mqtt_port": 1883,
    "topic_percentage": "templeOS/gas/persentase",
    "topic_status": "templeOS/gas/status",
    "topic_adc": "templeOS/gas/adc",
    "telegram_enabled": False,
    "telegram_bot_token": "",  # Set TELEGRAM_BOT_TOKEN in the server environment
    "telegram_chat_id": "0", # ID Chat Telegram
    "telegram_cooldown_seconds": 60,
    "telegram_notify_recovery": True,
}
PLOTLY_CONFIG = {
    "displayModeBar": True,
    "displaylogo": False,
    "modeBarButtonsToRemove": ["lasso2d", "select2d"],
    "toImageButtonOptions": {"format": "png", "filename": "Grafik_Gas_IoT", "height": 720, "width": 1280, "scale": 2},
}
WINDOWS = {"5 menit": 300, "15 menit": 900, "1 jam": 3600, "6 jam": 21600, "Semua": None}
def _stretch(fn):
    """ Menyesuaikan argumen lebar komponen dengan versi Streamlit yang digunakan. """
    try:
        if "width" in inspect.signature(fn).parameters:
            return {"width": "stretch"}
    except (TypeError, ValueError):
        pass
    return {"use_container_width": True}
PLOT_W = _stretch(st.plotly_chart)
DF_W = _stretch(st.dataframe)
BTN_W = _stretch(st.button)
DL_W = _stretch(st.download_button)
FSB_W = _stretch(st.form_submit_button)
st.markdown("""
<style>
html,body,.stApp { font-family:Inter,"Segoe UI",Arial,sans-serif; color:#14263d}
.stApp { background:#f3f6fa}
.block-container { padding-top:2rem; padding-bottom:3rem; max-width:1440px}
section[data-testid="stSidebar"] { background:#fff; border-right:1px solid #e3eaf2}
section[data-testid="stSidebar"] .block-container { padding-top:2rem}
h1 {
    font-size:2rem!important;
    letter-spacing:-.055rem;
    font-weight:750!important;
    color:#14263d
}
h2,h3 {
    letter-spacing:-.02rem;
    color:#14263d
}
[data-testid="stMetric"] {
    background:white;
    border:1px solid #e3eaf2;
    border-radius:16px;
    padding:18px
}
[data-testid="stMetricLabel"] { color:#66778d }
[data-testid="stVerticalBlockBorderWrapper"] { border-radius:16px }
.brand {
    font-size:1.5rem;
    font-weight:850;
    letter-spacing:.12rem;
    color:#102c42;
    margin-bottom:2px
}
.brand span { color:#0891b2 }
.brand-sub {
    color:#718096;
    font-size:.78rem;
    margin-bottom:30px
}
.eyebrow {
    font-size:.7rem;
    font-weight:750;
    letter-spacing:.15em;
    text-transform:uppercase;
    color:#718096
}
.hero {
    border:1px solid var(--line);
    border-left:5px solid var(--tone);
    background:var(--bg);
    border-radius:18px;
    padding:26px 30px;
    margin:16px 0 22px
}
.hero-top {
    display:flex;
    align-items:center;
    justify-content:space-between;
    gap:12px;
    flex-wrap:wrap
}
.badge {
    display:inline-block;
    background:white;
    border:1px solid var(--line);
    border-radius:100px;
    padding:6px 12px;
    font-size:.75rem;
    font-weight:650;
    color:var(--tone)
}
.hero h2 {
    font-size:2.15rem!important;
    font-weight:800!important;
    color:var(--tone);
    margin:10px 0 5px;
    padding:0
}
.hero p {
    margin:0;
    color:#40546b;
    font-size:.95rem;
    max-width:850px
}
.hero-foot {
    margin-top:18px;
    font-size:.77rem;
    color:#60768b;
    border-top:1px solid var(--line);
    padding-top:13px
}
.kpi {
    background:white;
    border:1px solid #e2e9f1;
    border-radius:16px;
    padding:20px;
    min-height:142px;
    margin-bottom:8px
}
.kpi .l {
    font-size:.76rem;
    color:#687c90;
    font-weight:600
}
.kpi .v {
    font-size:1.85rem;
    font-weight:750;
    line-height:1.6;
    letter-spacing:-.04rem;
    color:var(--c,#102c42)
}
.kpi .s {
    font-size:.75rem;
    color:#718096;
    line-height:1.5;
    overflow-wrap:anywhere
}
.note {
    background:#eaf3f8;
    border-radius:12px;
    padding:14px 16px;
    font-size:.82rem;
    line-height:1.6;
    color:#426077;
    margin:12px 0
}
.state-row {
    padding:14px 0;
    border-bottom:1px solid #e7edf3;
    display:flex;
    justify-content:space-between;
    gap:10px;
    font-size:.84rem
}
.state-row b { color:var(--c) }
.meter {
    height:8px;
    border-radius:10px;
    background:#e5edf3;
    margin:16px 0 10px;
    overflow:hidden
}
.meter-fill {
    height:100%;
    background:var(--c);
    border-radius:10px
}
.empty {
    border:1px dashed #cbd9e5;
    background:#f9fbfd;
    border-radius:14px;
    text-align:center;
    padding:58px 25px;
    color:#60768b
}
.empty b {
    display:block;
    color:#233e56;
    font-size:1.05rem;
    margin-bottom:8px
}
@media(max-width:700px) {
  .block-container { padding-top:1rem }
  .hero { padding:20px }
  .hero h2 { font-size:1.7rem!important }
  .kpi { min-height:110px; padding:16px }
  .kpi .v { font-size:1.5rem }
}
</style>
""", unsafe_allow_html=True)
def _to_int(v, default, lo, hi):
    """ Mengonversi nilai ke integer dan membatasi hasil pada rentang yang ditentukan. """
    try:
        return max(lo, min(hi, int(v)))
    except (TypeError, ValueError, OverflowError):
        return default
def _to_bool(value, default=False):
    """Jangan menganggap string 'false' sebagai True."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in ("true", "1", "yes", "on"):
            return True
        if text in ("false", "0", "no", "off", ""):
            return False
    return default

def read_config():
    """ Memuat konfigurasi aplikasi, menerapkan nilai default, dan memvalidasi tipe data. """
    cfg = DEFAULTS.copy()
    try:
        raw = json.loads(CONFIG.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            cfg.update({k: v for k, v in raw.items() if k in DEFAULTS})
    except (OSError, ValueError, UnicodeError):
        pass
    cfg["mqtt_port"] = _to_int(cfg["mqtt_port"], 1883, 1, 65535)
    cfg["telegram_cooldown_seconds"] = _to_int(cfg["telegram_cooldown_seconds"], 60, 10, 86400)
    cfg["telegram_enabled"] = _to_bool(cfg["telegram_enabled"])
    cfg["telegram_notify_recovery"] = _to_bool(cfg["telegram_notify_recovery"], True)
    for k in ("mqtt_host", "topic_percentage", "topic_status", "topic_adc", "telegram_bot_token", "telegram_chat_id"):
        cfg[k] = str(cfg[k] or "").strip()
    cfg.update(FIXED_MQTT)
    return cfg
def write_config(cfg):
    """ Menyimpan preferensi aplikasi ke berkas konfigurasi secara aman melalui berkas sementara. """
    cfg = {**cfg, **FIXED_MQTT}
    cfg["telegram_bot_token"] = read_config().get("telegram_bot_token", "")
    tmp = CONFIG.with_name(f".{CONFIG.name}.{uuid.uuid4().hex}.tmp")
    try:
        tmp.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
        tmp.replace(CONFIG)
    finally:
        tmp.unlink(missing_ok=True)
def get_token(cfg):
    """Token tetap dari server; tidak ada jalur penyuntingan melalui dashboard."""
    if FIXED_TELEGRAM_BOT_TOKEN.strip():
        return FIXED_TELEGRAM_BOT_TOKEN.strip()
    t = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    if t:
        return t
    try:
        t = str(st.secrets.get("TELEGRAM_BOT_TOKEN", "")).strip()
    except Exception:
        t = ""
    return t or cfg.get("telegram_bot_token", "").strip()
def send_telegram(token, chat_id, text, retries=2):
    """ Mengirim pesan Telegram dan menangani kegagalan koneksi dengan percobaan ulang. """
    token = str(token or "").strip()
    chat_id = str(chat_id or "").strip()
    if not token or not chat_id or chat_id == "0":
        return False, "Bot Token dan Chat ID wajib diisi."
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    last = "Tidak diketahui"
    for attempt in range(retries + 1):
        wait = 2 * (attempt + 1)
        try:
            r = requests.post(url, data={"chat_id": str(chat_id).strip(), "text": text, "parse_mode": "HTML"}, timeout=8)
            try:
                body = r.json()
            except ValueError:
                body = {}
            if not isinstance(body, dict):
                body = {}
            if r.status_code == 200 and body.get("ok"):
                return True, "Pesan berhasil dikirim."
            last = str(body.get("description") or f"HTTP {r.status_code}")
            if r.status_code in (400, 401, 403, 404):
                break  # salah konfigurasi, retry tidak membantu
            if r.status_code == 429:
                parameters = body.get("parameters")
                if isinstance(parameters, dict):
                    wait = _to_int(parameters.get("retry_after"), wait, 1, 10)
        except requests.RequestException as exc:
            last = f"Gagal koneksi ({type(exc).__name__})"  # tanpa URL agar token tidak bocor
        if attempt < retries:
            time.sleep(min(float(wait), 10))
    return False, last.replace(token, "***")
class Collector:
    def __init__(self, key):
        """ Menyiapkan kolektor MQTT, status sensor, pencatatan, dan konfigurasi notifikasi. """
        self.schema_version = COLLECTOR_SCHEMA_VERSION
        self.key = key
        self.host, self.port, self.tp, self.ts, self.ta = key
        self.csv_path = source_csv_path(key)
        self.live_times = {"pct": None, "status": None, "adc": None}
        self.pending = set()
        self.inflight = False
        self.lock = threading.RLock()
        self.file_lock = threading.Lock()
        self.connected = False
        self.error = None
        self.storage_error = None
        self.since = None
        self.pct = None
        self.pct_time = None
        self.adc = None
        self.adc_time = None
        self.status = None
        self.last_live = None
        self.msg_count = 0
        self.invalid_count = 0
        self.saved_count = 0
        self.alerts = deque(maxlen=100)
        self.tg = {"enabled": False, "token": "", "chat_id": "", "cooldown": 60, "recovery": True}
        self.last_alert = 0.0
        self.last_alert_status = None
        self.retry_after = 0.0
        self.alert_active = False
        self.client = None
        self._ensure_schema()
        self._start()
    def _start(self):
        """ Memulai koneksi MQTT dan menjalankan loop penerimaan pesan di latar belakang. """
        cid = f"gas-dash-{uuid.uuid4().hex[:16]}"
        try:
            client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=cid)
        except (AttributeError, TypeError):
            client = mqtt.Client(client_id=cid)
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_connect_fail = self._on_connect_fail
        client.on_message = self._on_message
        client.reconnect_delay_set(min_delay=1, max_delay=30)
        self.client = client
        try:
            client.connect_async(self.host, self.port, 45)
            client.loop_start()  # reconnect otomatis, termasuk bila koneksi awal gagal
        except Exception as exc:
            self.error = str(exc)
    def stop(self):
        """ Menghentikan koneksi dan loop MQTT saat kolektor tidak lagi digunakan. """
        try:
            self.client.disconnect()
            self.client.loop_stop()
        except Exception:
            pass
    def _on_connect(self, client, userdata, flags, rc, properties=None):
        """ Menangani hasil koneksi broker dan berlangganan ke topik sensor. """
        code = getattr(rc, "value", rc)
        with self.lock:
            if code == 0:
                self.connected = True
                self.error = None
                self.since = datetime.now()
                try:
                    result, _ = client.subscribe([(self.tp, 0), (self.ts, 0), (self.ta, 0)])
                    if result != mqtt.MQTT_ERR_SUCCESS:
                        self.error = f"Gagal berlangganan topik MQTT (kode {result})."
                except Exception as exc:
                    self.error = f"Gagal berlangganan MQTT: {type(exc).__name__}"
            else:
                self.connected = False
                self.error = str(rc)
    def _on_connect_fail(self, client, userdata):
        """ Mencatat kegagalan ketika klien tidak berhasil terhubung ke broker. """
        with self.lock:
            self.connected = False
            self.error = "Broker tidak dapat dijangkau. Periksa host, port, dan jaringan."
    def _on_disconnect(self, client, userdata, *args):
        """ Memperbarui status koneksi ketika klien terputus dari broker. """
        with self.lock:
            self.connected = False
            self.since = None
            self.live_times = dict.fromkeys(self.live_times)
            self.pending.clear()
    def _on_message(self, client, userdata, msg):
        """ Memvalidasi payload MQTT dan memperbarui data sensor yang diterima. """
        try:
            payload = msg.payload.decode("utf-8", errors="strict").strip()
            now = datetime.now()
            field = {self.tp: "pct", self.ts: "status", self.ta: "adc"}.get(msg.topic)
            if field is None:
                return
            with self.lock:
                if field == "status":
                    value = payload.upper()
                    if value not in VALID_STATUSES:
                        raise ValueError("Status tidak dikenal")
                else:
                    value = float(payload)
                    limit = ADC_MAX if field == "adc" else 100
                    if not math.isfinite(value) or not 0 <= value <= limit:
                        raise ValueError("Nilai sensor di luar rentang")
                    if field == "adc":
                        if not value.is_integer():
                            raise ValueError("ADC harus bilangan bulat")
                        value = int(value)
                self.msg_count += 1
                if msg.retain:
                    if self.live_times[field] is None:
                        setattr(self, field, value)
                    return
                setattr(self, field, value)
                self.live_times[field] = now
                self.last_live = now
                if field == "pct":
                    self.pct_time = now
                elif field == "adc":
                    self.adc_time = now
                self.pending.add(field)
                if {"pct", "status"} <= self.pending:
                    times = [self.live_times[k] for k in ("pct", "status")]
                    if abs((times[0] - times[1]).total_seconds()) <= PAIR_MAX_AGE:
                        adc_time = self.live_times["adc"]
                        adc = self.adc if adc_time and (now - adc_time).total_seconds() <= PAIR_MAX_AGE else None
                        try:
                            self._record(now, self.pct, self.status, adc)
                            self.storage_error = None
                        except OSError:
                            self.storage_error = "Riwayat gagal disimpan. Periksa izin dan ruang folder data."
                        self.pending.difference_update({"pct", "status"})
                if field == "status":
                    pct_time = self.live_times["pct"]
                    adc_time = self.live_times["adc"]
                    pct = self.pct if pct_time and (now - pct_time).total_seconds() <= STALE_AFTER else None
                    adc = self.adc if adc_time and (now - adc_time).total_seconds() <= STALE_AFTER else None
                    self._maybe_alert(self.status, pct, now, adc)
        except (ValueError, UnicodeError):
            with self.lock:
                self.invalid_count += 1
        except Exception as exc:
            with self.lock:
                self.error = f"Kesalahan pemrosesan pesan: {type(exc).__name__}"
    def _record(self, ts, pct, status, adc=None):
        """ Mencatat pembacaan sensor ke riwayat dengan format data yang konsisten. """
        adc_txt = "" if adc is None else str(adc)
        row = f"{ts.isoformat(sep=' ', timespec='seconds')},{pct:.2f},{status},{adc_txt}\n"
        with self.file_lock:
            self.csv_path.parent.mkdir(parents=True, exist_ok=True)
            new = not self.csv_path.exists() or self.csv_path.stat().st_size == 0
            with self.csv_path.open("a", encoding="utf-8", newline="") as f:
                if new:
                    f.write("timestamp,percentage,status,adc\n")
                f.write(row)
            self.saved_count += 1
            if self.saved_count % 1000 == 0:
                self._trim()
    def _trim(self):
        """ Membatasi ukuran berkas riwayat agar penggunaan penyimpanan tetap terkendali. """
        try:
            lines = self.csv_path.read_text(encoding="utf-8").splitlines()
            if len(lines) > MAX_ROWS + 1:
                self.csv_path.write_text("\n".join([lines[0]] + lines[-TRIM_TO:]) + "\n", encoding="utf-8")
        except OSError:
            pass
    def _ensure_schema(self):
        """ Memastikan berkas riwayat memiliki kolom dan struktur yang diperlukan. """
        with self.file_lock:
            try:
                if not self.csv_path.exists() or self.csv_path.stat().st_size == 0:
                    return
                lines = self.csv_path.read_text(encoding="utf-8").splitlines()
                if lines and not lines[0].strip().endswith("adc"):
                    out = [lines[0].strip() + ",adc"] + [ln + "," for ln in lines[1:] if ln.strip()]
                    self.csv_path.write_text("\n".join(out) + "\n", encoding="utf-8")
            except (OSError, UnicodeError):
                self.storage_error = "Riwayat lama tidak dapat dibaca. Periksa berkas dan izin folder data."
    def clear_history(self):
        """ Menghapus data riwayat sesuai mekanisme penguncian berkas kolektor. """
        with self.file_lock:
            try:
                if self.csv_path.exists():
                    self.csv_path.unlink()
            except OSError:
                return False
            self.saved_count = 0
        return True
    def update_telegram(self, tg):
        """ Memperbarui pengaturan tujuan dan perilaku notifikasi Telegram. """
        with self.lock:
            self.tg = dict(tg)
    def _maybe_alert(self, status, pct, now, adc=None):
        """ Menentukan apakah perubahan status sensor perlu memicu notifikasi. """
        tg = self.tg
        if not tg["enabled"]:
            return
        t = time.time()
        if self.inflight:
            return
        if t < self.retry_after:
            return
        level = "—" if pct is None else f"{pct:.0f}%"
        adc_txt = "—" if adc is None else str(adc)
        stamp = now.strftime("%d/%m/%Y %H:%M:%S")
        if status in ("WASPADA", "BAHAYA"):
            escalate = status == "BAHAYA" and self.last_alert_status != "BAHAYA"
            cooled = not self.alert_active or t - self.last_alert >= tg["cooldown"]
            if not (escalate or cooled):
                return
            title = "🚨 <b>PERINGATAN BAHAYA</b>" if status == "BAHAYA" else "⚠️ <b>PERINGATAN WASPADA</b>"
            text = (f"{title}\n━━━━━━━━━━━━━━\n📊 <b>ADC:</b> {adc_txt}\n📈 <b>Level Sinyal:</b> {level}\n🔎 <b>Status:</b> {status}\n🕒 <b>Waktu:</b> {stamp}\n━━━━━━━━━━━━━━\n⚡ Segera periksa lokasi. Hindari api/percikan listrik dan buka ventilasi.")
            self._dispatch(text, "peringatan", status, t)
        else:  # AMAN
            if self.alert_active and tg["recovery"]:
                text = f"✅ <b>KONDISI KEMBALI AMAN</b>\n━━━━━━━━━━━━━━\n📊 <b>ADC:</b> {adc_txt}\n📈 <b>Level Sinyal:</b> {level}\n🕒 <b>Waktu:</b> {stamp}\n━━━━━━━━━━━━━━\n🟢 Pemantauan sensor kembali berjalan."
                self._dispatch(text, "pulih", status, t)
            else:
                self.alert_active = False
                self.last_alert_status = "AMAN"
    def _dispatch(self, text, kind, status, t):
        """ Menjadwalkan pengiriman notifikasi tanpa menahan proses penerimaan sensor. """
        self.inflight = True
        self.retry_after = t + 15  # agar tidak spam saat kirim gagal / sedang berjalan
        try:
            threading.Thread(target=self._send_worker, args=(dict(self.tg), text, kind, status, t),
                             daemon=True).start()
        except RuntimeError:
            self.inflight = False
            self.error = "Pekerja notifikasi tidak dapat dimulai."
    def _send_worker(self, tg, text, kind, status, t):
        """ Mengirim notifikasi di thread pekerja dan memperbarui hasil pengiriman. """
        try:
            ok, detail = send_telegram(tg["token"], tg["chat_id"], text)
        except Exception as exc:
            ok, detail = False, f"Pengiriman gagal ({type(exc).__name__})"
        with self.lock:
            self.inflight = False
            self.alerts.appendleft({"waktu": datetime.now(), "jenis": kind, "status": status,
                                    "hasil": "Terkirim" if ok else "Gagal", "detail": detail})
            if ok:
                self.retry_after = 0.0
                self.last_alert = t
                self.last_alert_status = status
                self.alert_active = kind != "pulih"
            status_time = self.live_times["status"]
            now = datetime.now()
            if status_time and (now - status_time).total_seconds() <= STALE_AFTER and self.status != status:
                self._maybe_alert(self.status, None, now, None)
    def snapshot(self):
        """ Mengambil salinan status kolektor untuk digunakan oleh antarmuka dashboard. """
        with self.lock:
            return {
                "live_times": dict(self.live_times),
                "connected": self.connected, "error": self.error or self.storage_error, "since": self.since,
                "pct": self.pct, "status": self.status, "last_live": self.last_live,
                "has_retained": self.last_live is None and (self.pct is not None or self.status is not None),
                "msg_count": self.msg_count, "invalid_count": self.invalid_count,
                "alerts": list(self.alerts), "host": self.host, "port": self.port,
                "tp": self.tp, "ts": self.ts, "ta": self.ta, "adc": self.adc, "adc_time": self.adc_time,
                "tg_enabled": self.tg["enabled"],
            }
@st.cache_resource
def _holder():
    """ Menyediakan penyimpanan kolektor bersama agar tidak dibuat berulang kali. """
    return {"c": None, "lock": threading.Lock()}
def source_csv_path(key):
    """ Membuat jalur berkas CSV berdasarkan identitas sumber data. """
    return DATA_DIR / ("gas_" + hashlib.sha256(repr(key).encode()).hexdigest()[:12] + ".csv")
def collector_compatible(collector):
    """ Memeriksa apakah kolektor aktif memiliki versi skema dan atribut yang sesuai. """
    required = ("csv_path", "live_times", "pending", "inflight", "lock", "file_lock")
    return (getattr(collector, "schema_version", None) == COLLECTOR_SCHEMA_VERSION
            and all(hasattr(collector, name) for name in required))
def get_collector(cfg):
    """ Mengambil atau membuat satu kolektor MQTT untuk konfigurasi sumber yang ditetapkan. """
    h = _holder()
    fixed = FIXED_MQTT
    key = (fixed["mqtt_host"], int(fixed["mqtt_port"]), fixed["topic_percentage"], fixed["topic_status"], fixed["topic_adc"])
    with h["lock"]:
        current = h.get("c")
        if not collector_compatible(current) or getattr(current, "key", None) != key:
            if current is not None:
                try:
                    current.stop()
                except Exception:
                    pass
            h["c"] = Collector(key)
        return h["c"]
def empty_history():
    """ Membuat DataFrame riwayat kosong dengan tipe kolom yang konsisten. """
    return pd.DataFrame({"timestamp": pd.Series(dtype="datetime64[ns]"),
                         "percentage": pd.Series(dtype="float"),
                         "status": pd.Series(dtype="object"),
                         "adc": pd.Series(dtype="float")})
@st.cache_data(show_spinner=False, max_entries=4)
def _load_csv(path, sig):
    """ Memuat, membersihkan, dan memvalidasi data riwayat dari CSV. """
    try:
        df = pd.read_csv(path, on_bad_lines="skip")
        for col in ("timestamp", "percentage", "status", "adc"):
            if col not in df.columns:
                df[col] = None
        def parse_timestamp(value):
            try:
                stamp = pd.Timestamp(value)
                if stamp.tzinfo is not None:
                    stamp = stamp.tz_convert(datetime.now().astimezone().tzinfo).tz_localize(None)
                return stamp
            except (ValueError, TypeError, OverflowError):
                return pd.NaT
        df["timestamp"] = pd.to_datetime(df["timestamp"].map(parse_timestamp), errors="coerce")
        df["percentage"] = pd.to_numeric(df["percentage"], errors="coerce")
        df["adc"] = pd.to_numeric(df["adc"], errors="coerce").astype(float)
        df["status"] = df["status"].fillna("").astype(str).str.strip().str.upper()
        df = df.dropna(subset=["timestamp", "percentage"])
        df = df[df["percentage"].between(0, 100) & np.isfinite(df["percentage"])].copy()
        valid_adc = df["adc"].between(0, ADC_MAX) & np.isfinite(df["adc"]) & (df["adc"] % 1 == 0)
        df.loc[~valid_adc, "adc"] = np.nan
        df = df[df["status"].isin(VALID_STATUSES)]
        return df[["timestamp", "percentage", "status", "adc"]].sort_values("timestamp").tail(MAX_ROWS).reset_index(drop=True)
    except (OSError, ValueError, UnicodeError, pd.errors.ParserError):
        return empty_history()
def read_history(col):
    """ Membaca riwayat sensor dari CSV dengan perlindungan terhadap akses bersamaan. """
    path = getattr(col, "csv_path", None)
    if path is None:
        key = getattr(col, "key", None)
        candidate = source_csv_path(key) if key is not None else None
        path = candidate if candidate is not None and candidate.exists() else DATA_DIR / "gas_history.csv"
    path = Path(path)
    with getattr(col, "file_lock", None) or nullcontext():
        try:
            stt = path.stat()
        except OSError:
            return empty_history()
        return _load_csv(str(path), (stt.st_mtime_ns, stt.st_size))
def build_segments(df):
    """ Mengelompokkan pembacaan berurutan menjadi periode status sensor. """
    cols = ["mulai", "akhir", "status", "durasi_detik", "maks", "rata", "sampel"]
    if df.empty:
        return pd.DataFrame(columns=cols)
    d = df.sort_values("timestamp").reset_index(drop=True)
    gap = d["timestamp"].diff().dt.total_seconds().fillna(0) > GAP_SECONDS
    chg = d["status"] != d["status"].shift()
    d["grp"] = (gap | chg).cumsum()
    seg = d.groupby("grp").agg(mulai=("timestamp", "min"), akhir=("timestamp", "max"),
                               status=("status", "first"), maks=("percentage", "max"),
                               rata=("percentage", "mean"), sampel=("percentage", "size")).reset_index(drop=True)
    seg["durasi_detik"] = (seg["akhir"] - seg["mulai"]).dt.total_seconds() + SAMPLE_INTERVAL
    return seg[cols]
def status_summary(df, segs):
    """ Menghitung ringkasan durasi dan statistik berdasarkan periode status. """
    rows = []
    for s in VALID_STATUSES:
        sub = df[df["status"] == s]
        if sub.empty:
            continue
        dur = segs.loc[segs["status"] == s, "durasi_detik"].sum() if not segs.empty else 0
        row = {"Status": s, "Sampel": len(sub), "Durasi total": fmt_dur(dur),
               "Min (%)": float(sub["percentage"].min()),
               "Rata-rata (%)": round(float(sub["percentage"].mean()), 1),
               "Maks (%)": float(sub["percentage"].max())}
        adc = sub["adc"].dropna()
        if not adc.empty:
            row.update({"ADC min": int(adc.min()), "ADC rata-rata": int(round(adc.mean())), "ADC maks": int(adc.max())})
        rows.append(row)
    return pd.DataFrame(rows)
def downsample(d, n=2000, col="percentage"):
    """Bila data > n titik, ambil nilai puncak tiap interval agar lonjakan tidak hilang."""
    d = d.reset_index(drop=True)
    if len(d) <= n:
        return d
    step = int(math.ceil(len(d) / n))
    idx = d.groupby(np.arange(len(d)) // step)[col].idxmax()
    return d.loc[sorted(idx.tolist())].reset_index(drop=True)
def fmt_age(sec):
    """ Memformat umur data menjadi teks yang mudah dibaca. """
    if sec is None:
        return "—"
    sec = int(sec)
    if sec < 60:
        return f"{sec} dtk lalu"
    if sec < 3600:
        return f"{sec // 60} mnt {sec % 60} dtk lalu"
    return f"{sec // 3600} jam {(sec % 3600) // 60} mnt lalu"
def fmt_dur(sec):
    """ Memformat durasi dalam satuan waktu yang sesuai untuk ditampilkan. """
    sec = int(round(sec))
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h} jam {m} mnt {s} dtk"
    if m:
        return f"{m} mnt {s} dtk"
    return f"{s} dtk"
def style(fig, height=340):
    """ Menerapkan tata letak dan gaya dasar pada grafik Plotly. """
    fig.update_layout(
        template="plotly_white", height=height, paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#ffffff", margin=dict(l=10, r=10, t=40, b=10),
        font=dict(color="#334155"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig
def add_thresholds(fig, horizontal=True):
    """ Menambahkan garis ambang referensi ke grafik sensor. """
    for pct, name, color in ((PCT_WASPADA, f"Batas WASPADA ≈{PCT_WASPADA:.0f}% (ADC {ADC_WASPADA})", STATUS_COLORS["WASPADA"]),
                             (PCT_BAHAYA, f"Batas BAHAYA ≈{PCT_BAHAYA:.0f}% (ADC {ADC_BAHAYA})", STATUS_COLORS["BAHAYA"])):
        kw = dict(line_dash="dot", line_color=color, annotation_text=name, annotation_font_size=10)
        if horizontal:
            fig.add_hline(y=pct, annotation_position="top left", **kw)
        else:
            fig.add_vline(x=pct, annotation_position="top", **kw)
def plot(fig, key):
    """ Menampilkan grafik Plotly menggunakan konfigurasi dashboard. """
    st.plotly_chart(fig, key=key, config=PLOTLY_CONFIG, theme=None, **PLOT_W)
def chart_trend(win):
    """ Membangun grafik perubahan level sinyal sensor pada rentang waktu terpilih. """
    d = win.copy()
    d["ma"] = d["percentage"].rolling(10, min_periods=1).mean()
    d = downsample(d)
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=d["timestamp"], y=d["percentage"], mode="lines", name="Level sinyal (aktual)",
                             line=dict(color="rgba(8,145,178,.55)", width=1.3), fill="tozeroy",
                             fillcolor="rgba(8,145,178,.08)", hovertemplate="%{y:.0f}%<extra>Aktual</extra>"))
    fig.add_trace(go.Scatter(x=d["timestamp"], y=d["ma"], mode="lines", name="Rata-rata bergerak (10 sampel)",
                             line=dict(color=CYAN, width=3), hovertemplate="%{y:.1f}%<extra>Rata-rata</extra>"))
    for s, c in STATUS_COLORS.items():
        sub = d[d["status"] == s]
        if not sub.empty:
            fig.add_trace(go.Scatter(x=sub["timestamp"], y=sub["percentage"], mode="markers", name=f"Status {s}",
                                     marker=dict(color=c, size=5), hoverinfo="skip"))
    add_thresholds(fig)
    fig.update_yaxes(range=[0, 100], title="Level sinyal sensor (% dari ADC 0–4095)")
    fig.update_xaxes(title="Waktu (jam server)")
    fig.update_layout(hovermode="x unified")
    return style(fig, 300)
def chart_adc(win):
    """ Membangun grafik pembacaan ADC mentah dari sensor. """
    d = win.dropna(subset=["adc"])
    if d.empty:
        return None
    d = downsample(d, col="adc")
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=d["timestamp"], y=d["adc"], mode="lines", name="ADC mentah",
                             line=dict(color=CYAN, width=2), hovertemplate="%{y:.0f}<extra>ADC</extra>"))
    for s, c in STATUS_COLORS.items():
        sub = d[d["status"] == s]
        if not sub.empty:
            fig.add_trace(go.Scatter(x=sub["timestamp"], y=sub["adc"], mode="markers", name=f"Status {s}",
                                     marker=dict(color=c, size=5), hoverinfo="skip"))
    fig.add_hline(y=ADC_WASPADA, line_dash="dot", line_color=STATUS_COLORS["WASPADA"],
                  annotation_text=f"Batas WASPADA ({ADC_WASPADA})", annotation_position="top left",
                  annotation_font_size=10)
    fig.add_hline(y=ADC_BAHAYA, line_dash="dot", line_color=STATUS_COLORS["BAHAYA"],
                  annotation_text=f"Batas BAHAYA ({ADC_BAHAYA})", annotation_position="top left",
                  annotation_font_size=10)
    fig.update_yaxes(range=[0, ADC_MAX], title="Nilai ADC (0-4095)")
    fig.update_xaxes(title="Waktu (jam server)")
    fig.update_layout(hovermode="x unified")
    return style(fig, 300)
def kpi(label, value, sub, color="#0891b2"):
    """ Menampilkan kartu metrik ringkas untuk indikator utama dashboard. """
    return (f'<div class="kpi" style="--c:{color}"><div class="l">{html.escape(label)}</div>'
            f'<div class="v">{html.escape(value)}</div><div class="s">{html.escape(sub)}</div></div>')
@st.cache_data(show_spinner=False, max_entries=4)
def excel_bytes(filtered):
    """ Membuat berkas Excel berisi riwayat dan ringkasan status. """
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        filtered.to_excel(writer, index=False, sheet_name='Riwayat')
        status_summary(filtered, build_segments(filtered)).to_excel(writer, index=False, sheet_name='Ringkasan')
    return buf.getvalue()
def freshness(snap, field, now):
    """ Memeriksa apakah data sensor masih cukup baru untuk dianggap langsung. """
    timestamp = snap['live_times'].get(field)
    return bool(snap['connected'] and timestamp and 0 <= (now - timestamp).total_seconds() <= STALE_AFTER)
def browser_status_alert(state):
    """Tampilkan notifikasi Streamlit saat status berubah ke WASPADA/BAHAYA."""
    previous = st.session_state.get("_browser_alert_status")
    st.session_state["_browser_alert_status"] = state
    if state not in ("WASPADA", "BAHAYA") or state == previous:
        return
    if state == "BAHAYA":
        st.toast("Kondisi gas BAHAYA terdeteksi. Periksa indikator perangkat dan ikuti prosedur keselamatan.",icon="🚨")
    else:
        st.toast("Kondisi gas WASPADA terdeteksi. Periksa sensor dan indikator perangkat.",icon="⚠️")
def status_hero(snap, now):
    """ Menampilkan status sensor terkini beserta penjelasan dan waktu pembaruan. """
    active = freshness(snap, 'status', now)
    state = snap['status'] if active else 'MENUNGGU DATA'
    if not snap['connected']:
        state = 'KONEKSI TERPUTUS'
    messages = {
        'AMAN': ('Sinyal dalam rentang aman', 'ESP32 melaporkan ADC di bawah 1200. Pemantauan tetap berjalan.', '#13835e', '#eef9f4', '#ccebdc'),
        'WASPADA': ('Sinyal sensor meningkat', 'Periksa nilai gas di Wokwi dan respons indikator perangkat. Ambang waspada: ADC 1200.', '#a76509', '#fff8e9', '#f1dfb5'),
        'BAHAYA': ('Alarm kebocoran gas', 'ESP32 melaporkan ADC 2500 atau lebih. Periksa respons alarm di simulator; turunkan nilai gas untuk menguji pemulihan.', '#bb3546', '#fff1f2', '#f4ccd1'),
        'MENUNGGU DATA': ('Kondisi belum terkonfirmasi', 'Broker aktif, tetapi status sensor belum diterima atau sudah lewat 5 detik. Pastikan Wokwi sedang berjalan.', '#526982', '#edf3f9', '#d5e1ee'),
        'KONEKSI TERPUTUS': ('Sensor belum dapat dipantau', 'Dashboard mencoba menyambung ke broker secara otomatis. Periksa jaringan pada halaman Koneksi & bantuan.', '#526982', '#edf3f9', '#d5e1ee'),
    }
    title, desc, tone, bg, line = messages.get(state, messages['MENUNGGU DATA'])
    t = snap['live_times'].get('status')
    stamp = t.strftime('%H:%M:%S • %d %b %Y') if t else 'Belum ada status langsung'
    text = f'''<section class="hero" style="--tone:{tone};--bg:{bg};--line:{line}">
<div class="hero-top"><span class="eyebrow">Kondisi sensor saat ini</span><span class="badge">{html.escape(state)}</span></div>
<h2>{html.escape(title)}</h2><p>{html.escape(desc)}</p>
<div class="hero-foot">Status terakhir diterima: {html.escape(stamp)} · Waktu server · ESP32 / MQ-2</div></section>'''
    st.markdown(text, unsafe_allow_html=True)
    return state, tone
def render(cfg, col, page='Ringkasan'):
    """ Merender halaman dashboard sesuai menu yang sedang dipilih. """
    now = datetime.now()
    snap = col.snapshot()
    hist = read_history(col)
    titles = {'Ringkasan': ('Pemantauan gas', 'Kondisi terbaru, perubahan sinyal, dan aktivitas sensor Anda.'),
              'Riwayat': ('Riwayat pembacaan', 'Telusuri pembacaan terdahulu dan unduh data yang Anda perlukan.'),
              'Koneksi & bantuan': ('Koneksi & bantuan', 'Periksa jalur data Wokwi → MQTT → dashboard.')}
    title, desc = titles.get(page, titles['Ringkasan'])
    st.markdown('<div class="eyebrow">GASWATCH / WOKWI MONITOR</div>', unsafe_allow_html=True)
    st.title(title)
    st.caption(desc)
    state, tone = status_hero(snap, now)
    if snap['error']:
        st.warning(f"Diagnostik: {snap['error']}")
    if page == 'Ringkasan':
        browser_status_alert(state)
        pct_live, adc_live = freshness(snap, 'pct', now), freshness(snap, 'adc', now)
        age = (now - snap['last_live']).total_seconds() if snap['last_live'] else None
        cols = st.columns(4)
        cards = [
            ('Level Sinyal Sensor', f"{snap['pct']:.1f}%" if pct_live and snap['pct'] is not None else 'Menunggu data', 'Pemetaan relatif ADC ke skala 0–100%', CYAN),
            ('ADC Mentah', f"{snap['adc']:.0f}" if adc_live and snap['adc'] is not None else 'Tidak tersedia', 'Skala Pembacaan Sensor (ADC: 0–4095)', '#183e5a'),
            ('Pembaruan Terakhir', fmt_age(age) if age is not None else 'Belum ada data', 'Waktu penerimaan data sensor', '#183e5a'),
            ('Koneksi MQTT', 'Terhubung' if snap['connected'] else 'Terputus', 'Koneksi ke broker MQTT', '#13835e' if snap['connected'] else '#718096')]
        for target, card in zip(cols, cards):
            target.markdown(kpi(*card), unsafe_allow_html=True)
        st.write('')
        left, right = st.columns([2.7, 1], gap='large')
        with left:
            with st.container(border=True):
                st.subheader('Tren pembacaan')
                a, b = st.columns([1.6, 1])
                choice = a.selectbox('Rentang grafik', list(WINDOWS), index=1, key='win_choice')
                measure = b.selectbox('Pembacaan', ['Sinyal sensor (%)', 'ADC mentah'])
                sec = WINDOWS[choice]
                win = hist if sec is None else hist[hist['timestamp'] >= now - timedelta(seconds=sec)]
                if win.empty:
                    st.markdown('<div class="empty"><b>Menunggu pembacaan pertama</b>Jalankan Wokwi. Grafik akan muncul setelah persentase dan status diterima.</div>', unsafe_allow_html=True)
                else:
                    fig = chart_trend(win) if measure == 'Sinyal sensor (%)' else chart_adc(win)
                    if fig is None:
                        st.info('Topik ADC belum mengirim data pada rentang ini.')
                    else:
                        plot(fig, 'trend')
                    st.caption(f'{len(win):,} sampel pada rentang ini. Garis batas menunjukkan ambang firmware.')
        with right:
            with st.container(border=True):
                st.subheader('Panduan status')
                for name, label, color in [('AMAN', 'ADC < 1200', '#13835e'), ('WASPADA', 'ADC 1200–2499', '#a76509'), ('BAHAYA', 'ADC ≥ 2500', '#bb3546')]:
                    st.markdown(f'<div class="state-row" style="--c:{color}"><b>{name}</b><span>{label}</span></div>', unsafe_allow_html=True)
                if pct_live:
                    width = max(0, min(100, snap['pct']))
                    st.markdown(f'<div class="meter" style="--c:{tone}"><div class="meter-fill" style="width:{width:.2f}%"></div></div>', unsafe_allow_html=True)
                    st.caption(f"Sinyal saat ini: {snap['pct']:.1f}%")
                st.markdown('<div class="note">Persentase menunjukkan kekuatan sinyal sensor. Nilai ini bukan konsentrasi LPG dalam ppm atau %LEL.</div>', unsafe_allow_html=True)
            with st.container(border=True):
                st.subheader('Notifikasi')
                st.write('Telegram aktif' if snap['tg_enabled'] else 'Telegram nonaktif')
                if snap['alerts']:
                    latest = snap['alerts'][0]
                    st.caption(f"Terakhir: {latest['hasil']} • {latest['waktu']:%H:%M:%S}")
                else:
                    st.caption('Belum ada notifikasi terkirim.')
                st.caption('Token bot dikelola server dan dikunci.')
        with st.container(border=True):
            st.subheader('Pembacaan terbaru')
            if hist.empty:
                st.caption('Sampel akan tercatat setelah topik persentase dan status diterima langsung.')
            else:
                st.dataframe(history_view(hist.tail(5)), hide_index=True, **DF_W)
        st.caption('Nilai — berarti belum ada data langsung atau data lebih lama dari 5 detik. Status mengikuti ESP32. Proyek simulasi/edukasi.')
    elif page == 'Riwayat':
        with st.container(border=True):
            c1, c2 = st.columns(2)
            selected = c1.multiselect('Status', list(VALID_STATUSES), default=list(VALID_STATUSES))
            period = c2.selectbox('Periode', list(WINDOWS), index=4)
            filtered = hist[hist['status'].isin(selected)]
            if WINDOWS[period] is not None:
                filtered = filtered[filtered['timestamp'] >= now - timedelta(seconds=WINDOWS[period])]
            st.caption(f'{len(filtered):,} sampel sesuai filter')
            if filtered.empty:
                st.info('Tidak ada data sesuai filter atau riwayat masih kosong.')
            else:
                st.dataframe(history_view(filtered), hide_index=True, height=380, **DF_W)
                stamp = now.strftime('%Y%m%d_%H%M%S')
                a, b = st.columns(2)
                a.download_button('Unduh CSV', filtered.to_csv(index=False).encode('utf-8-sig'), file_name=f'GasWatch_{stamp}.csv', mime='text/csv', **DL_W)
                try:
                    b.download_button('Unduh Excel', excel_bytes(filtered), file_name=f'GasWatch_{stamp}.xlsx', mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', **DL_W)
                except ImportError:
                    b.caption('Pasang openpyxl untuk mengekspor Excel.')
        with st.expander('Log notifikasi Telegram', expanded=False):
            if snap['alerts']:
                st.dataframe(pd.DataFrame(snap['alerts']), hide_index=True, **DF_W)
            else:
                st.caption('Belum ada notifikasi sejak aplikasi dijalankan.')
        with st.expander('Kelola penyimpanan lokal'):
            confirm = st.checkbox('Saya memahami semua riwayat sumber ini akan dihapus.')
            if st.button('Hapus riwayat', disabled=not confirm):
                if col.clear_history():
                    _load_csv.clear()
                    excel_bytes.clear()
                    st.rerun()
                else:
                    st.error('Riwayat tidak dapat dihapus. Periksa izin folder data.')
    else:
        with st.container(border=True):
            st.subheader('Koneksi MQTT dikunci')
            st.caption('Broker, port, dan topik dikelola administrator. Pengguna dashboard hanya dapat melihat statusnya.')
            a, b = st.columns(2)
            a.metric('Broker', snap['host'])
            b.metric('Port TCP', str(snap['port']))
            rows = []
            for field, topic, example in [('adc', snap['ta'], '1800'), ('pct', snap['tp'], '43.96'), ('status', snap['ts'], 'WASPADA')]:
                t = snap['live_times'].get(field)
                rows.append({'Topik': topic, 'Payload contoh': example, 'Status penerimaan': 'Aktif' if freshness(snap, field, now) else 'Belum ada / usang', 'Terakhir': t.strftime('%H:%M:%S') if t else '—'})
            st.dataframe(pd.DataFrame(rows), hide_index=True, **DF_W)
            st.caption(f"Pesan valid: {snap['msg_count']} • Pesan ditolak: {snap['invalid_count']}")
        with st.container(border=True):
            st.subheader('Mulai dari Wokwi')
            st.markdown('1. Jalankan ESP32 dan sensor MQ-2 di Wokwi.\n2. Gunakan Wi-Fi *Wokwi-GUEST*, tanpa kata sandi.\n3. Samakan broker, port, dan topik pada firmware dengan tabel di atas.\n4. Kirim ADC, persentase, dan status setiap *1 detik*, meskipun nilainya tetap.\n5. Ubah nilai gas simulator dan amati hasilnya di Ringkasan.')
        with st.expander('Broker terhubung, tetapi data tidak muncul'):
            st.write('Periksa ejaan topik dan pastikan simulator berjalan. Gunakan payload teks biasa (bukan JSON). Pesan retained hanya data tersimpan; dashboard menunggu pesan langsung. Periksa setiap topik pada tabel di atas.')
        with st.expander('Nilai hilang setelah simulator dihentikan'):
            st.write('Data yang lewat 5 detik tidak ditampilkan sebagai nilai terkini. Riwayat tetap tersedia. Ini mencegah status lama disalahartikan sebagai kondisi sekarang.')
        with st.expander('Bagaimana token Telegram dikunci?'):
            st.write('Token dibaca dari konfigurasi server dan tidak dikirim ke formulir atau elemen tampilan. Pengguna dapat mengatur tujuan Chat ID serta notifikasi melalui Preferensi, tanpa mengganti token bot.')
        st.caption('Semua waktu mengikuti jam server. Pencatatan dan alarm membutuhkan proses Python tetap berjalan.')
def history_view(frame):
    """ Menyiapkan dan memformat tabel riwayat agar siap ditampilkan. """
    shown = frame.sort_values('timestamp', ascending=False).copy()
    shown['adc'] = shown['adc'].astype('Int64')
    shown['timestamp'] = shown['timestamp'].dt.strftime('%d/%m/%Y %H:%M:%S')
    return shown[['timestamp', 'status', 'adc', 'percentage']].rename(columns={
        'timestamp': 'Waktu server', 'status': 'Status', 'adc': 'ADC', 'percentage': 'Sinyal (%)'})
def render_preferences(cfg, token):
    """ Merender formulir preferensi penerima dan notifikasi Telegram. """
    st.markdown('<div class="eyebrow">GASWATCH / PREFERENSI</div>', unsafe_allow_html=True)
    st.title('Preferensi notifikasi')
    st.caption('Atur penerima dan frekuensi pesan. Koneksi MQTT dan token bot tetap dikunci.')
    with st.container(border=True):
        st.subheader('Bot Telegram')
        if token:
            st.success('Token server tersedia • terkunci')
        else:
            st.warning('Token server belum diisi. Administrator perlu mengaturnya sekali di server.')
        st.caption('Token tidak ditampilkan dan tidak dapat diganti melalui dashboard.')
        with st.form('notifications'):
            enabled = st.toggle('Aktifkan notifikasi Telegram', value=cfg['telegram_enabled'])
            chat = st.text_input('Chat ID penerima', value=cfg['telegram_chat_id'], help='ID chat atau grup penerima pesan dari bot yang telah ditetapkan.')
            cooldown = st.number_input('Jeda pengulangan peringatan (detik)', min_value=10, max_value=86400, value=cfg['telegram_cooldown_seconds'], step=10)
            recovery = st.toggle('Kirim pesan ketika kembali AMAN', value=cfg['telegram_notify_recovery'])
            if st.form_submit_button('Simpan preferensi', **FSB_W):
                if enabled and not token:
                    st.error('Token bot belum dikonfigurasi oleh administrator.')
                elif enabled and chat.strip() in ('', '0'):
                    st.error('Isi Chat ID penerima yang valid (bukan 0).')
                else:
                    updated = {**cfg, 'telegram_enabled': enabled, 'telegram_chat_id': chat.strip(), 'telegram_cooldown_seconds': int(cooldown), 'telegram_notify_recovery': recovery}
                    try:
                        write_config(updated)
                        st.session_state['flash'] = 'Preferensi notifikasi tersimpan.'
                        st.rerun()
                    except OSError:
                        st.error('Gagal menyimpan preferensi. Periksa izin folder aplikasi.')
        if st.button('Kirim pesan uji', disabled=not (token and cfg['telegram_chat_id'] not in ('', '0'))):
            with st.spinner('Mengirim pesan uji…'):
                ok, detail = send_telegram(token, cfg['telegram_chat_id'], '[UJI] GasWatch: koneksi Telegram berhasil.')
            (st.success if ok else st.error)(detail)
    st.info('Pengaturan berlaku bersama untuk semua pengguna pada proses aplikasi ini.')
def main():
    cfg = read_config()
    token = get_token(cfg)
    col = get_collector(cfg)
    col.update_telegram({'enabled': cfg['telegram_enabled'], 'token': token, 'chat_id': cfg['telegram_chat_id'],
                         'cooldown': cfg['telegram_cooldown_seconds'], 'recovery': cfg['telegram_notify_recovery']})
    with st.sidebar:
        st.markdown('<div class="brand">GAS<span>WATCH</span></div><div class="brand-sub">LPG MONITOR · WOKWI SIMULATOR</div>', unsafe_allow_html=True)
        page = st.radio('Navigasi', ['Ringkasan', 'Riwayat', 'Koneksi & bantuan', 'Preferensi'], label_visibility='collapsed')
        st.divider()
        st.caption('TAMPILAN LANGSUNG')
        auto = st.toggle('Perbarui otomatis', value=True)
        interval = st.select_slider('Interval pembaruan', options=[1, 2, 5, 10], value=2, disabled=not auto)
        if not auto:
            st.button('Perbarui sekarang', **BTN_W)
        st.caption('Pencatatan tetap berjalan saat tampilan dijeda.')
        st.divider()
        st.markdown("*Smart Gas Detection & Safety Monitoring*")
        st.caption(
            "Real-time gas sensor monitoring, hazard status detection, "
            "and safety alerts powered by ESP32 and IoT."
        )
    if st.session_state.get('flash'):
        st.success(st.session_state.pop('flash'))
    if page == 'Preferensi':
        render_preferences(cfg, token)
    else:
        @st.fragment(run_every=f'{interval}s' if auto else None)
        def live_dashboard():
            """ Memperbarui tampilan dashboard secara berkala jika pembaruan otomatis aktif. """
            render(cfg, col, page)
        live_dashboard()


if __name__ == "__main__":
    main()


