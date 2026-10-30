# GasWatch v2 — Monitor LPG untuk Wokwi

## Perubahan versi ini

- UI/UX dirombak: navigasi samping, banner status besar, kartu ringkas, grafik dengan pemilih rentang/pembacaan, panduan ambang, dan lima pembacaan terbaru.
- Empat halaman: **Ringkasan**, **Riwayat**, **Koneksi & bantuan**, serta **Preferensi**.
- **Broker, port, dan seluruh topik MQTT dikunci.** Tidak ada input koneksi; konfigurasi dari `config.json` tidak dapat menimpanya.
- **Token Telegram dikunci dari pengguna dashboard.** Tidak ada formulir token dan nilainya tidak ditampilkan. Pengguna hanya dapat mengatur notifikasi, Chat ID, jeda pengulangan, dan pesan pemulihan.
- Perbaikan `AttributeError: 'Collector' object has no attribute 'csv_path'`: objek cache lama diperiksa versi dan atributnya. Worker lama dihentikan, lalu Collector baru dibuat. Pembacaan riwayat juga menerima objek lama tanpa `csv_path`.

## Memperbarui aplikasi yang sudah ada

1. Hentikan Streamlit dengan **Ctrl+C** di terminal.
2. Cadangkan kode lama. Ganti file `dashboard.py` Anda dengan `dashboard.py` dari paket ini. Salin juga folder `.streamlit` dan `requirements.txt` bila diperlukan.
3. Pertahankan folder `data/`, `config.json`, dan `.streamlit/secrets.toml` milik Anda. Paket ini tidak berisi data pribadi atau token.
4. Jalankan dari folder aplikasi:

```powershell
python -m pip install -r requirements.txt
python -m streamlit run dashboard.py
```

`app.py` disertakan sebagai peluncur kompatibilitas, sehingga `python -m streamlit run app.py` juga dapat digunakan. Jangan menjalankan kedua peluncur bersamaan pada folder data yang sama.

Pemeriksaan versi cache otomatis juga bekerja saat hot reload, tetapi restart sekali setelah mengganti seluruh versi membantu memastikan seluruh worker versi lama telah berhenti.

## Instalasi baru

Gunakan Python 3.10 atau lebih baru. Ekstrak ZIP, buka terminal pada folder `dashboard_lpg`, lalu:

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
.venv\Scripts\python -m streamlit run dashboard.py
```

Buka alamat terminal, biasanya http://localhost:8501.

## MQTT tetap

| Pengaturan | Nilai |
|---|---|
| Broker | broker.hivemq.com |
| Port TCP | 1883 |
| Topik ADC | templeOS/gas/adc |
| Topik persentase | templeOS/gas/persentase |
| Topik status | templeOS/gas/status |

Nilai tersebut mengikuti kode yang Anda kirim. Konstanta `FIXED_MQTT` pada kode server menjadi sumber konfigurasi; pengguna dashboard tidak dapat mengubahnya. Hanya administrator yang memiliki akses file server yang bisa mengubah konstanta tersebut. Broker publik dapat menerima publikasi pengguna lain di topik yang sama.

Pada Wokwi, hubungkan ESP32 ke Wi-Fi **Wokwi-GUEST**, tanpa kata sandi. Samakan firmware dengan tabel di atas dan kirim ketiga topik setiap sekitar **1 detik**, walaupun nilainya tetap. Payload berupa teks biasa, misalnya `1800`, `43.96`, dan `WASPADA`, bukan objek JSON.

Firmware minimal tersedia dalam `contoh_wokwi.ino` dan menggunakan pustaka PubSubClient (`libraries.txt`). AO MQ-2 diasumsikan di GPIO34. Contoh hanya untuk simulasi dan belum mencakup OLED/LED/buzzer; ambil bagian pengiriman MQTT untuk proyek Anda yang sudah ada. Untuk perangkat fisik, sesuaikan rangkaian dengan batas tegangan ADC ESP32.

## Token bot Telegram tetap

Token tidak tersedia dalam lampiran Anda, sehingga paket ini tidak berisi token bot nyata. Jika token sudah disimpan dalam `config.json` versi lama, token itu tetap dibaca dan dipertahankan, tanpa bisa diedit melalui dashboard.

Untuk pemasangan baru, administrator mengisi token sekali di `.streamlit/secrets.toml`:

```toml
TELEGRAM_BOT_TOKEN = "isi-token-bot-Anda"
```

Contoh file tersedia di `.streamlit/secrets.toml.example`. Token tidak perlu dikirim melalui chat ini.

Urutan sumber token server: konstanta `FIXED_TELEGRAM_BOT_TOKEN` jika diisi, variabel lingkungan `TELEGRAM_BOT_TOKEN`, secrets Streamlit, lalu token lama `config.json`. Ini pengaturan server; tidak ada kontrol pengguna untuk menggantinya. Menyimpan preferensi akan mempertahankan token lama, meskipun ada nilai token berbeda pada data pengaturan yang diajukan.

Buka **Preferensi** untuk mengatur Chat ID penerima, mengaktifkan Telegram, mengatur jeda peringatan, dan mencoba pesan uji. Tombol uji mengirim pesan hanya saat diklik. Jangan membagikan secrets atau config yang berisi token.

## Membaca kondisi

| Status ESP32 | ADC |
|---|---|
| AMAN | 0–1199 |
| WASPADA | 1200–2499 |
| BAHAYA | 2500–4095 |

Status mengikuti firmware. Persentase = ADC / 4095 × 100, merupakan sinyal relatif, bukan ppm atau %LEL. Dashboard memeriksa kesegaran status, ADC, dan persentase secara terpisah. Lebih dari 5 detik tanpa data langsung, nilai terkini menjadi **—** dan status menjadi belum terkonfirmasi. Broker terhubung tidak otomatis berarti sensor sedang mengirim.

Pesan retained tidak dianggap kondisi langsung, tidak dicatat, dan tidak memicu notifikasi. Satu sampel riwayat membutuhkan pembaruan persentase dan status, dalam urutan apa pun, dengan jarak maksimum 5 detik. ADC dicatat jika baru diterima dalam 5 detik terakhir. Tiga topik terpisah tanpa ID sampel tidak menjamin ketiga nilai berasal dari siklus yang persis sama.

Semua waktu mengikuti jam server. Data tersimpan dalam `data/gas_<identitas-sumber>.csv`. File `gas_history.csv` lama tidak otomatis digabungkan ke sumber baru; cadangkan secara terpisah. Penyimpanan mempertahankan kira-kira 40–50 ribu sampel per sumber, sehingga ekspor berkala diperlukan untuk arsip panjang.

Pencatatan dan notifikasi membutuhkan proses Python tetap hidup. Menjeda pembaruan tampilan tidak menghentikan pencatatan. Semua sesi pengguna pada satu proses memakai konfigurasi yang sama. Proyek ini untuk simulasi/edukasi.

## Pengujian dan batas verifikasi

```powershell
python -m unittest discover -s tests -v
```

Pengujian mencakup cache Collector versi lama, objek tanpa `csv_path`, penguncian konfigurasi dan token, payload tidak valid, retained, kesegaran per topik, urutan pesan, grafik, ekspor, dan eksekusi render tanpa browser.

Di lingkungan pengerjaan, 18 pengujian lolos. Lima pengujian interaktif Streamlit AppTest dilewati karena socket loopback tidak dapat dibuat. Untuk menjalankan pengujian tanpa AppTest: `$env:SKIP_UI_TESTS = "1"` di PowerShell sebelum perintah pengujian. Pemeriksaan visual browser, koneksi ujung ke ujung dengan proyek Wokwi Anda, dan kompilasi firmware Wokwi belum dilakukan.

## Rujukan resmi

- [Streamlit fragments](https://docs.streamlit.io/develop/api-reference/execution-flow/st.fragment)
- [Paho MQTT Python](https://eclipse.dev/paho/clients/python/)
- [Wi-Fi ESP32 Wokwi](https://docs.wokwi.com/guides/esp32-wifi)
- [MQ-2 Wokwi](https://docs.wokwi.com/parts/wokwi-gas-sensor)
