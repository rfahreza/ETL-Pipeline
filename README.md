# Fashion Products ETL Pipeline

Sebuah pipeline ETL (Extract, Transform, Load) yang dirancang untuk mengekstrak data produk dari situs web fashion, membersihkan dan mengubahnya, lalu memuatnya ke berbagai tujuan data.

## Fitur

- **Ekstraksi Data**: Mengambil data produk dari situs web target menggunakan `requests` dan `BeautifulSoup`, dengan penanganan paginasi dan jeda acak untuk menghindari pemblokiran.
- **Transformasi Data**: Membersihkan dan menstandardisasi data mentah menggunakan `pandas`, termasuk konversi tipe data, penanganan nilai yang hilang, dan konversi harga.
- **Pemuatan Data**: Menyimpan data yang telah ditransformasi ke beberapa tujuan: file CSV, Google Sheets, dan tabel PostgreSQL.
- **Konfigurasi Terpusat**: Mengelola semua kredensial dan parameter (database, API) secara aman menggunakan file `.env`.
- **Logging Komprehensif**: Pencatatan log di setiap tahap (Extract, Transform, Load) untuk memudahkan _debugging_.
- **Pengujian Otomatis**: Dilengkapi dengan _unit tests_ menggunakan `pytest` untuk memastikan setiap komponen berfungsi dengan benar.

## Prasyarat

- Python 3.8+
- Server PostgreSQL yang sedang berjalan

## Instalasi

1.  **Clone Repositori**

2.  **Buat dan Aktifkan Virtual Environment**

    - Di Windows:
      ```bash
      python -m venv .venv
      .\.venv\Scripts\activate
      ```
    - Di macOS/Linux:
      ```bash
      python3 -m venv .venv
      source .venv/bin/activate
      ```

3.  **Konfigurasi Environment**
    Salin file contoh `.env.example` menjadi `.env`. File ini akan digunakan untuk menyimpan semua kredensial Anda.

    ```bash
    cp .env.example .env
    ```

    Selanjutnya, buka file `.env` dan isi semua variabel dengan nilai yang benar (kredensial database, ID Google Sheet, dan path ke file JSON kredensial Google).

4.  **Instal Dependensi**
    ```bash
    pip install -r requirements.txt
    ```

## Menjalankan Pipeline

Jalankan skrip utama dari direktori root proyek.

```
pip install -r requirements.txt
```

## Running

```
# Menjalankan skrip
python main.py


# Menjalankan unit test pada folder tests (-v = verbose/detail)
python -m pytest -v tests


# Menjalankan test coverage pada folder tests

#-- 1. Jalankan test dan catat coverage
coverage run -m pytest tests

#-- 2. Tampilkan laporan di terminal
coverage report

#-- 3. (Opsional) Buat laporan HTML interaktif
coverage html

```
