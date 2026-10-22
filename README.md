# Subaraya batch dan streaming ETL

Proyek ini menggunakan data lalu lintas, kualitas udara, dan kecelakaan untuk
mensimulasikan sistem analitik transportasi Kota Subaraya.

Nama teknis seperti `metropolia_warehouse`, `metropolia.events`, dan
`metropolia_daily_batch` tetap digunakan di dalam kode untuk menjaga
kompatibilitas dengan konfigurasi yang sudah dijalankan.

## Periode simulasi

- Historical backfill: 1 Januari 2022 sampai 31 Agustus 2025.
- Daily batch demo: 1 sampai 7 September 2025.
- Streaming cutover: mulai 8 September 2025.

Data traffic dan kualitas udara dari 1 Maret 2013 sampai 28 Februari 2017
digeser ke periode simulasi 2022 sampai 2025. Data kecelakaan menggunakan
tahun 2022 sampai 2025 tanpa pergeseran tahun.

Lokasi sumber dipetakan menjadi empat zona sintetis:

```text
Z01
Z02
Z03
Z04
```

## Arsitektur

### ETL batch

```text
Raw data
    -> Airflow
    -> Validasi input
    -> PySpark batch
    -> Staging PostgreSQL
    -> UPSERT fact
    -> Mart
```

Airflow menjalankan validasi, transformasi PySpark, dan pemuatan ke PostgreSQL.
Data staging ditulis ulang pada setiap batch. Data fact dan mart menggunakan
UPSERT agar proses dapat dijalankan ulang tanpa menggandakan baris.

### ETL streaming

```text
Manual/automatic producer
    -> Apache Kafka
    -> Spark Structured Streaming
    -> UPSERT fact
    -> Mart
```

Producer membentuk pesan JSON dan mengirimkannya ke topic Kafka
`metropolia.events`. Consumer PySpark membaca event dalam micro-batch,
memvalidasi dan mentransformasi data, lalu memperbarui fact table dan
`mart_metropolia_hourly`.

Streaming tidak menggunakan staging table batch.

## Struktur proyek

```text
metropolia-etl/
├── dags/
│   └── metropolia_pipeline_dag.py
├── data/
│   ├── raw/
│   ├── reference/
│   ├── batch/
│   ├── canonical/
│   ├── quarantine/
│   └── stream/
├── models/
├── scripts/
│   ├── check_input.py
│   └── spark_metropolia_etl.py
├── sql/
│   ├── init.sql
│   ├── init_streaming.sql
│   └── upsert_staging.sql
├── streaming/
│   ├── manual_producer.py
│   ├── automatic_producer.py
│   ├── spark_streaming_metropolia.py
│   ├── Dockerfile.producer
│   └── Dockerfile.spark
├── tests/
├── .env.example
├── .gitignore
├── docker-compose.yml
├── README.md
└── STREAMING_DEMO.md
```

## Fungsi komponen

- `data/raw`: dataset sumber sebelum transformasi.
- `data/reference`: kalender simulasi, periode pipeline, dan pemetaan zona.
- `scripts/check_input.py`: memeriksa file dan kolom yang dibutuhkan.
- `scripts/spark_metropolia_etl.py`: menjalankan ETL batch menggunakan PySpark.
- `dags/metropolia_pipeline_dag.py`: mendefinisikan DAG backfill dan daily batch.
- `sql/init.sql`: membuat warehouse, fact table, mart, dan modeling view.
- `sql/upsert_staging.sql`: memindahkan data staging ke fact dan mart.
- `sql/init_streaming.sql`: membuat tabel audit streaming.
- `streaming/manual_producer.py`: mengirim satu event secara manual.
- `streaming/automatic_producer.py`: menyimulasikan sensor secara periodik.
- `streaming/spark_streaming_metropolia.py`: consumer dan transformasi streaming.
- `models`: area kerja modeling yang dikelola anggota data science.
- `STREAMING_DEMO.md`: petunjuk lengkap demonstrasi ETL streaming.

## Prasyarat

1. Docker Desktop dengan Docker Compose.
2. Minimal sekitar 8 GB RAM tersedia untuk Docker.
3. Port 5432, 8080, dan 9092 tidak digunakan aplikasi lain.
4. Internet pada build pertama untuk mengunduh image dan dependency.

## Persiapan setelah clone

Buka CMD atau PowerShell pada folder repository.

Buat `.env` dari contoh konfigurasi:

```cmd
copy .env.example .env
```

File `.env` tidak diunggah ke GitHub.

Jika port tertentu sudah digunakan, ubah nilainya di `.env`.

## Validasi data input

Validasi dapat dijalankan sebelum Docker:

```cmd
python scripts/check_input.py --data-root data
```

Hasil yang benar memiliki:

```text
status             : OK
traffic_files      : 1
air_quality_files  : 12
accident_files     : 4
```

## Menjalankan seluruh infrastruktur

```cmd
docker compose up -d --build
```

Periksa status container:

```cmd
docker compose ps
```

Build pertama dapat berlangsung lebih lama karena Docker mengunduh image,
package Python, dan JAR Spark.

## Akses Airflow

Buka:

```text
http://localhost:8080
```

Gunakan:

```text
Username: admin
Password: admin
```

DAG yang tersedia:

```text
metropolia_historical_backfill
metropolia_daily_batch
```

## Menjalankan historical backfill

Historical backfill dijalankan satu kali melalui Airflow:

1. Buka DAG `metropolia_historical_backfill`.
2. Aktifkan DAG jika masih paused.
3. Tekan `Trigger DAG`.
4. Tunggu task berikut selesai:

```text
validate_backfill_inputs
    -> transform_historical_data
    -> upsert_historical_warehouse
```

Backfill memproses periode 1 Januari 2022 sampai 31 Agustus 2025.

## Daily batch

Daily batch menggunakan alur:

```text
validate_daily_inputs
    -> transform_daily_data
    -> upsert_daily_warehouse
```

Pada pelaksanaan tugas, DAG dijadwalkan pukul 14.00 WIB dan memetakan tanggal
eksekusi aktual ke tanggal simulasi 1 sampai 7 September 2025.

Konfigurasi runtime pada DAG menggunakan tanggal tetap. Oleh karena itu, clone
baru yang dijalankan setelah periode demonstrasi tidak otomatis membuat ulang
tujuh scheduled run. Untuk laporan dan presentasi, gunakan hasil eksekusi yang
sudah direkam. Jika jadwal perlu disimulasikan kembali, sesuaikan nilai berikut
di `dags/metropolia_pipeline_dag.py`:

```python
DAILY_RUNTIME_FIRST_DATE
start_date
end_date
```

Tanggal data simulasi tetap dimulai dari 1 September 2025.

## Koneksi PostgreSQL melalui DBeaver

```text
Connection name : Subaraya Warehouse
Host            : localhost
Port            : 5432
Database        : metropolia_warehouse
Username        : de_user
Password        : password123
```

Koneksi internal Airflow menggunakan host:

```text
target-postgres
```

Host tersebut hanya digunakan antar-container. DBeaver tetap menggunakan
`localhost`.

## Pemeriksaan hasil batch

```sql
SELECT *
FROM etl_batch_log
ORDER BY processed_at DESC;
```

```sql
SELECT
    MIN(event_hour) AS first_event,
    MAX(event_hour) AS latest_event,
    COUNT(*) AS total_rows
FROM mart_metropolia_hourly;
```

```sql
SELECT
    event_date,
    zone_id,
    traffic_volume_total,
    average_pm25,
    accident_count
FROM mart_metropolia_daily
ORDER BY event_date DESC, zone_id
LIMIT 20;
```

## Menjalankan ETL streaming

Jalankan database, Kafka, tabel audit, dan consumer:

```cmd
docker compose up -d target-postgres kafka kafka-init streaming-db-init streaming-consumer
```

Topic yang digunakan:

```text
Topic              : metropolia.events
Partition          : 1
Replication factor : 1
```

### Producer manual

Traffic:

```cmd
docker compose run --rm streaming-producer traffic --zone Z01 --volume 5200 --temperature 31 --rain 0 --weather Clear
```

Kualitas udara:

```cmd
docker compose run --rm streaming-producer air-quality --zone Z01 --pm25 85 --pm10 120 --no2 42 --co 900 --o3 30 --temperature 31 --rain 0 --wind-speed 2.5
```

Kecelakaan:

```cmd
docker compose run --rm streaming-producer accident --zone Z01 --severity serious --casualties 2 --vehicles 2
```

Input harus dikirim melalui producer. Melakukan `INSERT` langsung melalui
DBeaver melewati Kafka dan Spark, sehingga tidak menunjukkan ETL streaming.

### Producer otomatis

```cmd
docker compose --profile auto up -d --build streaming-producer-auto
```

Secara default producer otomatis:

- mengirim event traffic dan kualitas udara setiap lima detik;
- merotasi zona Z01 sampai Z04;
- mengirim event kecelakaan pada siklus pertama dan setiap 13 siklus;
- menggunakan waktu Asia/Jakarta;
- membuat UUID baru sebagai `event_id`.

Pantau producer:

```cmd
docker compose --profile auto logs -f streaming-producer-auto
```

Pantau consumer:

```cmd
docker compose logs -f streaming-consumer
```

Hentikan producer otomatis:

```cmd
docker compose stop streaming-producer-auto
```

Instruksi streaming yang lebih lengkap tersedia di `STREAMING_DEMO.md`.

## Pemeriksaan hasil streaming

Event yang berhasil diproses:

```sql
SELECT
    event_id,
    event_type,
    event_hour,
    zone_id,
    emitted_at,
    processed_at,
    processed_at - emitted_at AS processing_latency
FROM etl_stream_event_log
ORDER BY processed_at DESC
LIMIT 20;
```

Ringkasan micro-batch:

```sql
SELECT *
FROM etl_stream_batch_log
ORDER BY processed_at DESC
LIMIT 20;
```

Hasil pada mart:

```sql
SELECT
    event_hour,
    zone_id,
    traffic_volume,
    average_speed,
    congestion_level,
    pm25,
    pm10,
    accident_count,
    casualty_count,
    refreshed_at
FROM mart_metropolia_hourly
ORDER BY refreshed_at DESC
LIMIT 20;
```

## Modeling

Data untuk modeling tersedia melalui PostgreSQL view:

```text
model_training_dataset
```

Pembagian data yang digunakan:

- Training: 2022 sampai 2023.
- Validation: 2024.
- Test: 1 Januari sampai 31 Agustus 2025.
- Operasional: daily batch dan streaming.

Pembagian dilakukan berdasarkan waktu, bukan secara acak.

## Menghentikan layanan

Hentikan container tanpa menghapus data:

```cmd
docker compose stop
```

Nyalakan kembali:

```cmd
docker compose start
```

Jangan menggunakan perintah berikut jika data masih dibutuhkan:

```cmd
docker compose down -v
```

Opsi `-v` menghapus volume PostgreSQL, metadata Airflow, data Kafka, dan
checkpoint streaming.

## Troubleshooting

### Port 5432 atau 8080 sudah digunakan

Periksa container yang sedang berjalan:

```cmd
docker ps
```

Hentikan container lain yang menggunakan port yang sama atau ubah port host
pada `docker-compose.yml`.

### Log Airflow menghasilkan 403

Pastikan semua komponen Airflow menggunakan nilai
`AIRFLOW__WEBSERVER__SECRET_KEY` yang sama. Setelah mengubah nilai tersebut,
buat ulang container:

```cmd
docker compose down
docker compose up -d
```

Jangan menambahkan `-v`.

### Consumer streaming menggunakan terlalu banyak RAM

Ubah `.env`:

```env
STREAM_SPARK_DRIVER_MEMORY=512m
STREAM_SPARK_EXECUTOR_MEMORY=512m
```

Jangan menjalankan historical backfill dan Spark Structured Streaming secara
bersamaan pada komputer dengan RAM terbatas.

## Batasan simulasi

- Kota, zona, dan koordinat Subaraya merupakan lokasi sintetis.
- Data traffic dari satu sensor direplikasi ke empat zona menggunakan faktor tetap.
- Dua belas stasiun kualitas udara dipetakan ke empat zona.
- Data kecelakaan dipetakan secara deterministik menggunakan hash ID.
- Producer otomatis menghasilkan event simulasi baru.
- Konfigurasi Kafka menggunakan satu broker, satu topic, satu partition, dan
  replication factor satu.
- Hubungan antarsumber digunakan sebagai skenario simulasi dan tidak menunjukkan
  hubungan kausal pada kota nyata.
