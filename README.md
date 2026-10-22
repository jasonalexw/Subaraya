# Subaraya batch ETL

Proyek ini menggunakan data lalu lintas, kualitas udara, dan kecelakaan untuk
mensimulasikan smart traffic analytics Kota Subaraya.

## Periode

- Historical backfill: 2022-01-01 sampai 2025-08-31.
- Daily batch demo: 2025-09-01 sampai 2025-09-07.
- Streaming cutover: mulai 2025-09-08.

Traffic dan air quality dari 2013-03-01 sampai 2017-02-28 digeser ke periode
2022-01-01 sampai 2025-12-31. Accident menggunakan data 2022-2025 tanpa
pergeseran tahun.

## Komponen

- `data/raw`: data sumber yang tidak dimodifikasi.
- `data/reference`: kalender simulasi, fase pipeline, dan zona Subaraya.
- `scripts/check_input.py`: validasi file dan header sebelum ETL.
- `scripts/spark_metropolia_etl.py`: transformasi PySpark dan JDBC staging.
- `dags/metropolia_pipeline_dag.py`: DAG historical backfill dan daily batch.
- `sql/init.sql`: schema warehouse, mart, dan modeling view.
- `sql/upsert_staging.sql`: UPSERT idempoten dari staging ke warehouse.
- `data/canonical`: output Parquet hasil transformasi.
- `data/quarantine`: record yang gagal validasi.
- `data/stream/seed`: JSON event untuk replay Kafka/Redpanda.

## Prasyarat

1. Docker Desktop dengan Docker Compose.
2. Minimal sekitar 8 GB RAM tersedia untuk Docker.
3. Port 5432 dan 8080 tidak sedang digunakan aplikasi lain.

## Validasi input tanpa Docker

Jalankan dari folder proyek:

```powershell
python scripts/check_input.py --data-root data
```

Hasil yang benar memiliki status `OK`, 12 file air quality, dan 4 file accident.

## Menjalankan container

```powershell
docker compose up -d
```

Buka Airflow pada `http://localhost:8080` menggunakan:

```text
username: admin
password: admin
```

PostgreSQL dapat diakses melalui DBeaver:

```text
host: localhost
port: 5432
database: metropolia_warehouse
username: de_user
password: password123
```

## Urutan DAG

1. Trigger manual `metropolia_historical_backfill` satu kali.
2. Periksa bahwa data 2022-01-01 sampai 2025-08-31 masuk ke warehouse.
3. Aktifkan `metropolia_daily_batch`.
4. DAG membuat tujuh run untuk 2025-09-01 sampai 2025-09-07.

Setiap run menjalankan:

```text
validate input -> transform PySpark -> staging -> UPSERT -> mart
```

## Pemeriksaan database

```sql
SELECT MIN(event_hour), MAX(event_hour), COUNT(*)
FROM mart_metropolia_hourly;

SELECT *
FROM etl_batch_log
ORDER BY processed_at DESC;

SELECT event_date, zone_id, traffic_volume_total, average_pm25, accident_count
FROM mart_metropolia_daily
ORDER BY event_date DESC, zone_id
LIMIT 20;
```

## Menyiapkan data streaming

Setelah batch selesai, jalankan mode `stream_seed` di container Airflow atau
container lain yang memiliki PySpark dan PostgreSQL JDBC driver:

```bash
python /opt/airflow/scripts/spark_metropolia_etl.py \
  --mode stream_seed \
  --start-date 2025-09-08 \
  --end-date 2025-12-31
```

Mode ini tidak memasukkan data ke warehouse. Mode ini menghasilkan JSON event di:

```text
data/stream/seed/metropolia.traffic/
data/stream/seed/metropolia.air-quality/
data/stream/seed/metropolia.accident/
```

File tersebut menjadi sumber producer Kafka atau Redpanda pada tahap berikutnya.


## Troubleshooting log 403

Seluruh komponen Airflow menerima `AIRFLOW__WEBSERVER__SECRET_KEY` yang sama
melalui `x-airflow-common`. Setelah mengubah secret key, container harus dibuat
ulang agar environment baru diterapkan:

```powershell
docker compose down
docker compose up -d
```

Jangan menambahkan opsi `-v` karena opsi tersebut menghapus volume database.
Jika browser masih memakai sesi lama, logout/login kembali atau hapus cookie
untuk `localhost:8080`.

## Batasan simulasi

- Zona dan koordinat Metropolia merupakan lokasi sintetis.
- Traffic satu sensor direplikasi ke empat zona menggunakan faktor tetap.
- Dua belas stasiun air quality dipetakan ke empat zona.
- Accident dipetakan secara deterministik ke empat zona menggunakan hash ID.
- Hubungan antarsumber merupakan skenario simulasi, bukan bukti hubungan kausal
  pada kota nyata.
