# Dataset Metropolia

Folder ini menyiapkan satu sumber data untuk dua skenario:

1. ETL batch membaca data berdasarkan partisi tanggal.
2. ETL streaming memutar ulang event kanonik yang sama melalui Kafka atau Redpanda.

## Struktur

- `raw/traffic`: data lalu lintas UCI dalam bentuk asli dan CSV hasil ekstraksi.
- `raw/air_quality`: data kualitas udara UCI dalam bentuk asli dan 12 CSV stasiun.
- `raw/accident`: data collision tahun 2025 dari UK Department for Transport.
- `reference`: zona simulasi, pemetaan sumber ke zona, dan kalender simulasi.
- `canonical`: keluaran transformasi dengan schema seragam. Diisi oleh ETL, bukan manual.
- `batch/inbox`: partisi data harian untuk simulasi batch. Diisi oleh pipeline persiapan.
- `stream/seed`: event kanonik terurut untuk replay streaming. Disiapkan setelah ETL batch.
- `quarantine`: record yang gagal validasi.

## Sumber

- Traffic: https://archive.ics.uci.edu/dataset/492/metro+interstate+traffic+volume
- Air quality: https://archive.ics.uci.edu/dataset/501/beijing+multi+site+air+quality+data
- Accident 2022-2025: https://www.gov.uk/government/statistical-data-sets/road-safety-open-data

## Aturan penggunaan

- Jangan mengubah file di `raw`.
- Simpan waktu asli sebagai `source_event_time`.
- Buat waktu simulasi sebagai `event_time` berdasarkan `simulation_calendar.csv`.
- Gunakan `zone_id` dari `zone_reference.csv` dan `source_zone_mapping.csv`.
- Lokasi dan waktu Metropolia adalah data simulasi, bukan kondisi nyata Surabaya.
- ETL harus menangani cleaning, mapping, konversi unit, deduplikasi, dan agregasi.

## File utama untuk Spark

Traffic:

`/opt/airflow/data/raw/traffic/Metro_Interstate_Traffic_Volume.csv`

Air quality:

`/opt/airflow/data/raw/air_quality/stations/PRSA_Data_20130301-20170228/*.csv`

Accident:

`/opt/airflow/data/raw/accident/collisions_*.csv`

## Periode pipeline

- Historical backfill: 2022-01-01 sampai 2025-08-31.
- Demonstrasi daily batch: 2025-09-01 sampai 2025-09-07.
- Cutover streaming: mulai 2025-09-08.

Aturan lengkap tersimpan di `reference/pipeline_periods.csv`.
