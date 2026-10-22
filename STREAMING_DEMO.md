# Demo ETL Streaming Metropolia — Input Manual dan Otomatis

Dokumen ini menjelaskan cara menjalankan demo tanpa stream seed. Setiap perintah
producer mengirim satu baris/event manual melalui alur berikut:

```text
Input manual -> Kafka -> Spark Structured Streaming -> PostgreSQL -> DBeaver/dashboard
```

Jangan melakukan `INSERT` langsung melalui DBeaver untuk demo, karena tindakan
tersebut melewati Kafka dan Spark sehingga bukan demonstrasi ETL streaming.

Mode manual tetap tersedia untuk menelusuri satu event. Mode otomatis dapat
digunakan untuk menyimulasikan sensor yang mengirim data secara terus-menerus.

## 1. Bagian yang perlu disesuaikan di komputer Anda

Semua path di `docker-compose.yml` bersifat relatif terhadap folder project.
Lokasi clone tidak harus sama dengan lokasi pembuat project.

Yang perlu diganti hanya path pada perintah `cd`:

```cmd
cd /d "C:\LOKASI_CLONE_ANDA\metropolia-etl"
```

Contoh jika repository berada di folder GitHub:

```cmd
cd /d "C:\Users\NAMA_ANDA\Documents\GitHub\metropolia-etl"
```

Jangan mengubah volume relatif berikut:

```yaml
./streaming
./sql/init_streaming.sql
```

Docker Compose akan menyelesaikannya berdasarkan lokasi `docker-compose.yml`.

Jika port Kafka `9092` sudah digunakan, buat `.env` dari `.env.example` dan ubah:

```env
KAFKA_PORT=9093
```

Perubahan port host tidak mengubah koneksi internal container. Producer dan
consumer tetap menggunakan `kafka:29092`.

## 2. Prasyarat sebelum demo

1. Buka Docker Desktop.
2. Tunggu sampai Docker Engine berstatus running.
3. Pastikan historical backfill sebelumnya sudah tersedia.
4. Buka CMD atau PowerShell pada folder `metropolia-etl`.
5. Sisakan RAM yang cukup. Jangan menjalankan historical backfill bersamaan
   dengan Spark Streaming.

Periksa Docker:

```cmd
docker version
```

## 3. Build image streaming

Build hanya diperlukan pada pemakaian pertama atau setelah kode streaming berubah:

```cmd
docker compose build streaming-consumer streaming-producer
```

Build pertama memerlukan internet untuk mengunduh image, package Python, dan JAR
Spark Kafka.

## 4. Menjalankan infrastruktur

Jalankan database, Kafka, dan consumer:

```cmd
docker compose up -d target-postgres kafka kafka-init streaming-db-init streaming-consumer
```

Airflow batch tidak wajib aktif untuk demo input streaming manual. Jika ingin
menampilkan batch dan streaming bersamaan, jalankan juga:

```cmd
docker compose up -d airflow-meta-db airflow-init airflow-webserver airflow-scheduler
```

Periksa status:

```cmd
docker compose ps
```

Container berikut harus aktif:

```text
metropolia-target-postgres
metropolia-kafka
metropolia-kafka-init
metropolia-streaming-consumer
```

`metropolia-streaming-db-init` boleh berstatus `Exited (0)` karena tugasnya hanya
membuat tabel audit streaming lalu selesai.

`metropolia-kafka-init` juga boleh berstatus `Exited (0)` karena tugasnya hanya
membuat topic `metropolia.events` lalu selesai.

## 5. Pastikan consumer siap

```cmd
docker compose logs --tail 100 streaming-consumer
```

Tunggu sampai log menyatakan streaming aktif dan tidak ada error koneksi Kafka
atau PostgreSQL.

Untuk mengikuti log secara langsung:

```cmd
docker compose logs -f streaming-consumer
```

Tekan `Ctrl+C` untuk keluar dari tampilan log. Container tidak ikut berhenti.

## 5A. Menjalankan producer otomatis

Perintah ini membangun dan menjalankan simulator otomatis sekaligus memastikan
Kafka, database, dan consumer ikut dijalankan sebagai dependency:

```cmd
docker compose --profile auto up -d --build streaming-producer-auto
```

Perilaku default producer otomatis:

- mengirim traffic dan kualitas udara setiap 5 detik;
- merotasi lokasi Z01, Z02, Z03, dan Z04;
- mengirim accident pada siklus pertama lalu setiap 13 siklus (sekitar 65 detik);
- memakai waktu Asia/Jakarta saat event dibuat;
- membuat `event_id` UUID baru untuk setiap event.

Pantau event yang dibuat:

```cmd
docker compose --profile auto logs -f streaming-producer-auto
```

Pantau hasil pemrosesan Spark pada terminal lain:

```cmd
docker compose logs -f streaming-consumer
```

Ubah interval melalui `.env` apabila diperlukan:

```env
AUTO_STREAM_INTERVAL_SECONDS=5
AUTO_ACCIDENT_EVERY_CYCLES=13
AUTO_RANDOM_SEED=2026
```

`AUTO_ACCIDENT_EVERY_CYCLES=13` berarti accident dikirim pada siklus pertama,
kemudian setiap 13 siklus. Nilai seed membuat urutan angka simulasi dapat
direproduksi setelah container dibuat ulang.

Hentikan producer otomatis tanpa menghentikan Kafka dan consumer:

```cmd
docker compose stop streaming-producer-auto
```

Mode manual di bagian berikut tetap dapat digunakan saat producer otomatis aktif,
tetapi untuk demo satu event yang mudah diamati sebaiknya hentikan mode otomatis.

## 6. Kirim event traffic manual

Buka terminal baru pada folder project:

```cmd
docker compose run --rm streaming-producer traffic --zone Z01 --volume 5200 --temperature 31 --rain 0 --weather Clear
```

Transformasi yang dilakukan consumer:

- waktu dibulatkan ke jam;
- `average_speed` diestimasi dari volume dan kapasitas zona;
- `congestion_level` dihitung menjadi LOW, MEDIUM, atau HIGH;
- `is_weekend` dihitung dari tanggal;
- `is_rush_hour` dihitung dari jam;
- hasil di-upsert ke `fact_traffic_hourly`;
- `mart_metropolia_hourly` diperbarui.

Contoh untuk kemacetan tinggi di Z04:

```cmd
docker compose run --rm streaming-producer traffic --zone Z04 --volume 2900 --temperature 32 --rain 0 --weather Clear
```

## 7. Kirim event kualitas udara manual

```cmd
docker compose run --rm streaming-producer air-quality --zone Z01 --pm25 85 --pm10 120 --no2 42 --co 900 --o3 30 --temperature 31 --rain 0 --wind-speed 2.5
```

Consumer memvalidasi nilai PM2.5, mengubah tipe data, menambahkan waktu ingestion,
melakukan UPSERT ke `fact_air_quality_hourly`, kemudian memperbarui mart.

## 8. Kirim event kecelakaan manual

```cmd
docker compose run --rm streaming-producer accident --zone Z01 --severity serious --casualties 2 --vehicles 2
```

Nilai severity yang tersedia:

```text
fatal
serious
slight
```

Kecelakaan pada zona dan jam yang sama akan ditambahkan ke agregat jam tersebut.
`event_id` mencegah event identik diproses dua kali apabila Kafka melakukan replay.

## 9. Mengirim waktu tertentu

Secara default producer menggunakan waktu Asia/Jakarta saat perintah dijalankan.
Untuk menentukan waktu sendiri, tempatkan `--event-time` sebelum jenis event:

```cmd
docker compose run --rm streaming-producer --event-time "2026-09-25T14:30:00+07:00" traffic --zone Z02 --volume 4100
```

Untuk demo real-time, sebaiknya tidak mengisi `--event-time` agar waktu otomatis
mengikuti waktu saat presentasi.

## 10. Pemeriksaan di DBeaver

Gunakan koneksi database utama:

```text
Host     : localhost
Port     : 5432
Database : metropolia_warehouse
Username : de_user
Password : password123
```

Status streaming:

```sql
SELECT *
FROM v_metropolia_stream_status
ORDER BY event_type;
```

Event terakhir yang melewati Kafka dan Spark:

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

Micro-batch Spark terakhir:

```sql
SELECT *
FROM etl_stream_batch_log
ORDER BY processed_at DESC
LIMIT 20;
```

Hasil gabungan untuk dashboard:

```sql
SELECT
    event_hour,
    zone_id,
    traffic_volume,
    average_speed,
    congestion_level,
    pm25,
    accident_count,
    casualty_count,
    refreshed_at
FROM mart_metropolia_hourly
ORDER BY refreshed_at DESC
LIMIT 20;
```

## 11. Skenario presentasi yang disarankan

1. Tampilkan query `etl_stream_event_log` dan catat jumlah baris awal.
2. Tampilkan terminal log `streaming-consumer`.
3. Kirim satu traffic event.
4. Tunjukkan micro-batch Spark memproses event tersebut.
5. Refresh DBeaver dan tunjukkan baris traffic serta mart berubah.
6. Kirim air-quality event pada zona yang sama.
7. Refresh mart dan tunjukkan PM2.5 muncul pada baris yang sama.
8. Kirim accident event pada zona yang sama.
9. Refresh mart dan tunjukkan `accident_count` bertambah.
10. Jika dashboard sudah tersedia, refresh otomatis dashboard untuk menunjukkan
    perubahan tanpa menjalankan query manual.

## 12. Hubungan dengan ETL batch

Batch dan streaming menulis ke fact serta mart yang sama, tetapi jalurnya berbeda:

```text
Batch:
Airflow -> Spark batch -> staging -> UPSERT fact -> mart

Streaming:
Input manual -> Kafka -> Spark Structured Streaming -> UPSERT fact -> mart
```

Streaming tidak menggunakan tabel staging batch. Karena itu proses streaming
tidak terhapus ketika Spark batch menulis staging dengan mode overwrite.

## 13. Menghentikan streaming

```cmd
docker compose stop streaming-consumer kafka
```

Database dan Airflow tidak ikut dihentikan.

Menyalakannya kembali:

```cmd
docker compose up -d kafka kafka-init streaming-db-init streaming-consumer
```

Checkpoint membuat consumer melanjutkan offset Kafka terakhir.

## 14. Reset khusus demo streaming

Biasanya reset tidak diperlukan. Untuk menghapus event demo dari database,
jalankan dengan hati-hati melalui DBeaver:

```sql
BEGIN;

DELETE FROM mart_metropolia_hourly
WHERE event_hour >= DATE_TRUNC('hour', CURRENT_TIMESTAMP) - INTERVAL '1 day';

DELETE FROM fact_traffic_hourly
WHERE event_hour >= DATE_TRUNC('hour', CURRENT_TIMESTAMP) - INTERVAL '1 day';

DELETE FROM fact_air_quality_hourly
WHERE event_hour >= DATE_TRUNC('hour', CURRENT_TIMESTAMP) - INTERVAL '1 day';

DELETE FROM fact_accident_hourly
WHERE event_hour >= DATE_TRUNC('hour', CURRENT_TIMESTAMP) - INTERVAL '1 day';

DELETE FROM etl_stream_event_log
WHERE processed_at >= CURRENT_TIMESTAMP - INTERVAL '1 day';

DELETE FROM etl_stream_batch_log
WHERE processed_at >= CURRENT_TIMESTAMP - INTERVAL '1 day';

COMMIT;
```

Jangan menjalankan `docker compose down -v`, karena opsi `-v` juga menghapus
volume database backfill.

## 15. Troubleshooting

### Port Kafka 9092 bentrok

Buat `.env`:

```env
KAFKA_PORT=9093
```

Lalu buat ulang Kafka:

```cmd
docker compose up -d --force-recreate kafka
```

### Producer berhasil tetapi data belum muncul

Periksa:

```cmd
docker compose logs --tail 200 streaming-consumer
docker compose logs --tail 100 kafka
```

Tunggu lebih lama dari interval micro-batch, yaitu sekitar 2–5 detik, lalu
refresh query DBeaver.

### Consumer menghabiskan RAM

Ubah `.env`:

```env
STREAM_SPARK_DRIVER_MEMORY=512m
STREAM_SPARK_EXECUTOR_MEMORY=512m
```

Jangan jalankan historical backfill pada saat yang sama.

### Ingin memastikan tabel streaming sudah dibuat

```cmd
docker compose run --rm streaming-db-init
```

Skrip SQL bersifat idempoten sehingga aman dijalankan kembali.
