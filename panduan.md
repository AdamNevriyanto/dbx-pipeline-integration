# README — Panduan Pembuatan ETL Pipeline Tabel Baru (Bronze → Silver)

ETL Pipeline berbasis **Spark Declarative Pipeline (SDP)** di Databricks.

---

### 1 Git Workflow
Setiap perubahan (termasuk penambahan tabel baru) **wajib** melalui pull request/merge request, dengan minimal:
- Review oleh 1 orang lain sebelum merge ke `main`
- Pipeline sudah pernah di-*test run* di environment dev sebelum merge ke `main`


## 2. Flow Pembuatan Tabel Baru — Source dari S3 / JDBC

### Step 1 — Setup Struktur Folder (skip kalau sudah ada)

```
dbx-pipeline-integration/
├── configs/      # file konfigurasi YAML per tabel
├── pipelines/    # script utama ETL (SDP)
└── utils/        # fungsi reusable (transform, config loader, dll)
```

### Step 2 — Buat File Konfigurasi YAML

**2a. Buat file `configs/table_{nama_table}.yaml`**

**KONFIGURASI GENERAL YANG PERLU DISESUAIKAN :**
- `bronze.cluster_by_col` →  kolom untuk struktur clustering.
- `columns` → nama kolom, tipe data, dan cleansing rule tiap kolom (rename, trim, mapping, dll — lihat contoh tabel existing sebagai referensi)
- `silver.primary_keys` → kolom PK untuk MERGE/`apply_changes`
- `silver.partition_columns` → kolom untuk struktur clustering.
- `silver.incremental_columns` → kolom untuk komparasi data terbaru di Silver
  
**BERIKUT KONFIGURASI YAML YANG PERLU DISESUAIKAN UNTUK KONEKSI SOURCE DARI S3 :**
Sesuaikan minimal bagian berikut:
- `bronze.source_type` → = "s3"
- `bronze.s3.s3_path` → path S3 source
- `bronze.s3.read_options.cloudFiles.format` → format file source (csv/json/parquet)
- `bronze.s3.read_options.cloudFiles.schemaHints` →  kolom kolom yang secara tipe data dipaksa untuk disesuaikan dengan tipe data yang sudah di define dan tidak berubah jika source ada perubahan schema.
  
**BERIKUT KONFIGURASI YAML YANG PERLU DISESUAIKAN UNTUK KONEKSI SOURCE DARI JDBC :**
Sesuaikan minimal bagian berikut:
- `bronze.source_type` → = "jdbc"
- `bronze.jdbc.read_options.host` → Nama host 
- `bronze.jdbc.read_options.port` → Port yang digunakan DB
- `bronze.jdbc.read_options.database` → Nama Database
- `bronze.jdbc.read_options.schema` → Nama Schema
- `bronze.jdbc.read_options.table` → Nama Table 
- `bronze.jdbc.read_options.driver` → Driver
- `bronze.jdbc.read_options.secret_scope` → Secret Scope yang digunakan untuk menyimpan credential 
- `bronze.jdbc.read_options.secret_key_user` →  Username yang sudah disimpan dalam Secret Scope
- `bronze.jdbc.read_options.secret_key_password` → Password yang sudah disimpan dalam Secret Scope
- `bronze.jdbc.read_options.ssl_mode` → Force ke mode SSL

Opsional Parameter jika DB source ada partitioning:
- `bronze.jdbc.read_options.fetch_size` → Jumlah row yang di fetch dalam 1 kali batch
- `bronze.jdbc.read_options.partitionColumn` → Kolom yang digunakan sebagai partition
- `bronze.jdbc.read_options.lowerBound` → Batas bawah range partition
- `bronze.jdbc.read_options.UpperBound` → Batas atas range partition
- `bronze.jdbc.read_options.numPartitions` → Jumlah berapa banyak partition yang di read secara paralel


> ⚠️ **Wajib**: `primary_keys` tidak boleh kosong, karena dipakai sebagai key di `apply_changes`. Kolom PK juga akan di-filter `IS NOT NULL` otomatis di staging view.


**2b. Pastikan `utils/config_load.py` sudah ada**
Kalau folder `utils/` baru dibuat, copy file ini dari project/tabel yang sudah ada — tidak perlu ditulis ulang dari nol.
Sesuaikan : 
- `path` → path dimana folder **pipelines, utils dan config** disimpan.


### Step 3 — Siapkan Transform Function (Untuk Cleansing)

Pastikan `utils/transform/sdp_silver_transform.py` sudah ada di folder `utils/`. Kalau folder baru dibuat, copy dari project/tabel existing — jangan ditulis ulang.
Isi sdp_silver_transform : 
- Rename Nama Kolom
- Casting Tipe Data
- Trimming

### Step 4 — Pipeline Script

Clone script **TEMPLATE_PIPELINE.py** dari tabel yang sudah ada, simpan sebagai `pipelines/{terserah namanya}.py`.

**4.1** Penyesuaian yang **wajib**:
- `sys.path.insert(...)` : Sesuaikan path root project di Workspace kamu
- `TABLE_NAME` : Harus **identik** dengan `{nama_table}` di file YAML (case-sensitive)

**4.2** Penyesuaian jika **Source dari S3 :**

Gunakan Blok ini : 
def bronze_customer():
    ## load data dari S3
    df_raw = (
        spark.readStream.format("cloudFiles")
        .options(**source_cfg["read_options"])
        .load(source_cfg["s3_path"])
    )

- `.withColumn("source_file", F.col("_metadata.file_path"))` : Gunakan metadata ini untuk tulis file path source S3  

**4.3** Penyesuaian jika **Source dari JDBC :**

Gunakan Blok ini :
# load data dari jdbc
    jdbc_cfg = source_cfg["read_options"]
    df_raw = (
      spark.read
      .format("jdbc")
      .options(**get_jdbc_options(jdbc_cfg))
      .load()
    )   

- `.withColumn("source_file", F.lit(jdbc_cfg["table"]))` : Gunakan metadata ini untuk tulis nama table source  


### Step 5 — Deploy ke ETL Pipeline

1. Pastikan script yang sudah diclone dan disesuaikan tadi disimpan dalam folder `pipelines/`.
2. Buka **Jobs & Pipelines** → **Settings** pipeline terkait
3. Tambahkan path source code kedua script tersebut ke source list pipeline
4. Jalankan pipeline, monitor DAG graph untuk memastikan tabel baru muncul dan berjalan sesuai urutan dependency

---

