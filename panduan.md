# README — Panduan Pembuatan ETL Pipeline Tabel Baru (Bronze → Silver)

ETL Pipeline berbasis **Spark Declarative Pipeline (SDP)** di Databricks.

---

### 1 Git Workflow
Setiap perubahan (termasuk penambahan tabel baru) **wajib** melalui pull request/merge request, dengan minimal:
- Review oleh 1 orang lain sebelum merge ke `main`
- Pipeline sudah pernah di-*test run* di environment dev sebelum merge ke `main`


## 2. Flow Pembuatan Tabel Baru — Source dari S3

### Step 1 — Setup Struktur Folder (skip kalau sudah ada)

```
dbx-pipeline-integration/
├── configs/      # file konfigurasi YAML per tabel
├── pipelines/    # script utama ETL (SDP)
└── utils/        # fungsi reusable (transform, config loader, dll)
```

### Step 2 — Buat File Konfigurasi YAML

**2a. Buat file `configs/table_{nama_table}.yaml`**

Sesuaikan minimal bagian berikut:
- `bronze.s3.s3_path` → path S3 source
- `bronze.s3.format` → format file source (csv/json/parquet)
- `columns` → nama kolom, tipe data, dan cleansing rule tiap kolom (rename, trim, mapping, dll — lihat contoh tabel existing sebagai referensi)
- `silver.primary_keys` → kolom PK untuk MERGE/`apply_changes`
- `silver.partition_columns` → kolom untuk physical partitioning Delta table
- `silver.incremental_columns` → kolom untuk komparasi data terbaru di Silver


> ⚠️ **Wajib**: `primary_keys` tidak boleh kosong, karena dipakai sebagai key di `apply_changes`. Kolom PK juga akan di-filter `IS NOT NULL` otomatis di staging view.


**2b. Pastikan `utils/config_load.py` sudah ada**
Kalau folder `utils/` baru dibuat, copy file ini dari project/tabel yang sudah ada — tidak perlu ditulis ulang dari nol.

### Step 3 — Pipeline Bronze

Clone script bronze dari tabel yang sudah ada, simpan sebagai `pipelines/bronze_s3_{nama_table}.py`.

Penyesuaian yang **wajib**:
| Bagian | Yang diubah |
|---|---|
| `sys.path.insert(...)` | Sesuaikan path root project di Workspace kamu |
| `TABLE_NAME` | Harus **identik** dengan `{nama_table}` di file YAML (case-sensitive) |

### Step 4 — Siapkan Transform Function

Pastikan `utils/transform/sdp_silver_transform.py` sudah ada di folder `utils/`. Kalau folder baru dibuat, copy dari project/tabel existing — jangan ditulis ulang.

Kalau tabel baru butuh **transformasi khusus** (join, business logic kompleks, dsb yang tidak bisa dinyatakan lewat `mapping`/`special_char`/`numeric_only` di YAML), buat file terpisah di `utils/transform/custom_transforms/{nama_table}.py` — lihat pola *custom transform* di dokumentasi terpisah.

### Step 5 — Pipeline Silver


**5a. Clone script silver**, simpan sebagai `pipelines/silver_s3_{nama_table}.py`.

Penyesuaian yang **wajib** (sama seperti bronze):
| Bagian | Yang diubah |
|---|---|
| `sys.path.insert(...)` | Sesuaikan path root project |
| `TABLE_NAME` | Harus identik dengan `{nama_table}` di YAML |


### Step 6 — Deploy ke ETL Pipeline

1. Pastikan `pipelines/bronze_s3_{nama_table}.py` dan `pipelines/silver_s3_{nama_table}.py` sudah berada di folder `pipelines/`
2. Buka **Jobs & Pipelines** → **Settings** pipeline terkait
3. Tambahkan path source code kedua script tersebut ke source list pipeline
4. Jalankan pipeline, monitor DAG graph untuk memastikan tabel baru muncul dan berjalan sesuai urutan dependency

---

