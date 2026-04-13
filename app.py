import os
import pandas as pd
import stadata
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import datetime
from google.cloud import bigquery
from dotenv import load_dotenv

# =============================
# LOAD ENV
# =============================
load_dotenv()

API_KEY = os.getenv("API_KEY")
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET = os.getenv("DATASET")
TABLE_NAME = os.getenv("PROD_TABLE")
CREDENTIAL_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# set credential BigQuery
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIAL_PATH

TABLE_ID = f"{PROJECT_ID}.{DATASET}.{TABLE_NAME}"

timestamp = datetime.datetime.now().strftime("%Y%m%d")

# =============================
# INIT CLIENT
# =============================
client = stadata.Client(API_KEY)
bq_client = bigquery.Client()

STOPWORDS = {'di', 'menurut', 'dan', 'per', 'dengan'}

# =============================
# FUNCTIONS
# =============================
def generate_metric(title, var_id):
    title = title.lower()
    title = re.sub(r'[^a-z0-9\s]', '', title)
    words = [w for w in title.split() if w not in STOPWORDS]
    metric = "_".join(words[:6])
    return f"{metric}_{var_id}"


def fetch_and_transform(var_id):
    try:
        data = client.view_dynamictable(
            domain='1507',
            var=var_id,
            th='0,9'
        )

        # skip kalau kosong
        if data is None or len(data) == 0:
            print(f"[SKIP] {var_id}")
            return None

        # tambah metadata
        data['var_id'] = var_id
        data['metric'] = meta_map[var_id]['metric']
        data['title'] = meta_map[var_id]['title']
        data['sub_name'] = meta_map[var_id]['sub_name']
        data['unit'] = meta_map[var_id]['unit']

        # rename kolom
        data = data.rename(columns={
            'turunan variable': 'kategori'
        })

        year_cols = [col for col in data.columns if str(col).isdigit()]

        df_long = data.melt(
            id_vars=[
                'var_id', 'metric', 'title',
                'sub_name', 'unit', 'variable', 'kategori'
            ],
            value_vars=year_cols,
            var_name='tahun',
            value_name='value'
        )

        df_long['tahun'] = df_long['tahun'].astype(int)

        return df_long

    except Exception as e:
        print(f"[ERROR] var_id {var_id}: {e}")
        return None


def enforce_schema(df):
    df['var_id'] = df['var_id'].astype(int)
    df['metric'] = df['metric'].astype(str)
    df['title'] = df['title'].astype(str)
    df['sub_name'] = df['sub_name'].astype(str)
    df['unit'] = df['unit'].astype(str)
    df['variable'] = df['variable'].astype(str)
    df['kategori'] = df['kategori'].astype(str)
    df['tahun'] = df['tahun'].astype(int)
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    return df


# =============================
# MAIN PIPELINE
# =============================
print("🚀 Starting pipeline...")

# ambil metadata
df_meta = client.list_dynamictable(all=False, domain=['1507'])

df_meta['metric'] = df_meta.apply(
    lambda row: generate_metric(row['title'], row['var_id']),
    axis=1
)

meta_map = df_meta.set_index('var_id')[['title', 'sub_name', 'unit', 'metric']].to_dict('index')
var_ids = list(meta_map.keys())

results = []

with ThreadPoolExecutor(max_workers=5) as executor:
    futures = {
        executor.submit(fetch_and_transform, var_id): var_id
        for var_id in var_ids
    }

    for future in as_completed(futures):
        res = future.result()
        if res is not None:
            results.append(res)

# =============================
# CHECK DATA
# =============================
if not results:
    print("❌ Tidak ada data, batal upload")
    exit()

df_final = pd.concat(results, ignore_index=True)
df_final = enforce_schema(df_final)
df_final = df_final.fillna("")

print(f"✅ Total rows: {len(df_final)}")

# =============================
# EXPORT EXCEL
# =============================
file_name = f"data_{timestamp}.xlsx"
df_final.to_excel(file_name, index=False)
print(f"✅ Excel saved: {file_name}")

# =============================
# UPLOAD BIGQUERY
# =============================
job = bq_client.load_table_from_dataframe(
    df_final,
    TABLE_ID,
    job_config=bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE"
    )
)

job.result()

print("✅ Data berhasil diupload ke BigQuery")