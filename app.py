# app.py
import os
import io
import re
import json
import datetime as dt
from typing import List, Dict, Any

import pandas as pd
from flask import Flask, jsonify, request
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# =========================
# 環境変数
# =========================
API_KEY = os.environ.get("API_KEY")  # 任意の長いランダム文字列
FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID")  # Google DriveのフォルダID
SA_PATH = os.environ.get("SERVICE_ACCOUNT_FILE", "/etc/secrets/gcp-service-account.json")
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

# =========================
# Google認証（Secret File or 環境変数JSONのフォールバック）
# =========================
creds = None
if os.path.exists(SA_PATH):
    creds = service_account.Credentials.from_service_account_file(SA_PATH, scopes=SCOPES)
else:
    SA_JSON = os.environ.get("SERVICE_ACCOUNT_JSON")
    if SA_JSON:
        creds = service_account.Credentials.from_service_account_info(json.loads(SA_JSON), scopes=SCOPES)
    else:
        raise FileNotFoundError(f"Service account not found at {SA_PATH} and SERVICE_ACCOUNT_JSON not set")

drive = build("drive", "v3", credentials=creds)

app = Flask(__name__)

# =========================
# 認可
# =========================
def _auth_ok(req) -> bool:
    return (API_KEY is not None) and (req.headers.get("X-API-Key") == API_KEY)

# =========================
# Drive: 最新CSVの取得
# - ファイル名に含まれる日付(YYYY-MM-DD / YYYY_MM_DD / YYYYMMDD)を優先
# - 無い/壊れている場合は modifiedTime で降順
# - mimeType は text/csv と application/vnd.ms-excel を許容
# =========================
_DATE_IN_NAME = re.compile(r"(20\d{2})[-_]?(\d{2})[-_]?(\d{2})")

def _parse_date_from_name(name: str) -> dt.date:
    m = _DATE_IN_NAME.search(name or "")
    if not m:
        return dt.date.min
    y, mth, d = map(int, m.groups())
    try:
        return dt.date(y, mth, d)
    except ValueError:
        return dt.date.min

def _parse_iso_dt(s: str) -> dt.datetime:
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return dt.datetime.min

def download_latest_csv_from_drive(folder_id: str) -> bytes:
    if not folder_id:
        raise ValueError("GDRIVE_FOLDER_ID 未設定")

    # CSV候補を取得（共有ドライブにも対応）
    q = (
        f"'{folder_id}' in parents and trashed=false and "
        "("
        "mimeType='text/csv' or "
        "mimeType='application/vnd.ms-excel'"
        ")"
    )
    resp = drive.files().list(
        q=q,
        fields="files(id,name,modifiedTime,createdTime,size,mimeType)",
        orderBy="modifiedTime desc",
        pageSize=100,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    files = resp.get("files", [])
    if not files:
        raise FileNotFoundError("指定フォルダにCSVが見つかりません。")

    # ソートキー： (日付(ファイル名) or 最小), modifiedTime
    def sort_key(f):
        dname = _parse_date_from_name(f.get("name", ""))
        mtime = _parse_iso_dt(f.get("modifiedTime", ""))
        return (dname, mtime)

    files.sort(key=sort_key, reverse=True)
    latest = files[0]

    # 本体ダウンロード
    buf = io.BytesIO()
    req = drive.files().get_media(fileId=latest["id"])
    downloader = MediaIoBaseDownload(buf, req, chunksize=1024 * 1024)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    buf.seek(0)
    return buf.read()

# =========================
# CSV 正規化 → 直近7日
# =========================
def _colmap(columns: List[str]) -> Dict[str, str]:
    """
    各指標の列名をCSVの実際の列にマッピング（柔軟一致）。
    例:
      date / day / datetime / start_date
      steps / step_count
      sleep_hours / sleep_duration_hours
      active_energy_kcal / move_kcal
      resting_hr_bpm / resting_heart_rate
      weight_kg / body_mass_kg
    """
    lower_map = {c.lower(): c for c in columns}

    def pick(*cands: str) -> str:
        # 完全一致（lower）優先
        for key in cands:
            if key in lower_map:
                return lower_map[key]
        # サブストリング/正規表現っぽい柔軟一致
        for key in cands:
            for lc, orig in lower_map.items():
                if key in lc:
                    return orig
        return ""

    return {
        "date": pick("date", "day", "datetime", "start date", "start_date"),
        "steps": pick("steps", "step", "step_count"),
        "sleep_hours": pick("sleep_hours", "sleep hour", "sleep_duration", "sleep_duration_hours"),
        "active_energy_kcal": pick("active_energy_kcal", "move_kcal", "active energy", "active_kcal"),
        "resting_hr_bpm": pick("resting_hr_bpm", "resting heart", "resting_heart_rate", "rest hr", "restinghr"),
        "weight_kg": pick("weight_kg", "body mass", "weight"),
    }

def _to_float(x):
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)) or (isinstance(x, str) and x.strip() == ""):
            return None
        return float(x)
    except Exception:
        return None

def normalize_and_last_7(csv_bytes: bytes) -> List[Dict[str, Any]]:
    df = pd.read_csv(io.BytesIO(csv_bytes))
    cmap = _colmap(list(df.columns))

    # 日付
    if not cmap["date"]:
        raise ValueError("CSVに日付列が見つかりません。")
    df["_date"] = pd.to_datetime(df[cmap["date"]], errors="coerce").dt.date

    # 数値列
    def get_col(key: str):
        col = cmap[key]
        return df[col] if col in df.columns else None

    steps = get_col("steps")
    sleep = get_col("sleep_hours")
    active = get_col("active_energy_kcal")
    rhr = get_col("resting_hr_bpm")
    weight = get_col("weight_kg")

    out = []
    for _, row in df.iterrows():
        d = row["_date"]
        if pd.isna(d):
            continue
        rec = {
            "date": d.isoformat(),
            "steps": _to_float(row[steps.name]) if steps is not None else None,
            "sleep_hours": _to_float(row[sleep.name]) if sleep is not None else None,
            "active_energy_kcal": _to_float(row[active.name]) if active is not None else None,
            "resting_hr_bpm": _to_float(row[rhr.name]) if rhr is not None else None,
            "weight_kg": _to_float(row[weight.name]) if weight is not None else None,
        }
        out.append(rec)

    # 日付でソートして直近7件
    out.sort(key=lambda r: r["date"])
    return out[-7:]

# =========================
# ルーティング
# =========================
@app.get("/healthz")
def healthz():
    return "ok", 200

@app.get("/latest-health")
def latest_health():
    if not _auth_ok(request):
        return jsonify({"error": "Unauthorized"}), 401

    try:
        csv_bytes = download_latest_csv_from_drive(FOLDER_ID)
        data = normalize_and_last_7(csv_bytes)
        return jsonify(data), 200
    except FileNotFoundError as e:
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        return jsonify({"error": f"CSV取得/読込に失敗: {str(e)}"}), 500

if __name__ == "__main__":
    # ローカル実行用
    app.run(host="0.0.0.0", port=10000, debug=True)
