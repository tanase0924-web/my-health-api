import os
import io
import re
import pandas as pd
from flask import Flask, jsonify, request, abort
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

API_KEY = os.environ.get("API_KEY")  # 任意の長いランダム文字列
FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID")  # Google DriveのフォルダID

# RenderのSecret Filesに置くサービスアカウントJSONを読む
SA_PATH = os.environ.get("SERVICE_ACCOUNT_FILE", "/etc/secrets/gcp-service-account.json")
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

creds = service_account.Credentials.from_service_account_file(SA_PATH, scopes=SCOPES)
drive = build("drive", "v3", credentials=creds)

app = Flask(__name__)

def _auth_ok(req) -> bool:
    return (API_KEY is not None) and (req.headers.get("X-API-Key") == API_KEY)

def _latest_csv_file_id(folder_id: str) -> str:
    resp = drive.files().list(
        q=f"'{folder_id}' in parents and mimeType='text/csv'",
        fields="files(id, name, modifiedTime)",
        orderBy="modifiedTime desc",
        pageSize=1
    ).execute()
    files = resp.get("files", [])
    if not files:
        raise FileNotFoundError("指定フォルダにCSVが見つかりません。")
    return files[0]["id"]

def _download_csv_bytes(file_id: str) -> bytes:
    req = drive.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, req, chunksize=1024*1024)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    buf.seek(0)
    return buf.read()

def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    lower_map = {c.lower(): c for c in df.columns}
    def pick(*cands):
        for c in cands:
            if c in lower_map:
                return lower_map[c]
        pats = [re.compile(r) for r in cands if r]
        for lc, orig in lower_map.items():
            if any(p.search(lc) for p in pats):
                return orig
        return None

    col_date   = pick("date","day","日時","日付",r"^time",r"^date")
    col_steps  = pick("steps","歩数",r"step")
    col_active = pick("active_energy_kcal",r"active.*(kcal|energy)","アクティブエネルギー",r"消費.*kcal")
    col_sleep  = pick("sleep_hours",r"sleep.*(hour|hr)","睡眠",r"sleep.*dur")
    col_rhr    = pick("resting_hr_bpm",r"rest.*heart.*rate","安静時心拍",r"rhr")
    col_weight = pick("weight_kg",r"body.*mass","体重",r"weight")

    rename_map = {}
    if col_date:   rename_map[col_date]   = "date"
    if col_steps:  rename_map[col_steps]  = "steps"
    if col_active: rename_map[col_active] = "active_energy_kcal"
    if col_sleep:  rename_map[col_sleep]  = "sleep_hours"
    if col_rhr:    rename_map[col_rhr]    = "resting_hr_bpm"
    if col_weight: rename_map[col_weight] = "weight_kg"

    df2 = df.rename(columns=rename_map)
    if "date" in df2.columns:
        try:
            df2["date"] = pd.to_datetime(df2["date"]).dt.strftime("%Y-%m-%d")
            df2 = df2.sort_values("date")
        except Exception:
            pass

    cols = [c for c in ["date","steps","active_energy_kcal","sleep_hours","resting_hr_bpm","weight_kg"] if c in df2.columns]
    return df2[cols].tail(7)

@app.route("/latest-health", methods=["GET"])
def latest_health():
    if not _auth_ok(request):
        abort(401)
    if not FOLDER_ID:
        return jsonify({"error": "GDRIVE_FOLDER_ID が未設定です。"}), 500
    try:
        file_id = _latest_csv_file_id(FOLDER_ID)
        csv_bytes = _download_csv_bytes(file_id)
        df = pd.read_csv(io.BytesIO(csv_bytes))
        out = _normalize_df(df)
        return jsonify(out.to_dict(orient="records"))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/healthz", methods=["GET"])
def healthz():
    return "ok", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
