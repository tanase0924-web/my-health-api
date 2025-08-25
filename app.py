# app.py
import os
import io
import re
import json
import datetime as dt
from typing import List, Dict, Any, Tuple

import pandas as pd
from flask import Flask, jsonify, request, Response
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import pytz  # ← 追加（requirements.txt に pytz==2025.2 を追記）

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
        raise FileNotFoundError(
            f"Service account not found at {SA_PATH} and SERVICE_ACCOUNT_JSON not set"
        )

drive = build("drive", "v3", credentials=creds)

app = Flask(__name__)

# =========================
# 認可ヘルパ
# =========================
def _auth_ok(req) -> bool:
    return (API_KEY is not None) and (req.headers.get("X-API-Key") == API_KEY)

# =========================
# Drive: 最新CSVの取得（メタ付き）
# - ファイル名日付(YYYY-MM-DD / YYYY_MM_DD / YYYYMMDD) > modifiedTime の順で決定
# - mimeType は text/csv, application/vnd.ms-excel を許容
# - 指定フォルダ「直下」を探索
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

def download_latest_csv_from_drive_with_meta(folder_id: str) -> Tuple[bytes, Dict[str, Any]]:
    if not folder_id:
        raise ValueError("GDRIVE_FOLDER_ID 未設定")

    q = (
        f"'{folder_id}' in parents and trashed=false and ("
        "mimeType='text/csv' or mimeType='application/vnd.ms-excel'"
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

    def sort_key(f):
        dname = _parse_date_from_name(f.get("name", ""))
        mtime = _parse_iso_dt(f.get("modifiedTime", ""))
        return (dname, mtime)

    files.sort(key=sort_key, reverse=True)
    latest = files[0]

    buf = io.BytesIO()
    req = drive.files().get_media(fileId=latest["id"])
    downloader = MediaIoBaseDownload(buf, req, chunksize=1024 * 1024)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    buf.seek(0)
    meta = {
        "id": latest["id"],
        "name": latest.get("name"),
        "modifiedTime": latest.get("modifiedTime"),  # ISO UTC
    }
    return buf.read(), meta

# =========================
# CSV 正規化 → 直近7日
# =========================
def _colmap(columns: List[str]) -> Dict[str, str]:
    lower_map = {c.lower(): c for c in columns}

    def pick(*cands: str) -> str:
        # 完全一致（lower）優先
        for key in cands:
            if key in lower_map:
                return lower_map[key]
        # サブストリング柔軟一致
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
        if x is None:
            return None
        if isinstance(x, float) and pd.isna(x):
            return None
        if isinstance(x, str) and x.strip() == "":
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

    def get_col(key: str):
        col = cmap[key]
        return df[col] if col and col in df.columns else None

    steps = get_col("steps")
    sleep = get_col("sleep_hours")
    active = get_col("active_energy_kcal")
    rhr = get_col("resting_hr_bpm")
    weight = get_col("weight_kg")

    out: List[Dict[str, Any]] = []
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

    out.sort(key=lambda r: r["date"])
    return out[-7:]

# =========================
# YAMLダッシュボード生成
# =========================
def _round0(x):
    return None if x is None else int(round(float(x)))

def _round1(x):
    return None if x is None else round(float(x), 1)

def build_yaml_dashboard(rows: List[Dict[str, Any]]) -> str:
    import statistics as st
    rows = sorted(rows, key=lambda r: r["date"])
    latest = rows[-1]

    def safe_vals(key):
        return [r[key] for r in rows if r.get(key) is not None]

    avg_steps  = _round0(st.mean(safe_vals("steps"))) if safe_vals("steps") else None
    avg_sleep  = _round1(st.mean(safe_vals("sleep_hours"))) if safe_vals("sleep_hours") else None
    avg_active = _round0(st.mean(safe_vals("active_energy_kcal"))) if safe_vals("active_energy_kcal") else None
    avg_weight = _round1(st.mean(safe_vals("weight_kg"))) if safe_vals("weight_kg") else None

    flags = []
    if latest.get("steps", 0) < 5000:
        flags.append("今日の歩数が少なめ→途中経過の可能性または活動不足")
    short_sleep_days = [d for d in rows if (d.get("sleep_hours") is not None and d["sleep_hours"] < 6.0)]
    if short_sleep_days:
        flags.append(f"睡眠6h未満の日あり（{len(short_sleep_days)}日）→回復重視を推奨")
    if latest.get("resting_hr_bpm") and latest["resting_hr_bpm"] >= 75:
        flags.append("安静時心拍がやや高め→睡眠/ストレス/水分を確認")

    def fmt_day(d):
        return (
            f"    - {{日付: {d['date']}, 歩数: {_round0(d.get('steps'))}, "
            f"睡眠_h: {_round1(d.get('sleep_hours'))}, 活動_kcal: {_round0(d.get('active_energy_kcal'))}, "
            f"体重_kg: {_round1(d.get('weight_kg'))}}}"
        )

    y = []
    y.append(f"日付: {latest['date']}")
    y.append("最新値:")
    y.append(f"  体重_kg: {_round1(latest.get('weight_kg'))}")
    y.append(f"  睡眠_h: {_round1(latest.get('sleep_hours'))}")
    y.append(f"  歩数: {_round0(latest.get('steps'))}")
    y.append(f"  活動エネルギー_kcal: {_round0(latest.get('active_energy_kcal'))}")
    y.append(f"  安静時心拍_bpm: {_round1(latest.get('resting_hr_bpm'))}")
    y.append("週平均:")
    y.append(f"  歩数: {avg_steps}")
    y.append(f"  睡眠_h: {avg_sleep}")
    y.append(f"  活動_kcal: {avg_active}")
    y.append(f"  体重_kg: {avg_weight}")
    y.append("週次サマリ:")
    y.append(f"  期間: {rows[0]['date']}〜{rows[-1]['date']}")
    y.append("  日別一覧:")
    y.extend(fmt_day(d) for d in rows)
    if flags:
        y.append("注意点:")
        y.extend([f"  - {f}" for f in flags])
    return "\n".join(y)

# =========================
# 時刻系ユーティリティ
# =========================
def to_local_from_iso_utc(dt_str: str, tz_name: str) -> str:
    """ISO UTC文字列（例: 2025-08-25T00:42:10.123Z）をローカル時刻に整形"""
    if not dt_str:
        return ""
    try:
        tz = pytz.timezone(tz_name)
    except Exception:
        tz = pytz.timezone("Asia/Tokyo")
    dt_utc = dt.datetime.fromisoformat(dt_str.replace("Z", "+00:00")).astimezone(dt.timezone.utc)
    return dt_utc.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S")

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
        csv_bytes, _ = download_latest_csv_from_drive_with_meta(FOLDER_ID)
        data = normalize_and_last_7(csv_bytes)
        return jsonify(data), 200
    except FileNotFoundError as e:
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        return jsonify({"error": f"CSV取得/読込に失敗: {str(e)}"}), 500

@app.get("/daily-dashboard")
def daily_dashboard():
    if not _auth_ok(request):
        return jsonify({"error": "Unauthorized"}), 401
    tz_name = request.args.get("tz", "Asia/Tokyo")
    force_today = request.args.get("force_today", "0").lower() in ("1", "true", "yes")

    try:
        csv_bytes, meta = download_latest_csv_from_drive_with_meta(FOLDER_ID)
        rows = normalize_and_last_7(csv_bytes)
        text = build_yaml_dashboard(rows)

        # 見出し日付の上書き（途中経過） & 最終更新メタの追記
        modified_local = to_local_from_iso_utc(meta.get("modifiedTime", ""), tz_name)
        header_line = f"最終更新: {modified_local} [{meta.get('name')}]"
        if force_today:
            try:
                today_local = dt.datetime.now(pytz.timezone(tz_name)).date().isoformat()
            except Exception:
                today_local = dt.date.today().isoformat()
            text = text.replace(f"日付: {rows[-1]['date']}", f"日付: {today_local}（途中経過）")
        text = header_line + "\n" + text

        resp = Response(text, content_type="text/plain; charset=utf-8")
        resp.headers["Cache-Control"] = "no-store"
        return resp, 200
    except FileNotFoundError as e:
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        return jsonify({"error": f"生成失敗: {str(e)}"}), 500

@app.get("/daily-dashboard.json")
def daily_dashboard_json():
    if not _auth_ok(request):
        return jsonify({"error": "Unauthorized"}), 401
    tz_name = request.args.get("tz", "Asia/Tokyo")
    force_today = request.args.get("force_today", "0").lower() in ("1", "true", "yes")

    try:
        csv_bytes, meta = download_latest_csv_from_drive_with_meta(FOLDER_ID)
        rows = normalize_and_last_7(csv_bytes)
        text = build_yaml_dashboard(rows)

        # 同様に加工して JSON で返す
        modified_local = to_local_from_iso_utc(meta.get("modifiedTime", ""), tz_name)
        header_line = f"最終更新: {modified_local} [{meta.get('name')}]"
        if force_today:
            try:
                today_local = dt.datetime.now(pytz.timezone(tz_name)).date().isoformat()
            except Exception:
                today_local = dt.date.today().isoformat()
            text = text.replace(f"日付: {rows[-1]['date']}", f"日付: {today_local}（途中経過）")
        text = header_line + "\n" + text

        return jsonify({"content": text}), 200
    except FileNotFoundError as e:
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        return jsonify({"error": f"生成失敗: {str(e)}"}), 500

if __name__ == "__main__":
    # ローカル実行用
    app.run(host="0.0.0.0", port=10000, debug=True)
