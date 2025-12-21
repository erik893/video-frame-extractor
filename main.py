import os, json, tempfile, subprocess, uuid
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import requests

app = FastAPI()
API_KEY = os.environ.get("API_KEY", "")

class Req(BaseModel):
    fileId: str
    parentFolderId: str
    frames: int = 20
    min_gap_sec: float = 2.0
    max_width: int = 640

def _access_token():
    r = requests.get(
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
        headers={"Metadata-Flavor": "Google"},
        timeout=10,
    )
    r.raise_for_status()
    return r.json()["access_token"]

def _hdr(token: str):
    return {"Authorization": f"Bearer {token}"}

def download_drive_video(file_id: str, out_path: str):
    token = _access_token()
    url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media&supportsAllDrives=true"
    with requests.get(url, headers=_hdr(token), stream=True, timeout=300) as r:
        if r.status_code == 404:
            raise HTTPException(404, "Drive file not found (shared with service account?)")
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

def probe_duration(video_path: str) -> float:
    p = subprocess.run(
        ["ffprobe","-v","error","-show_entries","format=duration",
         "-of","default=noprint_wrappers=1:nokey=1", video_path],
        capture_output=True, text=True
    )
    try:
        return float((p.stdout or "").strip() or "0")
    except:
        return 0.0

def extract_frames(video_path: str, frames_dir: str, n: int, min_gap: float, max_width: int):
    dur = probe_duration(video_path)
    if dur <= 0:
        timestamps = [0.0]
    else:
        raw_step = dur / max(n, 1)
        step = max(min_gap, raw_step)
        timestamps, t = [], 0.0
        while t < dur and len(timestamps) < n:
            timestamps.append(t)
            t += step
        if not timestamps:
            timestamps = [max(0.0, dur - 0.1)]

    out_files = []
    for i, t in enumerate(timestamps):
        out = os.path.join(frames_dir, f"frame_{i:03d}.jpg")
        vf = f"scale='min({max_width},iw)':-2"
        subprocess.run(
            ["ffmpeg","-hide_banner","-loglevel","error",
             "-ss", str(max(0.0, t)),
             "-i", video_path,
             "-frames:v","1",
             "-vf", vf,
             "-q:v","3",
             out],
            check=False
        )
        if os.path.exists(out):
            out_files.append(out)
    return out_files, dur

def create_folder(parent_id: str, name: str) -> str:
    token = _access_token()
    url = "https://www.googleapis.com/drive/v3/files?supportsAllDrives=true"
    meta = {"name": name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]}
    r = requests.post(url, headers={**_hdr(token), "Content-Type":"application/json"}, data=json.dumps(meta), timeout=30)
    if r.status_code >= 300:
        raise HTTPException(r.status_code, f"Create folder failed: {r.text}")
    return r.json()["id"]

def upload_jpg(folder_id: str, filename: str, jpg_bytes: bytes) -> str:
    token = _access_token()
    boundary = "====BOUNDARY" + uuid.uuid4().hex
    metadata = {"name": filename, "parents": [folder_id]}

    part1 = (f"--{boundary}\r\n"
             "Content-Type: application/json; charset=UTF-8\r\n\r\n"
             f"{json.dumps(metadata)}\r\n").encode("utf-8")
    part2 = (f"--{boundary}\r\n"
             "Content-Type: image/jpeg\r\n\r\n").encode("utf-8") + jpg_bytes + b"\r\n"
    ending = f"--{boundary}--\r\n".encode("utf-8")
    body = part1 + part2 + ending

    url = "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&supportsAllDrives=true"
    r = requests.post(url, headers={**_hdr(token), "Content-Type": f"multipart/related; boundary={boundary}"}, data=body, timeout=60)
    if r.status_code >= 300:
        raise HTTPException(r.status_code, f"Upload failed: {r.text}")
    return r.json()["id"]

@app.post("/extract-and-save")
def extract_and_save(req: Req, request: Request):
    if API_KEY and request.headers.get("x-api-key","") != API_KEY:
        raise HTTPException(401, "Unauthorized")

    with tempfile.TemporaryDirectory() as td:
        video_path = os.path.join(td, "video.mp4")
        frames_dir = os.path.join(td, "frames")
        os.makedirs(frames_dir, exist_ok=True)

        download_drive_video(req.fileId, video_path)
        frame_files, dur = extract_frames(video_path, frames_dir, req.frames, req.min_gap_sec, req.max_width)

        frames_folder_id = create_folder(req.parentFolderId, f"__frames__{req.fileId}")

        ids = []
        for i, fp in enumerate(frame_files):
            with open(fp, "rb") as f:
                ids.append(upload_jpg(frames_folder_id, f"frame_{i:03d}.jpg", f.read()))

        return {"videoId": req.fileId, "durationSec": dur, "framesFolderId": frames_folder_id, "frameFileIds": ids}
