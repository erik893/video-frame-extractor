import os, json, tempfile, subprocess, uuid
import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

# --------- Shared: Cloud Run -> OAuth token via metadata server ----------
def _access_token():
    r = requests.get(
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
        headers={"Metadata-Flavor": "Google"},
        timeout=10,
    )
    r.raise_for_status()
    return r.json()["access_token"]

def _auth_hdr(token: str):
    return {"Authorization": f"Bearer {token}"}


# ======================================================================
# 1) COUNT MEDIA IN A DRIVE FOLDER
# ======================================================================
class CountReq(BaseModel):
    folderId: str

@app.post("/count-media")
def count_media(req: CountReq):
    token = _access_token()
    q = f"'{req.folderId}' in parents and trashed=false"

    url = "https://www.googleapis.com/drive/v3/files"
    params = {
        "q": q,
        "fields": "files(id,name,mimeType)",
        "pageSize": 1000,
        "supportsAllDrives": "true",
        "includeItemsFromAllDrives": "true",
    }

    r = requests.get(url, headers=_auth_hdr(token), params=params, timeout=30)
    if r.status_code >= 300:
        raise HTTPException(r.status_code, r.text)

    files = r.json().get("files", [])

    videos = 0
    images = 0
    other = 0
    for f in files:
        mt = f.get("mimeType", "")
        if mt.startswith("video/"):
            videos += 1
        elif mt.startswith("image/"):
            images += 1
        else:
            other += 1

    return {
        "folderId": req.folderId,
        "videos": videos,
        "images": images,
        "other": other,
        "total": len(files),
    }


# ======================================================================
# 2) EXTRACT FRAMES FROM ONE VIDEO AND SAVE FRAMES IN THE SAME FOLDER
# ======================================================================
class ExtractReq(BaseModel):
    fileId: str
    frames: int = 20
    min_gap_sec: float = 2.0
    max_width: int = 640

def download_drive_video(file_id: str, out_path: str):
    token = _access_token()
    url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media&supportsAllDrives=true"

    with requests.get(url, headers=_auth_hdr(token), stream=True, timeout=300) as r:
        if r.status_code == 404:
            raise HTTPException(404, "Drive file not found (share file/folder with service account)")
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

def get_video_parent_folder(file_id: str) -> str:
    token = _access_token()
    url = f"https://www.googleapis.com/drive/v3/files/{file_id}"
    params = {
        "fields": "parents",
        "supportsAllDrives": "true",
    }

    r = requests.get(url, headers=_auth_hdr(token), params=params, timeout=20)
    if r.status_code >= 300:
        raise HTTPException(r.status_code, f"Cannot read video metadata: {r.text}")

    parents = r.json().get("parents", [])
    if not parents:
        raise HTTPException(400, "Video has no parent folder (cannot decide where to store frames)")

    return parents[0]

def upload_jpg(folder_id: str, filename: str, jpg_bytes: bytes) -> str:
    token = _access_token()
    boundary = "====BOUNDARY" + uuid.uuid4().hex

    metadata = {"name": filename, "parents": [folder_id]}

    part1 = (
        f"--{boundary}\r\n"
        "Content-Type: application/json; charset=UTF-8\r\n\r\n"
        f"{json.dumps(metadata)}\r\n"
    ).encode("utf-8")

    part2 = (
        f"--{boundary}\r\n"
        "Content-Type: image/jpeg\r\n\r\n"
    ).encode("utf-8") + jpg_bytes + b"\r\n"

    ending = f"--{boundary}--\r\n".encode("utf-8")
    body = part1 + part2 + ending

    url = "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&supportsAllDrives=true"
    r = requests.post(
        url,
        headers={**_auth_hdr(token), "Content-Type": f"multipart/related; boundary={boundary}"},
        data=body,
        timeout=60
    )
    if r.status_code >= 300:
        raise HTTPException(r.status_code, f"Upload failed: {r.text}")

    return r.json()["id"]

@app.post("/extract-and-save")
def extract_and_save(req: ExtractReq):
    with tempfile.TemporaryDirectory() as td:
        video_path = os.path.join(td, "video.mp4")
        frames_dir = os.path.join(td, "frames")
        os.makedirs(frames_dir, exist_ok=True)

        # 1) Download video
        download_drive_video(req.fileId, video_path)

        # 2) Extract frames
        frame_files, dur = extract_frames(
            video_path,
            frames_dir,
            req.frames,
            req.min_gap_sec,
            req.max_width,
        )

        # 3) Find parent folder of the video
        parent_folder_id = get_video_parent_folder(req.fileId)

        # 4) Upload frames into SAME folder as the video
        ids = []
        for i, fp in enumerate(frame_files):
            with open(fp, "rb") as f:
                ids.append(
                    upload_jpg(
                        parent_folder_id,
                        f"{req.fileId}_frame_{i:03d}.jpg",
                        f.read(),
                    )
                )

        return {
            "videoId": req.fileId,
            "durationSec": dur,
            "frameFileIds": ids,
            "savedToFolderId": parent_folder_id
        }
