import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

class Req(BaseModel):
    folderId: str

def _access_token():
    r = requests.get(
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
        headers={"Metadata-Flavor": "Google"},
        timeout=10,
    )
    r.raise_for_status()
    return r.json()["access_token"]

@app.post("/count-media")
def count_media(req: Req):
    token = _access_token()

    q = f"'{req.folderId}' in parents and trashed = false"
    url = "https://www.googleapis.com/drive/v3/files"
    params = {
        "q": q,
        "fields": "files(id,name,mimeType)",
        "pageSize": 1000,
        "supportsAllDrives": "true",
        "includeItemsFromAllDrives": "true",
    }

    r = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        params=params,
        timeout=30,
    )

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
