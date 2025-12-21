from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def root():
    return {"message": "Cloud Run is working"}

@app.get("/ping")
def ping():
    return {"status": "ok"}
