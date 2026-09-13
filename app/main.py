from fastapi import FastAPI

app = FastAPI(title="Cerberus")


@app.get("/")
def root():
    return {"message": "Cerberus is running"}