from ec2_metadata import ec2_metadata
from fastapi import FastAPI

app = FastAPI()

@app.get("/")
async def get_root():
    return {
        'az': ec2_metadata.availability_zone,
        'region': ec2_metadata.region,
    }
