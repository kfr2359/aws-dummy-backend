from contextlib import asynccontextmanager

from ec2_metadata import ec2_metadata
from fastapi import FastAPI

from db import get_engine, init_db
from routers.images import router as images_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: create tables if needed (single-table demo setup).
    init_db(get_engine())
    yield
    # Shutdown: nothing to clean up explicitly.


app = FastAPI(lifespan=lifespan)


app.include_router(images_router, prefix="/images", tags=["images"])


@app.get("/")
async def get_root():
    return {
        'az': ec2_metadata.availability_zone,
        'region': ec2_metadata.region,
    }
