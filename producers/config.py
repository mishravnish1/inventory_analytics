from dotenv import load_dotenv
import os
from pathlib import Path

env_path = f"{Path(__file__).resolve().parent}/.env_sample"
load_dotenv(env_path)

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP.SERVERS")
