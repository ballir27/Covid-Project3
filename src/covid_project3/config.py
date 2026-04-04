from pathlib import Path

from dotenv import dotenv_values

# Resolve the repository root and load environment variables from `.env`.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / ".env"

# Keep the public name `config` for existing imports across the project.
config = dotenv_values(ENV_FILE)
