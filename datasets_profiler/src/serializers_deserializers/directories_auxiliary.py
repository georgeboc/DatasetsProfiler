import os
from pathlib import Path


def try_create_directory(file_path):
    try:
        os.makedirs(Path(file_path).parent)
    except OSError:
        pass