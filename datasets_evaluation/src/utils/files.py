import pathlib

MAIN_DIRECTORY = pathlib.Path(__file__).parent.parent.parent.parent


def get_full_path(path):
    return str(MAIN_DIRECTORY / path)