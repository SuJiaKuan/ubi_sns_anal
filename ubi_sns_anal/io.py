import pathlib


def mkdir_p(dir_path):
    pathlib.Path(dir_path).mkdir(parents=True, exist_ok=True)
