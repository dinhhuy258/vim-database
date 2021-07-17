import os
from os import path


def is_file_exists(file_path: str) -> bool:
    return path.exists(file_path) and path.isfile(file_path)


def create_folder_if_not_present(folder_path: str) -> None:
    if not is_folder_exists(folder_path):
        os.makedirs(folder_path)


def is_folder_exists(folder_path: str) -> bool:
    return path.exists(folder_path) and path.isdir(folder_path)
