import os

import psutil

from logger import logging


def get_file_size_mb(file_path):
    file_stats = os.stat(file_path)
    return file_stats.st_size / (1024 * 1024)


def list_files(path):
    return next(os.walk(path))[2]


def list_directories(path):
    return next(os.walk(path))[1]


def get_basename_without_extension(filename):
    return os.path.splitext(os.path.basename(filename))[0]


def get_filename_extension(filename):
    return os.path.splitext(os.path.basename(filename))[1]


s3_opened_files = None


def filter_opened_files(files, base_folder, warnings=True):
    global s3_opened_files
    if s3_opened_files is None:
        s3_opened_files = [opened_file
                           for opened_files_by_process in get_opened_files(warnings)
                           for opened_file in opened_files_by_process
                           if base_folder in opened_file]
    return [file for file in files
            if file not in s3_opened_files]


def get_opened_files(warnings=True):
    for pid in psutil.pids():
        try:
            yield (file[0] for file in psutil.Process(pid).open_files())
        except psutil.AccessDenied as e:
            if warnings:
                logging.warning("Access denied while getting process opened files")
                logging.warning(str(e))
