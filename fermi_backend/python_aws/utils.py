import pathlib
import sys
import os
import threading
import string
import random
from typing import Union, Dict
from fastapi import UploadFile
import shutil
import uuid



class ProgressPercentage(object):
    """
    A class to track the progress of an upload process (e.g. S3 upload)
    """
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size, percentage
                )
            )
            sys.stdout.flush()


def id_generator(size: int = 8, chars: str = string.ascii_uppercase + string.digits) -> str:
    """
    Generate a random string with a given length *size* and a given collection of characters. Each element in the
    random string is picked from *chars*
    """
    return ''.join(random.SystemRandom().choice(chars) for _ in range(size))


def is_prime(num: int) -> bool:
    """
    Check if an input *num* is a prime number or not.  Return True is *num* is a prime number
    """
    if num > 1:
        # Iterate from 2 to n / 2
        for i in range(2, num // 2):
            # If num is divisible by any number between
            # 2 and n / 2, it is not prime
            if (num % i) == 0:
                return False
        else:
            return True

    else:
        return False


def find_primes(num_start: int, num_end: int) -> int:
    """
    Return the total number of primes between *num_start* and *num_end*
    """
    if num_start > num_end:
        num_start, num_end = num_end, num_start

    count = 0
    for num in range(num_start, num_end+1):
        if is_prime(num):
            count += 1
    # print("count is: {}".format(count))
    return count


def wrapperFn(data: Dict[str, Union[int, str]]) -> Dict[str, Union[int, str]]:
    """
    Calculate number of primes between df["start"] and df["end"], and write the result to df["count"]
    """
    try:
        data["count"] = find_primes(data["start"], data["end"])
        return data
    except ValueError as err:
        print("Compute failed with error: {0}".format(err))

def save_upload_file(upload_file: UploadFile, destination: pathlib.Path, unique_mode=True) -> pathlib.Path:
    if unique_mode:
        suffix = pathlib.Path(upload_file.filename).suffix
        stem = pathlib.Path(upload_file.filename).stem
        destination = destination.parent.joinpath(stem + "_" + str(uuid.uuid4())[:8] + suffix)

    try:
        with destination.open("wb") as buffer:
            shutil.copyfileobj(upload_file.file, buffer)
            tmp_path = pathlib.Path(destination.name)
    except:
        raise FileNotFoundError
    finally:
        upload_file.file.close()
    return tmp_path