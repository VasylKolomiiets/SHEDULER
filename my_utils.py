"""
Created on Wed Jan  6 12:44:09 2021.

@author: Vasil
"""

import sys
import os
import re


if sys.version_info[0] == 2:  # Python 2.x
    text_type = unicode
else:
    text_type = str

_windows_device_files = (
    "CON",
    "AUX",
    "COM1",
    "COM2",
    "COM3",
    "COM4",
    "LPT1",
    "LPT2",
    "LPT3",
    "PRN",
    "NUL",
)

_filename_strip_re = re.compile(r"[^A-Za-zа-яА-ЯёЁєЄїЇіІ0-9_.-]")


def secure_filename(filename: str) -> str:
    """Secure filename with ukrainian letters.

    Author:
    https://ru.stackoverflow.com/questions/587855/%D0%98%D1%81%D0%BF%D0%BE%D0%BB%D1%8C%D0%B7%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D0%B5-werkzeug-secure-filename-%D1%81-%D1%80%D1%83%D1%81%D1%81%D0%BA%D0%B8%D0%BC%D0%B8-%D1%81%D0%B8%D0%BC%D0%B2%D0%BE%D0%BB%D0%B0%D0%BC%D0%B8
    """
    if isinstance(filename, text_type):
        from unicodedata import normalize
        filename = normalize("NFKD", filename)

    for sep in os.path.sep, os.path.altsep:
        if sep:
            filename = filename.replace(sep, " ")

    filename = str(_filename_strip_re.sub("", "_".join(filename.split()))).strip(
        "._"
    )

    if (
        os.name == "nt"
        and filename
        and filename.split(".")[0].upper() in _windows_device_files
    ):
        filename = f"_{filename}"

    return filename
