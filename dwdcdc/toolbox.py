#!/usr/bin/env python
# coding: utf-8

"""
General tooling tailored for DWD data.
Expected to fit for all datasets (may need refactoring over time).

Created: 25.09.20
"""

from datetime import datetime
from typing import Union


def static_vars(**kwargs):
    # https://stackoverflow.com/questions/279561/what-is-the-python-equivalent-of-static-variables-inside-a-function
    def decorate(func):
        for k in kwargs:
            setattr(func, k, kwargs[k])
        return func
    return decorate


def dwdts2iso(s: str) -> str:
    """
    Formats a CDC timestamp as ISO, e.g. 19690523 -> 1969-05-23
    :param s: Timestamp like YYYYMMDD or YYYYMMDDHH
    :return: ISO Representation like YYYY-MM-DD or YYYY-MM-DD HH:00:00
    """
    if len(s) == 8:
        # 19690523 -> 1969-05-23
        return "%s-%s-%s" % (s[0:4], s[4:6], s[6:])
    elif len(s) == 10:
        # 1969052307 -> 1969-05-23 07:00:00 (not clear what time would be the "right" one,
        #   so go for one that preserves the day)
        return "%s-%s-%s %s:00:00" % (s[0:4], s[4:6], s[6:8], s[8:10])


# frei nach https://www.giga.de/ratgeber/specials/abkuerzungen-der-bundeslaender-in-deutschland-tabelle/
LAND_MAP = {
    "Baden-Württemberg": "BaWü",  # BW
    "Bayern": "BY",
    "Berlin": "BER",  # BE
    "Brandenburg": "BB",
    "Bremen": "HB",
    "Hamburg": "HH",
    "Hessen": "HE",
    "Mecklenburg-Vorpommern": "MV",
    "Niedersachsen": "NDS",
    "Nordrhein-Westfalen": "NRW",  # NW
    "Rheinland-Pfalz": "RLP",  # RP
    "Saarland": "SL",
    "Sachsen": "SN",
    "Sachsen-Anhalt": "ST",
    "Schleswig-Holstein": "SH",
    "Thüringen": "TH"
}


def dwdland2short(dwdland: str) -> str:
    if dwdland in LAND_MAP:
        return LAND_MAP[dwdland]
    else:
        return "?"


def station_from_fnam(fnam: str) -> int:
    return int(fnam.split(".")[0].split("_")[2])


def _check(value: str) -> None:
    """
    Check whether string looks valid on first sight.
    :param value: dwdts or iso day or hour
    :return: raises AssertionErrors when format is wrong
    """
    if "-" in value:
        y = value[:4]
        assert y.isdigit() and ("1700" <= y <= "2100"), "invalid year"
        m = value[5:7]
        assert m.isdigit() and ("01" <= m <= "12"), "invalid month"
        d = value[8:10]
        assert d.isdigit() and ("01" <= d <= "31"), "invalid day"
        if len(value) > 10:
            h = value[11:13]
            assert h.isdigit() and ("01" <= h <= "23"), "invalid hour"
    else:
        y = value[:4]
        assert y.isdigit() and ("1700" <= y <= "2100"), "invalid year"
        m = value[4:6]
        assert m.isdigit() and ("01" <= m <= "12"), "invalid month"
        d = value[6:8]
        assert d.isdigit() and ("01" <= d <= "31"), "invalid day"
        if len(value) > 8:
            h = value[8:10]
            assert h.isdigit() and ("00" <= h <= "23"), "invalid hour"


def to_iso(x: Union[str, datetime]) -> str:
    """
    Transform dwdts (like "20211216") or datetime to iso date (like "2021-12-16").
    :param x: dwdts or datetime
    :return: iso date as string
    """
    if isinstance(x, str):
        assert len(x) == 8
        _check(x)
        return f"{x[0:4]}-{x[4:6]}-{x[6:]}"
    elif isinstance(x, datetime):
        return x.strftime("%Y-%m-%d")
    else:
        assert False, f"to_iso({x}: {type(x)})"


def to_dwd(x: Union[str, datetime]):
    """
    Transform iso date (like "2021-12-16") or datetime to dwdts (like "20211216")
    :param x: iso date or datetime
    :return: dwdts
    """
    if isinstance(x, str) and "-" in x:
        assert len(x) == 10
        _check(x)
        return x.replace("-", "")
    elif isinstance(x, datetime):
        return x.strftime("%Y%m%d")
    else:
        assert False, f"to_dwd({x}: {type(x)})"
