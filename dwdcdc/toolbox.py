#!/usr/bin/env python
# coding: utf-8

"""
General tooling tailored for DWD data.
Expected to fit for all datasets (may need refactoring over time).

Created: 25.09.20
"""

from datetime import datetime, date, timedelta
from typing import Union
import re


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

# regex shall not test "too well" to not issue misguiding error messages
REX_ISO_DAILY = re.compile(r"[0-9]{4,4}-[0-9]{2,2}-[0-9]{2,2}")
REX_ISO_HOURLY = re.compile(r"[0-9]{4,4}-[0-9]{2,2}-[0-9]{2,2} [0-9]{2,2}")
REX_DWDTS_DAILY = re.compile(r"[0-9]{4,4}[0-9]{2,2}[0-9]{2,2}")
REX_DWDTS_HOURLY = re.compile(r"[0-9]{4,4}[0-9]{2,2}[0-9]{2,2}[0-9]{2,2}")

class PointInTime:

    def __init__(self, value: Union[str, date, datetime]):
        """
        Initialize a daily or hourly PointInTime. Daily PointInTime will be created from date or string like
        "20211216" or "2021-12-16". Hourly PointInTime will be created from datetime or string like "2021121614"
        or "2021-12-16 14".
        :param value: variant value as described above
        """
        self.value = None
        if isinstance(value, datetime):
            hourly = True
            self.value = value
        elif isinstance(value, date):
            hourly = False
            self.value = value
        else:
            if not isinstance(value, str):
                raise ValueError("pass date, datetime or string", type(value), f"{value}")
            if "-" in value or " " in value:  # dash or space in it -> must be iso
                if len(value) == 10:
                    hourly = False
                    if not REX_ISO_DAILY.match(value):
                        raise AssertionError("format must match YYYY-MM-DD", value)
                else:
                    if not (len(value) == 13 and REX_ISO_HOURLY.match(value)):
                        raise ValueError("format must match YYYY-MM-DD HH", value)
                    hourly = True
                # draft check whether it looks right
                y = value[:4]
                if not (y.isdigit() and ("1700" <= y <= "2100")):
                    raise ValueError("invalid year", value)
                m = value[5:7]
                if not (m.isdigit() and ("01" <= m <= "12")):
                    raise ValueError("invalid month", value)
                d = value[8:10]
                if not (d.isdigit() and ("01" <= d <= "31")):
                    raise ValueError("invalid day", value)
                if len(value) > 10:
                    h = value[11:13]
                    if not (h.isdigit() and ("00" <= h <= "23")):
                        raise ValueError("invalid hour", value)
                if hourly:
                    self.value = datetime.strptime(value, "%Y-%m-%d %H")
                else:
                    self.value = datetime.strptime(value, "%Y-%m-%d").date()
            else:  # no dash in it -> must be dwdts
                if len(value) == 8:
                    hourly = False
                    if not REX_DWDTS_DAILY.match(value):
                        raise AssertionError("format must match YYYYMMDD")
                else:
                    if not (len(value) == 10 and REX_DWDTS_HOURLY.match(value)):
                        raise ValueError("format must match YYYYMMDDHH", value)
                    hourly = True
                # draft check whether it looks right
                y = value[:4]
                if not (y.isdigit() and ("1700" <= y <= "2100")):
                    raise ValueError("invalid year", value)
                m = value[4:6]
                if not (m.isdigit() and ("01" <= m <= "12")):
                    raise ValueError("invalid month", value)
                d = value[6:8]
                if not (d.isdigit() and ("01" <= d <= "31")):
                    raise ValueError("invalid day", value)
                if len(value) > 8:
                    h = value[8:10]
                    if not (h.isdigit() and ("00" <= h <= "23")):
                        raise ValueError("invalid hour", value)
                if hourly:
                    self.value = datetime.strptime(value, "%Y%m%d%H")
                else:
                    self.value = datetime.strptime(value, "%Y%m%d").date()
        self.hourly = hourly

    def dwdts(self) -> str:
        """
        Retrieve day in DWD format (like "20211216").
        :return: value in DWD format (like "20211216")
        """
        if self.hourly:
            return self.value.strftime("%Y%m%d%H")
        else:
            return self.value.strftime("%Y%m%d")

    def iso(self) -> str:
        """
        Retrieve day in ISO format (like "2021-12-16").
        :return: value in ISO format (like "2021-12-16")
        """
        if self.hourly:
            return self.value.strftime("%Y-%m-%d %H")
        else:
            return self.value.strftime("%Y-%m-%d")

    def datetime(self) -> Union[date, datetime]:
        """
        Retrieve day in datetime format.
        :return: datetime or date value (dependig whether Pit it's hourly)
        """
        return self.value

    def __str__(self):
        return self.iso()

    def __repr__(self):
        return f"dwdcdc.toolbox.PointInTime('{self.iso()}',{self.hourly})"

    def __sub__(self, other) -> int:
        """
        Calculate interval length in days for daily PointInTimes, i.e. "2019-12-24" - "2019-12-23" = 2 and
        interval lengths in hours for hourly PointInTimes, i.e. "2019-12-24 15" - "2019-12-24 14" = 2.
        :param other: another PointInTime
        :return: interval length [self, other] including endpoints
        """

        # DONE make other a PointInTime, if it's not yet
        # assert isinstance(other, PointInTime), "Pit-minus only supported for other PointInTime (yet)."
        # auto-typecast
        if not isinstance(other, PointInTime):
            other = PointInTime(other)

        if self.hourly != other.hourly:
            if self.hourly:
                raise ValueError("PointInTime subtract: second operand must also be hourly.", self.iso(), other.iso())
            else:
                raise ValueError("PointInTime subtract: second operand must also be daily.", self.iso(), other.iso())
        ts = self.value
        to = other.value
        if ts < to:
            to, ts = ts, to
        td = int(round((ts - to).total_seconds(), 0))
        if self.hourly:
            td = td // 3600 + 1
        else:
            td = td // 86400 + 1
        return td

    def next(self):
        """
        Add 1 day to a daily PointInTime or 1 hour to a hourly PointInTime and return the resulting PointInTime.
        :return: next valid value
        """
        if self.hourly:
            dt = timedelta(hours=1)
        else:
            dt = timedelta(days=1)
        return PointInTime(self.value + dt)

    def prev(self):
        """
        Subtract 1 day from a daily PointInTime or 1 hour from a hourly PointInTime and return the resulting PointInTime.
        :return: next valid value
        """
        if self.hourly:
            dt = timedelta(hours=-1)
        else:
            dt = timedelta(days=-1)
        return PointInTime(self.value + dt)


class Pit(PointInTime):
    pass
