#!/usr/bin/env python
# coding: utf-8

"""
General tooling tailored for DWD data.
Expected to fit for all datasets (may need refactoring over time).

Created: 25.09.20
"""

from datetime import datetime, date, timedelta
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


def _check(v: str) -> None:
    """
    Check whether string looks valid on first sight.
    :param v: dwdts or iso day or hour
    :return: raises AssertionErrors when format is wrong
    """
    if "-" in v:
        y = v[:4]
        assert y.isdigit() and ("1700" <= y <= "2100"), "invalid year"
        m = v[5:7]
        assert m.isdigit() and ("01" <= m <= "12"), "invalid month"
        d = v[8:10]
        assert d.isdigit() and ("01" <= d <= "31"), "invalid day"
        if len(v) > 10:
            h = v[11:13]
            assert h.isdigit() and ("01" <= h <= "23"), "invalid hour"
    else:
        y = v[:4]
        assert y.isdigit() and ("1700" <= y <= "2100"), "invalid year"
        m = v[4:6]
        assert m.isdigit() and ("01" <= m <= "12"), "invalid month"
        d = v[6:8]
        assert d.isdigit() and ("01" <= d <= "31"), "invalid day"
        if len(v) > 8:
            h = v[8:10]
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


class PointInTime:

    def __init__(self, value: Union[str, date, datetime], hourly: bool = None):
        """
        Initialize a daily or hourly PointInTime. Daily PointInTime will be created from date or string like
        "20211216" or "2021-12-16". Hourly PointInTime will be created from datetime or string like "2021121614"
        or "2021-12-16 14".
        :param value: variant value as described above
        :param hourly: (deprecated) value shall be interpreted as hourly value
        """
        self.value = None

        # autodetect hourly if not specified
        if hourly is None:
            if isinstance(value, datetime):
                hourly = True
            elif isinstance(value, date):
                hourly = False
            else:
                assert isinstance(value, str), "pass date, datetime or string, smurf."
                if "-" in value:
                    if len(value) == 10:
                        hourly = False
                    else:
                        assert len(value) == 13, "format must be YYYY-MM-DD HH"
                        hourly = True
                else:
                    if len(value) == 8:
                        hourly = False
                    else:
                        assert len(value) == 10, "format must be YYYY-MM-DD HH"
                        hourly = True

        # autodetect was added later, so some checks may be redundant
        self.hourly = hourly
        if isinstance(value, datetime):
            assert hourly, "pass date instead of datetime when hourly==True, smurf."
            if hourly:  # store datetime
                self.value = value
        elif isinstance(value, date):
            assert not hourly, "pass datetime instead of date when hourly==True, smurf."
            self.value = value  # store date
        else:
            assert isinstance(value, str), "pass date, datetime or string, smurf."
            if "-" in value:
                if hourly:  # format "YYYY-MM-DD HH"
                    assert len(value) == 13, "format must be YYYY-MM-DD HH"
                    _check(value)
                    self.value = datetime.strptime(value, "%Y-%m-%d %H")
                else:
                    assert len(value) == 10, "format must be YYYY-MM-DD"
                    _check(value)
                    self.value = datetime.strptime(value, "%Y-%m-%d").date()
            else:
                if hourly:  # format "YYYYMMDDHH"
                    assert len(value) == 10, "format must be YYYYMMDDHH"
                    _check(value)
                    self.value = datetime.strptime(value, "%Y%m%d%H")
                else:
                    assert len(value) == 8, "format must be YYYYMMDD"
                    _check(value)
                    self.value = datetime.strptime(value, "%Y%m%d").date()

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

        assert self.hourly == other.hourly, "a-b only supported for same hourly-value."
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
