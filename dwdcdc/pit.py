#!/usr/bin/env python
# coding: utf-8

"""
Handle the different representations of a point in time: DWD timestamps ("MESS_DATUM") are looking like '20211216'
(daily data) or '2019032600' (hourly data). ISO like representations like '2021-12-16' (daily data) or '2019-03-26 00'
(hourly data) have enhanced readbility. For calculations, dateime.date (daily data) or datetime.datetime (hourly data)
are better suited.
"""
# Created: 01.10.20


from datetime import datetime, date, timedelta
from typing import Union
import re


# regex shall not test "too well" to not issue misguiding error messages
REX_ISO_DAILY = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
REX_ISO_HOURLY = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}")
REX_DWDTS_DAILY = re.compile(r"[0-9]{4}[0-9]{2}[0-9]{2}")
REX_DWDTS_HOURLY = re.compile(r"[0-9]{4}[0-9]{2}[0-9]{2}[0-9]{2}")


class PointInTime:
    """
    General container for a point in time in either representation.
    """

    def __init__(self, value: Union[str, date, datetime]):
        """
        Initialize a daily or hourly PointInTime. Daily PointInTime will be created from date values or strings
        like "20211216" and "2021-12-16". Hourly PointInTime will be created from datetime values or strings like
        "2021121614" and "2021-12-16 14".
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
        Calculate difference between two PointInTimes. Differene is returned in days for daily PointInTimes,
        i.e. "2019-12-24" - "2019-12-23" = 1. Difference is returned in hours for hourly PointInTimes, i.e.
        "2019-12-24 15" - "2019-12-24 14" = 1.
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
            td = td // 3600
        else:
            td = td // 86400
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
        Subtract 1 day from a daily PointInTime or 1 hour from a hourly PointInTime and return the resulting
        PointInTime.
        :return: previous valid value
        """
        if self.hourly:
            dt = timedelta(hours=-1)
        else:
            dt = timedelta(days=-1)
        return PointInTime(self.value + dt)


class Pit(PointInTime):
    pass
