#!/usr/bin/env python
# coding: utf-8

"""
Check completeness of data.

Created: 27.09.20
"""

import json
from typing import List, Union
from datetime import datetime, timedelta
from time import perf_counter
import logging
from dataclasses import dataclass

import johanna

from dwdcdc.toolbox import PointInTime
from dwdcdc.dbtable import get_column_list, get_indicator_select, get_data_fields, get_two, filter_fields

def get_data(station: int, field: str, tabname: str) -> List[list]:
    """

    :param station:
    :param field:
    :param tabname:
    :return:
    """
    # TODO für Stundenwerte können das 1,2 Mio Sätze werden -> irgendwie anders machen
    sql = "SELECT " + \
          f"""  dwdts, {field} 
                FROM {tabname} 
                WHERE station = ? 
                    AND {field} IS NOT NULL 
                ORDER BY dwdts"""
    with johanna.Connection(text="select values") as c:
        c.cur.execute(sql, (station, ))
        rows = c.cur.fetchall()
    return rows


def dwdts_diff(ts0: str, ts1: str) -> int:
    """

    :param ts0: like '19690523'
    :param ts1: like '19690525' >= ts1
    :return: difference (in days)
    """

def complete(station: int, fields: List[str], tabname: str = "readings", hours: bool = False) -> dict:
    """

    :param station: numerical station id
    :param fields: list of field names to check
    :param tabname: table name (must have primary key (station, dwdts)
    :param hours: False when daily data is expected (hourly data not supported yet)
    :return: dict with key field and value list of timefremes with consecutive data
    """

    assert not hours, "hourly data not supported yet"
    day = 86400

    resu = {}
    for field in fields:
        resu[field] = []
        rows = get_data(station, field, tabname)
        if len(rows) == 0:
            break
        dwdts0 = rows[0][0]
        ts0 = datetime.strptime(dwdts0, '%Y%m%d')
        tf = [dwdts0, None]
        resu[field].append(tf)
        for i, row in enumerate(rows[1:]):
            dwdts = row[0]
            ts = datetime.strptime(dwdts, '%Y%m%d')
            if int(round((ts - ts0).total_seconds(),0)) != day:
                tf[1] = dwdts0
                tf = [dwdts, None]
                resu[field].append(tf)
            dwdts0 = dwdts
            ts0 = ts
        tf[1] = dwdts
    return resu


def iso(x: Union[str, datetime]) -> str:
    """
    Transform dwdts (like "20211216") or datetime to iso date (like "2021-12-16").
    :param x: dwdts or datetime
    :return: iso date as string
    """
    if isinstance(x, str):
        return f"{x[0:4]}-{x[4:6]}-{x[6:]}"
    elif isinstance(x, datetime):
        return x.strftime("%Y-%m-%d")
    else:
        assert False, type(x)


def dwd(x: Union[str, datetime]):
    """
    Transform iso date (like "2021-12-16") or datetime to dwdts (like "20211216")
    :param x: iso date or datetime
    :return: dwdts
    """
    if isinstance(x, str) and "-" in x:
        return x.replace("-", "")
    elif isinstance(x, datetime):
        return x.strftime("%Y%m%d")
    else:
        assert False, type(x)


def delta(isots0: str, isots1: str) -> int:
    """
    Calculate days between (including ends) two dates, i.e. "1989-12-25" - "1989-12-24" = 2.
    :param isots0: an ISO date (like "1989-12-24")
    :param isots1: an ISO date (like "1989-12-25") >= isots0
    :return: days between the two parameters, including ends
    """
    ts0 = datetime.strptime(isots0, '%Y-%m-%d')
    ts1 = datetime.strptime(isots1, '%Y-%m-%d')
    if ts1 < ts0:
        ts0, ts1 = ts1, ts0  # I always wanted to code such :)
    return int(round((ts1 - ts0).total_seconds() / 86400.0,0)) + 1


@dataclass
class Timeframe:
    ts_from: PointInTime
    ts_to: PointInTime
    indicators: str
    days: int
    rows: list

    def _json(self, with_rows: bool = False):
        o = {
                "from": self.ts_from.iso(),
                "to": self.ts_to.iso(),
                "indicators": self.indicators,
                "days": self.days
            }
        if with_rows:
            if hasattr(self, "rows"):
                o["rows"] = self.rows
            else:
                o["rows"] = None
        return o

def _persist(tfl: List[Timeframe], fields: List[str], name: Union[str, int], with_rows: bool = False) -> None:
    assert isinstance(tfl, list)
    assert isinstance(fields, list)
    if isinstance(name, int):
        name = str(name)
    assert isinstance(name, str)
    assert isinstance(with_rows, bool)

    folder = johanna.private._DOTFOLDER
    s = json.dumps({
        "fields": fields,
        "timeframes": [tf._json(with_rows=with_rows) for tf in tfl]
    }, indent=4)
    fnam = folder / f"{name}-new.json"
    with open(fnam, "w") as fh:
        fh.write(s)
    logging.info(f"List[Timeframe] ({len(tfl)} rows) -> {fnam}")


def overview(station: int, tabname: str = "readings", fields: List[str] = None, with_rows: bool = False) -> List[Timeframe]:
    assert isinstance(station, int)
    assert isinstance(tabname, str)
    if not fields:
        fields = get_data_fields(tabname=tabname)
    assert isinstance(fields, list)
    assert isinstance(with_rows, bool)

    sql = get_indicator_select(tabname=tabname, fields=fields)
    with johanna.Connection(f"select from {tabname}") as c:
        rows = c.cur.execute(sql, (station, )).fetchall()
    tfs = []
    ts0 = PointInTime(rows[0][0])
    srow0 = "".join(rows[0][1:])  # indicator string
    tf = Timeframe(ts0, None, srow0, None, None)
    tfs.append(tf)
    for i, row in enumerate(rows[1:]):
        ts = PointInTime(row[0])
        srow = "".join(row[1:])  # indicator string
        if ts - ts0 > 1:  # not next day
            # we passed an occurence of '---------' ('-' only)
            #   -> insert n/a interval: [x, _, old] -> [x, ts0, old], [ts0+1, ts-1, n/a], [ts, _, new]
            tf.ts_to = ts0
            tfs.append(Timeframe(ts0.next(), ts.prev(), "no data", None, None))
            tf = Timeframe(ts, None, srow, None, None)
            tfs.append(tf)
        elif srow != srow0:
            tf.ts_to = ts0
            tf = Timeframe(ts, None, srow, None, None)
            tfs.append(tf)
        ts0 = ts
        srow0 = srow
    tf.ts_to = ts
    for tf in tfs:
        tf.days = tf.ts_to - tf.ts_from + 1
    if with_rows:
        for tf in tfs:
            tf.rows = get_two(station, tf.ts_to.dwdts(), tabname=tabname, fields=fields)
    return tfs


def show_overview(station: int, tabname: str = "readings", fields: List[str] = None, with_rows: bool = False) -> List[Timeframe]:
    assert isinstance(station, int)
    assert isinstance(tabname, str)
    if not fields:
        fields = get_data_fields(tabname=tabname)
    assert isinstance(fields, list)
    assert isinstance(with_rows, bool)

    tfs = overview(station=station, tabname=tabname, fields=fields)
    for tf in tfs:
        tf.rows = get_two(station, tf.ts_to.dwdts(), tabname=tabname, fields=fields)
    print()
    for tf in tfs:
        tf_str = f"{tf.ts_from} -{tf.days}-> {tf.ts_to}"
        print(f"{tf_str:30s}      {tf.indicators}")
        if with_rows:
            print("    " + f"{tf.rows[0]}"[1:-1])
            if len(tf.rows) == 2:
                print("    " + f"{tf.rows[1]}"[1:-1])  # remove tuple brackets
    print(f"{len(tfs)} timeframes")
    print(fields)
    print()
    return tfs


def show_timeframens(tfs: List[Timeframe], fields: List[str], with_rows: bool = False) -> None:
    assert isinstance(tfs, list)
    assert isinstance(fields, list)
    assert isinstance(with_rows, bool)

    print()
    for tf in tfs:
        tf_str = f"{tf.ts_from} -{tf.days}-> {tf.ts_to}"
        print(f"{tf_str:30s}      {tf.indicators}")
        if with_rows:
            print("    " + f"{tf.rows[0]}"[1:-1])
            if len(tf.rows) == 2:
                print("    " + f"{tf.rows[1]}"[1:-1])  # remove tuple brackets
    print(f"{len(tfs)} timeframes")
    print(fields)
    print()


if __name__ == "__main__":
    pc0 = perf_counter()
    johanna.interactive(dotfolder="~/.dwd-cdc", dbname="kld.sqlite")

    tabname = "readings"
    fields = get_data_fields()
    # fields = ['resp', 'resp_form', 'temp2m_max', 'temp2m_min']
    # fields = ['temp2m_avg', 'temp2m_max', 'temp2m_min']
    print(fields)
    with_rows = True

    for station in [2444, 2290, 5906]:
        tfs = overview(station=station, tabname=tabname, fields=fields, with_rows=with_rows)
        show_timeframens(tfs, fields, with_rows=with_rows)
        _persist(tfs, fields, station, with_rows=with_rows)

    a = 17
    logging.info(f"total elapased: {perf_counter()-pc0}")