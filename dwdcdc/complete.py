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

from dwdcdc.toolbox import Day
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
    iso_from: str  # TODO change to toolbox.PointInTime
    iso_to: str
    indicators: str
    days: int
    rows: list


def overview(station: int, tabname: str = "readings", fields: List[str] = None) -> List[Timeframe]:
    day = 86400
    td_day = timedelta(days=1)

    if not fields:
        fields = get_data_fields(tabname=tabname)
    sql = get_indicator_select(tabname=tabname, fields=fields)
    with johanna.Connection(f"select from {tabname}") as c:
        rows = c.cur.execute(sql, (station, )).fetchall()
    resu = []
    dwdts0 = rows[0][0]
    ts0 = datetime.strptime(dwdts0, '%Y%m%d')
    srow0 = "".join(rows[0][1:])  # indicator string
    tf = Timeframe(iso(ts0), None, srow0, None, None)
    resu.append(tf)
    for i, row in enumerate(rows[1:]):
        dwdts = row[0]
        ts = datetime.strptime(dwdts, '%Y%m%d')
        srow = "".join(row[1:])  # indicator string
        if int(round((ts - ts0).total_seconds(),0)) != day:
            x = (ts - ts0).total_seconds()
            # we misssed an occurence of '---------' ('-' only)
            # insert n/a interval: [x, _, old] -> [x, t0, old], [t0+1, t-1, n/a], [t, _, new]
            tf.iso_to = iso(ts0)
            # resu.append(Timeframe(iso(ts0 + td_day), iso(ts - td_day), "."*len(srow), None, None))
            resu.append(Timeframe(iso(ts0 + td_day), iso(ts - td_day), "no data", None, None))
            tf = Timeframe(iso(ts), None, srow, None, None)
            resu.append(tf)
        elif srow != srow0:
            tf.iso_to = iso(ts0)
            tf = Timeframe(iso(ts), None, srow, None, None)
            resu.append(tf)
        dwdts0 = dwdts
        ts0 = ts
        srow0 = srow
    tf.iso_to = iso(ts)
    for tf in resu:
        tf.days = delta(tf.iso_from, tf.iso_to)
    return resu


def show_overview(station: int, tabname: str = "readings", fields: List[str] = None) -> None:
    if not fields:
        fields = get_data_fields(tabname=tabname)
    tfs = overview(station=station, tabname=tabname, fields=fields)
    for tf in tfs:
        tf.rows = get_two(station, tf.iso_to, tabname=tabname, fields=fields)
    print()
    for tf in tfs:
        tf_str = f"{tf.iso_from} -{tf.days}-> {tf.iso_to}"
        print(f"{tf_str:30s}      {tf.indicators}")
        print("    " + f"{tf.rows[0]}"[1:-1])
        if len(tf.rows) == 2:
            print("    " + f"{tf.rows[1]}"[1:-1])  # remove tuple brackets
    print(f"{len(tfs)} timeframes")
    print(fields)
    print()


if __name__ == "__main__":
    station = 2444  #2290  # 5906
    pc0 = perf_counter()
    johanna.interactive(dotfolder="~/.dwd-cdc", dbname="kld.sqlite")
    fields = get_data_fields()
    # fields = ['resp', 'resp_form', 'temp2m_max', 'temp2m_min']
    # fields = ['temp2m_avg', 'temp2m_max', 'temp2m_min']

    print(fields)
    if 1 == 0:
        l = complete(station=5906, fields=fields)
        for field in l:
            print()
            print(field)
            for i, tf in enumerate(l[field]):
                if i > 0:
                    miss = ( datetime.strptime(tf[0], '%Y%m%d') -
                             datetime.strptime(l[field][i-1][1], '%Y%m%d') ).days - 1
                    miss = f"{miss:5d}"
                else:
                    miss = "     "
                consec = ( datetime.strptime(tf[1], '%Y%m%d') -
                             datetime.strptime(tf[0], '%Y%m%d') ).days + 1
                print(f"{miss} {consec:5d} | {iso(tf[0])} .. {iso(tf[1])}")
        print()
        print(l["temp2m_min"] == l["temp2m_max"])
    show_overview(station=station)

    tfs = overview(station=station,fields=fields)
    for tf in tfs:
        tf.rows = get_two(station, tf.iso_to, fields=fields)
    print()
    for tf in tfs:
        tf_str = f"{tf.iso_from} -{tf.days}-> {tf.iso_to}"
        print(f"{tf_str:30s}      {tf.indicators}")
        print("    " + f"{tf.rows[0]}"[1:-1])
        if len(tf.rows) == 2:
            print("    " + f"{tf.rows[1]}"[1:-1])
    print(f"{len(tfs)} timeframes")
    print(fields)
    print()
    a = 17
    logging.info(f"total elapased: {perf_counter()-pc0}")