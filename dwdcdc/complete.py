#!/usr/bin/env python
# coding: utf-8

"""
Check completeness of data.

Created: 27.09.20
"""

import json
from typing import List
from datetime import datetime
from time import perf_counter
import logging

import johanna


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










def iso(s):
    return f"{s[0:4]}-{s[4:6]}-{s[6:]}"

if __name__ == "__main__":
    pc0 = perf_counter()
    johanna.interactive(dotfolder="~/.dwd-cdc", dbname="kld.sqlite")
    l = complete(station=5906, fields=["dwdts", "wind_max", "wind_avg", "resp", "temp2m_min", "temp2m_avg", "temp2m_max"])
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
    a = 17
    logging.info(f"total elapased: {perf_counter()-pc0}")