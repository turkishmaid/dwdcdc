#!/usr/bin/env python
# coding: utf-8

"""
Analyze for which timeframes what data is available. This turns out to be a mojor problem in teh DWD data because days
without any readings are not listed in the produkt_*.txt files and it varis a lot what data was recorded (or is being
made available for each day. Similar is expected for the hourly values.
"""
# Created: 01.10.20

import json
from typing import List, Union
import logging
from dataclasses import dataclass
from time import perf_counter

import johanna

from dwdcdc.pit import PointInTime
from dwdcdc.dbtable import get_data_fields


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
    fnam = folder / f"{name}.json"
    with open(fnam, "w") as fh:
        fh.write(s)
    logging.info(f"List[Timeframe] ({len(tfl)} rows) -> {fnam}")


def get_indicator_select(tabname: str = "readings", fields: List[str] = None) -> str:
    if not fields:
        fields = get_data_fields(tabname)
    fl = ", \n".join([f"    case when {f} is not null then 'x' else '-' end as {f}" for f in fields])
    return "select \n    dwdts, \n" + f"{fl} \nfrom {tabname} \nwhere station = ? \norder by dwdts"


def get_two(station: int, dwdts: str, tabname: str = "readings", fields: List[str] = None):
    if "-" in dwdts:
        dwdts = dwdts.replace("-", "")
    if not fields:
        fields = get_data_fields(tabname)
    sql = "select " + f"dwdts, {', '.join(fields)} from {tabname} where station = ? and dwdts >= ? order by dwdts limit 2"
    # logging.info(sql)
    with johanna.Connection(f"from dwdts = {dwdts}", quiet=True) as c:
        rows = c.cur.execute(sql, (station, dwdts)).fetchall()
    return rows


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


def show_timeframes(tfs: List[Timeframe], fields: List[str], with_rows: bool = False) -> None:
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


def spot_check_overview():
    tabname = "readings"
    fields = get_data_fields()
    # fields = ['resp', 'resp_form', 'temp2m_max', 'temp2m_min']
    # fields = ['temp2m_avg', 'temp2m_max', 'temp2m_min']
    print(fields)
    with_rows = True

    for station in [2444, 2290, 5906]:
        tfs = overview(station=station, tabname=tabname, fields=fields, with_rows=with_rows)
        show_timeframes(tfs, fields, with_rows=with_rows)
        _persist(tfs, fields, station, with_rows=with_rows)


if __name__ == "__main__":
    pc0 = perf_counter()
    johanna.interactive(dotfolder="~/.dwd-cdc", dbname="kld.sqlite")
    spot_check_overview()
    a = 17
    logging.info(f"total elapased: {perf_counter() - pc0}")