#!/usr/bin/env python
# coding: utf-8

"""
Check completeness of data.

Created: 27.09.20
"""

import json
from typing import List, Union, Tuple
from datetime import datetime, timedelta
from time import perf_counter
import logging
from dataclasses import dataclass

import johanna

from dwdcdc.toolbox import PointInTime
from dwdcdc.dbtable import get_column_list, get_indicator_select, get_data_fields, get_two, filter_fields


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


def spot_check_overview():
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

"""
Sample SELECT:

select y.year, y.days,
       case when r.dwdts is not null then r.dwdts else 0 end / 1.0 / y.days as dwdts,
       case when r.temp_max is not null then r.temp_max else 0 end / 1.0 / y.days as temp_max,
       case when r.temp_avg is not null then r.temp_avg else 0 end / 1.0 / y.days as temp_avg,
       case when r.temp_min is not null then r.temp_min else 0 end / 1.0 / y.days as temp_min,
       case when r.resp is not null then r.resp else 0 end / 1.0 / y.days as resp
    from (select year, days from years) y
    left outer join (select year,
                sum(case when dwdts is not null then 1 else 0 end) as dwdts,
                sum(case when temp2m_max is not null then 1 else 0 end) as temp_max,
                sum(case when temp2m_avg is not null then 1 else 0 end) as temp_avg,
                sum(case when temp2m_min is not null then 1 else 0 end) as temp_min,
                sum(case when resp is not null then 1 else 0 end) as resp
            from readings
            where station = 2290
            group by year) r
        on y.year = r.year
    join stations s on s.station = 2290
        where y.year between substr(s.isodate_from, 1, 4) and substr(s.isodate_to, 1, 4);
"""

def generate_missingdays_select(tabname: str = "readings") -> Tuple[str, list]:
    fields = ["dwdts"]
    fields.extend(get_data_fields(tabname))
    sep = ", \n"
    cs = [f"y.days - case when r.{f} is not null then r.{f} else 0 end as {f}" for f in fields]
    ss = [f"sum(case when {f} is not null then 1 else 0 end) as {f}" for f in fields]
    sql = "select " + f"""y.year, y.days,
           {sep.join(cs)}
        from (select year, days from years) y
        left outer join (select year,
                    {sep.join(ss)}
                from {tabname}
                where station = ?
                group by year) r
            on y.year = r.year
        join stations s on s.station = ?
            where y.year between substr(s.isodate_from, 1, 4) and substr(s.isodate_to, 1, 4)
        order by y.year desc;
    """
    # print(sql)
    return sql, fields


def good_from(station: int, missing_days: int = 4, tabname: str = "readings") -> dict:
    """
    Determines from which year we have almost consecutive data for each value. "Almost" will be assessed by the number
    of days where thae value is not available. A year is almost complete when max. missing_days readings for that value
    are missing.
    :param station: the station to asses
    :param missing_days: measure for "almost complete"
    :param tabname: name of the table where the readings are stored, defualts to "readings"
    :return:
    """
    sql, fields = generate_missingdays_select(tabname)
    result = {}
    with johanna.Connection(f"missing days") as c:
        rows = c.cur.execute(sql, (station, station, )).fetchall()
    for row in rows:
        year = row[0]
        for i, field in enumerate(fields):
            if row[i+2] > missing_days:
                if not field in result:
                    result[field] = int(year) + 1
        if len(result) == len(row) - 2:
            logging.info(f"nothing good enough further back than {year+1}, data back until {rows[-1][0]}")
            break
    for field in result:
        logging.info(f"   {field:20s} -> {result[field]}")
    return result




if __name__ == "__main__":
    pc0 = perf_counter()
    johanna.interactive(dotfolder="~/.dwd-cdc", dbname="kld.sqlite")
    #spot_check_overview()
    #generate_missingdays_select()
    good_from(2290)

    a = 17
    logging.info(f"total elapased: {perf_counter()-pc0}")