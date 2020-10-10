#!/usr/bin/env python
# coding: utf-8

"""
Check completeness of data. This is fumbling around so far...

Sample select for missing days per year:

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
        where y.year between substr(s.isodate_from, 1, 4) and substr(s.isodate_to, 1, 4)
    order by y.year desc;

"""
# Created: 27.09.20

from typing import List, Union, Tuple
from datetime import datetime, timedelta
from time import perf_counter
import logging
from dataclasses import dataclass

import johanna

from dwdcdc.dbtable import get_data_fields, update_years


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


def get_missingdays(station: int, tabname: str = "readings") -> Tuple[list, list]:
    """
    Retrieves
    :param station: the station to assess
    :param tabname: name of the table where the readings are stored, defualts to "readings"
    :return: hit set with the data calculated and list of data fields (see example select)
    """
    sql, fields = generate_missingdays_select(tabname)
    with johanna.Connection(f"missing days") as c:
        rows = c.cur.execute(sql, (station, station, )).fetchall()
    return rows, fields


def good_from(station: int, missing_days: int = 4, tabname: str = "readings", data: tuple = None) -> dict:
    """
    Determines from which year we have almost consecutive data for each value. "Almost" will be assessed by the number
    of days where thae value is not available. A year is almost complete when max. missing_days readings for that value
    are missing.
    :param station: the station to assess
    :param missing_days: the measure for "almost complete", that many missing days per year are regarded acceptable
    :param tabname: name of the table where the readings are stored, defualts to "readings"
    :param data: optional you can pass tha data to process. Must be output of get_missingdays().
    :return:
    """
    if data:
        rows, fields = data
    else:
        rows, fields = get_missingdays(station, tabname=tabname)

    # draft check
    assert isinstance(station, int)
    assert isinstance(missing_days, int) and 365 > missing_days >= 0
    assert isinstance(tabname, str)
    assert isinstance(rows, list)
    assert isinstance(fields, list)

    result = {}
    for row in rows:
        year = row[0]
        for i, field in enumerate(fields):
            if row[i+2] > missing_days:
                if not field in result:
                    result[field] = int(year) + 1
        if len(result) == len(row) - 2:
            logging.info(f"nothing good enough further back than {year+1}, data back until {rows[-1][0]}")
            break
    return result

if __name__ == "__main__":
    pc0 = perf_counter()
    johanna.interactive(dotfolder="~/.dwd-cdc", dbname="kld.sqlite")
    update_years()
    data = get_missingdays(2290)

    # TODO column header
    print()
    fmt = "%4d (%3d) | " + "%3d  " * (len(data[0][0]) - 2)
    for row in data[0]:
        print(fmt % row)
    print()

    # result = good_from(2290, data=data)
    result = good_from(2290, data=data)

    print()
    for field in result:
        print(f"   {field:20s} -> {result[field]}")
    print()

    a = 17
    logging.info(f"total elapased: {perf_counter()-pc0}")