#!/usr/bin/env python
# coding: utf-8

"""
Utilities for SQLite database tables-

Created: 28.09.20
"""

import json
from typing import List
import logging
from datetime import date

import johanna

from dwdcdc.toolbox import static_vars


SQL_COLUMNS = "SELECT" + """
        p.name, p.type, p.pk
    FROM sqlite_master m
    LEFT OUTER JOIN pragma_table_info((m.name)) p
        ON m.name = ? AND m.name <> p.name
    ORDER BY p.cid
"""


@static_vars(buffer={})
def get_columns(tabnam: str = "readings") -> List[tuple]:
    """
    Get column list for table. Buffered, so you can access as often as you like.
    :param tabnam: table name in current johanna database
    :return: list of tuples (colnam: str, type: str, primary_key: int)
    """
    if not tabnam in get_columns.buffer:
        with johanna.Connection(f"columns of {tabnam}") as c:
            rows = c.cur.execute(SQL_COLUMNS, (tabnam, )).fetchall()
        get_columns.buffer[tabnam] = rows
    return get_columns.buffer[tabnam]


def verify(tabnam: str = "readings") -> bool:
    """
    Verify assumption on a readings table, namely that the primary key is
    (station: INTEGER, dwdts: TEXT)
    :param tabnam: table name in current johanna database
    :return: indicator whether tyble is formed properly
    """
    columns = get_columns(tabnam)
    if len(columns) < 2:
        logging.info(f"table {tabnam}: not enough fields")
        return False
    if columns[0] != ("station", "INTEGER", 1) or \
            columns[1] != ("dwdts", "TEXT", 2):
        logging.info(f"table {tabnam}: primary key is not (station INTEGER, dwdts TEXT)")
        return False
    good = True
    for col in columns[2:]:
        if col[2] != 0:
            logging.info(f"table {tabnam}: too many fields in primary key")
            return False
    return True


def get_column_list(tabnam: str = "readings") -> list:
    """
    Get simple list of column names for table.
    :param tabnam: table name in current johanna database
    :return: list of all columns
    """
    columns = get_columns(tabnam)
    return [c[0] for c in columns]


def filter_fields(fields: List[str]) -> List[str]:
    a = []
    for f in fields:
        if f in ["station", "dwdts", "day", "month", "year"]:  # skip optional fields
            continue
        if f.startswith("qn") and f[2:].isdigit:
            continue
        a.append(f)
    return a


def get_data_fields(tabname: str = "readings") -> list:
    fields = get_column_list(tabname)
    return filter_fields(fields)


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
    #logging.info(sql)
    with johanna.Connection(f"from dwdts = {dwdts}", quiet=True) as c:
        rows = c.cur.execute(sql, (station, dwdts)).fetchall()
    return rows


SQL_CREATE_YEARS = """
    CREATE TABLE IF NOT EXISTS years (
        year INTEGER,
        days INTEGER,
        PRIMARY KEY (year)
    );
"""

SQL_INSERT_YEARS = """
    INSERT OR REPLACE INTO years
        VALUES (?, ?)
"""


def create_years():

    def days(year):
        if year == thisyear:
            last = date.today()
        else:
            last = date(year,12,31)
        return (last - date(year,1,1)).days + 1

    thisyear = date.today().year
    with johanna.Connection(text=f"create? table years") as c:
        c.cur.executescript(SQL_CREATE_YEARS)
    years = [(y, days(y)) for y in range(1700,2051)]
    with johanna.Connection(text=f"insert? {len(years)} years") as c:
        c.cur.executemany(SQL_INSERT_YEARS, years)
        c.commit()


if __name__ == "__main__":
    johanna.interactive(dotfolder="~/.dwd-cdc", dbname="kld.sqlite")
    create_years()

    if 1 == 0:
        johanna.interactive(dotfolder="~/.dwd-cdc", dbname="kld.sqlite")
        assert verify()
        columns = get_columns()
        column_list = get_column_list()
        print(get_indicator_select())
        hurz = 17