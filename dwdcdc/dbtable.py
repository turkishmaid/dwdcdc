#!/usr/bin/env python
# coding: utf-8

"""
Utilities for SQLite database tables-

Created: 28.09.20
"""

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
    Get column list for table. Buffered, so you can access as often as you like. But does not return copies,
    so do not modify the list returned.
    :param tabnam: table name in current johanna database
    :return: list of tuples (colnam: str, type: str, primary_key: int)
    """
    if tabnam not in get_columns.buffer:
        with johanna.Connection(f"columns of {tabnam}") as c:
            rows = c.cur.execute(SQL_COLUMNS, (tabnam, )).fetchall()
        get_columns.buffer[tabnam] = rows
    return get_columns.buffer[tabnam]


def verify(tabnam: str = "readings") -> bool:
    """
    Verify assumption on a readings table, namely that the primary key is
    (station: INTEGER, dwdts: TEXT)
    :param tabnam: table name in current johanna database
    :return: indicator whether table is formed properly
    """
    columns = get_columns(tabnam)
    if len(columns) < 2:
        logging.info(f"table {tabnam}: not enough fields")
        return False
    if columns[0] != ("station", "INTEGER", 1) or \
            columns[1] != ("dwdts", "TEXT", 2):
        logging.info(f"table {tabnam}: primary key is not (station INTEGER, dwdts TEXT)")
        return False
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
        if f in ["station", "dwdts", "hour", "day", "month", "year"]:  # skip optional fields
            continue
        if f.startswith("qn") and f[2:].isdigit:
            continue
        a.append(f)
    return a


def get_data_fields(tabname: str = "readings") -> list:
    fields = get_column_list(tabname)
    return filter_fields(fields)


def update_years():
    """
    To compensate for gaps in the DWD data, where no readings are available. The table is suitable to left outer join
    yearly aggrates from reading tables to it.
    :return:
    """

    def days(year):
        if year == thisyear:
            last = date.today()
        else:
            last = date(year, 12, 31)
        return (last - date(year, 1, 1)).days + 1

    thisyear = date.today().year
    with johanna.Connection(text=f"create? table years") as c:
        c.cur.executescript("""
            CREATE TABLE IF NOT EXISTS years (
                year INTEGER,
                days INTEGER,
                PRIMARY KEY (year)
            );
        """)
    # TODO years interval could be retrieved from the stations table
    # TODO could be optimized a little bit to not insert when first year in range ia already there and last one is ok
    years = [(y, days(y)) for y in range(1700, 2051)]
    with johanna.Connection(text=f"insert? {len(years)} years") as c:
        c.cur.executemany("INSERT OR REPLACE INTO years VALUES (?, ?)", years)
        c.commit()


if __name__ == "__main__":
    johanna.interactive(dotfolder="~/.dwd-cdc", dbname="kld.sqlite")
    update_years()
