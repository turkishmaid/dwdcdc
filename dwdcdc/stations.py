#!/usr/bin/env python
# coding: utf-8

"""
Parse a general station list.

Created: 25.09.20
"""

import logging
from dataclasses import dataclass
from typing import List, Union

import johanna
from dwdcdc import ftplight
from dwdcdc import toolbox


# known sources
# this is not a library, so we will simply extend the list when more sources are covered
DATASOURCES = {
    "AT2H": {  # Air Temperature 2m, hourly
        "path": "climate_environment/CDC/observations_germany/climate/hourly/air_temperature/historical",
        "fnam": "TU_Stundenwerte_Beschreibung_Stationen.txt"
    },
    "KLD": {  # Climate KL, daily
        "path": "climate_environment/CDC/observations_germany/climate/daily/kl/recent",
        "fnam": "KL_Tageswerte_Beschreibung_Stationen.txt"
    }
}


"""
enhanced, but upwards compatible version of table "stationen"
small volume -> accept redundancy, may reduce effort when rewriting joins
"""
SQL_CREATE_STATIONS = """
    CREATE TABLE IF NOT EXISTS stations (
        -- fields like in at2h schema
        station INTEGER PRIMARY KEY,  -- I had better named that id ...
        yyyymmdd_von TEXT,
        yyyymmdd_bis TEXT,
        hoehe INTEGER,
        breite REAL,
        laenge REAL,
        name TEXT,
        land TEXT,
        -- new fields from here
        dwddate_from TEXT,
        dwddate_to TEXT,
        isodate_from TEXT,
        isodate_to TEXT,
        description TEXT,
        land_short TEXT
    );
"""


def _download(ds: str) -> List[str]:
    """
    Download station list from DWD
    :param ds: one of the shorthands defined in DATASOURCES
    :return: lines from the datasource
    """
    assert ds in DATASOURCES, f"no such shorthand: {ds}"
    with johanna.Timer() as t:
        ftp = ftplight.dwd(DATASOURCES[ds]["path"])
        lines = ftplight.ftp_retrlines(ftp, from_fnam=DATASOURCES[ds]["fnam"], verbose=True)
        ftp.quit()  # TODO quit() or close()
        logging.info(f"Closed FTP connection to DWD  {t.read()}")
    return lines


def _parse(lines: List[str]) -> List[tuple]:
    """
    Parse station list into tuples for database
    :param lines:
    :return: list of tuples suitable for insert into table like described in SQL_CREATE_STATIONS
    """
    with johanna.Timer() as t:
        rows = []
        for line in lines:
            # Format is the same for all files so far...
            if line.startswith("Stations_id") or line.startswith("-----------"):
                pass
            else:
                """
                ....,....1....,....2....,....3....,....4....,....5....,....6....,....7....,....8....,....9....,....0....,....1....,....2....,....3
                04692 20080301 20181130            229     50.8534    7.9966 Siegen (Kläranlage)                      Nordrhein-Westfalen
                """
                parts = line.split()
                station = int(parts[0])
                name = " ".join(parts[6:-1])
                land_short = toolbox.dwdland2short(parts[-1])
                description = f"{station}: {name} [{land_short}]"  # 5717: Wuppertal-Buchenhofen [NRW]
                isodate_from = toolbox.dwdts2iso(parts[1])
                isodate_to = toolbox.dwdts2iso(parts[2])
                tup = (
                    # --- at2h - stationen
                    station,            # station integer,
                    isodate_from,       # yymmdd_von text,
                    isodate_to,         # yymmdd_bis text,
                    int(parts[3]),      # hoehe integer,
                    float(parts[4]),    # breite real,
                    float(parts[5]),    # laenge real,
                    name,               # name text,
                    parts[-1],          # (bundes)land text
                    # --- new fields
                    parts[1],           # dwddate_from TEXT,
                    parts[2],           # dwddate_to TEXT,
                    isodate_from,       # isodate_from TEXT,
                    isodate_to,         # isodate_to TEXT,
                    description,        # description TEXT,
                    land_short,         # land_short TEXT
                )
                rows.append(tup)
        logging.info(f"Found {len(rows)} stations  {t.read()}")
        return rows


def _upsert(rows: List[tuple]) -> None:
    with johanna.Timer() as t:
        # database supplied by johanna
        with johanna.Connection(text=f"create? table stations") as c:
            c.cur.executescript(SQL_CREATE_STATIONS)
        with johanna.Connection("insert stations") as c:
            # https://database.guide/how-on-conflict-works-in-sqlite/
            c.cur.executemany("""
                INSERT OR REPLACE INTO stations
                VALUES (?,?,?,?,?,?,?,?, ?,?,?,?,?,?)
            """, rows)
            c.commit()
    logging.info(f"Upserted {len(rows)} stations to the database  {t.read()}")


def get(ds: str) -> None:
    lines = _download(ds)
    rows = _parse(lines)
    _upsert(rows)


@dataclass
class Station:
    """
    Simple data accessor @dataclass to table stations.
    Use attributes to get the data.
    is_populated: bool indicates whether data could be retrieved or not
    """
    station: int
    name: str
    land_short: str
    isodate_from: str
    isodate_to: str
    description: str
    populated: bool = False
    dwdts_table: str = "1699"

    def __init__(self, station: Union[int, str]):
        if isinstance(station, str):
            station = int(station)
        sql = """select 
                name, land_short, isodate_from, isodate_to, description
            from stations
            where station = ?"""
        self.station = station
        with johanna.Timer() as t:
            with johanna.Connection(f"Station.__init__({station})") as c:
                c.cur.execute(sql, (station,))
                row = c.cur.fetchone()
                if row:
                    self.name, self.land_short, self.isodate_from, self.isodate_to, self.description = row
                    self.populated = True
                else:
                    self.name, self.land_short, self.isodate_from, self.isodate_to, self.description = (None,) * 5
                    self.populated = False
        logging.info(f"got {self.description}: {self.isodate_from}..{self.isodate_to}  {t.read()}")

    def set_dwdts_table(self, dwdts_table: str) -> None:
        self.dwdts_table = dwdts_table


