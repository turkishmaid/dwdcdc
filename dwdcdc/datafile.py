#!/usr/bin/env python
# coding: utf-8

"""
EXPALINWHATTHISISALLABOUTHERE

Created: 25.09.20
"""

import logging
from ftplib import FTP
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile
import csv
import abc
from datetime import date, timedelta

import johanna
from dwdcdc import ftplight
from dwdcdc import stations


class AbstractDataFile(abc.ABC):
    """
    Downloads, parses and saves a CDC data file from an open FTP connection.
    You MUST implement abstract method parse_one_row and @property target_table_name.
    The tuple returned by parse_one_line MUST match the order of fields in the target table.
    The target table MUST have fields "station" and "dwdts" forming the primary key.
    Also asserts that the CDC data file is ;-separated csv with stations_id and mess_datum as the first two fields.
    The two fixed fields are used to implement a do-not-insert-before logic and drops all lines with mess_datum's that
    are not after the last available dwdts in the target table.
    """

    def __init__(self, ftp: FTP, fnam: str, verbose: bool = False):
        """
        :param ftp: open FTP connections with working directoy set
        :param fnam: name of file to download
        :param verbose: log progress in console  -- DO NOT USE IN PRODUCTION
        """
        with johanna.Timer() as t:
            self._verbose = verbose
            self.did_download = False
            logging.info("-"*80)
            logging.info(f'{type(self).__name__}(<FTP>,"{fnam}")')  # shows subclass

            station_nr = self._get_station_nr_from_filename(fnam)
            station = stations.Station(station_nr)
            assert station.populated
            station.set_dwdts_table(self._get_most_recent_dwdts_in_table(station))

            if self._is_data_expected(fnam, station):
                with TemporaryDirectory() as temp_dir:
                    temp_dir = Path(temp_dir)
                    logging.info(f"Temporary directory: {temp_dir}")
                    zipfile_path = ftplight.ftp_retrbinary(ftp, from_fnam=fnam, to_path=temp_dir/fnam, verbose=True)
                    if not zipfile_path:
                        johanna.flag_as_error()
                        logging.error(f"CAN NOT download data for station {station.description}")
                        return
                    produkt_path = self._extract(zipfile_path, temp_dir)
                    readings = self._parse(produkt_path, station)
                    if readings:
                        # TODO connection mit retry absichern
                        with johanna.Connection("insert readings") as c:
                            self._insert_readings(readings, c)
                            # last_date = self._update_recent(readings, c)
                            c.commit()  # gemeinsamer commit ist sinnvoll
                if temp_dir.exists():
                    johanna.flag_as_error()
                    logging.error(f"Temp folder {temp_dir} was NOT properly removed")
            else:
                logging.info(f"File {fnam} will not be downloaded, b/c no new data is expected")
        logging.info(f"DataFile {fnam} was processed.  {t.read()}")

    def _get_station_nr_from_filename(self, fnam: str) -> int:
        """
        Extract the station number from the filename. The default implementation
        returns the 3rd underscore separated part of the filename, which seems
        to be a good default.
        :param fnam: name of file to be downloaded
        :return: numerical station ID
        """
        return int(fnam.split(".")[0].split("_")[2])

    def _get_dwdts_to_from_hist_filename(self, fnam: str) -> str:
        """
        Extract the last day to be expected in a _hist.zip file from it's name.
        :param fnam: name of file to be downloaded
        :return: dwdts_to string from filename
        """
        # patterns:
        #   stundenwerte_TU_00003_19500401_20110331_hist.zip
        #   tageswerte_KL_00001_19370101_19860630_hist.zip
        return fnam.split(".")[0].split("_")[4]

    @abc.abstractmethod
    def target_table_name(self) -> str:
        """
        Implement this as `@property` that returns the name of the target table
        into which the readings shall be upserted. The table must have fields
        `station` and `dwdts` that form a unique key.
        :return: table name
        """
        return "Nullinger."

    def _get_most_recent_dwdts_in_table(self, s: stations.Station) -> str:
        """
        Select max(dwdts) from target table.
        :param s: the Station
        :return: max(dwdts) for station
        """
        # bad code style to cope with not-smart-enough Pycharm coder inspection
        sql = "select " + f"max(dwdts) from {self.target_table_name} where station = ?"
        with johanna.Connection(text="select " + f"max(dwdts) from {self.target_table_name}") as c:
            c.cur.execute(sql, (s.station,))
            # returns (None,) when no record is max'ed
            row = c.cur.fetchone()
            if row[0]:
                last = row[0]
                logging.info(f"Data until {last} is available in {self.target_table_name} for station {s.description}")
            else:
                last = "1699"  # will also compare fine against hours
                logging.info(f"No data yet in {self.target_table_name} for station {s.description}")
        return last

    def _is_data_expected(self, fnam: str, s: stations.Station) -> bool:
        """
        Use already downloaded data to find out whether the file to be downloaded
        is expected to provide new data.
        :param fnam: name of file to be downloaded
        :param s: Station object
        :return: indicator whether new data can be expected
        """
        dwdts_table = s.dwdts_table  # self.get_most_recent_dwdts_in_table(s)
        if dwdts_table < "1700":  # no data
            return True
        # data is available
        if fnam.endswith("_hist.zip"):
            dwdts_file = self._get_dwdts_to_from_hist_filename(fnam)
            result = dwdts_table < dwdts_file
        else:
            assert fnam.endswith("_akt.zip"), fnam
            # we do not have to care for time zone here, b/c DWD will typically upload yesterday's
            # data between 9am and 10am local time
            dwdts_yesterday = (date.today() + timedelta(days=-1)).strftime("%Y%m%d") + "23"
            # When data is available for yesterday -> no download
            # TODO Wenn Daten bis zum Ende der Station schon da sind -> nix tun
            result = dwdts_table < dwdts_yesterday
        logging.info(f"New data {'IS' if result else 'IS NOT'} expected in {fnam}")
        return result

    def _extract(self, zipfile_path: Path, target_path: Path) -> Path:
        """
        The zip files will contain several html and txt files which are ignored so far.
        The data is in a csv file named produkt_*.txt. This will be unzipped to the target_path.
        :param zipfile_path: Path of zip-file
        :param target_path: Path of folder to place produkt_*.txt-file in
        :return: Path of produkt_*.txt-file
        """
        zipfile = ZipFile(zipfile_path)
        for zi in zipfile.infolist():
            if zi.filename.startswith("produkt_"):
                produkt = zipfile.extract(zi, path=str(target_path))
                logging.info(f"Data in {produkt}")
                return Path(produkt)
        raise ValueError(f"No produkt_*.txt-file found in {zipfile_path}")

    @abc.abstractmethod
    def parse_one_row(self, row: list) -> tuple:
        """
        Parse one line of input file. Return tuple suitable for insert into the
        target table or None if this line should be skipped for some reason.
        :param row: line of the input file as parsed csv
        :return: tuple ready for insert into target table
        """
        pass

    # TODO prüfen, dass alle Werte auch von der gewünschten Station kommen
    def _parse(self, produkt_path: Path, s: stations.Station) -> list:
        """
        Parse data. the Station shall already have dwdts_table populated.
        :param produkt_path: Path of the data file
        :return: list of tuples ready for insert to target table
        """
        with johanna.Timer() as t:
            readings = list()
            with open(produkt_path, newline='') as csvfile:
                spamreader = csv.reader(csvfile, delimiter=';')
                cnt = 0
                shown = 0
                skipped = -1
                for row in spamreader:
                    cnt += 1
                    if cnt == 1:                    # skip header line
                        continue
                    # surpress data that might be in DB already
                    # assumes row starts with stations_id and mess_datum
                    if row[1] <= s.dwdts_table:
                        continue
                    elif skipped == -1:  # now uncond.
                        skipped = cnt - 2  # current and first excluded
                        logging.info(f"Skipped {skipped} lines with timestamp <= {s.dwdts_table}")
                    if shown < 1:  # show first row taken
                        shown += 1
                        # logging.info(f"{row[0]}, {row[1]}")
                        logging.info(f"1st row to be taken: {row}")
                    tup = self.parse_one_row(row)
                    if tup:
                        if tup[0] == s.station:
                            readings.append(tup)
                        else:
                            johanna.flag_as_error()
                            logging.error(f"Found line for station {tup[0]} in file for station {s.station}")
        logging.info(f"Found {len(readings)} new readings for station {s.description}  {t.read()}")
        return readings

    def _insert_readings(self, readings: list, c: johanna.Connection) -> None:
        if len(readings) == 0:
            logging.info("no rows to insert!")
            return
        ph = "(" + ",".join(("?",) * len(readings[0])) + ")"
        with johanna.Timer() as t:
            sql = f"INSERT OR IGNORE INTO {self.target_table_name} VALUES {ph}"
            c.cur.executemany(sql, readings)
            # c.commit() -- commit außerhalb
        logging.info(f"Inserted {len(readings)} rows into {self.target_table_name}  {t.read()}")
        logging.info(f"Last timestamp now: {readings[-1][1]}")
        johanna.collect_stat("db_readings_inserted", len(readings))

    # def _update_recent(self, readings: list, c: johanna.Connection) -> str:
    #     # get station, assuming that is the same in all tuples
    #     station = readings[0][0]
    #     # get max time of reading from last line
    #     # alternatively: https://stackoverflow.com/a/4800441/3991164
    #     yyyymmddhh = readings[-1][1]
    #     with johanna.Timer() as t:
    #         # cf. https://stackoverflow.com/a/4330694/3991164
    #         c.cur.execute("""
    #             INSERT OR REPLACE
    #             INTO recent (station, yyyymmddhh)
    #             VALUES (?, ?)
    #         """, (station, yyyymmddhh))
    #         # c.commit() -- commit außerhalb
    #     logging.info(f"Neuester Messwert {yyyymmddhh} in der Datenbank vermerkt {t.read()}")
    #     return yyyymmddhh


def float_or_none(x):
    return None if x.strip() == "-999" else float(x)


def int_or_none(x):
    return None if x.strip() == "-999" else int(x)


class AirTemperature2mHourlyDataFile(AbstractDataFile):

    @property
    def target_table_name(self) -> str:
        return "readings"

    def parse_one_row(self, row: list) -> tuple:
        dwdts = row[1]
        y = int(dwdts[:4])
        m = int(dwdts[4:6])
        d = int(dwdts[6:8])
        h = int(dwdts[-2:])
        tup = (
            int(row[0]),  # station
            row[1],  # dwdts
            y, m, d, h,  # dwdts
            int(row[2]),  # q
            float_or_none(row[3]),  # temp
            float_or_none(row[4]),  # humid
        )
        return tup


if __name__ == "__main__":
    pass
