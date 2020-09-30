import unittest
from dwdcdc import toolbox

from datetime import datetime, date


class TestPointInTime1Daily(unittest.TestCase):

    def test_from_dwdts(self):
        d = toolbox.PointInTime("20211216")
        self.assertFalse(d.hourly)
        self.assertEqual(d.dwdts(), "20211216")
        self.assertEqual(d.iso(), "2021-12-16")
        self.assertTrue(isinstance(d.datetime(), date))
        self.assertEqual(d.datetime(), date(2021, 12, 16))

    def test_from_int(self):
        # shall not instantiate from other data types
        with self.assertRaises(ValueError) as e:
            d = toolbox.PointInTime(20211216)
        self.assertEqual(e.exception.args[0], "pass date, datetime or string")
        self.assertEqual(e.exception.args[1], int)
        self.assertEqual(e.exception.args[2], '20211216')

    def test_from_iso(self):
        d = toolbox.PointInTime("2021-12-16")
        self.assertFalse(d.hourly)
        self.assertEqual(d.dwdts(), "20211216")
        self.assertEqual(d.iso(), "2021-12-16")
        self.assertTrue(isinstance(d.datetime(), date))
        self.assertEqual(d.datetime(), date(2021, 12, 16))

    def test_from_date(self):
        d = toolbox.PointInTime(date(2021, 12, 16))
        self.assertFalse(d.hourly)
        self.assertEqual(d.dwdts(), "20211216")
        self.assertEqual(d.iso(), "2021-12-16")
        self.assertTrue(isinstance(d.datetime(), date))
        self.assertEqual(d.datetime(), date(2021, 12, 16))

    def test_wrong_format_dwdts_year(self):
        with self.assertRaises(ValueError) as e:
            toolbox.PointInTime("21219916")
        self.assertEqual(e.exception.args[0], "invalid year")

    def test_wrong_format_dwdts_month(self):
        with self.assertRaises(ValueError) as e:
            toolbox.PointInTime("20219916")
        self.assertEqual(e.exception.args[0], "invalid month")
        self.assertEqual(e.exception.args[1], "20219916")  # occasional check whether 2nd argument is passed

    def test_wrong_format_dwdts_day(self):
        with self.assertRaises(ValueError) as e:
            toolbox.PointInTime("20211200")
        self.assertEqual(e.exception.args[0], "invalid day")

    def test_wrong_format_iso_year(self):
        # bad year, month, day
        with self.assertRaises(ValueError) as e:
            toolbox.PointInTime("2121-99-16")
        self.assertEqual(e.exception.args[0], "invalid year")

    def test_wrong_format_iso_month(self):
        with self.assertRaises(ValueError) as e:
            toolbox.PointInTime("2021-99-16")
        self.assertEqual(e.exception.args[0], "invalid month")

    def test_wrong_format_iso_day(self):
        with self.assertRaises(ValueError) as e:
            toolbox.PointInTime("2021-12-00")
        self.assertEqual(e.exception.args[0], "invalid day")

    def test_wrong_length_dwdts_short(self):
        with self.assertRaises(ValueError) as e:
            toolbox.PointInTime("2021120")
        self.assertIn("YYYYMMDD", e.exception.args[0])

    def test_wrong_length_dwdts_long(self):
        with self.assertRaises(ValueError) as e:
            toolbox.PointInTime("202112014")
        self.assertIn("YYYYMMDD", e.exception.args[0])

    def test_wrong_length_iso_short(self):
        with self.assertRaises(ValueError) as e:
            toolbox.PointInTime("2021-12-0")
        self.assertIn("YYYY-MM-DD", e.exception.args[0])

    def test_wrong_length_iso_long(self):
        with self.assertRaises(ValueError) as e:
            toolbox.PointInTime("2021-12-014")
        self.assertIn("YYYY-MM-DD", e.exception.args[0])

    def test_bad_date_dwdts(self):
        # not a leap year
        with self.assertRaises(ValueError) as e:
            toolbox.PointInTime("20210229")
        self.assertEqual(e.exception.args[0], "day is out of range for month")

    def test_bad_date_iso(self):
        # not a leap year
        with self.assertRaises(ValueError) as e:
            toolbox.PointInTime("2021-02-29")
        self.assertEqual(e.exception.args[0], "day is out of range for month")


class TestPointInTime2Hourly(unittest.TestCase):

    def test_from_dwdts(self):
        d = toolbox.PointInTime("2021121614")
        self.assertTrue(d.hourly)
        self.assertEqual(d.dwdts(), "2021121614")
        self.assertEqual(d.iso(), "2021-12-16 14")
        self.assertTrue(isinstance(d.datetime(), datetime))
        self.assertEqual(d.datetime(), datetime(2021, 12, 16, 14))

    def test_from_iso(self):
        d = toolbox.PointInTime("2021-12-16 14")
        self.assertTrue(d.hourly)
        self.assertEqual(d.dwdts(), "2021121614")
        self.assertEqual(d.iso(), "2021-12-16 14")
        self.assertTrue(isinstance(d.datetime(), datetime))
        self.assertEqual(d.datetime(), datetime(2021, 12, 16, 14))

    def test_from_datetime(self):
        d = toolbox.PointInTime(datetime(2021, 12, 16, 14))
        self.assertTrue(d.hourly)
        self.assertEqual(d.dwdts(), "2021121614")
        self.assertEqual(d.iso(), "2021-12-16 14")
        self.assertTrue(isinstance(d.datetime(), datetime))
        self.assertEqual(d.datetime(), datetime(2021, 12, 16, 14))

    def test_wrong_format_dwdts_year_late(self):
        with self.assertRaises(ValueError) as e:
            toolbox.PointInTime("2121991614")
        self.assertEqual(e.exception.args[0], "invalid year")

    def test_wrong_format_dwdts_year_character(self):
        with self.assertRaises(ValueError) as e:
            toolbox.PointInTime("21X1991614")
        self.assertEqual(e.exception.args[0], "format must match YYYYMMDDHH")

    def test_wrong_format_dwdts_month(self):
        with self.assertRaises(ValueError) as e:
            toolbox.PointInTime("2021991614")
        self.assertEqual(e.exception.args[0], "invalid month")

    def test_wrong_format_dwdts_day(self):
        with self.assertRaises(ValueError) as e:
            toolbox.PointInTime("2021120014")
        self.assertEqual(e.exception.args[0], "invalid day")

    def test_wrong_format_dwdts_hour(self):
        with self.assertRaises(ValueError) as e:
            toolbox.PointInTime("2021121688")
        self.assertEqual(e.exception.args[0], "invalid hour")

    def test_wrong_format_iso_year(self):
        # bad year, month, day
        with self.assertRaises(ValueError) as e:
            toolbox.PointInTime("2121-99-16 14")
        self.assertEqual(e.exception.args[0], "invalid year")

    def test_wrong_format_iso_month_number(self):
        with self.assertRaises(ValueError) as e:
            toolbox.PointInTime("2021-99-16 14")
        self.assertEqual(e.exception.args[0], "invalid month")

    def test_wrong_format_iso_month_character(self):
        with self.assertRaises(ValueError) as e:
            toolbox.PointInTime("2021-9X-16 14")
        self.assertEqual(e.exception.args[0], "format must match YYYY-MM-DD HH")

    def test_wrong_format_iso_separator_space(self):
        with self.assertRaises(ValueError) as e:
            toolbox.PointInTime("2021 99 16 14")
        self.assertEqual(e.exception.args[0], "format must match YYYY-MM-DD HH")

    def test_wrong_format_iso_separator_dash(self):
        with self.assertRaises(ValueError) as e:
            toolbox.PointInTime("2021-99-16-14")
        self.assertEqual(e.exception.args[0], "format must match YYYY-MM-DD HH")

    def test_wrong_format_iso_day(self):
        with self.assertRaises(ValueError) as e:
            toolbox.PointInTime("2021-12-00 14")
        self.assertEqual(e.exception.args[0], "invalid day")

    def test_wrong_format_iso_hour(self):
        with self.assertRaises(ValueError) as e:
            toolbox.PointInTime("2021-12-16 88")
        self.assertEqual(e.exception.args[0], "invalid hour")

    def test_wrong_length_dwdts_short(self):
        with self.assertRaises(ValueError) as e:
            toolbox.PointInTime("2021120")
        self.assertIn("YYYYMMDDHH", e.exception.args[0])

    def test_wrong_length_dwdts_long(self):
        with self.assertRaises(ValueError) as e:
            toolbox.PointInTime("20211216145")
        self.assertIn("YYYYMMDDHH", e.exception.args[0])

    def test_wrong_length_iso_short(self):
        with self.assertRaises(ValueError) as e:
            toolbox.PointInTime("2021-12-0")
        self.assertIn("YYYY-MM-DD HH", e.exception.args[0])

    def test_wrong_length_iso_long(self):
        with self.assertRaises(ValueError) as e:
            toolbox.PointInTime("2021-12-16 145")
        self.assertIn("YYYY-MM-DD HH", e.exception.args[0])

    def test_bad_date_dwdts(self):
        # not a leap year
        with self.assertRaises(ValueError) as e:
            toolbox.PointInTime("2021022914")
        self.assertEqual(e.exception.args[0], "day is out of range for month")

    def test_bad_date_iso(self):
        # not a leap year
        with self.assertRaises(ValueError) as e:
            toolbox.PointInTime("2021-02-29 14")
        self.assertEqual(e.exception.args[0], "day is out of range for month")


class TestPointInTime3Sub(unittest.TestCase):

    def test_days(self):
        d0 = toolbox.PointInTime("2020-12-24")
        d1 = toolbox.PointInTime("2020-12-23")
        self.assertFalse(d0.hourly)
        self.assertEqual(d1 - d0, 2)
        self.assertEqual(d0 - d1, 2)

    def test_hourly(self):
        d0 = toolbox.PointInTime("2020-12-24 18")
        d1 = toolbox.PointInTime("2020-12-24 17")
        self.assertTrue(d0.hourly)
        self.assertEqual(d1 - d0, 2)
        self.assertEqual(d0 - d1, 2)

    def test_not_compare(self):
        d0 = toolbox.PointInTime("2020-12-24 18")
        d1 = toolbox.PointInTime("2020-12-23")
        with self.assertRaises(ValueError) as e:
            d1 - d0
        self.assertIn("second operand must also be daily", e.exception.args[0])
        d0 = toolbox.PointInTime("2020-12-24")
        d1 = toolbox.PointInTime("2020-12-23 18")
        with self.assertRaises(ValueError) as e:
            d1 - d0
        self.assertIn("second operand must also be hourly", e.exception.args[0])

    def test_typecast_good_iso_iso(self):
        d = toolbox.PointInTime("2021-12-16")
        self.assertEqual(d - "2021-12-15", 2)

    def test_typecast_good_iso_dwdts(self):
        d = toolbox.PointInTime("2021-12-16")
        self.assertEqual(d - "20211215", 2)

    def test_typecast_good_iso_date(self):
        d = toolbox.PointInTime("2021-12-16")
        self.assertEqual(d - date(2021, 12, 15), 2)

    def test_typecast_bad_hourly_dwdts(self):
        d = toolbox.PointInTime("2021-12-16")
        with self.assertRaises(ValueError) as e:
            d - "2021121514"
        self.assertIn("second operand must also be daily", e.exception.args[0])

    def test_typecast_bad_hourly_iso(self):
        d = toolbox.PointInTime("2021-12-16")
        with self.assertRaises(ValueError) as e:
            d - "2021-12-15 14"
        self.assertIn("second operand must also be daily", e.exception.args[0])

    def test_typecast_bad_string_iso(self):
        d = toolbox.PointInTime("2021-12-16")
        with self.assertRaises(AssertionError) as e:
            d - "Hurz Hurz-"
        self.assertEqual(e.exception.args[0], "format must match YYYY-MM-DD")

    def test_typecast_bad_string_dwdts(self):
        d = toolbox.PointInTime("2021-12-16")
        with self.assertRaises(AssertionError) as e:
            d - "HurzHurz"
        self.assertEqual(e.exception.args[0], "format must match YYYYMMDD")


class TestPointInTime5NextPrev(unittest.TestCase):

    def test_next_daily(self):
        silvester = toolbox.PointInTime("2021-12-31")
        neujahr = silvester.next()
        self.assertEqual(neujahr.iso(), "2022-01-01")

    def test_next_hourly(self):
        silvester = toolbox.PointInTime("2021-12-31 23")
        neujahr = silvester.next()
        self.assertEqual(neujahr.iso(), "2022-01-01 00")

    def test_prev_daily(self):
        neujahr = toolbox.PointInTime("2022-01-01")
        silvester = neujahr.prev()
        self.assertEqual(silvester.iso(), "2021-12-31")

    def test_prev_hourly(self):
        neujahr = toolbox.PointInTime("2022-01-01 00")
        silvester = neujahr.prev()
        self.assertEqual(silvester.iso(), "2021-12-31 23")

    def test_funny(self):
        silvester = toolbox.PointInTime("2021-12-31")
        neujahr = silvester.next()
        self.assertEqual(neujahr - silvester, 2)


if __name__ == '__main__':
    unittest.main()
