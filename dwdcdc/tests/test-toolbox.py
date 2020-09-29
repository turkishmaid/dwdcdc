import unittest
from dwdcdc import toolbox

from datetime import datetime, date


class TestPointInTimeDaily(unittest.TestCase):

    def test_from_dwdts(self):
        d = toolbox.PointInTime("20211216", hourly=False)
        self.assertEqual(d.dwdts(), "20211216")
        self.assertEqual(d.iso(), "2021-12-16")
        self.assertTrue(isinstance(d.datetime(), date))
        self.assertEqual(d.datetime(), date(2021, 12, 16))

    def test_from_iso(self):
        d = toolbox.PointInTime("2021-12-16", hourly=False)
        self.assertEqual(d.dwdts(), "20211216")
        self.assertEqual(d.iso(), "2021-12-16")
        self.assertTrue(isinstance(d.datetime(), date))
        self.assertEqual(d.datetime(), date(2021, 12, 16))

    def test_from_date(self):
        d = toolbox.PointInTime(date(2021, 12, 16), hourly=False)
        self.assertEqual(d.dwdts(), "20211216")
        self.assertEqual(d.iso(), "2021-12-16")
        self.assertTrue(isinstance(d.datetime(), date))
        self.assertEqual(d.datetime(), date(2021, 12, 16))

    def test_from_datetime(self):
        with self.assertRaises(AssertionError) as e:
            toolbox.PointInTime(datetime(2021, 12, 16), hourly=False)
        self.assertTrue(e.exception.args[0].startswith("pass date instead"))

    def test_wrong_format_dwdts(self):
        # bad year, month, day
        with self.assertRaises(AssertionError) as e:
            toolbox.PointInTime("21219916", hourly=False)
        self.assertEqual(e.exception.args[0], "invalid year")
        with self.assertRaises(AssertionError) as e:
            toolbox.PointInTime("20219916", hourly=False)
        self.assertEqual(e.exception.args[0], "invalid month")
        with self.assertRaises(AssertionError) as e:
            toolbox.PointInTime("20211200", hourly=False)
        self.assertEqual(e.exception.args[0], "invalid day")

    def test_wrong_format_iso(self):
        # bad year, month, day
        with self.assertRaises(AssertionError) as e:
            toolbox.PointInTime("2121-99-16", hourly=False)
        self.assertEqual(e.exception.args[0], "invalid year")
        with self.assertRaises(AssertionError) as e:
            toolbox.PointInTime("2021-99-16", hourly=False)
        self.assertEqual(e.exception.args[0], "invalid month")
        with self.assertRaises(AssertionError) as e:
            toolbox.PointInTime("2021-12-00", hourly=False)
        self.assertEqual(e.exception.args[0], "invalid day")

    def test_wrong_length(self):
        with self.assertRaises(AssertionError) as e:
            s = "2021120"
            toolbox.PointInTime(s, hourly=False)
        self.assertIn("YYYYMMDD", e.exception.args[0], s)
        with self.assertRaises(AssertionError) as e:
            s = "202112014"
            toolbox.PointInTime(s, hourly=False)
        self.assertIn("YYYYMMDD", e.exception.args[0], s)
        with self.assertRaises(AssertionError) as e:
            s = "2021-12-0"
            toolbox.PointInTime(s, hourly=False)
        self.assertIn("YYYY-MM-DD", e.exception.args[0], s)
        with self.assertRaises(AssertionError) as e:
            s = "2021-12-014"
            toolbox.PointInTime(s, hourly=False)
        self.assertIn("YYYY-MM-DD", e.exception.args[0], s)

    def test_bad_date(self):
        # not a leap year
        with self.assertRaises(ValueError):
            toolbox.PointInTime("20210229", hourly=False)


class TestPointInTimeHourly(unittest.TestCase):

    def test_from_dwdts(self):
        d = toolbox.PointInTime("2021121614", hourly=True)
        self.assertEqual(d.dwdts(), "2021121614")
        self.assertEqual(d.iso(), "2021-12-16 14")
        self.assertTrue(isinstance(d.datetime(), datetime))
        self.assertEqual(d.datetime(), datetime(2021, 12, 16, 14))

    def test_from_iso(self):
        d = toolbox.PointInTime("2021-12-16 14", hourly=True)
        self.assertEqual(d.dwdts(), "2021121614")
        self.assertEqual(d.iso(), "2021-12-16 14")
        self.assertTrue(isinstance(d.datetime(), datetime))
        self.assertEqual(d.datetime(), datetime(2021, 12, 16, 14))

    def test_from_datetime(self):
        d = toolbox.PointInTime(datetime(2021, 12, 16, 14), hourly=True)
        self.assertEqual(d.dwdts(), "2021121614")
        self.assertEqual(d.iso(), "2021-12-16 14")
        self.assertTrue(isinstance(d.datetime(), datetime))
        self.assertEqual(d.datetime(), datetime(2021, 12, 16, 14))

    def test_from_date(self):
        with self.assertRaises(AssertionError) as e:
            toolbox.PointInTime(date(2021, 12, 16), hourly=True)
        self.assertTrue(e.exception.args[0].startswith("pass datetime instead"))

    def test_wrong_format_dwdts(self):
        # bad year, month, day
        with self.assertRaises(AssertionError) as e:
            toolbox.PointInTime("2121991614", hourly=True)
        self.assertEqual(e.exception.args[0], "invalid year")
        with self.assertRaises(AssertionError) as e:
            toolbox.PointInTime("2021991614", hourly=True)
        self.assertEqual(e.exception.args[0], "invalid month")
        with self.assertRaises(AssertionError) as e:
            toolbox.PointInTime("2021120014", hourly=True)
        self.assertEqual(e.exception.args[0], "invalid day")
        with self.assertRaises(AssertionError) as e:
            toolbox.PointInTime("2021121688", hourly=True)
        self.assertEqual(e.exception.args[0], "invalid hour")

    def test_wrong_format_iso(self):
        # bad year, month, day
        with self.assertRaises(AssertionError) as e:
            toolbox.PointInTime("2121-99-16 14", hourly=True)
        self.assertEqual(e.exception.args[0], "invalid year")
        with self.assertRaises(AssertionError) as e:
            toolbox.PointInTime("2021-99-16 14", hourly=True)
        self.assertEqual(e.exception.args[0], "invalid month")
        with self.assertRaises(AssertionError) as e:
            toolbox.PointInTime("2021-12-00 14", hourly=True)
        self.assertEqual(e.exception.args[0], "invalid day")
        with self.assertRaises(AssertionError) as e:
            toolbox.PointInTime("2021-12-16 88", hourly=True)
        self.assertEqual(e.exception.args[0], "invalid hour")

    def test_wrong_length(self):
        with self.assertRaises(AssertionError) as e:
            s = "2021120"
            toolbox.PointInTime(s, hourly=True)
        self.assertIn("YYYYMMDDHH", e.exception.args[0], s)
        with self.assertRaises(AssertionError) as e:
            s = "20211216145"
            toolbox.PointInTime(s, hourly=True)
        self.assertIn("YYYYMMDDHH", e.exception.args[0], s)
        with self.assertRaises(AssertionError) as e:
            s = "2021-12-0"
            toolbox.PointInTime(s, hourly=True)
        self.assertIn("YYYY-MM-DD HH", e.exception.args[0], s)
        with self.assertRaises(AssertionError) as e:
            s = "2021-12-16 145"
            toolbox.PointInTime(s, hourly=True)
        self.assertIn("YYYY-MM-DD HH", e.exception.args[0], s)
        with self.assertRaises(AssertionError) as e:
            s = "2021-12-16"
            toolbox.PointInTime(s, hourly=True)
        self.assertIn("YYYY-MM-DD HH", e.exception.args[0], s)

    def test_bad_date(self):
        # not a leap year
        with self.assertRaises(ValueError):
            toolbox.PointInTime("2021022914", hourly=True)
        with self.assertRaises(ValueError):
            toolbox.PointInTime("2021-02-29 14", hourly=True)


class TestPointInTimeSub(unittest.TestCase):

    def test_days(self):
        d0 = toolbox.PointInTime("2020-12-24", hourly=False)
        d1 = toolbox.PointInTime("2020-12-23", hourly=False)
        self.assertEqual(d1 - d0, 2)
        self.assertEqual(d0 - d1, 2)

    def test_hourly(self):
        d0 = toolbox.PointInTime("2020-12-24 18", hourly=True)
        d1 = toolbox.PointInTime("2020-12-24 17", hourly=True)
        self.assertEqual(d1 - d0, 2)
        self.assertEqual(d0 - d1, 2)


class TestPointInTimeAutodetect(unittest.TestCase):

    def test_daily(self):
        s = "20211216"
        d = toolbox.PointInTime(s)
        self.assertEqual(d.dwdts(), s)
        self.assertFalse(d.hourly, s)
        s = "2021-12-16"
        d = toolbox.PointInTime(s)
        self.assertEqual(d.iso(), s)
        self.assertFalse(d.hourly, s)
        s = date(2021, 12, 26)
        d = toolbox.PointInTime(s)
        self.assertEqual(d.datetime(), s)
        self.assertFalse(d.hourly, s)

    def test_hourly(self):
        s = "2021121614"
        d = toolbox.PointInTime(s)
        self.assertEqual(d.dwdts(), s)
        self.assertTrue(d.hourly, s)
        s = "2021-12-16 14"
        d = toolbox.PointInTime(s)
        self.assertEqual(d.iso(), s)
        self.assertTrue(d.hourly, s)
        s = datetime(2021, 12, 26, 14)
        d = toolbox.PointInTime(s)
        self.assertEqual(d.datetime(), s)
        self.assertTrue(d.hourly, s)

    def test_illformed(self):
        with self.assertRaises(AssertionError):
            toolbox.PointInTime("202112161")
        with self.assertRaises(AssertionError):
            toolbox.PointInTime("2021-12-161")


if __name__ == '__main__':
    unittest.main()
