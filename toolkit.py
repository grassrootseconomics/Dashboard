from datetime import datetime
from datetime import timezone
from datetime import timedelta


#test23
class Date:
    '''
    Class to handle dates that are timezone aware and the default
    timezone is UTC+3 (Kenya)
    '''
    tz = timezone(timedelta(hours=3))

    @classmethod
    def today(cls):
        return datetime.now(tz=cls.tz).date()

    @classmethod
    def n_days_ago(cls, days):
        return datetime.now(tz=cls.tz).date() - timedelta(days=days)

    @classmethod
    def n_days_ahead(cls, days):
        return datetime.now(tz=cls.tz).date() + timedelta(days=days)

    @staticmethod
    def from_plotly(plotly_rep):
        return datetime.strptime(plotly_rep, "%Y-%m-%d").date()

    @classmethod
    def from_timestamp(cls, timestamp):
        if isinstance(timestamp, str):
            timestamp = int(timestamp)
        return datetime.fromtimestamp(timestamp, tz=cls.tz).date()


class DateTime:
    '''
    Class to handle dates that are timezone aware and the default
    timezone is UTC+3 (Kenya and Israel)
    '''
    tz = timezone(timedelta(hours=3))

    @classmethod
    def now(cls):
        return datetime.now(tz=cls.tz)

    @classmethod
    def from_timestamp(cls, timestamp):
        if isinstance(timestamp, str):
            timestamp = int(timestamp)
        return datetime.fromtimestamp(timestamp, tz=cls.tz)


