import datetime


class MysqlMigrationError:
    error_type: str
    error_msg: str
    error_date: datetime.datetime

    def __init__(self, error_type: str, error_msg: str, error_date: str | datetime.datetime):
        self.error_type = error_type
        self.error_msg = error_msg
        if isinstance(error_date, datetime.datetime):
            self.error_date = error_date
        else:
            self.error_date = datetime.datetime.strptime(error_date, "%Y-%m-%d %H:%M:%S.%f%z")
