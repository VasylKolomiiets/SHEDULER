"""
Created on Tue Jan  5 13:40:07 2021.

@author: Vasil
To add new task: https://forms.gle/w3jEBCL7Bab2bTn88

error 401:
    https://support.ordoro.com/amazon-us-and-ordoro-setup/
    https://stackoverflow.com/questions/60263634/aws-401-access-denied-details-correct-permissions-set
    https://docs.developer.amazonservices.com/en_US/subscriptions/Subscriptions_ErrorCodes.html
    https://docs.developer.amazonservices.com/en_US/dev_guide/DG_Errors.html

    headers = {'User-Agent': 'python-amazon-a2a/0.1 (Language=Python)'}   # %%% mws 237 line
"""
from logger import log, logging_init

from dataclasses import dataclass
import typing

from datetime import datetime
import datetime as dt


@dataclass
class NewTask:
    """структура для данных очередного запроса на обработку из googlesheet."""

    post_time: str = ""
    client_name: str = ""
    task_type: str = ""
    e_mail: str = ""
    status: str = ""
    close_time: str = ""


@dataclass
class ScheduledTask:
    """структура для работы с задачами на обработку отчетов, поставленными в очередь."""

    sheduled_task_id: int = 0
    client_id: int = 0
    post_time: str = ""
    task_type_id: int = 0
    e_mail: str = ""
    start_time: str = ""
    files_prefix: str = ""
    finished: bool = False
    finish_time: str = ""

    def __hash__(self):
        """Hash for using in set()."""
        return hash(self.sheduled_task_id)


@dataclass
class ScheduledReport:
    """структура для работы с отчетами, поставленными в очередь."""

    id_sheduled: int = 0
    task_id: int = 0
    report_id: int = 0
    start_time: str = ""
    amzn_rqst_id: str = ""
    amzn_rprt_id: str = ""
    date_from: str = ""
    date_to: str = ""
    restart_time: str = ""
    status: str = ""
    saved: bool = False
    filename: str = ""


@dataclass
class ClientConn:
    """Connection to Amazon server by token."""

    client_id: int = 0
    name: str = "YuChud"
    seller_id: str = 'A3EVIIHGE6S6KI'
    auth_token: str = None
    access_key: str = None
    secret_key: str = None
    last_date: str = ""
    x: 'typing.Any' = None   # connection to Amazon


# ---------
@dataclass
class ReportsData:
    """структура для данных SQLite очередного отчета в задании на обработку."""

    id_rep_type: int        # - specific ID of this kind report in current task
    reportstart: str        # data in ISO format
    reportend: str          # data in ISO format
    min_duration: int       # min_duration of the report period
    report_amz_name: str    # Amazon API report name
    usual_name: str
    files_to_get: str       # Type of reports processing  (ALL / LAST)


@dataclass
class Report_dates:
    """структура для данных отчета GetReportRequestList(), полученных от сервера Amazon."""

    amazon_id: str = ""
    SubmittedDate: str = ""
    StartDate: str = ""
    EndDate: str = ""
    CompletedDate: str = ""
    ReportProcessingStatus: str = ""


# ------------------------------- end dataclass block -----------

def datetime_now_iso_str():
    """Take  datetime now in ISO format."""
    return datetime.now().isoformat()[:19]


def now_plus_datetime_iso_str(days=0, hours=0, minutes=0, seconds=0):
    """Form  datetime.now() + seconds=0.5 in ISO format string without timezone."""
    return (datetime.now() + dt.timedelta(days=days,
                                          hours=hours,
                                          minutes=minutes,
                                          seconds=seconds,
                                          # microseconds=0, milliseconds=0, weeks=0
                                          )
            ).isoformat()[:19]


def delta_days(end_iso_format, start_iso_format):
    """Calculate TimeDelta between two strings - datetime in ISO format.

    Returns.
    Type: TimeDelta
    """
    return datetime.fromisoformat(end_iso_format) - datetime.fromisoformat(start_iso_format)


# --------------------------------- end functions block -----------------

"""
_SUBMITTED_
_IN_PROGRESS_
_CANCELLED_
_DONE_
_DONE_NO_DATA_
"""
STATUS = {
    "a2a_SCHED": "_a2a_SCHED_",
    "a2a_HAS_RQST_ID": "_a2a_HAS_RQST_ID_",
    "a2a_HAS_NO_HMR": "_a2a_HAS_NO_HMR_",
    "SUBMITTED": "_SUBMITTED_",
    "IN_PROGRESS": "_IN_PROGRESS_",
    "CANCELLED": "_CANCELLED_",
    "DONE": "_DONE_",
    "DONE_NO_DATA": "_DONE_NO_DATA_",
    }

# str(row[0], encoding="utf-8")
