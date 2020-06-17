'''

#  process_new_query:
#   check_source_table
#   if is_new_queryes:
#        for each_new_query:
#          identify the new task
#          set the tasks files set
#          receive the reports_ids and statuses
#          put the tasks commands to database

# looping:
#   if time_to_check_news :
#        process_new_query
#   if time_to_check_tasks:
#      read the tasks:
#      for "_done_" reports  get the reports   (need grooping to pakadge???)
#      save the reports to disk
#      change subtask status it the shedule (save the saved file name to DB? )
#      for other kinds of status - reset the check time to GetReportRequestListResponse again
#
#   if all subtasks  _done_:
#          run the task processing
#          mark the task as _done_ in db
'''
import sqlite3

import time
from datetime import datetime
import datetime as dt

from pathlib import Path

from dataclasses import dataclass
import typing

from request_g import Get_requests



def renew_trottling_data():
    pass


def get_sheduled_tasks():
    return []


def time_to_check_news(start='', delta=dt.timedelta(hours=2)):
    return True


def all_tasks_shedule():
    pass


def is_sheduled_task():
    pass


def all_reports_sheduled(task):
    return True



def sheduler_tasks_reports(new_task):
    pass


def get_sheduled_reports(c):
    '''

    Parameters
    ----------
    c : TYPE
        DESCRIPTION.

    Returns
    -------
    list
        DESCRIPTION.

    '''
    return [Scheduled_report(*sheduled) for sheduled
            in c.execute('SELECT * FROM reports_sheduled WHERE report_gotten=0').fetchall()
            if datetime.fromisoformat(sheduled[6]) <= datetime.now()
            # '2020-05-28 17:49:03' - datetime without milisec
            ]


def tasks_reports_collecting():
    return []


def hand_made_reports_exists(task):
    return True


def get_report(report_id):
    '''
    Parameters
    ----------
    report_id : TYPE
        DESCRIPTION.

    Returns
    -------
    None.

    '''
    pass  # returns pandas df


def report_to_csv(path, df):
    return "test.xlsx"


def data_processing():
    pass


def result_sending(files_to_send):
    pass


@dataclass
class Scheduled_report:
    id_: int = 0
    clients_id: int = 0
    task_type_id: int = 0
    report_id: int = 0
    start_time: str = ""
    status: str = ""
    restart_time: str = ""
    report_gotten: bool = False
    filename: str = ""


@dataclass
class NewTask:
    # TODO: all
    id_: int = 0
    clients_id: int = 0
    task_type_id: int = 0



NEWS_CHECKING = dt.timedelta(seconds=60*60*2)

client_folder = ""  # clients folder path

conn = sqlite3.connect(r"E:\OneDrive\PyCodes\asyncio\clients.db")
c = conn.cursor()

connect = Get_requests()


sheduled_tasks = get_sheduled_tasks()
sheduled_reports = get_sheduled_reports(c)
renew_trottling_data()

while True:

    if time_to_check_news():
        for row in connect.check_new_task_query():
            new_task = NewTask(row)
            sheduler_tasks_reports(new_task)
            sheduled_tasks.append(new_task)

    if sheduled_tasks:
        for task in sheduled_tasks:
            if not all_reports_sheduled(task):
                sheduler_tasks_reports(task)
            if hand_made_reports_exists(task):
                pass

    for report in get_sheduled_reports(c):
        if report.status == "_DONE_":
            filename = report_to_csv(client_folder, get_report(task.report_id))  # packaging?
            # c.execute("UPDATE reports_... SET report_gotten=? WHERE id=?", (True, task.id))
            c.execute(f"""UPDATE reports_sheduled SET report_gotten=True
                      filename = {filename} WHERE id={task.id}
                      
                      """)
        conn.commit()

    tasks_finished = tasks_reports_collecting()

    if tasks_finished:
        result_sending(data_processing())
