# =============================================================================
#
#   1. получить задачу
#   прочесть письмо, проверить синтаксис  subj,
#   итог: получена конкретная задача по конкретному клиенту в конкретное время (время отправки письма)
#
#   2. выполнить задачу
#       проверить наличие ключевых отчетов. если нет - отправить ответ письмом
#       если есть, но в ожидании - поставить проверку готовности в очередь (+письмо?)
#       если ключевые отчеты готовы - поставить на скачивание:
#           - ключевые файлы
#           - дополнительные файлы по списку (если есть)
#                 проверить наличие ключевых отчетов. если нет - сделать запросы на отчтеты
#                 если есть, но в ожидании - поставить проверку готовности в очередь (+письмо?)
#       обработать полученные файлы
#       сформировать (и отформатировать) итоговые файлы
#
#   3. ответить письмом (файлы исходые и итоговые в архиве с комментариями в теле письма)
#
#
# =============================================================================
'''
#  process_new_query:
#   check_source_table
#   if is_new_queryes:
#        for each_new_query:
#          identify the new task
#          set the tasks files set
#          receive the reports_ids and statuses
#          put the tasks commands to database
#
# looping:
#   if time_to_check_news :
#        process_new_query
#   if time_to_check_tasks:
#      read the tasks:
#      for "_done_" reports  get the reports   (need grooping to pakadge???)
#      save the reports to disk
#      change subtask status it the shedule (save the saved file name to DB? )
#      for other kinds of status - reset the check time
#                                   to GetReportRequestListResponse again
#
#   if all subtasks  _done_:
#          run the task processing
#          mark the task as _done_ in db


    # now = dt.datetime.now()
    # midnight = dt.datetime.combine(now.date(), dt.time())
    # time_from_midnight = (now - midnight)
'''
import sqlite3

import time
from datetime import datetime
import datetime as dt

import sys
from pathlib import Path

from dataclasses import dataclass
# import typing

from logger import log, logging_init
from request_g import Get_requests
import api2api


# modify this path to match your environment
sys.path.append(r"E:\OneDrive\Projects\Chud_Amaz\Soft_in_dev\moduled_way")
from package_processing import send_mail_with_attach


@dataclass
class ScheduledReport:
    """
    структура для работы с отчетами, поставленными в очередь
    """
    id_: int = 0
    clients_id: int = 0
    task_type_id: int = 0
    report_id: int = 0
    start_time: str = ""
    amzn_rprt_id: str = ""
    status: str = ""
    restart_time: str = ""
    report_gotten: bool = False
    filename: str = ""
    task_type: str = ""


@dataclass
class NewTask:
    """
    структура для получения данных очередного запроса на обработку
    """
    time: str = ""
    clients_name: str = ""
    task_type: str = ""
    e_mail: str = ""
    status: str = ""
    close_time: str = ""


def renew_trottling_data():
    """


    Returns
    -------
    None.

    """
    pass


@log
def get_sheduled_tasks():
    """


    Returns
    -------
    list
        DESCRIPTION.

    """
    return []


@log
def time_to_check_news(cursor, delta=dt.timedelta(hours=2)):
    # from math import modf
    """

    Parameters
    ----------
    cursor :   >> курсор на открытую БД.
    delta : dt.timedelta >>  временной интервал , optional. Время, через которое надо
        проверить наличие новых запросов на обработку отчетов.
        DESCRIPTION. The default is dt.timedelta(hours=2).

    Returns
    -------
    bool
        Возвращает True, если время записаное в БД для очередной проверки входа прошло.
        В противном случае возвращает  False

    """

    def set_news_checked(cursor):
        """
        устанавливает все "открытые" запросы в статус "проверен"

        Parameters   None.
        ----------
        Returns     None.
        -------

        """
        cursor.execute("UPDATE check_news SET is_checked=True, checked_time=? "
                       "WHERE is_checked=False",
                       (dt.datetime.today().isoformat(), ))
        conn.commit()

#
    to_check = cursor.execute('SELECT * FROM check_news WHERE is_checked=0').fetchall()
    time_to_check = True
    if to_check:
        if dt.datetime.fromisoformat(to_check[-1][1]) <= dt.datetime.now():
            set_news_checked(cursor)
        else:
            time_to_check = False
    else:   # по каким-то причинам в базе нет записей с указанием времени проверки
        set_news_checked(cursor)   # задаем новое время проверки на наличие запросов

    return time_to_check


@log
def add_new_time_to_check_news(cursor, delta=dt.timedelta(hours=2)):
    """

    Parameters
    ----------
    delta : TYPE, optional
        DESCRIPTION. The default is dt.timedelta(hours=2).

    Returns
    -------
    None.

    """
    cursor.execute("INSERT INTO check_news (check_time) VALUES (?)",
                   ((dt.datetime.today() + delta).isoformat(),))
    conn.commit()


def all_tasks_shedule():
    """

    Returns
    -------
    None.

    """
    pass


def is_sheduled_task():
    """

    Returns
    -------
    bool
        DESCRIPTION.

    """
    return True


@log
def all_reports_sheduled(task):
    """

    Parameters
    ----------
    task : TYPE
        DESCRIPTION.

    Returns
    -------
    bool
        DESCRIPTION.

    """
    return True


@log
def tasks_reports_sheduler(cursor, new_task):
    """
    It puts all needed reports to queue by storing them to DB

    Parameters
    ----------
    cursur: cursor to SQLite DB
    new_task: NewTask. Consists all informaition about Tasks type

    Returns
    -------
    None.

    """
    # receiving client_id and task_type_id by names:
    client_id = cursor.execute(
        'SELECT id FROM auth_code WHERE firms_name=?',
        (new_task.clients_name,)
        ).fetchone()[0]   # have to be only one result in DB
    task_type_id = cursor.execute(
        'SELECT id FROM task_types WHERE task_type=?',
        (new_task.task_type,)
        ).fetchone()[0]    # have to be only one result in DB
    # Shedules the new_task to DB
    cursor.execute("""INSERT INTO tasks_sheduled
                   (client_id, post_time, task_type_id, e_mail )
                   VALUES (?, ?, ?, ? )
                   """,
                   (client_id, new_task.time, task_type_id, new_task.e_mail)
                   )

    # Gets the reports names list
    reports = cursor.execute('SELECT * FROM tasks_reports WHERE task_types_id=?',
                             (task_type_id, )
                             ).fetchall()
    # Puts the reports demands to DB
    for report in reports:
        """
        select foods.name, food_types.name
        from foods, food_types
        where foods.type_id=food_types.id limit 10;
        """
        cursor.execute("""INSERT INTO reports_sheduled
               (client_id, task_type_id, report_id)
               VALUES (?, ?, ?)
               """,
                       (client_id, task_type_id, report[0])
                       )
    conn.commit()


@log
def get_sheduled_reports(c):
    '''
    It takes from DB sheduled reports with theirs statuses/

    Parameters
    ----------
    c : cursor to SQLite DB

    Returns
    -------
    list   of sheduled to DB reports

    '''
    shedule = c.execute('SELECT * FROM reports_sheduled WHERE report_gotten=0').fetchall()
    return [ScheduledReport(*sheduled) for sheduled in shedule
            if datetime.fromisoformat(sheduled[4]) <= datetime.now()]


def tasks_reports_collecting():
    """

    Returns
    -------
    list
        DESCRIPTION.

    """
    return sheduled_tasks  # []


def check_handmade_reports(cursor, task):
    """
    It shecks all "handmaide" reports from task for previous manual creating
    Parameters
    ----------
    cursor: SQLite database cursor
    task : NewTask
        current processing task.

    Returns
    -------
    list of str or bool
        False - if handmade report not exists
        "amazon_report_id"  if it exists
    """
    # SELECT handmaded report(s) in task
    cursor.execute(
        'SELECT * FROM reports_sheduled WHERE is_handmaded and client_id=? and status !=?',
        (task.client_id, "__DONE__")
        )
    # go to Amazon and check with API - really it's created?
    api2api.is_report_created[task.)
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
    """

    Parameters
    ----------
    path : TYPE
        DESCRIPTION.
    df : TYPE
        DESCRIPTION.

    Returns
    -------
    str
        DESCRIPTION.

    """
    return "test.xlsx"


def data_processing(task: NewTask):
    """

    Returns
    -------
    None.

    """
    return ([Path(r"E:\OneDrive\PyCodes\SHEDULER\tenor_Mister_Bin.gif")], task)


def result_sending(files_to_send, task):
    """

    Parameters
    ----------
    files_to_send : TYPE
        DESCRIPTION.

    Returns
    -------
    None.

    """

    send_mail_with_attach(task.e_mail,
                          f"{task.clients_name}-s data for {task.task_type} report",
                          files_to_attach=files_to_send)


logger = logging_init()

NEWS_CHECKING_TIME_STEP = dt.timedelta(seconds=60*60*2)
TIME_TO_SLEEP = 1/8  # timeshift for generation  TODO: timeshift for downlading

client_folder = ""  # clients folder path

conn = sqlite3.connect(r"E:\OneDrive\PyCodes\SHEDULER\clients.db")
c = conn.cursor()

connect_g = Get_requests()

sheduled_tasks = get_sheduled_tasks()
sheduled_reports = get_sheduled_reports(c)
renew_trottling_data()


# while True:
for i in range(3):

    if time_to_check_news(c):
        for row in connect_g.check_new_task_query():
            new_task = NewTask(*list(row.values()))
            sheduled_tasks.append(new_task)
            tasks_reports_sheduler(c, new_task)
        add_new_time_to_check_news(c, delta=NEWS_CHECKING_TIME_STEP)

    for task in sheduled_tasks:  # if no cheduled tasks - so no tasks )))
        handmade_reports_ids = check_handmade_reports(c, task)
        if all(handmade_reports_ids):
            for id in handmade_reports_ids:
                get_report(id)

        else:   # send the message -"do handmade reports" and mark the task as DONE
            pass

    for report in get_sheduled_reports(c):
        # get reports
        if report.status == "_DONE_":
            filename = report_to_csv(client_folder, get_report(task.report_id))  # zipit?
            c.execute("UPDATE reports_sheduled SET report_gotten=? WHERE id=?",
                      (True, task.id))
            conn.commit()

    tasks_finished = tasks_reports_collecting()

    for task in tasks_finished:
        result_sending(*data_processing(task))
        sheduled_tasks.remove(task)
        connect_g.mark_as_DONE(task.time)

    time.sleep(TIME_TO_SLEEP)
conn.close()
