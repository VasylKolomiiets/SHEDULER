# =============================================================================.
#
#   1. получить задачу
#
#   прочесть письмо, проверить синтаксис  subj,
#   итог:
#    получена конкретная задача по конкретному клиенту в конкретное время (время отправки письма)
#
#   2. выполнить задачу
#       проверить наличие ключевых отчетов. если нет - отправить ответ письмом
#       если есть, но в ожидании - поставить проверку готовности в очередь
#       если ключевые отчеты готовы - поставить на скачивание:
#           - ключевые файлы
#           - дополнительные файлы по списку (если есть)
#                 проверить наличие ключевых отчетов. если нет - сделать запросы на отчтеты
#                 если есть, но в ожидании - поставить проверку готовности в очередь
#       обработать полученные файлы
#       сформировать (и отформатировать) итоговые файлы
#
#   3. ответить письмом (файлы исходые и итоговые в архиве с комментариями в теле письма)
#
# =============================================================================

"""
# Algoritm dscription.
# process_new_query:
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
"""
import sqlite3
from functools import lru_cache

import time

import sys
from pathlib import Path

import configparser

import pandas as pd


from request_g import Get_requests
import api2api as a2a
from api2api import CODING

from models import (
    log, logging_init,
    # models dataclasses
    ClientConn, NewTask, ScheduledReport, ScheduledTask,
    datetime_now_iso_str, now_plus_datetime_iso_str,  # restart_datetime_iso_str,
    STATUS_SCHED,
                    )

from my_utils import secure_filename

# modify this path to match your environment
sys.path.append(r"D:\OneDrive\Projects\Chud_Amaz\Soft_in_dev\moduled_way")

import package_processing as pp
from data_processing import data_processing, excel_writer
from excel_formatting import excel_file_formatting


# @lru_cache(1024)
def get_client_token(cursore, client_id):
    """Connect to Amazon server."""
    record = cursore.execute(f'SELECT * FROM auth_code WHERE id={client_id}').fetchone()
    amz_conn = ClientConn(*record)
    return amz_conn


@log
def get_sheduled_tasks(cursor):
    """
    Get sheduled tasks from DB and put them to returned dictionary.

    Returns
    -------
    dictionary
        sheduled tasks with id as Key.
    """
    tasks = dict()

    records = cursor.execute('SELECT * FROM tasks_sheduled WHERE not finished').fetchall()
    tasks = {r["id"]: ScheduledTask(*r) for r in records}
    return tasks


@log
def time_to_check_news(cursor, hours=2):
    """
    Проверить время получения порции запросов на отчеты. Если время пришло - возвращает True.

    Parameters
    ----------
    cursor :   >> курсор на открытую БД.
    delta : dt.timedelta >>  временной интервал , optional. Время, через которое надо
        проверить наличие новых запросов на обработку отчетов.
        The default is dt.timedelta(hours=2).

    Returns
    -------
    bool
        Возвращает True, если время записаное в БД для очередной проверки входа прошло.
        В противном случае возвращает  False

    """

    def set_news_checked(cursor):
        """Устанавливает все "открытые" запросы в статус "проверен"."""
        cursor.execute("UPDATE check_news SET is_checked=True, checked_time=? "
                       "WHERE is_checked=False",
                       (datetime_now_iso_str(), ))
        conn.commit()
#
    to_check = cursor.execute('SELECT * FROM check_news WHERE not is_checked').fetchall()
    time_to_check = True
    if to_check:
        if to_check[-1]['check_time'] <= datetime_now_iso_str():
            set_news_checked(cursor)
        else:
            time_to_check = False
    else:   # по каким-то причинам в базе нет записей с указанием времени проверки
        add_new_time_to_check_news(c, hours=hours)
        time_to_check = False

    return time_to_check


@log
def add_new_time_to_check_news(cursor, hours=2):
    """Add_new_time_to_check_news."""
    cursor.execute("INSERT INTO check_news (check_time) VALUES (?)",
                   (now_plus_datetime_iso_str(hours=hours),))
    conn.commit()


# @lru_cache(1024)
def get_client_id_by_name(cursor, client_name):
    """Receiving client_id by client name."""
    return cursor.execute(
        'SELECT id FROM auth_code WHERE firms_name=?',
        (client_name,)).fetchone()["id"]   # have to be only one result in DB


# @lru_cache(1024)
def task_type_id(cursor, task_type_name):
    """Receiving task_type_id by task type name."""
    return cursor.execute(
        'SELECT id FROM task_types WHERE task_type=?',
        (task_type_name,)).fetchone()["id"]    # have to be only one result in DB


def task_schedule(cursor, new_task):
    """
    Put new (empty) task into DB.

    Parameters
    ----------
    cursur: cursor to SQLite DB
    new_task: NewTask. Consists all informaition about Tasks type

    Returns: int
    -------
       the added record ID in DB
    """
    task_to_shedule = ScheduledTask(post_time=new_task.post_time, e_mail=new_task.e_mail)
    task_to_shedule.task_type_id = task_type_id(c, new_task.task_type)
    task_to_shedule.client_id = get_client_id_by_name(c, new_task.client_name)

    client_name = secure_filename(new_task.client_name.replace(" ", ""))
    task_type = new_task.task_type
    task_post_time = secure_filename(new_task.post_time)
    # file prefix looks like: "Premier_Snapshot_15.12.2020_084803"
    task_to_shedule.files_prefix = "_".join([client_name, task_type, task_post_time])
    # Shedules the new_task to DB
    try:
        cursor.execute(
            """INSERT INTO tasks_sheduled
            (client_id, post_time, task_type_id, e_mail, files_prefix)
            VALUES (?, ?, ?, ?, ?)
            """,
            (task_to_shedule.client_id,
             task_to_shedule.post_time, task_to_shedule.task_type_id, task_to_shedule.e_mail,
             task_to_shedule.files_prefix)
        )
    except Exception as e:    # TODO: right Exception  == sqlite3.IntegrityError
        assert False, f"==============have to log! - task_shedule record adding fail \n {e}"

    last_id = cursor.lastrowid   # The last Id of the inserted row
    conn.commit()
    task_to_shedule.sheduled_task_id = last_id
    return task_to_shedule


@log
def tasks_reports_inserting(cursor, new_task):
    """
    Put all needed BLANK reports to further queue by storing them to DB.

    Parameters
    ----------
    cursur: cursor to SQLite DB
    new_task: ScheduledTask. Consists all informaition about Tasks type

    Returns
    -------
    None.

    """
    # Gets the reports names list
    reports = cursor.execute('SELECT * FROM tasks_reports WHERE task_type_id=?',
                             (new_task.task_type_id, )
                             ).fetchall()
    # Puts the reports demands to DB
    for report in reports:
        cursor.execute(  # BLANK report adding - without time to check and real amazon report+id
            """
            INSERT INTO reports_sheduled (task_id, report_id, start_time)
            VALUES (?, ?, ?)
            """,
            (new_task.sheduled_task_id, report["id"], datetime_now_iso_str())
                       )
    conn.commit()


def task_time_shedule(cursor, new_task):
    """
    Put start time to record - usually after - all reports sheduking to DB.

    If some bag or interrupt was heppened - we can see it by unfilled start time field.

    Parameters
    ----------
    cursur: cursor to SQLite DB
    new_task: ScheduledTask. Consists all informaition about Tasks type

    Returns: None
    -------

    """
    # Shedules the new_task to DB
    cursor.execute("UPDATE tasks_sheduled SET start_time=? WHERE id=?",
                   (datetime_now_iso_str(), new_task.sheduled_task_id))
    conn.commit()


@log
def get_sheduled_reports(c):
    """
    Take from DB sheduled reports with theirs statuses.

    Parameters
    ----------
    c : cursor to SQLite DB

    Returns
    -------
    list   of sheduled to DB reports
    """
    shedule = c.execute('SELECT * FROM reports_sheduled WHERE not saved').fetchall()
    return [ScheduledReport(*sheduled) for sheduled in shedule
            if sheduled["restart_time"] <= datetime_now_iso_str()]


def check_handmade_reports(cursor, task):
    """
    Check all "handmaide" reports from task for previous manual creating.

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
    # TODO: rework it ????
    cursor.execute(
        'SELECT * FROM reports_sheduled WHERE is_handmaded and client_id=? and status!=?',
        (task.client_id, "__DONE__")
        )
    # go to Amazon and check with API - really it's created?
    a2a.is_report_created[task]
    return True


@log
def hm_report_shedule(cursor, task, report_data, report_dates):
    """
    Find record with blank record on report_type.

      calculate time to get report
    """
    restart_time = now_plus_datetime_iso_str()

    #  store the Amazon report ID  AND  time to get report - "restart_time"
    cursor.execute("""
                   UPDATE reports_sheduled SET status=?, restart_time=?,
                           amzn_rprt_id=?, date_from=?, date_to=?
                   WHERE task_id=? AND report_id=?
                   """,
                   (STATUS_SCHED, restart_time,
                    report_dates.amazon_id, report_dates.StartDate, report_dates.EndDate,
                    task.sheduled_task_id, report_data.report_id)
                   )
    conn.commit()


def auto_report_shedule(cursor, report_data, report_amazon_id):
    """Find record with blank record on report_type.

    # TODO: 1st - check DB for sheduled reports for best time setting
    """
    restart_time = now_plus_datetime_iso_str()

    #  store the Amazon report ID  AND  time to get report - "restart_time"
    cursor.execute("""
                   UPDATE reports_sheduled SET status=?, restart_time=?,
                   amzn_rprt_id=?, date_from=?, date_to=?
                   WHERE id=?
                   """,
                   (STATUS_SCHED, restart_time,
                    report_amazon_id, report_data.reportstart, report_data.reportend,
                    report_data.report_id)
                   )
    conn.commit()


# @lru_cache(1024)
def get_report_usual_name(cursor, report_id):
    """Get from DB report usual name."""
    return cursor.execute(""" SELECT usual_name FROM tasks_reports WHERE id=?  """,
                          (report_id, )).fetchone()["usual_name"]


@log
def mark_report_as_saved(c, report_id, file_name):
    """Set field 'saved' to True and store file_name to DB."""
    c.execute("""UPDATE reports_sheduled SET saved=?, filename=? WHERE id=? """,
              (True, file_name, report_id)
              )
    conn.commit()


@log
def mark_task_as_finished(cursor, task):
    """Set task 'finished' to True and store current time to  field 'finish_time' in DB.

    Parameters
    ----------
    cursur: cursor to SQLite DB
    task: ScheduledTask. Consists all informaition about Tasks type

    Returns: current_time sheduled
    -------

    """
    now_time = datetime_now_iso_str()
    cursor.execute("UPDATE tasks_sheduled SET finish_time=?, finished=? WHERE id=?",
                   (now_time, True, task.sheduled_task_id))
    conn.commit()
    return now_time


def set_report_status(c, report):
    """Set report status."""
    c.execute("UPDATE reports_sheduled SET status=? WHERE id=?",
              (report.status, report.id_sheduled))
    conn.commit()


def get_task_type_name(cursor, task):
    """Get task type name."""
    rezult = cursor.execute(""" SELECT task_type FROM task_types WHERE id=?  """,
                            (task.task_type_id, )).fetchone()["task_type"]
    return rezult


def get_task_time(cursor, task):
    """Get task post time in ms from start epoche."""
    # from werkzeug.utils import secure_filename

    rezult = cursor.execute('SELECT post_time FROM tasks_sheduled WHERE id=?',
                            (task.sheduled_task_id, )
                            ).fetchone()["post_time"]
    return rezult


def get_task_filenames(cursor, task):
    """Pick all report's file names from DB."""
    record_tuples = cursor.execute("SELECT filename FROM reports_sheduled WHERE task_id=?",
                                   (task.sheduled_task_id, )).fetchall()
    file_names = [record["filename"] for record in record_tuples]
    return file_names


@log
def save_df_to_csv_file(df, REPORTS_FOLDER, task_file_prefix, report):
    """
    Save task report Pandas DataFrame to *.csv with specified name.

    Parameters
    ----------
    df : pandas DataFrame
        table to saving in CSV format.
    task_file_prefix : str
        prefix for tasks files saving.
    report : ScheduledReport
        Scheduled Report of 'task'

    Returns
    -------
    file_name : str
        saved file filename.

    ------
    task_file_name looks like:
        Premier_Snapshot_15.12.2020_084803_Adjustments.zip
    """
    report_usual_name = get_report_usual_name(c, report.report_id)
    file_name = "_".join([task_file_prefix, report_usual_name])

    arch_ext, arch_method = '.zip', 'zip'
    compression_opts = dict(method=arch_method, archive_name=file_name + '.csv')
    df.to_csv(str(REPORTS_FOLDER / (file_name + arch_ext)), index=False,
              compression=compression_opts)

    return file_name + arch_ext


def pick_finished_tasks(cursor, renewed_tasks: set) -> dict:
    """
    Check reports to be full set of _DONE_ status.

    Returns
    -------
    dict
        dict of pair task_id: task of fully downloaded set of reports (if it is).

    """
    tasks_to_work_out = dict()
    for task in renewed_tasks:  # TODO: sum() == count()
        rezult = cursor.execute("""SELECT status == ? as zo FROM reports_sheduled
                                    WHERE task_id=? and saved""",
                                ('_DONE_', task.sheduled_task_id, )).fetchall()
        zeroones = [row["zo"] for row in rezult]
        if all(zeroones):
            tasks_to_work_out[task.sheduled_task_id] = task

    return tasks_to_work_out


@log
def r_files_reading(file_names: list):
    """
    Take all files and make the resulting report file(s).

    Полный аналог "files_reading(folders_path, client_folder)" из пакета package_processing.

    Returns
    -------
    list:
        resulting files (names).

    """
    # pick all reports files
    files = dict.fromkeys(["Reconciliation", "Reimbursements", "Adjustments"])

    # process the files
    for key in files:
        file_name = [file_name for file_name in file_names  # extract kee value from file name
                     # Premier_Snapshot_15.12.2020_084803_32_Adjustments.zip
                     if file_name.split("_")[4].split(".")[0] == key][0]
        files[key] = file_name

    separator = ","  # source file field separators: '\t' = tab or "," = comma

    #  !chardetect direct_adj_20285364484018375.csv
    en_codings = ["latin1", "cp1252", "cp1251", "cp1250"]

    pd_files = dict.fromkeys(files.keys())
    readed = False
    for en_coding in en_codings:
        try:
            for key, file_name in files.items():
                pd_files[key] = pd.read_csv(str(REPORTS_FOLDER / file_name),
                                            error_bad_lines=False,
                                            sep=separator,
                                            encoding=en_coding,
                                            )
                pp.rename_df_columns(pd_files[key])
            readed = True
            break
        except ValueError as error:
            print(error)

    if not readed:
        raise Exception(f'неизвестная кодировка. не из {en_codings}')

    return pd_files["Reconciliation"], pd_files["Adjustments"], pd_files["Reimbursements"]


def connect_with_client_id(c, client_id):
    """Connect to Amazon with client_id. Returns ClientConn dataclass."""
    client_conn = get_client_token(c, client_id)
    client_conn.x = a2a.amazon_connect(client_conn, *get_dev_keys(c))
    return client_conn


def get_dev_keys(cursor):
    """Do TODO:  developer keys depends on region."""
    #  TODO:  developer keys depends on region
    row = cursor.execute('SELECT * FROM dev').fetchone()
    return row["access_key"], row["secret_key"]


#  --- main --------------------------------------------------------------------------------------
logger = logging_init()


parser = configparser.ConfigParser()    # allow_no_value=True
parser.read("settings.ini")

# Читаем некоторые значения из конфиг. файла.
NEWS_CHECKING_TIME_STEP_HOURS = float(parser.get("Intervals", "NEWS_CHECKING_TIME_STEP_HOURS"))
# timeshift for generation  TODO: timeshift for downlading
TIME_TO_SLEEP = float(parser.get("Intervals", "TIME_TO_SLEEP_SECS"))
DAYS_REPORTS_LOOKUP = int(parser.get("Intervals", "DAYS_REPORTS_LOOKUP"))

sqlite_path = parser.get("Folders.Files", "SQLite_path")

REPORTS_FOLDER = Path(parser.get("Folders.Files", "work_path"))  # work folder path
JOCK_FILE = Path(r"D:\OneDrive\PyCodes\SHEDULER\tenor_Mister_Bin.gif")


conn = sqlite3.connect(sqlite_path)
conn.row_factory = sqlite3.Row

c = conn.cursor()

connect_g = Get_requests()

sheduled_tasks = get_sheduled_tasks(c)

clients_conn = dict()
for task in sheduled_tasks.values():
    if task.client_id not in clients_conn:
        clients_conn[task.client_id] = connect_with_client_id(c, task.client_id)

while True:     # for i in range(20):
    b_fromdate = bytes(now_plus_datetime_iso_str(days=-DAYS_REPORTS_LOOKUP), CODING)
    b_todate = bytes(now_plus_datetime_iso_str(days=1), CODING)  # tommorow

    if time_to_check_news(c, hours=NEWS_CHECKING_TIME_STEP_HOURS / 30):  # check the task inputflow
        for row in connect_g.check_new_task_query():
            # ==== 1. Receive the task. Get new processing request from cloud sheet ==============
            new_task = NewTask(*list(row.values()))     # filling new task parameters
            sch_task = task_schedule(c, new_task)

            if sch_task.client_id not in clients_conn:
                clients_conn[sch_task.client_id] = connect_with_client_id(c, sch_task.client_id)
            x = clients_conn[sch_task.client_id].x

            sheduled_tasks[sch_task.sheduled_task_id] = sch_task
            tasks_reports_inserting(c, sch_task)
            task_time_shedule(c, sch_task)  # подтверждение нормального ввода новых отчетов в базу
            # ====================================================================================
            hm_report_data = a2a.get_handmade_report_params(c, sch_task.task_type_id)
            handmade_reports = a2a.get_report_ids(
                x,
                hm_report_data.reportstart,
                hm_report_data.reportend,
                reports_types=(bytes(hm_report_data.report_amz_name, CODING), ),
                fromdate=b_fromdate,
                todate=b_todate)

            if hm_report_data.files_to_get == "ALL":  # TODO: сделать числа? -1, 0, 2 ...  ? %%%
                chains_count = a2a.check_chains_count(handmade_reports, hm_report_data.reportstart,
                                                      DAYS_REPORTS_LOOKUP)
                if chains_count > 0:
                    handmade_reports_ids = a2a.get_reports_chains(c, x,
                                                                  handmade_reports, chains_count)
                    # TODO: позже написать построение правильной цепочки по датам и размерам
                    for id_ in handmade_reports_ids:
                        pass

                else:   # send the message -"do handmade reports" and mark the task as DONE
                    pass
            else:   # like Snapshots
                hm_report_dates = a2a.check_snapshots_handmade_report(handmade_reports,
                                                                      hm_report_data)
                if not hm_report_dates:   # нужного отчета нет - sch_task ошибочен !!
                    new_files_path = {"xlsx": JOCK_FILE}
                    body_text = """"Task macros reply: 'Nothing to do'.
                    Please do handmade report with correct start/end dates.
                    I means {hm_report_data.usual_name} report for
                    client '{clients_conn[sch_task.client_id].name}'
                    """
                # поставить получение отчета в очередь
                # для этого надо найти запись в шедулере, соответствующую данному отчету
                # и вписать время считывания отчета через API Amazon

                hm_report_shedule(c, sch_task, hm_report_data, hm_report_dates)

            #   auto reports requesting (or creating if needs)
            for auto_report in a2a.auto_reports(c, sch_task, hm_report_dates):
                report_amazon_id = a2a.report_request_nd_report_id(x, auto_report)
                auto_report_shedule(c, auto_report, report_amazon_id)
            # g mark as picked
        # ====================================================================================
        add_new_time_to_check_news(c, hours=NEWS_CHECKING_TIME_STEP_HOURS / 30)

    #  --
    sheduled_reports = get_sheduled_reports(c)
    print({f"sheduled_reports={sheduled_reports}"})
    tasks_with_new_done_report = set()
    for report in sheduled_reports:
        task = sheduled_tasks[report.task_id]   # get_report_task
        task_f_prefix = task.files_prefix
        x = clients_conn[task.client_id].x
        if report.status == "_DONE_":   # _DONE_NO_DATA_  TODO:
            df = a2a.get_report_file(x, report.amzn_rprt_id)
            # put it in file
            file_name = save_df_to_csv_file(df, REPORTS_FOLDER, task_f_prefix, report)
            # put the file name to the record DB and mark it as 'saved'
            mark_report_as_saved(c, report.id_sheduled, file_name)
            tasks_with_new_done_report.add(task)
        else:  # RENEW STATUS
            report.status = a2a.get_current_status(c, x, report)
            # store NEW report.status to DB  & change restart_time ))
            set_report_status(c, report)

    if not tasks_with_new_done_report:      # TODO: stop ir and check all? not only changed
        continue
    # here was at least one report with "_DONE_" status
    finished_tasks = pick_finished_tasks(c, tasks_with_new_done_report)
    for task_id, task in finished_tasks.items():
        task_f_prefix = task.files_prefix
        # data_processing(df_rec, df_adj, df_rei)
        file_names = get_task_filenames(c, task)
        files = data_processing(*r_files_reading(file_names))
        if all(tuple(map(lambda d_f: not d_f.empty, files.values()))):  # check if any DF is ampty
            print("===ExcelWriter==")
            client_folder = ""      # there is no client folders - all together
            new_files_path = excel_writer(REPORTS_FOLDER, client_folder, task_f_prefix, files)

            print("===Excelformattig==")
            excel_file_formatting(str(new_files_path["xlsx"]))
            body_text = "Task macros reply: 'Ok'"
        else:
            new_files_path = {"xlsx": JOCK_FILE}
            body_text = "Task macros reply: 'Nothing to do'"
        #
        print("===send_answer==")
        pp.send_mail_with_attach(task.e_mail, task_f_prefix,
                                 mail_body=body_text,
                                 files_to_attach=[
                                     new_files_path["xlsx"],
                                     *[REPORTS_FOLDER / file_name for file_name in file_names],
                                     ]
                                 )

        mark_task_as_finished(c, task)
        connect_g.mark_as_DONE(task.post_time)
        sheduled_tasks.pop(task_id)     # remove task from processing list. The task is done/

    time.sleep(TIME_TO_SLEEP)
conn.close()

# TODO: split requests and report status checking

# TODO: in case of Error 401 skip task and (?) send a letter
# TODO: process situation with _CANCELLED_ and _DONE_NO_DATA_ statuses as known

# TODO: set and account timeout for auto reports next check time
# TODO: different timeout for different auto reports ???
# time.sleep(50)

# TODO: rework module pp

"""
_SUBMITTED_
_IN_PROGRESS_
_CANCELLED_
_DONE_
_DONE_NO_DATA_
"""
