#   Призначення утиліти - первинне створення файлу параметрів
#   для модуля  parserparser.
#   Подалі - можна правити файл параметрів вручну
#

import configparser


def createparser(path):
    """
    Create a parser file
    """
    parser = configparser.ConfigParser()

    parser.add_section("Report.task.kinds")
    parser.set("Report.task.kinds", "tasks list", "FEE,  Order_Discrepancy, Snapshot")

    parser.add_section("Intervals")
    parser.set("Intervals", "DAYS_REPORTS_LOOKUP", "3")
    parser.set("Intervals", "NEWS_CHECKING_TIME_STEP_HOURS", "2")
    parser.set("Intervals", "TIME_TO_SLEEP_SECS", "0.125")

    parser.add_section("Folders.Files")
    parser.set("Folders.Files", "SQLite_path", "E:\\OneDrive\\PyCodes\\SHEDULER\\clients.db")
    parser.set("Folders.Files", "work_path", "E:\\OneDrive\\PyCodes\\SHEDULER\\work_path")

    with open(path, "w") as config_file:
        parser.write(config_file)


if __name__ == "__main__":
    path = "settings.ini"
    createparser(path)
