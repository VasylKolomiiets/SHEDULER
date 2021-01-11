# -*- coding: utf-8 -*-
"""
Created on Thu May 21 12:59:24 2020

@author: Vasil
"""
from datetime import datetime

from logger import log

import gspread
from oauth2client.service_account import ServiceAccountCredentials


scope = ["https://spreadsheets.google.com/feeds",
         'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file",
         "https://www.googleapis.com/auth/drive"]


class Get_requests:
    def __init__(self):
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            r"D:\goocloud\Chud\QueriesForm\Query_for_processing-b9c431e58946.json", scope)
        connect = gspread.authorize(creds)
        self.sheet = connect.open('processing_requests').sheet1

    """
    1               2               3                                                       4                        5        6
    Позначка часу	Фирма-Продавец	Тип обработки, для которой я заказал(а) ключевые отчеты	Електронна адреса	     status   out_time
12.06.2020 18:50:35	Quality	        FEE	                                                    vikolo@i.ua
12.06.2020 19:53:01	Echelon	        FEE	                                                    vasilij.kolomiets@gmail.com
12.06.2020 19:54:37	Quality	        Orders Returns	                                        vikolo@i.ua

    """

    @log
    def check_new_task_query(self):
        '''
        Check google sheet for new task requests

        Parameters
        ----------
        sheet : google shet from api
            DESCRIPTION.

        Returns
        -------
        list
            - empty list if all statuses is DONE
            - list of dicts next structure:
                [{'Позначка часу': '12.06.2020 18:50:35',
                  'Фирма-Продавец': 'Quality',
                  'Тип обработки, для которой я заказал(а) ключевые отчеты': 'FEE',
                  'Електронна адреса': 'vikolo@i.ua',
                  'status': ''},]

        '''
        data = self.sheet.get_all_records()
        return [new_task for new_task in data if not new_task["status"]]

    @log
    def mark_as_DONE(self, time_str):
        row_found = self.sheet.find(time_str).row
        self.sheet.update_cell(row_found, 5, "__DONE__")
        self.sheet.update_cell(row_found, 6, datetime.today().isoformat())


'''
data = sheet.get_all_records()
pprint(data)

row = sheet.row_values(1)
pprint(row)

col = sheet.col_values(1)
pprint(col)

cell = sheet.cell(1,2).value
print(cell)

insert_row = [3, "4U", "Transaction", "vikolo@i.ua", False ]
sheet.insert_row(insert_row, 4)
sheet.update_cell(4, 5, True)

sheet.delete_row(4)

len(data)
print("cols=", max([len(row) for row in data]))
'''
