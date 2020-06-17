# -*- coding: utf-8 -*-  DCU
"""
Created on Tue May 19 15:46:57 2020

@author: Vasil
"""


"""
   формат данных:
       <any_text> ::>  <client_name> :: <report_name>   <:: <any_text>
       any_text := [0-1a-zа-я_-!"Ё№;%:?*()_+/"]
       client_name := [0-1a-zа-я_- ]
       report_name := snapshot | FBA_fee |  returns
"""
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
from lxml import objectify  # etree,
from sys import getsizeof
from mws import mws
import pandas as pd
import io
import time
from pathlib import Path
from datetime import datetime as dt

DAYS_REPORT_FRESH = 3

# FBA Inventory Adjustments Report Enumeration value:
b"_GET_FBA_FULFILLMENT_INVENTORY_ADJUSTMENTS_DATA_"
# FBA Reimbursements Report Enumeration value:
b"_GET_FBA_REIMBURSEMENTS_DATA_"

# FBA Amazon Fulfilled Shipments Report Enumeration value:
b"_GET_AMAZON_FULFILLED_SHIPMENTS_DATA_"
# FBA Customer Shipment Sales Report Enumeration value:
b"_GET_FBA_FULFILLMENT_CUSTOMER_SHIPMENT_SALES_DATA_"

# FBA Inventory Event Detail Report (huge) Enumeration value:
b"_GET_FBA_FULFILLMENT_INVENTORY_SUMMARY_DATA_"

# FBA_RECONCILIATION_REPORT (undocumented)
b"_GET_FBA_RECONCILIATION_REPORT_DATA_"
# DATE_RANGE_FINANCIAL_TRANSACTION  (undocumented)
b"_GET_DATE_RANGE_FINANCIAL_TRANSACTION_DATA_"


def subj_analize(subj):
    pass


def is_report_created(report_type):
    ...


def is_report_done(report_type):
    pass


def reports(task_type):
    pass

''' Quality:
MWS Seller Id: AKARFMLPQ2SZG
MWS Auth Token: amzn.mws.9cb93871-ed24-0387-2df7-daffa8edd8fd
'''

access_key = 'AKIAJPLD4BKK4HDRBAXA'
secret_key = '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'
token = "amzn.mws.a6b63958-a11b-05f9-76c3-20d0788d69c2"
seller_id = "A1Q792NB1UKZ7V"
x = mws.Reports(access_key=access_key,
                secret_key=secret_key,
                account_id=seller_id,
                auth_token=token)

z = x.get_report_list(types=b"_GET_FBA_RECONCILIATION_REPORT_DATA_")
z_text = z.response.text  #
'''
<ReportId>20762715767018401</ReportId>
<AvailableDate>2020-05-19T14:44:47+00:00</AvailableDate> -> max
'''
print(x.get_report("20762715767018401").response.text)

dt_now = dt.datetime.now()
dt_now_minus_3 = bytes((dt_now - dt.timedelta(days=3)).isoformat()[:19], "utf-8")
dt_now_plus_1 = bytes((dt_now + dt.timedelta(days=1)).isoformat()[:19], "utf-8")
# from get_report_request_list pick up all reports with date >= today()- 3
x_list = x.get_report_request_list(fromdate=dt_now_minus_3,
                                   todate=dt_now_plus_1,
                                   max_count=b"100",
                                   types=(b"_GET_FBA_RECONCILIATION_REPORT_DATA_",),
                                   )

 # from get_report_list // get_report pick latest report wich has 18 monthes period
 # get_report reimb;  fix the date range
 # For the same period  get_report rec // get_report adj
 # save 3 files as csv
