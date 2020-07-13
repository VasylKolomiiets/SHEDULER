"""
   формат данных:
       <any_text> ::>  <client_name> :: <report_name>   <:: <any_text>
       any_text := [0-1a-zа-я_-!"Ё№;%:?*()_+/"]
       client_name := [0-1a-zа-я_- ]
       report_name := snapshot | FBA_fee |  returns
"""
from lxml import objectify  # etree,
from sys import getsizeof
from mws import mws
import pandas as pd
import io
import time
from pathlib import Path

from dataclasses import dataclass, field
import typing


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
# fee_preview  (undocumented)
b"_GET_FBA_ESTIMATED_FBA_FEES_TXT_DATA_"


def is_report_created(task):
    """

    Parameters
    ----------
    task : TYPE
        DESCRIPTION.

    Returns
    -------
    None.

    """

    def fee_checker(task):

        return None

    def snapshots_checker(task):
        return None

    def adjustments_checker(task):
        return None

    cheker = {
        "FEE": fee_checker,
        "SNAPSHOTS": snapshots_checker,
        "ADJUSTMENTS": adjustments_checker,
        }

    return cheker[task]


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
