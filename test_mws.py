# -*- coding: utf-8 -*-
from lxml import objectify  # etree,
from sys import getsizeof
from mws import mws
import pandas as pd
import io
import time
from pathlib import Path

from dataclasses import dataclass, field
import typing

import sqlite3

#  docs:
'''

#    report.response.iter_lines() - итератор по строка. всё в bytes

    report.response.headers
    -----------------------
    {'Server': 'Server',
     'Date': 'Fri, 08 May 2020 18:56:35 GMT',
     'Content-Type': 'text/plain;charset=Cp1252',
     'Content-Length': '6940',
     'Connection': 'keep-alive',
     'x-mws-quota-max': '60.0',
     'x-mws-quota-remaining': '59.0',
     'x-mws-quota-resetsOn': '2020-05-08T19:37:00.000Z',
     'Content-MD5': 'qadvYaCYDbq5pwoyFKnbeQ==',
     'x-mws-response-context': 'jqjCalH+tVNhMB1e3xjTOQRnpks5IIrza6anArhk8lf3H4mkv+5y4lOXxkWnH3DrmQXOz7EkizQ=, +p7ZNbqRK/mVMIxP9YX1wtAKoaip8svhpuP8lj2tE2imLasRmkXgbIC3Dhx87y6fBpTkpzk40fE=',
     'x-amz-request-id': '4f2b0a92-05ac-4b82-916c-081b9d4cdf6b',
     'x-mws-request-id': '4f2b0a92-05ac-4b82-916c-081b9d4cdf6b',
     'x-mws-timestamp': '2020-05-08T18:56:35.318Z',
     'x-amz-rid': 'EDMZMX9A8KFZVD9AA971',
     'Vary': 'Accept-Encoding,X-Amzn-CDN-Cache,X-Amzn-AX-Treatment,User-Agent'}

    report.response.status_code
    ---------------------------
    200
    400

    report.response.text
    --------------------
    ==?==  str(report.original, report.response.encoding)





<?xml version="1.0"?>
<GetReportRequestListResponse xmlns="http://mws.amazonaws.com/doc/2009-01-01/">
  <GetReportRequestListResult>
    <HasNext>false</HasNext>
    <ReportRequestInfo>
      <ReportType>_GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2_</ReportType>
      <ReportProcessingStatus>_DONE_</ReportProcessingStatus>
      <EndDate>2020-03-31T19:40:45+00:00</EndDate>
      <Scheduled>false</Scheduled>
      <ReportRequestId>574066018352</ReportRequestId>
      <StartedProcessingDate>2020-03-31T19:58:40+00:00</StartedProcessingDate>
      <SubmittedDate>2020-03-31T19:58:40+00:00</SubmittedDate>
      <StartDate>2020-03-24T19:40:45+00:00</StartDate>
      <CompletedDate>2020-03-31T19:58:40+00:00</CompletedDate>
      <GeneratedReportId>19888811060018352</GeneratedReportId>
    </ReportRequestInfo>
    <ReportRequestInfo>
      <ReportType>_GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2_</ReportType>
      <ReportProcessingStatus>_DONE_</ReportProcessingStatus>
      <EndDate>2020-03-30T16:44:20+00:00</EndDate>
      <Scheduled>false</Scheduled>
      <ReportRequestId>573552018351</ReportRequestId>
      <StartedProcessingDate>2020-03-30T17:04:21+00:00</StartedProcessingDate>
      <SubmittedDate>2020-03-30T17:04:21+00:00</SubmittedDate>
      <StartDate>2020-03-23T16:44:20+00:00</StartDate>
      <CompletedDate>2020-03-30T17:04:21+00:00</CompletedDate>
      <GeneratedReportId>19869300380018351</GeneratedReportId>
    </ReportRequestInfo>
    ...
    ...
  </GetReportRequestListResult>
  <ResponseMetadata>
    <RequestId>22815add-c990-4614-8bba-f9c57da13171</RequestId>
  </ResponseMetadata>
</GetReportRequestListResponse>




ReportProcessingStatus in GetReportRequestList()
    _SUBMITTED_,
    _IN_PROGRESS_,
    _CANCELLED_,
    _DONE_,
    _DONE_NO_DATA_
'''


@dataclass
class Client:
    name: str = "YuChud"
    seller_id: str = 'A3EVIIHGE6S6KI'
    auth_token: str = None
    last_date: str = ""
    activity: int = 0


@dataclass
class Report_dates:
    id_: str = ""
    StartDate: str = ""
    EndDate: str = ""
    CompletedDate: str = ""
    ReportProcessingStatus: str = ""


@dataclass
class Client_data(Client):
    x_report: 'typing.Any' = None
    reports: typing.List[Report_dates] = field(default_factory=list)
    calls_count: int = 0
    df: 'typing.Any' = field(default_factory=pd.DataFrame)
    df_size: int = 0
    is_file_saved: bool = False
    file_name: str = ""


def sleep120(time_start, curren_report_num, time_lag=120, reports_before_trottling=15):
    ''' '''
    print(f"curren_report_num={curren_report_num}  ",
          max(0, time_lag - (time.time()-time_start)))
    if curren_report_num > reports_before_trottling:
        time.sleep(max(0, time_lag - (time.time()-time_start)))


def rename_df_columns(df_):
    '''
    the df columns inplace renaming  by replacing character "-" with "_"
    for avoid syntax problems
    '''
    dict_ = {x: x.replace("-", "_") for x in df_.columns}
    df_.rename(columns=dict_, inplace=True)


def get_clietns():
    conn = sqlite3.connect("clients.db")
    c = conn.cursor()
    return [Client_data(name=client[1],
                        seller_id=client[2],
                        auth_token=client[3],
                        activity=client[4],
                        ) for client in c.execute('SELECT * FROM auth_code')
            ]


def get_report_ids(x, reportstart, reportend,
                   reports_types=(b"_GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2_",),
                   fromdate=None, todate=None):
    reports = []
    # в цикле выводим всю информацию про элементы (тэги и текст).
    calls_count = 0
    is_next = True
    next_token = None
    while is_next:
        time_start = time.time()
        x_list = x.get_report_request_list(
            fromdate=fromdate,
            todate=todate,
            max_count=b"100",
            types=reports_types,
            next_token=next_token
            )

        x_xml = x_list.response.text
        print(x_xml)
        root = objectify.fromstring(x_xml)

        for el in root.GetReportRequestListResult.ReportRequestInfo:
            report = Report_dates(
                StartDate=el.StartDate.text,  # [:10]
                EndDate=el.EndDate.text,      # [:10]
                ReportProcessingStatus=el.ReportProcessingStatus.text
                )

            filter_ = (
                (reportstart <= report.StartDate <= reportend)
                or
                (reportstart <= report.EndDate <= reportend)
                )
            print(f"filter_={ filter_}")
            if (report.ReportProcessingStatus == "_DONE_") and filter_:
                report.id_ = el.GeneratedReportId.text
                report.CompletedDate = el.CompletedDate.text
                reports.append(report)

        next_token = None if not root.GetReportRequestListResult.HasNext else \
            root.GetReportRequestListResult.NextToken
        is_next = bool(next_token)

        sleep120(time_start, calls_count)
        calls_count += 1

    # если есть два отчета за одион период - сохранять с макс  *.CompletedDate ("свежий")
    reports.sort(key=lambda report: report.StartDate+report.CompletedDate)
    for i, r_data in enumerate(reports):
        if i > 0:
            if r_data.StartDate == reports[i-1].StartDate  \
                    and r_data.EndDate == reports[i-1].EndDate:
                reports[i-1] = False  # "помечаем на удаление"
    reports = [r for r in reports if r]   # удаляем "помеченные на удаление"

    # TODO: проверять связность цепочки периодов
    from datetime import datetime as dt
    __ = reports[:]
    for i, report in enumerate(__):
        if report:
            work_EndDate = report.EndDate
            work_i = i
            for irep in range(i+1, len(__)):
                if bool(__[irep]) and work_EndDate == __[irep].StartDate:
                    __[work_i] = False
                    work_i, work_EndDate = irep, __[irep].EndDate
    __ = [r for r in __ if r]   # удаляем "помеченные на удаление"

    return reports, calls_count


def text_to_df(text, encoding, region="USA", sep="\t"):
    '''
    '''
    columns_names = {
        "USA": ['order-id', 'transaction-type', 'amount-type',
                'amount-description', 'amount', 'posted-date',
                'sku', 'quantity-purchased', 'marketplace-name'],
        }

    return pd.read_csv(io.StringIO(text), sep=sep, encoding=encoding,
                       usecols=columns_names[region])


def df_append_df(df, df_add, region="USA"):
    '''
    '''
    descriptions = {
        "USA": ['FBACustomerReturnPerOrderFee',
                'FBACustomerReturnPerUnitFee',
                'FBACustomerReturnWeightBasedFee',
                'FBAPerOrderFulfillmentFee',
                'FBAPerUnitFulfillmentFee',
                'FBAWeightBasedFee',
                ],
        }
    return df.append(df_add[df_add['amount-description'].isin(descriptions[region])])


def prepare_df(df):
    '''
    '''
    rename_df_columns(df)
    # Propagating of sku trough NaN valiues
    df.sku = df.groupby('order_id').sku.ffill()
    df.sku = df.groupby('order_id').sku.bfill()
    # Dropping all SKUs left with NaN
    df.dropna(subset=['sku'], inplace=True)


def df_to_csv_saving(df, path_to, name):
    '''
    '''
    # Save to the file
    compression_opts = dict(method='zip', archive_name=f'{name}.csv')
    df.to_csv(f'{name}.zip', index=False, compression=compression_opts)


if __name__ == "__main__":

    access_key = 'AKIAJPLD4BKK4HDRBAXA'
    secret_key = '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'

    fromdate, todate = b"2019-12-01T00:00:00+00:00", b"2020-05-01T00:00:00+00:00"
    report_start, report_end = "2020-01-01T00:00:00+00:00", "2020-05-01T00:00:00+00:00"
# clients data filling
    '''
    token: amzn.mws.a6b63958-a11b-05f9-76c3-20d0788d69c2
    seller_id="A1Q792NB1UKZ7V",

    clients = [Client_data(name="=First=",
                           seller_id="A1Q792NB1UKZ7V",
                           auth_token="amzn.mws.a6b63958-a11b-05f9-76c3-20d0788d69c2"
                           )]
    clients = [Client_data()]
    '''
    clients = get_clietns()
# connections to server  &  reports lists forming
    for i, x in enumerate(clients):
        clients[i].x_report = mws.Reports(access_key=access_key,
                                          secret_key=secret_key,
                                          account_id=x.seller_id,
                                          auth_token=x.auth_token)

        reports, calls_count = get_report_ids(x.x_report,
                                              report_start,
                                              report_end,
                                              fromdate=fromdate,
                                              todate=todate
                                              )
        clients[i].reports, clients[i].calls_count = reports, calls_count

    max_reports = max([len(el.reports) for el in clients])
    max_calls_used = max([el.calls_count for el in clients])

    path_to = ""
    for i in range(max_reports):
        time_start = time.time()
        for k, x in enumerate(clients):  # x is readed alias for clients[k]
            if len(x.reports) > i:
                report = x.x_report.get_report(report_id=x.reports[i].id_)
                text = report.response.text
                df_add = text_to_df(text, report.response.encoding)
                df_to_csv_saving(df_add, path_to, x.name+"_"+x.reports[i].id_)
                clients[k].df = df_append_df(x.df, df_add)

                if len(x.reports) == i+1:
                    path_to = ""
                    prepare_df(x.df)
                    df_to_csv_saving(x.df, path_to, x.name)
                    x.is_file_saved = True
                    x.df_size = getsizeof(x.df)
            sleep120(time_start, i + max_calls_used, time_lag=60)

    max_size = max(([el.df_size for el in clients])) >> 20   # size in Mb
    sum_size = sum([el.df_size for el in clients]) >> 20  # size in Mb

    f_data = []
    with open("_max_.txt", "r") as f:
        for line in f:
            f_data.append(line.split())

    old_max_size, old_max_sum = int(f_data[0][1]), int(f_data[1][1])
    if old_max_size < max_size or old_max_sum < sum_size:
        f_data[0][1] = str(max(old_max_size, max_size))  # max_size
        f_data[1][1] = str(max(old_max_sum, sum_size))  # sum_size
        with open("_max_.txt", "w") as f:
            for line in f_data:
                f.write(" ".join(line)+"\n")


"""
r_type=b"_GET_FBA_FULFILLMENT_INVENTORY_ADJUSTMENTS_DATA_"
z = clients[0].x_report.request_report(r_type, start_date=b"2020-03-01T00:00:00+00:00")
# извлечь номер запроса для экономии последующей выборки ??
zzz = clients[0].x_report.get_report_request_list(types=(r_type,))
print(zzz.response.text)
"""



'''   hash для проверки целостности полученного  r.response.header
 for key,val in r.response.headers.items(): print(key,":", val)
Server : Server
Date : Fri, 17 Jul 2020 17:51:17 GMT
Content-Type : text/plain;charset=Cp1252        # ---- ! ----  coding
Content-Length : 31287
Connection : keep-alive
x-mws-quota-max : 60.0
x-mws-quota-remaining : 53.0                    # ---- ! ----  trottling
x-mws-quota-resetsOn : 2020-07-17T18:37:00.000Z
Content-MD5 : Q8pkh4Ys9a+OfdYhSkz/Sg==
x-mws-response-context : UB91thltAjCscqVpaGmZmKfejuENf400t/OBFP88uIGm34LGtJ/+BhjMyTUOrf14qI4ww1LUBn8=, MGqGq7YIvSUK8dQCsPsojktqzSO5u5461mh8rCP28JjA/4DEPjHTXCnmpARrZGV75kIvrXsQiiU=
x-amz-request-id : aabc6d23-ad3f-42d9-9dd2-72c74f6d05fc
x-mws-request-id : aabc6d23-ad3f-42d9-9dd2-72c74f6d05fc
x-mws-timestamp : 2020-07-17T17:51:17.041Z
Vary : Content-Type,Accept-Encoding,X-Amzn-CDN-Cache,X-Amzn-AX-Treatment,User-Agent
x-amz-rid : N306V13J1XQQ3CBJ3D54

'''

'''
    d2 = dt.fromisoformat('2019-12-30T16:44:20+00:00')
    d1 = dt.fromisoformat('2019-12-07T16:44:20+00:00')
    (d2-d1).days


'''