"""
    Api to api.

   формат данных:
       <any_text> ::>  <client_name> :: <report_name>   <:: <any_text>
       any_text := [0-1a-zа-я_-!"Ё№;%:?*()_+/"]
       client_name := [0-1a-zа-я_- ]
       report_name := snapshot | FBA_fee |  returns

                                TR  NEXT(secs) IN HOUR
    get_report_request_list     10  45          80     60*60 / (80 - 10) = 51.4
    get_report_list             10  60          60     60*60 / (60 - 10) = 72
    get_report                  15  60          60     60*60 / (60 - 15) = 80
    request_report              15  60          60

    GetReportRequestListByNextToken 30 2 1800



trottling response:

<?xml version="1.0"?>
<ErrorResponse xmlns="http://mws.amazonaws.com/doc/2009-01-01/">
  <Error>
    <Type>  </Type>
    <Code>RequestThrottled</Code>
    <Message>Request is throttled</Message>
  </Error>
  <RequestID>3de465dc-bef2-4820-a206-cdda99fd05b9</RequestID>
</ErrorResponse>


<?xml version="1.0"?>
<ErrorResponse xmlns="http://mws.amazonaws.com/doc/2009-01-01/">
  <Error>
    <Type>Sender</Type>
    <Code>RequestThrottled</Code>
    <Message>Report creation request for merchant A265CUS4KA8OEM is metered due to exceeding limit of 17 per 5 minutes. There have been 17 requests of this type in that timeframe.</Message>
  </Error>
  <RequestId>b6b53a5c-22bb-4762-a595-b35a2fa82528</RequestId>
</ErrorResponse>


MWSError: <?xml version="1.0"?>
<ErrorResponse xmlns="http://mws.amazonaws.com/doc/2009-01-01/">
  <Error>
    <Type>Sender</Type>
    <Code>AccessDenied</Code>
    <Message>Access to Reports.GetReportRequestList is denied</Message>
  </Error>
  <RequestID>7b170ab1-675b-4b10-85ef-940fd22ac78b</RequestID>
</ErrorResponse>
"""
from models import log    # for logging


from lxml import objectify

from mws import mws

import pandas as pd

import io
import time

from datetime import timedelta

# from pathlib import Path

from models import (
    ReportsData, Report_dates,
    delta_days, datetime_now_iso_str, now_plus_datetime_iso_str)

# from sys import getsizeof
from pympler import asizeof     # asizeof.asizeof({})

CODING = "cp1252"

DAY = timedelta(days=1)

TROTTLING_MESSAGE = '<Code>RequestThrottled</Code>'


def try_or_sleep(func, sleep_secs=60):
    """
    Обертка на метод-запрос для учета троттлинга.

    Parameters
    ----------
    func : метод-запрос к Amazon.
    sleep_secs : количество секунд до возможности обработки сервером нового запрос


    Returns
    -------
    объект mws.request
        структурированный ответ сервера Amazon.

    """
    print('> декоратор с аргументами...')

    def decorated(*args, **kwargs):
        print('до вызова функции', func.__name__)
        infinity = True
        while infinity:
            try:
                request = func(*args, **kwargs)
                break
            except Exception as e:
                if TROTTLING_MESSAGE in str(e):  # e.args[0]
                    print(f"{40*'='} awaiting {sleep_secs} secs")
                    time.sleep(sleep_secs + 1)
                else:
                    assert False, f"Unknown error: \n {e}"

        print('после вызова функции', func.__name__)
        return request
    return decorated


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

# FBA Reconciliation REPORT (undocumented)
b"_GET_FBA_RECONCILIATION_REPORT_DATA_"
# DATE_RANGE_FINANCIAL_TRANSACTION  (undocumented)
b"_GET_DATE_RANGE_FINANCIAL_TRANSACTION_DATA_"
# fee_preview  (undocumented)
b"_GET_FBA_ESTIMATED_FBA_FEES_TXT_DATA_"
'''
snapshots:
    start date == start date handmade genereted recon.
    end date == today - 1
'''


def amazon_connect(client,
                   access_key,
                   secret_key,
                   region="US",
                   ):
    """Return client connection to Amazon."""
    if len(client.auth_token) > 9 and client.auth_token[:9] == "amzn.mws.":
        x = mws.Reports(access_key=access_key,
                        secret_key=secret_key,
                        account_id=client.seller_id,
                        auth_token=client.auth_token,
                        region=region,
                        )
    else:
        x = mws.Reports(access_key=client.access_key,
                        secret_key=client.secret_key,
                        account_id=client.seller_id,
                        # auth_token=client.auth_token,
                        region=region,
                        )
    return x


def get_report_ids(x,
                   reportstart, reportend,
                   any_diapason=False,  # ???
                   reports_types=(b"_GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2_",),
                   fromdate=None, todate=None,
                   only_DONE_=True,
                   ):
    """
    Get report ids.

    Parameters
    ----------
    x : TYPE
        DESCRIPTION.
    reportstart : TYPE
        DESCRIPTION.
    reportend : TYPE
        DESCRIPTION.
    reports_types : TYPE, optional
        DESCRIPTION. The default is (b"_GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2_",).
    fromdate : TYPE, optional
        DESCRIPTION. The default is None.
    todate : TYPE, optional
        DESCRIPTION. The default is None.

    Returns
    -------
    reports : TYPE
        DESCRIPTION.
    """
    # response.text example
    '''
    <?xml version="1.0"?>
<GetReportRequestListResponse xmlns="http://mws.amazonaws.com/doc/2009-01-01/">
  <GetReportRequestListResult>
    <HasNext>false</HasNext>
    <ReportRequestInfo>
      <ReportType>_GET_FBA_RECONCILIATION_REPORT_DATA_</ReportType>
      <ReportProcessingStatus>_DONE_</ReportProcessingStatus>
      <EndDate>2020-08-01T06:59:59+00:00</EndDate>
      <Scheduled>false</Scheduled>
      <ReportRequestId>164088018480</ReportRequestId>
      <StartedProcessingDate>2020-08-06T19:05:03+00:00</StartedProcessingDate>
      <SubmittedDate>2020-08-06T19:04:52+00:00</SubmittedDate>
      <StartDate>2020-02-01T08:00:00+00:00</StartDate>
      <CompletedDate>2020-08-06T19:05:10+00:00</CompletedDate>
      <GeneratedReportId>22294256065018480</GeneratedReportId>
    </ReportRequestInfo>
    <ReportRequestInfo>
      <ReportType>_GET_FBA_RECONCILIATION_REPORT_DATA_</ReportType>
      <ReportProcessingStatus>_CANCELLED_</ReportProcessingStatus>
      <EndDate>2020-08-01T06:59:59+00:00</EndDate>
      <Scheduled>false</Scheduled>
      <ReportRequestId>164068018480</ReportRequestId>
      <StartedProcessingDate>2020-08-06T18:20:49+00:00</StartedProcessingDate>
      <SubmittedDate>2020-08-06T18:20:44+00:00</SubmittedDate>
      <StartDate>2019-01-01T08:00:00+00:00</StartDate>
      <CompletedDate>2020-08-06T18:20:55+00:00</CompletedDate>
    </ReportRequestInfo>
  </GetReportRequestListResult>
  <ResponseMetadata>
    <RequestId>0938b87c-0f89-4e79-9cdb-d750395cbb35</RequestId>
  </ResponseMetadata>
</GetReportRequestListResponse>

ReportProcessingStatus in GetReportRequestList()
    _SUBMITTED_,
    _IN_PROGRESS_,
    _CANCELLED_,
    _DONE_,
    _DONE_NO_DATA_

    '''
    # TODO: _DONE_NO_DATA_
    reports = []
    # в цикле выводим всю информацию про элементы (тэги и текст).
    is_next = True
    next_token = None
    while is_next:

        x_list = try_or_sleep(x.get_report_request_list)(
            fromdate=fromdate, todate=todate,
            max_count=b"100",
            types=reports_types,
            processingstatuses=((b"_DONE_", ) if only_DONE_ else ()),
            next_token=next_token
            )

        # trottling count is:  x_list.response.headers['x-mws-quota-remaining']
        x_xml = x_list.response.text
        print(x_xml)
        root = objectify.fromstring(x_xml)
        # "request_result" - for shorter code writing
        request_result = (root.GetReportRequestListByNextTokenResult if next_token
                          else root.GetReportRequestListResult)

        try:     # check  "Nothing found"
            for el in request_result.ReportRequestInfo:
                report = Report_dates(
                    SubmittedDate=el.SubmittedDate.text,
                    StartDate=el.StartDate.text,  # [:10]
                    EndDate=el.EndDate.text,      # [:10]
                    ReportProcessingStatus=el.ReportProcessingStatus.text
                    )

                filter_on_dates = ((reportstart[:10] <= report.StartDate[:10] <= reportend[:10])
                                   and
                                   (reportstart[:10] <= report.EndDate[:10] <= reportend[:10])
                                   )
                try:
                    if any_diapason or filter_on_dates:
                        report.amazon_id = el.GeneratedReportId.text
                        report.CompletedDate = el.CompletedDate.text
                        reports.append(report)
                except AttributeError:
                    continue
        except AttributeError:
            assert False, "Nothing found"
            # TODO: branch to send mail?
            # return reports    # empty ...

        next_token = None if not request_result.HasNext else bytes(request_result.NextToken.text,
                                                                   CODING)
        is_next = bool(next_token)

    return reports


def get_handmade_report_params(c, task_type_id):
    '''

    Parameters
    ----------
    task_type_id : task type id in DB.

    Returns
    -------
    reportstart: byte date - (in ISO format) begining to collect data for report
    reportend: byte - end date (in ISO format) to collect data for report.
    report_amz_name: str - Amazon report name

    '''
    handmade_report_params = c.execute(
        '''SELECT * FROM tasks_reports WHERE is_handmade and (task_type_id=?) ''',
        (task_type_id,)).fetchall()

    if len(handmade_report_params) > 1:
        assert False, "Больше одного 'handmade' отчета"

    params_row = handmade_report_params[0]

    pattern_id = params_row["id"]
    reportstart = now_plus_datetime_iso_str(days=-params_row["minus_days_from"])
    reportend = now_plus_datetime_iso_str(days=-params_row["minus_days_to"])
    report_amz_name = params_row["report_amz_name"]
    usual_name = params_row["usual_name"]
    min_duration = params_row["min_duration"]
    files_to_get = params_row["files_to_get"]

    return ReportsData(pattern_id,
                       reportstart, reportend,
                       min_duration,
                       report_amz_name, usual_name,
                       files_to_get)


def check_snapshots_handmade_report(handmade_reports, hm_report_data):
    """
    Validate snapshots handmade reports for minimal duration report period.

    if returned value is None - so fitting report was not found.
    """
    z = [r for r in handmade_reports
         if delta_days(r.EndDate, r.StartDate) >= DAY * hm_report_data.min_duration
         ]
    z.sort(key=lambda x: x.SubmittedDate)
    return z[-1] if z else None


def check_chains_count(reports_dates, r_start, DAYS_REPORT_FRESH):
    """

    Parameters
    ----------
    reports_dates : list[Report_dates]
        список данных об анализируемых отчетах.
    r_start : str
        строка, содержащая датув формате ISO. От этой даты строится цепочка отчетов.

    Returns
    -------
    chains_count : int
        количество выявленных цепочек отчетов

    """
    # проверка связности цепочки периодов
    reports_dates.sort(key=lambda report: report.StartDate+report.CompletedDate, reverse=True)
    __ = reports_dates[:]
    chains_count = 0
    for i, report in enumerate(__):
        if report:
            work_srart_date = report.StartDate
            work_i = i
        for irep in range(i+1, len(__)):
            if (__[irep]) and work_srart_date == __[irep].EndDate:
                __[work_i] = False
                work_i, work_srart_date = irep, __[irep].StartDate

                if delta_days(work_srart_date, r_start) <= DAYS_REPORT_FRESH:
                    chains_count += 1
    return chains_count


@log
def auto_reports(c, task, report_dates):
    # TODO: now only one report each type!!
    # it only for - SNAPSHOTS - not date range!
    # TODO:  ceck parameter __ALL__ / __ONE__
    """
    Auto report parameters collectinh.

    Parameters
    ----------
    task_id : Sheduled in DB task data, containing id.  ID of concrete task

    Returns
    -------
    reportstart: byte date - (in ISO format) begining to collect data for report
    reportend: byte - end date (in ISO format) to collect data for report.
    report_amz_name: str - Amazon report name

    """
    auto_sheduled = c.execute(
        '''SELECT * FROM reports_sheduled INNER JOIN tasks_reports
            ON reports_sheduled.report_id = tasks_reports.id
            WHERE NOT is_handmade and (task_id=?) ''',
        (task.sheduled_task_id,)).fetchall()

    rez = []
    for joint_params in auto_sheduled:
        report_id = joint_params["id"]       # real report ID!! not report type ID here!
        # reportstart = report_dates.StartDate    # EQ tosnapshots dates !!
        reportstart = now_plus_datetime_iso_str(days=-365*2)  # TODO: to DB !!! ???
        reportend = datetime_now_iso_str()         # ... filled for snapshots  # TODO: to DB ???
        report_amz_name = joint_params["report_amz_name"]  # 3
        usual_name = joint_params["usual_name"]       # 4
        min_duration = joint_params["min_duration"]     # 7 in tasks_reports
        files_to_get = joint_params["files_to_get"]

        rez.append(ReportsData(report_id,
                               reportstart, reportend,
                               min_duration,
                               report_amz_name, usual_name,
                               files_to_get)
                   )

    return rez


def report_request_nd_report_id(x, auto_report):
    """
    Request report and get requestet report Amazon id.

    Parameters
    ----------
    x :                -  AMazon API connection
    auto_report : TYPE

    Returns
    -------
    report_id:str

    """
    # check if exist exact start-end report current type
    b_amz_report_name = bytes(auto_report.report_amz_name, encoding=CODING)
    reportstart = auto_report.reportstart
    reportend = auto_report.reportend

    reports = get_report_ids(x,
                             reportstart, reportend,
                             reports_types=(b_amz_report_name, ),
                             fromdate=None, todate=None)
    # TODO: fromdate=?, todate=? save the calls

    ids = [report for report in reports
           if (report.StartDate[:10] <= reportstart[:10]) and
           (report.EndDate[:10] >= reportend[:10])]  # for Snapshots.
    # [:10] - для избегания 4-х часового троттлинга. Сравнивая дата+время, мы можем попасть
    # под тротлинг автоматических отчетов. при сравнении периодов учитывается только день.
    # Время отбрасывается Amazon-ом при провенрке на троттлинг.

    if ids:
        auto_report.reportstart, auto_report.reportend = ids[0].StartDate, ids[0].EndDate
        return ids[0].amazon_id
    #
    #  This branch - when exact the same report not exists becouse of 30 minuts & 4 hours  pause
    #
    #  http://docs.developer.amazonservices.com/en_US/fba_guide/FBAGuide_DoNotRequestFBAReports.html
    #  request.response.headers['x-mws-quota-remaining']  - rest to trottling quata
    #
    b_reportstart = bytes(reportstart, encoding=CODING)
    b_reportend = bytes(reportend, encoding=CODING)
    request = try_or_sleep(x.request_report)(b_amz_report_name,
                                             start_date=b_reportstart,
                                             end_date=b_reportend,)

    root = objectify.fromstring(request.response.text)
    request_id = root.RequestReportResult.ReportRequestInfo.ReportRequestId.text
    max_exceptions, except_delay = 10, 60       # can weight 10 times 60 secs  TODO:
    time.sleep(except_delay*3)    # weight a bit for Amazon quering to save 1 call before trottling
    for _ in range(max_exceptions):
        report_data = try_or_sleep(x.get_report_list)(
            requestids=(bytes(request_id, encoding=CODING),)
            )
        root = objectify.fromstring(report_data.response.text)
        try:
            result = root.GetReportListResult.ReportInfo
            amazon_id = result.ReportId.text
            break
        except AttributeError:
            time.sleep(except_delay)
    else:
        assert False, f"{max_exceptions} times was {str(AttributeError)}"  # TODO: send answer ???

    return amazon_id


def get_amz_report_name(c, report):
    """Get standart Amazon report name."""
    return c.execute('SELECT report_amz_name FROM tasks_reports WHERE id=?', (report.report_id, )
                     ).fetchone()[0]


def get_current_status(c, x, report):
    """Get report current status on Amazon."""      # TODO: save to mongoDB
    amz_report_name = get_amz_report_name(c, report)

    matching_reports = get_report_ids(x,
                                      report.date_from, report.date_to,
                                      reports_types=(bytes(amz_report_name, CODING), ),
                                      only_DONE_=False,
                                      )

    statuses = [a.ReportProcessingStatus for a in matching_reports
                if a.amazon_id == report.amzn_rprt_id]      # TODO: put it in 'get_report_ids()'?
    status = statuses[0]
    report.status = status
    return status


def text_to_df(text, encoding, region="USA", sep="\t"):
    '''
    df_encoding? latin1
    '''
    sep_tab, sep_coma = "\t", ","
    sep = sep_tab if text[:255].count(sep_tab) > text[:255].count(sep_coma) else sep_coma
    return pd.read_csv(io.StringIO(text), sep=sep, encoding=encoding)


def get_report_file(x, amazon_report_id):
    """Get report and convert it to DF."""
    report = try_or_sleep(x.get_report)(report_id=bytes(str(amazon_report_id), CODING))
    text = report.response.text
    return text_to_df(text, report.response.encoding)


if __name__ == "__main__":
    pass
