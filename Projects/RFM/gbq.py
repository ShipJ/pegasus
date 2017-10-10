import os
import pandas as pd
import pandas_gbq as gbq
from datetime import datetime, date, timedelta
pd.set_option('display.width', 320)


def bq_getdata(project, query):
    if not os.path.isfile('./bigquery_credentials.dat'):
        print 'This is the first time you have connected to Big Query - you will' \
              ' need to follow the link below to get an authorisation token.\n'
    return gbq.read_gbq(query=query, project_id=project)


def bq_query(day_id):
    """
    Produces the query that will grab data for the correct day.
    :param day_id: Str - An ID that identifies each table in BQ, corresponding to a specific date
    :return: Str - Query that can retrieve appropriate data from the correct table
    """
    query = """ SELECT WhamId, COUNT(DISTINCT(visitId)) AS Sessions, MAX(Datetime) AS MostRecent, MIN(Datetime) As LeastRecent, SUM(PageViews) AS PageViews, SUM(SecsOnSite) AS SecsOnSite
                FROM
                  (SELECT WhamId, visitId, MIN(Datetime) AS Datetime, MIN(PageViews) AS PageViews, Min(SecsOnSite) AS SecsOnSite
                  FROM
                    (SELECT MAX(IF(hits.customDimensions.index=1, hits.customDimensions.value, NULL)) WITHIN hits AS WhamId,
                        visitId, SEC_TO_TIMESTAMP(visitstarttime) AS Datetime, Totals.PageViews AS PageViews, Totals.TimeOnSite AS SecsOnSite
                    FROM [hypnotic-bounty-755:89731917.ga_sessions_%s]) Agg1
                  WHERE WhamId IS NOT NULL AND SecsOnSite IS NOT NULL
                  GROUP BY WhamId, visitId) Agg2
                GROUP BY WhamId
                """ % day_id
    return query


def get_dates(n):
    """
    From the number of days specified in 'get_days', produce all dates in that range (number of days preceding today)
    in the format 'yyyyddmm'
    :param n: Int - Number of days before today, over which to analyse the data.
    :return: [Str] - Each date in the date range specified
    """
    list_of_dates = []
    today = datetime.today()
    if n == 'all':
        n = abs(today.date() - date(2014, 8, 10)).days - 1
    for i in range(1, n + 1):
        list_of_dates.append(datetime.strftime(today - timedelta(days=i), '%Y%m%d'))
    return list_of_dates


def get_days():
    """
    Asks user for Time period over which to analyse data.
    :return: Int - Number of days corresponding to 3 months, 1 year etc. 
    """
    print 'Over how long should the data be analysed?\n\nYesterday: [1]\nLast Week: [2]\nLast Month: [3]\n' \
          'Last Quarter: [4]\nLast 6 Months: [5]\nLast Year: [6]\nAll Time: [7]\n'
    source = raw_input()
    dayMap = {'1': 1, '2': 7, '3': 28, '4': 84, '5': 168, '6': 336, '7': 'all'}
    if source in ['1', '2', '3', '4', '5', '6', '7']:
        return dayMap[source]
    else:
        print 'Time Period not recognised, please select a valid option... \n'
        return get_days()
