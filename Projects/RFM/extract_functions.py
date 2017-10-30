import os
import pandas as pd
import numpy as np
import pandas_gbq as gbq
from Config.structure import get_df
from datetime import datetime, date, timedelta
pd.set_option('display.width', 500)


def open_click_rate(df):
    df = df.dropna(axis=1, how='all')
    delivered = df[df['ActivityType'] == 'Email Delivered'][['ContactId', 'NumRecords']].reset_index(drop=True)
    delivered.columns=['ContactId', 'Delivered']
    opened = df[df['ActivityType'] == 'Open Email'][['ContactId', 'NumRecords']].reset_index(drop=True)
    opened.columns = ['ContactId', 'Opened']
    clicked = df[df['ActivityType'] == 'Click Email'][['ContactId', 'NumRecords']].reset_index(drop=True)
    clicked.columns = ['ContactId', 'Clicked']
    unsub = df[df['ActivityType'] == 'Unsubscribe Email'][['ContactId', 'NumRecords']].reset_index(drop=True)
    unsub.columns = ['ContactId', 'Unsubscribe']
    df = delivered.merge(opened, on='ContactId', how='outer').merge(clicked, on='ContactId', how='outer').merge(unsub, on='ContactId', how='outer')
    df['Opened'] = df['Opened'].fillna(0)
    df['Clicked'] = df['Clicked'].fillna(0)
    df['Unsubscribe'] = df['Unsubscribe'].fillna(0)
    df['OpenRate'] = np.divide(df['Opened'], df['Delivered'])
    df['ClickRate'] = np.divide(df['Clicked'], df['Delivered'])
    df['UnsubscribeRate'] = np.divide(df['Unsubscribe'], df['Delivered'])
    return df



def aggregate(project, range):
    """
    For each day in the date range inputted above, request big query for the data and concat all results
    :param project: Str - Name of the project within BQ
    :param date_range: List of dates in yyyyddmm format, required for identifying each BQ table
    :return: 
    """
    results = []
    for date in range:
        query = bq_query(date)
        result = bq_getdata(project, query)
        results.append(result)
    results = pd.DataFrame(pd.concat(results))
    results.to_csv('/Users/JackShipway/Desktop/3months.csv', index=None)
    results = pd.DataFrame(results.groupby(['WhamAccountId', 'WhamContactId']).agg({'Sessions': np.sum,
                                                                                    'Latest': np.max,
                                                                                    'Earliest': np.min,
                                                                                    'Bounces': np.sum,
                                                                                    'PageViews': np.sum,
                                                                                    'SecsOnSite': np.sum})).reset_index()
    return results


def rf_compute(df):
    """
    Compute the recency and frequency of interaction of users from the BQ data
    :param df: DF - Pandas dataframe containing user level activity
    :return: DF - aggregated DF, summarising values for each user
    """
    most_recent, least_recent = max(df['Latest']), min(df['Earliest'])
    recent_diff = (most_recent - least_recent) / 5
    df['Recency'] = (df['Latest'] - least_recent) / recent_diff
    df['Recency'] = df['Recency'].round().astype(int)
    most_frequent, least_frequent = max(df['Sessions']), min(df['Sessions'])
    frequent_diff = (most_frequent - least_frequent) / 5
    df['Frequency'] = (df['Sessions'] - least_frequent) / frequent_diff
    df['Frequency'] = df['Frequency'].round().astype(int)
    df['GARFScore'] = df['Recency'] + df['Frequency']
    return pd.DataFrame(df)


def bigquery():
    my_str = 'Obtaining Big Query Data...'
    print '\n%s\n%s\n' % (my_str, '-' * len(my_str))
    # BQ Project Name
    project = 'hypnotic-bounty-755'
    # How many days over which to compute RFM
    days = get_days()
    # Specific dates of each day (for BQ table names)
    dates = get_dates(days)
    # Aggregate results over given date range
    agg = aggregate(project, dates)
    return agg


def get_local(path, data):
    my_str = 'Obtaining %s Data' % str(data).title()
    print '\n%s\n%s\n' % (my_str, '-' * len(my_str))
    df = get_df(path + '/Raw/%s' % str(data).lower())
    return df


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
    query = """ 
                SELECT *
                FROM
                    (SELECT WhamAccountId, WhamContactId, Date,
                           COUNT(DISTINCT(visitId)) AS Sessions,
                           MAX(Datetime) AS Latest,
                           MIN(Datetime) As Earliest,
                           SUM(Bounces) AS Bounces,
                           SUM(PageViews) AS PageViews,
                           SUM(SecsOnSite) AS SecsOnSite
                    FROM
                           (SELECT WhamAccountId, WhamContactId, Date, visitId,
                                MIN(Datetime) AS Datetime,
                                MIN(Bounces) AS Bounces,
                                MIN(PageViews) AS PageViews,
                                MIN(SecsOnSite) AS SecsOnSite
                        FROM
                              (SELECT MAX(IF(hits.customDimensions.index=2, hits.customDimensions.value, NULL)) WITHIN hits AS WhamAccountId,
                                      MAX(IF(hits.customDimensions.index=1, hits.customDimensions.value, NULL)) WITHIN hits AS WhamContactId,
                                      Date,
                                      visitId,
                                      SEC_TO_TIMESTAMP(visitstarttime) AS Datetime,
                                      Totals.Bounces AS Bounces,
                                      Totals.PageViews AS PageViews,
                                      Totals.TimeOnSite AS SecsOnSite
                        FROM [hypnotic-bounty-755:89731917.ga_sessions_%s]) Agg1
                        WHERE WhamAccountId NOT IN ('null', '0', '', 'N/A', '-1') AND WhamContactId NOT IN ('null', '0', '', 'N/A', '-1')
                      GROUP BY WhamAccountId, WhamContactId, Date, visitId) Agg2
                    GROUP BY WhamAccountId, WhamContactId, Date) A
            # INNER JOIN
            #     (SELECT WhamContactId, Date, COUNT(*) AS Logins
            #     FROM
            #       (SELECT WhamContactId, visitId, Date, MIN(Login) AS Login, MIN(Success) AS Success
            #        FROM
            #         (SELECT MAX(IF(hits.customDimensions.index=1, hits.customDimensions.value, NULL)) WITHIN hits AS WhamContactId,
            #                 Date,
            #                 visitId,
            #                 hits.page.pagePathLevel2 AS Login,
            #                 hits.page.pagePathLevel3 AS Success
            #         FROM [hypnotic-bounty-755:89731917.ga_sessions_20171010]) Agg1
            #         WHERE WhamContactId NOT IN ('null', '0', '', 'N/A') AND Login = '/login/' AND Success = '/success'
            #       GROUP BY WhamContactId, visitId, Date) Agg2
            #     GROUP BY WhamContactId, Date) B 
            # ON A.WhamContactId = B.WhamContactId AND A.Date = B.Date
                
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


def days_between(d1, d2):
    d1 = date(int(d1[:4]), int(d1[4:6]), int(d1[6:]))
    d2 = date(int(d2[:4]), int(d2[4:6]), int(d2[6:]))
    return abs((d2 - d1).days)


def get_days():
    """
    Asks user for Time period over which to analyse data.
    :return: Int - Number of days corresponding to 3 months, 1 year etc. 
    """
    print 'Over how long should the data be analysed?\n\n' \
          'Yesterday: [1]\nLast Week: [2]\nLast Month: [3]\nLast Quarter: [4]\nLast 6 Months: [5]\nLast Year: [6]' \
          '\nAll Time: [7]\nSelect Dates: [8]\n'
    source = raw_input()
    dayMap = {'1': 1, '2': 7, '3': 28, '4': 84, '5': 168, '6': 336, '7': 'all'}
    if source in ['1', '2', '3', '4', '5', '6', '7']:
        return dayMap[source]
    if source == '8':
        print 'Please enter the first date:\n(YYYYMMDD):\n'
        d1 = raw_input()
        print 'Please enter the second date:\n(YYYYMMDD):\n'
        d2 = raw_input()
        datediff = days_between(d1, d2)
        return datediff
    else:
        print 'Time Period not recognised, please select a valid option... \n'
        return get_days()
