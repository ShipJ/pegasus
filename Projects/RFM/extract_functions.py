import os
import pandas as pd
import pandas_gbq as gbq
from Config.structure import get_df
from datetime import datetime, date, timedelta


def aggregate(project, date_range):
    """
    For each day in the date range inputted above, request big query for the data and concat all results
    :param project: Str - Name of the project within BQ
    :param date_range: List of dates in yyyyddmm format, required for identifying each BQ table
    :return: 
    """
    results = []
    for date in date_range:
        query = bq_query(date)
        result = bq_getdata(project, query)
        results.append(result)
    results = pd.DataFrame(pd.concat(results))
    results = pd.DataFrame(results.groupby('WhamId').agg({'Sessions': sum, 'MostRecent': max, 'LeastRecent': min, 'PageViews': sum, 'SecsOnSite': sum})).reset_index()
    return results


def rf_compute(df):
    """
    Compute the recency and frequency of interaction of users from the BQ data
    :param df: DF - Pandas dataframe containing user level activity
    :return: DF - aggregated DF, summarising values for each user
    """
    most_recent, least_recent = max(df['MostRecent']), min(df['LeastRecent'])
    recent_diff = (most_recent - least_recent) / 5
    df['Recency'] = (df['MostRecent'] - least_recent) / recent_diff
    df['Recency'] = df['Recency'].round().astype(int)
    most_frequent, least_frequent = max(df['Sessions']), min(df['Sessions'])
    frequent_diff = (most_frequent - least_frequent) / 5
    df['Frequency'] = (df['Sessions'] - least_frequent) / frequent_diff
    df['Frequency'] = df['Frequency'].round().astype(int)
    df['RFScore'] = df['Recency'] + df['Frequency']
    return pd.DataFrame(df)


def big_query():
    print '\nObtaining Big Query Data...\n---------------------------\n'
    # BQ Project Name
    project = 'hypnotic-bounty-755'
    # How many days over which to compute RFM
    days = get_days()
    # Specific dates of each day (for BQ table names)
    dates = get_dates(days)
    # Aggregate results over given date range
    agg = aggregate(project, dates)
    return agg


def salesforce(path):
    print '\nObtaining Salesforce Data\n-------------------------\n'
    sf = get_df(path + '/Raw/salesforce_2.0')
    return sf


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
