import pandas as pd
import Projects.RFM.gbq as gbq


def aggregate(project, date_range):
    """
    For each day in the date range inputted above, request big query for the data and concat all results
    :param project: Str - Name of the project within BQ
    :param date_range: List of dates in yyyyddmm format, required for identifying each BQ table
    :return: 
    """
    results = []
    for date in date_range:
        query = gbq.bq_query(date)
        result = gbq.bq_getdata(project, query)
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