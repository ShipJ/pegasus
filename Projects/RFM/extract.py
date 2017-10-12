import pandas as pd
import Projects.RFM.gbq as gbq
import Projects.RFM.analyse as an
from Config.structure import get_path, file_check, save_df, get_df


def big_query():
    print 'Obtaining Big Query Data...\n\n'
    # BQ Project Name
    project = 'hypnotic-bounty-755'
    # How many days over which to compute RFM
    days = gbq.get_days()
    # Specific dates of each day (for BQ table names)
    dates = gbq.get_dates(days)
    # Compute model over given range9
    agg = an.aggregate(project, dates)
    print 'Big Query Data Obtained.'
    return agg


def salesforce(path):
    print 'Obtaining Salesforce Data...'
    sf = pd.DataFrame(pd.read_csv(path+'/Raw/salesforce_2.0.csv'))
    print '...Received Salesforce Data\n\n'
    return sf


def main():

    path = get_path()

    # bq = big_query()
    sf = salesforce(path)
    print sf

    # compute recency, frequency etc
    # rfm = an.rf_compute(agg)






if __name__ == '__main__':
    main()