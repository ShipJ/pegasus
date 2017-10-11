import pandas as pd
import Projects.RFM.gbq as gbq
import Projects.RFM.analyse as an
from Config.structure import get_path, file_check, save_df, get_df


def main():

    # BQ Project Name
    project = 'hypnotic-bounty-755'
    # How many days over which to compute RFM
    days = gbq.get_days()
    # Specific dates of each day (for BQ table names)
    dates = gbq.get_dates(days)
    # Compute model over given range
    agg = an.aggregate_rfm(project, dates)
    # compute recency, frequency etc
    # rfm = an.rf_compute(agg)











    # # Grab data
    # path = get_path()
    # # Wham
    # extension = '/Raw/wham'
    # wham = get_df(path+extension)





    #
    # sys.exit()
    # # Salesforce
    # extension = '/Raw/salesforce'
    # sf = get_df(path+extension)
    #
    #
    # # # Merge the data on Wham ID, then join with salesforce.
    # # df = rfm.merge(wham)
    # # df = rfm.merge(sf)
    # #
    # #
    # # # Save results to file
    # # save_df(data, path, extension)




if __name__ == '__main__':
    main()