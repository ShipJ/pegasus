import sys
import Projects.RFM.gbq as gbq
import Projects.RFM.analyse as an
from Config.structure import get_path
from Config.security import get_pwd



def main():

    # BQ Project Name
    project = 'hypnotic-bounty-755'
    # How many days over which to compute RFM
    days = gbq.get_days()
    # Specific dates of each day (for BQ table name)
    dates = gbq.get_dates(days)
    # Compute model over given range
    results = an.aggregate_rfm(project, dates)
    # compute recency, frequency etc

    path = get_path()



if __name__ == '__main__':
    main()