import Projects.RFM.gbq as gbq
import Projects.RFM.analyse as an


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

    print results


if __name__ == '__main__':
    main()