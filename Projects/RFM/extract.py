from Config.structure import get_path
from Projects.RFM.extract_functions import big_query, salesforce


def summarise(df):
    print 'Number of Rows: %d' % len(df)


def main():
    path = get_path()

    bq = big_query()

    sf = salesforce(path)


if __name__ == '__main__':
    main()
