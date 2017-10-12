from Config.structure import get_path
from Projects.RFM.extract_functions import big_query, salesforce


def main():

    path = get_path()

    bq = big_query()
    sf = salesforce(path)

    print bq
    print sf




if __name__ == '__main__':
    main()