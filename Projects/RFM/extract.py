import sys
import numpy as np
import pandas as pd
from Config.structure import get_path
from Projects.RFM.extract_functions import bigquery, rf_compute


def main():
    path = get_path()

    bq = bigquery()
    bq = rf_compute(bq)


    bq.to_csv('/Users/JackShipway/Desktop/Joined/gabq', index=None)
















if __name__ == '__main__':
    main()
