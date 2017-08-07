import pandas as pd
import numpy as np
from datetime import date, timedelta

a = ('a', 'b', 'c')


def weeks_for_year(year):
    last_week = date(year, 12, 28)
    return last_week.isocalendar()[1]


def get_missing_weeks(input_year, ):
    _total_weeks = weeks_for_year(input_year)
    _weeks_list = range(1, _total_weeks + 1)
    return set(_weeks_list)


def get_missing_data(df_grpby_year):
    for year, grp_dataset in df_grpby_year:
        # print year
        print grp_dataset
        grp_dataset['week_num'] = grp_dataset['isocalendar'].map(lambda x: x[1])
        week_num_grp = grp_dataset.groupby(['week_num'], as_index=False)[['quantity', 'q_indep_p']]

        _weeks_in_year = grp_dataset['isocalendar'].map(lambda x: x[1])
        print _weeks_in_year.unique()

        # return None


def transform_data(raw_data):
    """
    This fucntion receives data which has been already been grouped by customernumber and matnr
    :param raw_data: grouped dataframe
    :return: yet to decide
    """
    for matnr, group in raw_data:
        group['date'] = group['date'].map(str)
        group['date_parse'] = pd.to_datetime(group['date'])
        group['isocalendar'] = group['date_parse'].map(lambda x: x.isocalendar())
        group['year'] = group['date_parse'].map(lambda x: x.year)
        # group['week_num'] = group['isocalendar'].map(lambda x: x[1])
        raw_data_grp = group.groupby(['year'], as_index=False)
        get_missing_data(raw_data_grp)


# print weeks_for_year(2015)
# print get_missing_weeks(2015)
# print type(get_missing_weeks(2015))

raw_data = pd.read_csv("./skywaymart_90.txt", sep="\t", header=None,
                       names=['customernumber', 'matnr', 'date', 'quantity', 'q_indep_p'])[
    ['matnr', 'date', 'quantity', 'q_indep_p']]

raw_data_grp = raw_data.groupby(['matnr'], as_index=False)

transform_data(raw_data_grp)
