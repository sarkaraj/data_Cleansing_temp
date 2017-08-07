from datetime import date
from datetime import datetime

import numpy as np
import pandas as pd


def make_date(date_string):
    return datetime.strptime(date_string, "%Y-%W-%w")

def weeks_for_year(year):
    last_week = date(year, 12, 28)
    return last_week.isocalendar()[1]


def get_missing_weeks(input_year, existing_weeks):
    _total_weeks = weeks_for_year(input_year)
    _weeks_list = set(range(1, _total_weeks + 1))
    _existing = set(existing_weeks)
    _missing = _weeks_list - _existing
    return list(_missing)




def get_missing_data(df_grpby_year):
    final_data_df = pd.DataFrame()
    for year, grp_dataset in df_grpby_year:
        missing_data_df_per_grp = pd.DataFrame()
        grp_dataset['week_num'] = grp_dataset['isocalendar'].map(lambda x: x[1])
        # print grp_dataset
        _weeks_missed = get_missing_weeks(year, grp_dataset['week_num'])
        _weeks_missed_Series = pd.Series(_weeks_missed).map(str)
        d = pd.DataFrame(np.zeros((len(_weeks_missed), 2)), columns=['quantity', 'q_indep_p'])
        missing_data_df_per_grp = pd.concat([d, _weeks_missed_Series], axis=1)
        missing_data_df_per_grp.columns = ['quantity', 'q_indep_p', 'week_num']
        missing_data_df_per_grp['year'] = str(year)
        missing_data_df_per_grp['isocalendar'] = (missing_data_df_per_grp['year'] + "-" + missing_data_df_per_grp['week_num'] + "-" + "3").map(lambda x: make_date(x).isocalendar())

        final_data_df = pd.concat([final_data_df, missing_data_df_per_grp], axis=0)

    return final_data_df


def get_cmplt_missing_data(raw_data):
    """
    This fucntion receives raw data
    :param raw_data: grouped dataframe
    :return: yet to decide
    """
    raw_data_grp = raw_data.groupby(['customernumber', 'matnr'], as_index=False)
    final_data_df_1 = pd.DataFrame()
    for name, group in raw_data_grp:
        # print name[0]

        final_data_df_2 = pd.DataFrame()
        group['date'] = group['date'].map(str)
        group['date_parse'] = pd.to_datetime(group['date'])
        group['isocalendar'] = group['date_parse'].map(lambda x: x.isocalendar())
        group['year'] = group['date_parse'].map(lambda x: x.year)

        # group['week_num'] = group['isocalendar'].map(lambda x: x[1])
        raw_data_grp = group[['matnr', 'quantity', 'q_indep_p', 'isocalendar', 'year']].groupby(['year'], as_index=False)
        missing_data = get_missing_data(raw_data_grp)
        missing_data['matnr'] = str(name[1])
        missing_data['customernumber'] = str(name[0])
        # print missing_data
        # print "******************************************************************"
        final_data_df_2 = pd.concat([final_data_df_2, missing_data], axis=0)
        # print final_data_df_2
        # print "##################################################################"
        final_data_df_1 = pd.concat([final_data_df_1, final_data_df_2], axis=0)

    return final_data_df_1


# print weeks_for_year(2015)
# print get_missing_weeks(2015)
# print type(get_missing_weeks(2015))

raw_data = pd.read_csv("./skywaymart_90.txt", sep="\t", header=None,
                       names=['customernumber', 'matnr', 'date', 'quantity', 'q_indep_p'])

print raw_data
print get_cmplt_missing_data(raw_data)
