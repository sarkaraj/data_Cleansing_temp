from datetime import date
from datetime import datetime

import numpy as np
import pandas as pd

import datetime


def iso_year_start(iso_year):
    "The gregorian calendar date of the first day of the given ISO year"
    fourth_jan = datetime.date(iso_year, 1, 4)
    delta = datetime.timedelta(fourth_jan.isoweekday() - 1)
    return fourth_jan - delta


def iso_to_gregorian(iso_year, iso_week, iso_day):
    "Gregorian calendar date for the given ISO year, week and day"
    year_start = iso_year_start(iso_year)
    return year_start + datetime.timedelta(days=iso_day - 1, weeks=iso_week - 1)


def make_date(iso_date_string):
    date_tuple = iso_date_string.split(",")
    iso_year = int(date_tuple[0])
    iso_week = int(date_tuple[1])
    iso_day = int(date_tuple[2])
    return iso_to_gregorian(iso_year=iso_year, iso_week=iso_week, iso_day=iso_day)


def weeks_for_year(year, max_week_present, min_week_present):
    first_week = date(year, 1, 4).isocalendar()
    last_week = date(year, 12, 28).isocalendar()

    if (first_week < min_week_present):
        first_week = min_week_present
        if (max_week_present < last_week):
            last_week = max_week_present
            return (first_week[1], last_week[1])
        else:
            return (first_week[1], last_week[1])
    else:
        if (max_week_present < last_week):
            last_week = max_week_present
            return (first_week[1], last_week[1])
        else:
            return (first_week[1], last_week[1])


def get_missing_weeks(input_year, existing_weeks, max_week_present, min_week_present):
    (_first_week, _total_weeks) = weeks_for_year(input_year, max_week_present, min_week_present)
    _weeks_list = set(range(_first_week, _total_weeks + 1))
    _existing = set(existing_weeks)
    _missing = _weeks_list - _existing
    return list(_missing)


def get_missing_data(df_grpby_year, grp_min_date, grp_max_date):
    final_data_df = pd.DataFrame()
    for year, grp_dataset in df_grpby_year:
        missing_data_df_per_grp = pd.DataFrame()
        grp_dataset['week_num'] = grp_dataset['isocalendar'].map(lambda x: x[1])
        # print grp_dataset
        max_week_present = grp_max_date
        min_week_present = grp_min_date
        _weeks_missed = get_missing_weeks(year, grp_dataset['week_num'], max_week_present, min_week_present)
        _weeks_missed_Series = pd.Series(_weeks_missed).map(str)
        d = pd.DataFrame(np.zeros((len(_weeks_missed), 2)), columns=['quantity', 'q_indep_p'])
        missing_data_df_per_grp = pd.concat([d, _weeks_missed_Series], axis=1)
        missing_data_df_per_grp.columns = ['quantity', 'q_indep_p', 'week_num']
        missing_data_df_per_grp['year'] = str(year)
        missing_data_df_per_grp['isocalendar'] = (
        missing_data_df_per_grp['year'] + "," + missing_data_df_per_grp['week_num'] + "," + "4").map(
            lambda x: make_date(x).isocalendar())

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

        group_min_date = group.ix[group['date_parse'].argmin()].iloc[6]
        group_max_date = group.ix[group['date_parse'].argmax()].iloc[6]

        # print group_min_date
        # print group_max_date

        raw_data_grp = group[['matnr', 'quantity', 'q_indep_p', 'isocalendar', 'year']].groupby(['year'], as_index=False)
        missing_data = get_missing_data(raw_data_grp, group_min_date, group_max_date)
        missing_data['matnr'] = str(name[1])
        missing_data['customernumber'] = str(name[0])
        # print missing_data
        # print "******************************************************************"
        final_data_df_2 = pd.concat([final_data_df_2, missing_data], axis=0)
        # print final_data_df_2
        # print "##################################################################"
        final_data_df_1 = pd.concat([final_data_df_1, final_data_df_2], axis=0)

    final_data_df_1 = final_data_df_1.drop(['week_num', 'year'], axis=1)

    return final_data_df_1

def transform_raw_data(raw_data):
    result_df = pd.DataFrame()
    raw_data_copy = raw_data.copy()
    raw_data_copy['date'] = raw_data_copy['date'].map(str)
    raw_data_copy['date_parse'] = pd.to_datetime(raw_data_copy['date'])
    raw_data_copy['isocalendar'] = raw_data_copy['date_parse'].map(lambda x: x.isocalendar())
    raw_data_copy = raw_data_copy.drop(['date', 'date_parse'], axis=1)

    missing_Data = get_cmplt_missing_data(raw_data)

    result_df = pd.concat([raw_data_copy, missing_Data], axis=0)

    return result_df


def weekly_aggregate(data):
    data['year_weekNum'] = data['isocalendar'].map(lambda x: (x[0], x[1]))
    data = data.drop(['isocalendar'], axis=1)
    data_grp = data.groupby(['customernumber', 'matnr', 'year_weekNum'], as_index=False)[
        ['quantity', 'q_indep_p']].sum()
    data_grp['dt_week'] = data_grp['year_weekNum'].map(lambda x: iso_to_gregorian(int(x[0]), int(x[1]), 4))

    data_grp = data_grp.drop(['year_weekNum'], axis=1)
    # print data_grp
    return data_grp


def get_weekly_aggregate(inputfile, outputfile, input_sep="\t", output_sep=","):
    raw_data = pd.read_csv(inputfile, sep=input_sep, header=None,
                           names=['customernumber', 'matnr', 'date', 'quantity', 'q_indep_p'])
    dataset_cmplt = transform_raw_data(raw_data)
    result = weekly_aggregate(dataset_cmplt)
    result.to_csv(outputfile, sep=output_sep, index=False)


get_weekly_aggregate(inputfile="./skywaymart_90.txt", outputfile="./skywaymart_90_agg.txt")
