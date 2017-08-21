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
    """
    Converts ISO Calendar to Gregorian Date
    Gregorian calendar date for the given ISO year, week and day
    :param iso_year: ISO Year
    :param iso_week: ISO Week Number
    :param iso_day: ISO Day of Week
    :return: Gregorian Date Object
    """
    year_start = iso_year_start(iso_year)
    return year_start + datetime.timedelta(days=iso_day - 1, weeks=iso_week - 1)


def gregorian_to_iso(x):
    """
    Converts Gregorian to ISO Object
    :param x: x -> Tuple of (year, month, day of month)
    :return: ISOCalendar Object
    """
    if (isinstance(x[0], int) and isinstance(x[1], int) and isinstance(x[2], int)):
        year = x[0]
        month = x[1]
        day = x[2]
    else:
        year = int(x[0])
        month = int(x[1])
        day = int(x[2])

    return date(year=year, month=month, day=day).isocalendar()


def make_date(iso_date_string):
    """
    Converts ISO to Gregorian - Support function for iso_to_gregorian()
    :param iso_date_string: String of date separated by ","
    :return: Gregorian Date Object
    """
    date_tuple = iso_date_string.split(",")

    iso_year = date_tuple[0]
    iso_week = date_tuple[1]
    iso_day = date_tuple[2]

    # Sanity check for Integer Objects
    if (isinstance(iso_year, int) and isinstance(iso_week, int) and isinstance(iso_day, int)):
        return iso_to_gregorian(iso_year=iso_year, iso_week=iso_week, iso_day=iso_day)
    else:
        iso_year = int(date_tuple[0])
        iso_week = int(date_tuple[1])
        iso_day = int(date_tuple[2])
        return iso_to_gregorian(iso_year=iso_year, iso_week=iso_week, iso_day=iso_day)


def weeks_for_year(year, max_week_present, min_week_present):
    """
    Given a year, max_week_present and min_week_present, obtain weeks
    :param year: Input Year
    :param max_week_present: Maximum week present (maximum date) in dataset
    :param min_week_present: Minimum week present (minimum date) in dataset
    :return: Tuple of (first_week, last_week)
    """
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


def months_for_year(year, max_month_present, min_month_present):
    """
    Given a year, max_month_present and min_month_present, obtain months
    :param year: Input Year
    :param max_month_present: Maximum month present (maximum date) in dataset
    :param min_month_present: Minimum month present (minimum date) in dataset
    :return: Tuple of (first_month, last_month)
    """
    first_month = date(year, 1, 4).isocalendar()
    last_month = date(year, 12, 28).isocalendar()

    if first_month < min_month_present:
        first_month = ','.join(map(str, min_month_present))
        if max_month_present < last_month:
            last_month = ','.join(map(str, max_month_present))
            return make_date(first_month).month, make_date(last_month).month
        else:
            last_month = ','.join(map(str, last_month))
            return make_date(first_month).month, make_date(last_month).month
    else:
        if max_month_present < last_month:
            first_month = ','.join(map(str, first_month))
            last_month = ','.join(map(str, max_month_present))
            return make_date(first_month).month, make_date(last_month).month
        else:
            first_month = ','.join(map(str, first_month))
            last_month = ','.join(map(str, last_month))
            return make_date(first_month).month, make_date(last_month).month


def get_missing_weeks(input_year, existing_weeks, max_week_present, min_week_present):
    """
    Given year, existing weeks in year, max_week_present, min_week_present - get missing weeks
    :param input_year: Input Year
    :param existing_weeks: Existing weeks in year
    :param max_week_present: Maximum week present (maximum date) in dataset
    :param min_week_present: Minimum week present (minimum date) in dataset
    :return: List of missing weeks in year
    """
    (_first_week, _total_weeks) = weeks_for_year(input_year, max_week_present, min_week_present)
    _weeks_list = set(range(_first_week, _total_weeks + 1))
    _existing = set(existing_weeks)
    _missing = _weeks_list - _existing
    return list(_missing)


def get_missing_months(input_year, existing_months, max_month_present, min_month_present):
    """
    Given year, existing weeks in year, max_week_present, min_week_present - get missing months
    :param input_year: Input Year
    :param existing_months: Existing months in year
    :param max_month_present: Maximum month present (maximum date) in dataset
    :param min_month_present: Minimum month present (minimum date) in dataset
    :return: List of missing months in year
    """
    (_first_month, _total_months) = months_for_year(input_year, max_month_present, min_month_present)
    _month_list = set(range(_first_month, _total_months + 1))
    _existing = set(existing_months)
    _missing = _month_list - _existing
    return list(_missing)


def get_missing_data_weekly(df_grpby_year, grp_min_date, grp_max_date):
    """
    Generate dataframe containing missing datapoints at weekly aggregate
    :param df_grpby_year: data grouped by year
    :param grp_min_date: min_date of data grouped by matnr and customernumber
    :param grp_max_date: max_date of data grouped by matnr and customernumber
    :return: Return missing data for (customernumber, matnr) combo for all years
    """
    final_data_df = pd.DataFrame()
    for year, group in df_grpby_year:
        missing_data_df_per_grp = pd.DataFrame()
        grp_dataset = group.copy()
        grp_dataset['week_num'] = grp_dataset['isocalendar'].map(lambda x: x[1])

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


def get_missing_data_monthly(df_grpby_year, grp_min_date, grp_max_date):
    """
    Generate dataframe containing missing datapoints at monthly aggregate
    :param df_grpby_year: data grouped by year
    :param grp_min_date: min_date of data grouped by matnr and customernumber
    :param grp_max_date: max_date of data grouped by matnr and customernumber
    :return: Return missing data for (customernumber, matnr) combo for all years
    """
    final_data_df = pd.DataFrame()
    for year, group in df_grpby_year:
        missing_data_df_per_grp = pd.DataFrame()
        grp_dataset = group.copy()
        grp_dataset['month_num'] = grp_dataset['isocalendar'].map(lambda x: make_date(','.join(map(str, x))).month)

        max_month_present = grp_max_date
        min_month_present = grp_min_date

        _months_missed = get_missing_months(year, grp_dataset['month_num'], max_month_present, min_month_present)
        _months_missed_Series = pd.Series(_months_missed).map(str)
        d = pd.DataFrame(np.zeros((len(_months_missed), 2)), columns=['quantity', 'q_indep_p'])
        missing_data_df_per_grp = pd.concat([d, _months_missed_Series], axis=1)
        missing_data_df_per_grp.columns = ['quantity', 'q_indep_p', 'month_num']

        missing_data_df_per_grp['year'] = str(year)

        missing_data_df_per_grp['year_month_day'] = missing_data_df_per_grp['year'].astype(str) + "," + \
                                                    missing_data_df_per_grp['month_num'].astype(str) + "," + "4"
        missing_data_df_per_grp['isocalendar'] = missing_data_df_per_grp['year_month_day'].map(
            lambda x: gregorian_to_iso(x.split(",")))
        missing_data_df_per_grp = missing_data_df_per_grp.drop(['year_month_day'], axis=1)
        final_data_df = pd.concat([final_data_df, missing_data_df_per_grp], axis=0)

    return final_data_df


def get_cmplt_missing_data_weekly(raw_data):
    """
    Get complete missing (weekly aggregated) sets of data for all (customernumber, matnr) combo
    :param raw_data: raw_data as a Pandas dataframe
    :return: Complete missing datasets
    """
    """
    This fucntion receives raw data
    :param raw_data: grouped dataframe
    :return: yet to decide
    """
    raw_data_grp = raw_data.groupby(['customernumber', 'matnr'], as_index=False)
    final_data_df_1 = pd.DataFrame()
    for name, group in raw_data_grp:

        final_df_2 = group.copy()
        final_df_2['year'] = final_df_2['date_parse'].map(lambda x: x.year)

        group_min_date = final_df_2.ix[final_df_2['date_parse'].argmin()].iloc[6]
        group_max_date = final_df_2.ix[final_df_2['date_parse'].argmax()].iloc[6]

        final_df_2.drop(['date', 'date_parse'], axis=1)
        raw_data_grp_year = final_df_2[
            ['customernumber', 'matnr', 'quantity', 'q_indep_p', 'isocalendar', 'year']].groupby(['year'],
                                                                                                 as_index=False)
        missing_data = get_missing_data_weekly(raw_data_grp_year, group_min_date, group_max_date)
        missing_data['matnr'] = name[1]
        missing_data['customernumber'] = name[0]

        final_data_df_1 = pd.concat([final_data_df_1, missing_data], axis=0)

    final_data_df_1 = final_data_df_1.drop(['week_num', 'year'], axis=1)

    return final_data_df_1


def get_cmplt_missing_data_monthly(raw_data):
    """
    Get complete missing (monthly aggregated) sets of data for all (customernumber, matnr) combo
    :param raw_data: raw_data as a Pandas dataframe
    :return: Complete missing datasets
    """
    raw_data_grp = raw_data.groupby(['customernumber', 'matnr'], as_index=False)
    final_data_df_1 = pd.DataFrame()
    for name, group in raw_data_grp:
        final_df_2 = group.copy()
        final_df_2['year'] = final_df_2['date_parse'].map(lambda x: x.year)

        group_min_date = final_df_2.ix[final_df_2['date_parse'].argmin()].iloc[6]
        group_max_date = final_df_2.ix[final_df_2['date_parse'].argmax()].iloc[6]

        final_df_2.drop(['date', 'date_parse'], axis=1)
        raw_data_grp_year = final_df_2[
            ['customernumber', 'matnr', 'quantity', 'q_indep_p', 'isocalendar', 'year']].groupby(['year'],
                                                                                                 as_index=False)
        missing_data = get_missing_data_monthly(raw_data_grp_year, group_min_date, group_max_date)
        missing_data['matnr'] = name[1]
        missing_data['customernumber'] = name[0]

        final_data_df_1 = pd.concat([final_data_df_1, missing_data], axis=0)

    final_data_df_1 = final_data_df_1.drop(['month_num', 'year'], axis=1)

    return final_data_df_1


def transform_raw_data_weekly(raw_data):
    """
    Transform raw data for weekly aggregate + add missing data as well
    :param raw_data: Dataframe of raw_data
    :return: Tranformed dataset of raw data containing only required columns
    """
    result_df = pd.DataFrame()
    raw_data_copy = raw_data.copy()
    raw_data_copy['customernumber'] = raw_data_copy['customernumber'].map(str)
    raw_data_copy['date'] = raw_data_copy['date'].map(str)
    raw_data_copy['date_parse'] = pd.to_datetime(raw_data_copy['date'])
    raw_data_copy['isocalendar'] = raw_data_copy['date_parse'].map(lambda x: x.isocalendar())
    missing_Data = get_cmplt_missing_data_weekly(raw_data_copy)
    raw_data_copy = raw_data_copy.drop(['date', 'date_parse'], axis=1)
    result_df = pd.concat([raw_data_copy, missing_Data], axis=0)
    return result_df


def transform_raw_data_monthly(raw_data):
    """
    Transform raw data for monthly aggregate + add missing data as well
    :param raw_data: Dataframe of raw_data
    :return: Tranformed dataset of raw data containing only required columns
    """
    result_df = pd.DataFrame()
    raw_data_copy = raw_data.copy()
    raw_data_copy['customernumber'] = raw_data_copy['customernumber'].map(str)
    raw_data_copy['date'] = raw_data_copy['date'].map(str)
    raw_data_copy['date_parse'] = pd.to_datetime(raw_data_copy['date'])
    raw_data_copy['isocalendar'] = raw_data_copy['date_parse'].map(lambda x: x.isocalendar())
    missing_Data = get_cmplt_missing_data_monthly(raw_data_copy)
    raw_data_copy = raw_data_copy.drop(['date', 'date_parse'], axis=1)
    result_df = pd.concat([raw_data_copy, missing_Data], axis=0)
    return result_df



def weekly_aggregate(data):
    """
    Aggregate data - weekly level
    :param data: dataset for weekly aggreagated
    :return: weekly aggregated dataset
    """
    data['year_weekNum'] = data['isocalendar'].map(lambda x: (x[0], x[1]))
    data = data.drop(['isocalendar'], axis=1)
    data_grp = data.groupby(['customernumber', 'matnr', 'year_weekNum'], as_index=False)[
        ['quantity', 'q_indep_p']].sum()
    data_grp['dt_week'] = data_grp['year_weekNum'].map(lambda x: iso_to_gregorian(int(x[0]), int(x[1]), 4))

    data_grp = data_grp.drop(['year_weekNum'], axis=1)
    return data_grp


def monthly_aggregate(data):
    """
    Aggregate data - monthly level
    :param data: dataset for monthly aggreagated
    :return: monthly aggregated dataset
    """
    data['year_monthNum'] = data['isocalendar'].map(lambda x: (x[0], make_date(','.join(map(str, x))).month))
    data = data.drop(['isocalendar'], axis=1)
    data_grp = data.groupby(['customernumber', 'matnr', 'year_monthNum'], as_index=False)[
        ['quantity', 'q_indep_p']].sum()
    data_grp['dt_week'] = data_grp['year_monthNum'].map(lambda x: date(int(x[0]), int(x[1]), 15))

    data_grp = data_grp.drop(['year_monthNum'], axis=1)
    return data_grp


def get_weekly_aggregate(inputfile, input_sep="\t"):
    """
    Consolidated function for weekly aggregation
    :param inputfile: input file path
    :param input_sep: input file separators
    :return:
    """
    raw_data = pd.read_csv(inputfile, sep=input_sep, header=None,
                           names=['customernumber', 'matnr', 'date', 'quantity', 'q_indep_p'])

    dataset_cmplt = transform_raw_data_weekly(raw_data)
    result = weekly_aggregate(dataset_cmplt)
    return result


def get_monthly_aggregate(inputfile, input_sep="\t"):
    """
    Consolidated function for weekly aggregation
    :param inputfile: input file path
    :param input_sep: input file separators
    :return:
    """
    raw_data = pd.read_csv(inputfile, sep=input_sep, header=None,
                           names=['customernumber', 'matnr', 'date', 'quantity', 'q_indep_p'])
    dataset_cmplt = transform_raw_data_monthly(raw_data)

    result = monthly_aggregate(dataset_cmplt)
    return result

# data_month = get_monthly_aggregate(inputfile="./skywaymart_90.txt")
# print data_month
# print data_month['matnr'].unique()
# print data_month['customernumber'].unique()
#
# data_week = get_weekly_aggregate(inputfile="./skywaymart_90.txt")
# print data_week
# print data_week['matnr'].unique()
# print data_week['customernumber'].unique()
