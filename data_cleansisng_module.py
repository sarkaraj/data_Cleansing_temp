import pandas as pd
from datetime import datetime

def assign_delivery_date(x):
    if(x >=1 and x<=3):
        return 2
    elif(x >= 4 and x <=7 ):
        return 5
    else:
        return None

def make_date(date_string):
    return datetime.strptime(date_string, "%Y-%W-%w")


def detect_days_in_week(week_array):
    return tuple(set(week_array))


def detect_freq(df_date, freq='auto'):
    # create ISO (year, week, day_of_week) dateformat from timestamp object: df_date
    isoCalendar = df_date.map(lambda x: x.isocalendar())
    # get week number from ISO date
    _weekNum = isoCalendar.map(lambda x: x[1])
    # get day of week from ISO date
    _day = isoCalendar.map(lambda x: str(x[2]))
    # create temp dataframe
    temp_df = pd.concat([_weekNum, _day], axis=1)
    temp_df.columns = ['weekNum', 'day_of_week']
    # group by week of year, aggregate over count and unique elements
    temp_df_grp_1 = temp_df.groupby(['weekNum'], as_index=False).agg(['count', 'unique'])
    # get delivery cycles throughout whole time period, return tuples to make hashable elems
    temp_df_grp_1['cycle'] = temp_df_grp_1['day_of_week']['unique'].map(lambda x: detect_days_in_week(x))
    # make another temp dataframe
    temp_df_2 = pd.concat([temp_df_grp_1['day_of_week']['count'], temp_df_grp_1['cycle']], axis=1)
    # group by cycles
    temp_df_2_grp = temp_df_2.groupby(['cycle'], as_index=False).sum()

    if (freq == 'auto'):
        # get cycle with max occurrences,
        max_cycle = temp_df_2_grp.ix[temp_df_2_grp['count'].argmax()].iloc[0]
        print type(max_cycle)
    else:
        return None

        # return None


raw_data = pd.read_csv("./skywaymart_90.txt", sep="\t", header=None,
                       names=['customernumber', 'matnr', 'date', 'quantity', 'q_indep_p'])[
    ['matnr', 'date', 'quantity', 'q_indep_p']]

raw_data = raw_data[raw_data['matnr'] == 103029]

# print raw_data

raw_data['date'] = raw_data['date'].astype(str)
raw_data['date_parse'] = pd.to_datetime(raw_data['date'])
raw_data['isocalendar'] = raw_data['date_parse'].map(lambda x : x.isocalendar())
# raw_data['week_num'] = raw_data['isocalendar'].map(lambda x: x[1])
# raw_data['month'] = raw_data['date_parse'].map(lambda x: x.month)
# raw_data['year'] = raw_data['date_parse'].map(lambda x: x.year)
raw_data['weekday'] = raw_data['isocalendar'].map(lambda x: x[2])
raw_data['weekday_modified'] = raw_data['weekday'].map(lambda x: assign_delivery_date(x))

raw_data_sorted = raw_data.sort_values(by='date_parse')[
    ['quantity', 'q_indep_p', 'isocalendar', 'weekday_modified']].copy()

# raw_data_sorted = raw_data.sort_values(by='date_parse')

raw_data_sorted_grp = raw_data_sorted.groupby(['isocalendar', 'weekday_modified'], as_index=False).sum()

raw_data_sorted_grp['year'] = raw_data_sorted_grp['isocalendar'].map(lambda x: x[0])
raw_data_sorted_grp['week_num'] = raw_data_sorted_grp['isocalendar'].map(lambda x: x[1])
raw_data_sorted_grp['fabri_date_iso'] = raw_data_sorted_grp['year'].map(str) + "-" + raw_data_sorted_grp[
    'week_num'].map(str) + "-" + raw_data_sorted_grp['weekday_modified'].map(str)
raw_data_sorted_grp['fabri_date'] = raw_data_sorted_grp['fabri_date_iso'].map(lambda x: make_date(x))

# print raw_data_sorted_grp

detect_freq(raw_data_sorted_grp['fabri_date'])

# date_frm_isocal = raw_data_sorted_grp


# print raw_data_sorted_grp.dtypes()

# print raw_data_sorted_grp

# print raw_data[raw_data['day_of_week']==6]['date_parse'].unique()

# print raw_data.dtypes
