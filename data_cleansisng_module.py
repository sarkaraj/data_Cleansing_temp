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
    return datetime.strptime(date_string, "%Y %W %w")

raw_data = pd.read_csv("./skywaymart_90.txt", sep="\t", header=None, names=['customernumber', 'matnr', 'date', 'quantity', 'q_indep_p'])[[
    'matnr','date', 'quantity', 'q_indep_p']]
raw_data['date'] = raw_data['date'].astype(str)
raw_data['date_parse'] = pd.to_datetime(raw_data['date'])
raw_data['isocalendar'] = raw_data['date_parse'].map(lambda x : x.isocalendar())
# raw_data['week_num'] = raw_data['isocalendar'].map(lambda x: x[1])
# raw_data['month'] = raw_data['date_parse'].map(lambda x: x.month)
# raw_data['year'] = raw_data['date_parse'].map(lambda x: x.year)
raw_data['weekday'] = raw_data['isocalendar'].map(lambda x: x[2])
raw_data['weekday_modified'] = raw_data['weekday'].map(lambda x: assign_delivery_date(x))

raw_data_sorted = raw_data.sort_values(by='date_parse')[['matnr', 'quantity', 'q_indep_p', 'isocalendar', 'weekday_modified']].copy()

# raw_data_sorted = raw_data.sort_values(by='date_parse')

raw_data_sorted_grp = raw_data_sorted.groupby(['matnr', 'isocalendar', 'weekday_modified'], as_index=False).sum()

raw_data_sorted_grp['year'] = raw_data_sorted_grp['isocalendar'].map(lambda x: x[0])
raw_data_sorted_grp['week_num'] = raw_data_sorted_grp['isocalendar'].map(lambda x: x[1])
raw_data_sorted_grp['fabri_date_iso'] = raw_data_sorted_grp['year'].map(str) + " " + raw_data_sorted_grp['week_num'].map(str) + " " + raw_data['weekday_modified'].map(str)
raw_data_sorted_grp['fabri_date'] = raw_data_sorted_grp['fabri_date_iso'].map(lambda x: make_date(x))

# date_frm_isocal = raw_data_sorted_grp


# print raw_data_sorted_grp.dtypes()

print raw_data_sorted_grp

# print raw_data[raw_data['day_of_week']==6]['date_parse'].unique()

# print raw_data.dtypes