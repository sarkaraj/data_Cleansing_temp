from date_aggregator_transformers import *


class weekObject(object):
    def __init__(self, array_of_dates):
        self.week = array_of_dates[0][1]
        self.year = array_of_dates[0][0]
        self.days = tuple(set([iso_date[2] for iso_date in array_of_dates]))

    def getKVPair(self):
        tuple_obj = (self.week, self.year, self.days)
        return tuple_obj


def assign_delivery_date(x):
    if(x >=1 and x<=3):
        return 2
    elif(x >= 4 and x <=7 ):
        return 5
    else:
        return None

def detect_days_in_week(week_array):
    return tuple(set(week_array))


def transform_2(data):
    iso_date = data.iloc[:, 3]
    week_num = iso_date.map(lambda x: x[1])
    year_ = iso_date.map(lambda x: x[0])
    iso_date = pd.concat([iso_date, week_num, year_], axis=1)
    iso_date.columns = ['iso_date', 'week_num', 'year']
    # print iso_date
    week_kv = iso_date.groupby(['week_num', 'year'], as_index=False).apply(
        lambda tdf: pd.Series(dict([[vv, tdf[vv].unique().tolist()] for vv in tdf if vv not in ['week_num', 'year']])))
    week_kv['week_day_KV'] = week_kv['iso_date'].map(lambda date_array: weekObject(date_array).getKVPair())
    week_kv['freq_key'] = week_kv['week_day_KV'].map(lambda x: x[2])
    # print week_kv
    agg_freq = week_kv.groupby(['freq_key'], as_index=False)[['iso_date']].agg('count')
    agg_freq = agg_freq.sort_values(['iso_date'], ascending=False)
    agg_freq.columns = ['freq_key', 'count']
    print agg_freq
    print week_kv['freq_key'].unique()
    return True


def transformation_raw_data(raw_data):
    raw_data['date'] = raw_data['date'].map(str)
    raw_data['date_parse'] = pd.to_datetime(raw_data['date'])
    raw_data['isocalendar'] = raw_data['date_parse'].map(lambda x: x.isocalendar())
    raw_data = raw_data.drop(['date', 'date_parse'], axis=1)
    # raw_data['sanity_check'] = raw_data['isocalendar'].map(lambda x: iso_to_gregorian(x[0], x[1], x[2]))
    raw_data.columns = ['matnr', 'quantity', 'q_indep_p', 'iso_date']
    return raw_data



raw_data = pd.read_csv("./skywaymart_90.txt", sep="\t", header=None,
                       names=['customernumber', 'matnr', 'date', 'quantity', 'q_indep_p'])[
    ['matnr', 'date', 'quantity', 'q_indep_p']]

single_pdt = raw_data.loc[(raw_data['matnr'] == 103029), ['matnr', 'date', 'quantity', 'q_indep_p']]

temp = transformation_raw_data(single_pdt)
print transform_2(temp)

# detect_freq(raw_data['fabri_date'])

# date_frm_isocal = raw_data_sorted_grp


# print raw_data_sorted_grp.dtypes()

# print raw_data_sorted_grp

# print raw_data[raw_data['day_of_week']==6]['date_parse'].unique()

# print raw_data.dtypes
