import pandas as pd
import numpy as np
import itertools
import warnings
import statsmodels.api as sm
from fbprophet import Prophet
from dateutil import parser
import matplotlib.pylab as plt
% matplotlib
inline
from matplotlib.pylab import rcParams

rcParams['figure.figsize'] = 15, 6

# Define minimum test and train days
test_points = 2
min_train_days = 731


def is_outlier(value, p25, p75):
    """Check if value is an outlier
    """
    lower = p25 - 2 * (p75 - p25)
    upper = p75 + 2 * (p75 - p25)
    return value <= lower or value >= upper


def get_indices_of_outliers(values):
    """Get outlier indices (if any)
    """
    p25 = np.percentile(values, 25)
    p75 = np.percentile(values, 75)

    indices_of_outliers = []
    for ind, value in enumerate(values):
        if is_outlier(value, p25, p75):
            indices_of_outliers.append(ind)
    return indices_of_outliers


# data transformation
data = get_weekly_aggregate(inputfile="c001_data_52.txt", outputfile="c001_data_52_agg.txt")
data.dt_week = data.dt_week.apply(str).apply(parser.parse)
data.head()

# single product data
for cus_no in data.customernumber.unique():
    cus_data = data.loc[(data['customernumber'] == cus_no)]
    for mat_no in cus_data.matnr.unique():
        prod = cus_data[cus_data.matnr == mat_no]

        prod = prod.sort_values('dt_week')
        prod = prod.reset_index()
        prod = prod.rename(columns={'dt_week': 'ds', 'quantity': 'y'})
        prod = prod[['ds', 'y']]
        prod = prod.drop(prod.index[[0, len(prod.y) - 1]]).reset_index(drop=True)
        # prod['y']=prod['y'].replace(0,prod['y'].mean())
        #         prod.dtypes
        #         ############################ Data Prep################################

        # Incremental Test
        train = prod[
            prod.ds <= (max(prod.ds) - pd.DateOffset(days=(max(prod.ds) - min(prod.ds)).days - min_train_days))]
        test = prod[(max(train.index) + 1):(max(train.index) + 1 + test_points)]
        rem_data = prod[(max(train.index) + test_points):]
        output_result = pd.DataFrame()

        indices_of_outliers = get_indices_of_outliers(prod.y)

        fig = plt.figure()
        #         ax = fig.add_subplot(111)
        plt.plot(prod.ds, prod.y, 'b-', label='Quantity')
        plt.plot(
            prod.ds[indices_of_outliers],
            prod.y[indices_of_outliers],
            'ro',
            markersize=7,
            label='outliers')
        plt.legend(loc='best')

        save_file = os.path.join(dir_name, str(cus_no) + "_" + str(mat_no) + "_raw_data.png")

        plt.savefig(save_file, bbox_inches='tight')
        plt.close(fig)

        # remove outlier
        prod.loc[indices_of_outliers, 'y'] = None

        # plotting Data
        fig = plt.figure()
        plt.plot(prod.ds, prod.y)
        plt.xlabel('Date')
        plt.ylabel('Quantity')
        plt.legend()

        save_file = os.path.join(dir_name, str(cus_no) + "_" + str(mat_no) + "_clean_data.png")

        plt.savefig(save_file, bbox_inches='tight')
        plt.close(fig)

        while (len(rem_data.ds) >= 2):

            # ARIMA Model Data Transform
            train_arima = train.set_index('ds', drop=True)
            # train_arima.head()
            test_arima = test.set_index('ds', drop=True)

            # ARIMA Model
            # grid search parameters
            p = d = q = range(0, 2)

            # Generate all different combinations of p, q and q triplets
            pdq = list(itertools.product(p, d, q))

            # Generate all different combinations of seasonal p, q and q triplets
            seasonal_pdq = [(x[0], x[1], x[2], 52) for x in list(itertools.product(p, d, q))]

            print('Next Test Starts...')

            # grid search
            warnings.filterwarnings("ignore")  # specify to ignore warning messages
            min_aic = 9999999
            for param in pdq:
                for param_seasonal in seasonal_pdq:
                    try:
                        mod = sm.tsa.statespace.SARIMAX(train_arima, order=param, seasonal_order=param_seasonal,
                                                        enforce_stationarity=False, enforce_invertibility=False,
                                                        measurement_error=True)

                        results = mod.fit()
                        if results.aic < min_aic:
                            min_aic = results.aic
                            opt_param = param
                            opt_param_seasonal = param_seasonal

                        #                         print('ARIMA{}x{}12 - AIC:{}'.format(param, param_seasonal, results.aic))
                    except:
                        continue
            print('Optimal ARIMA{}x{}12 - AIC:{}'.format(opt_param, opt_param_seasonal, min_aic))

            # fitting Model
            mod = sm.tsa.statespace.SARIMAX(train_arima, order=opt_param, seasonal_order=opt_param_seasonal,
                                            enforce_stationarity=False, enforce_invertibility=False,
                                            measurement_error=True)
            result = mod.fit(disp=False)

            # forecast Train
            pred_train = results.get_prediction(start=pd.to_datetime(min(train_arima.index)), dynamic=False)
            pred_train_ci = pred_train.conf_int()

            # forecast test
            pred_test = results.get_prediction(start=pd.to_datetime(max(train_arima.index)),
                                               end=pd.to_datetime(max(test_arima.index)), dynamic=True)
            pred_test_ci = pred_test.conf_int()

            # ceating test and train emsembled result
            # test result
            result_test = test
            result_test['y_ARIMA'] = np.array(pred_test.predicted_mean)[1:]

            # prophet
            m = Prophet(weekly_seasonality=False, yearly_seasonality=False, changepoint_prior_scale=5)
            m.fit(train);

            # creating pred train and test data frame
            past = m.make_future_dataframe(periods=0, freq='W')
            future = pd.DataFrame(test['ds'])
            pf_train_pred = m.predict(past)
            pf_test_pred = m.predict(future)
            pf_train_pred = pf_train_pred[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].set_index([past.index])
            pf_test_pred = pf_test_pred[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].set_index([future.index])

            # ceating test and train emsembled result
            # test result
            result_test['y_Prophet'] = np.array(pf_test_pred.yhat)

            # Ansemble
            result_test['y_Ensembled'] = result_test[["y_ARIMA", "y_Prophet"]].mean(axis=1)

            train = prod[:(max(train.index) + 1 + test_points)]
            test = prod[(max(train.index) + 1):(max(train.index) + 1 + test_points)]
            rem_data = prod[(max(train.index) + test_points):]

            output_result = pd.concat([output_result, result_test], axis=0)
            output_result['Error'] = np.subtract(output_result.y_Ensembled, output_result.y)
            output_result['Error_Cumcum'] = output_result.Error.cumsum() / output_result.y.cumsum() * 100

        # Plot incremental test result
        fig = plt.figure()
        plt.plot(output_result.ds, output_result.y, label='Observed_test')
        plt.plot(output_result.ds, output_result.y_ARIMA, label='ARIMA')
        plt.plot(output_result.ds, output_result.y_Prophet, label='Prophet')
        plt.plot(output_result.ds, output_result.y_Ensembled, label='Ensembled')
        plt.xlabel('Date')
        plt.ylabel('Quantity')
        plt.legend()
        save_file = os.path.join(dir_name, str(cus_no) + "_" + str(mat_no) + "_prediction.png")

        plt.savefig(save_file, bbox_inches='tight')
        plt.close(fig)

        # plot error
        fig = plt.figure()

        output_result['Error'] = np.subtract(output_result.y_Ensembled, output_result.y)
        output_result['Error_Cumsum'] = output_result.Error.cumsum() / output_result.y.cumsum() * 100

        output_result['Error_prophet'] = np.subtract(output_result.y_Prophet, output_result.y)
        output_result['Error_Cumsum_prophet'] = output_result.Error_prophet.cumsum() / output_result.y.cumsum() * 100

        output_result['Error_arima'] = np.subtract(output_result.y_ARIMA, output_result.y)
        output_result['Error_Cumsum_arima'] = output_result.Error_arima.cumsum() / output_result.y.cumsum() * 100

        plt.plot(output_result.ds[2:], output_result.Error_Cumsum_arima[2:], label='ARIMA')
        plt.plot(output_result.ds[2:], output_result.Error_Cumsum_prophet[2:], label='Prophet')
        plt.plot(output_result.ds[2:], output_result.Error_Cumsum[2:], label='Ensembled')

        plt.xlabel('Date')
        plt.ylabel('% Cumulative Error')
        plt.legend()

        save_file = os.path.join(dir_name, str(cus_no) + "_" + str(mat_no) + "_cum_error.png")

        plt.savefig(save_file, bbox_inches='tight')
        plt.close(fig)
