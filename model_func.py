def ensemble_model(data, test_points, min_train_days):
    train = prod[prod.ds <= (max(prod.ds) - pd.DateOffset(days=(max(prod.ds) - min(prod.ds)).days - min_train_days))]
    test = prod[(max(train.index) + 1):(max(train.index) + 1 + test_points)]
    rem_data = prod[(max(train.index) + test_points):]

    output_result = pd.DataFrame()

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
                #         print('Optimal ARIMA{}x{}12 - AIC:{}'.format(opt_param, opt_param_seasonal, min_aic))

        # fitting Model
        mod = sm.tsa.statespace.SARIMAX(train_arima, order=opt_param, seasonal_order=opt_param_seasonal,
                                        enforce_stationarity=False, enforce_invertibility=False, measurement_error=True)
        mod_ARIMA = mod.fit(disp=False)

        #         # forecast Train
        #         pred_train = mod_ARIMA.get_prediction(start=pd.to_datetime(min(train_arima.index)), dynamic=False)
        #         pred_train_ci = pred_train.conf_int()

        # forecast test
        pred_test = mod_ARIMA.get_prediction(start=pd.to_datetime(max(train_arima.index)),
                                             end=pd.to_datetime(max(test_arima.index)), dynamic=True)
        pred_test_ci = pred_test.conf_int()

        # ceating test and train emsembled result  
        # test result
        result_test = test
        result_test['y_ARIMA'] = np.array(pred_test.predicted_mean)[1:]

        # prophet
        m = Prophet(weekly_seasonality=False, yearly_seasonality=False, changepoint_prior_scale=5)
        mod_prophet = m.fit(train);

        # creating pred train and test data frame
        #         past = mod_prophet.make_future_dataframe(periods=0, freq= 'W')
        future = pd.DataFrame(test['ds'])
        #         pf_train_pred = mod_prophet.predict(past)
        pf_test_pred = mod_prophet.predict(future)
        #         pf_train_pred = pf_train_pred[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].set_index([past.index])
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

    # Error calculation
    output_result['Error_Ensembled'] = np.subtract(output_result.y_Ensembled, output_result.y)
    output_result['Error_Cumsum_Ensembled'] = output_result.Error.cumsum() / output_result.y.cumsum() * 100

    output_result['Error_prophet'] = np.subtract(output_result.y_Prophet, output_result.y)
    output_result['Error_Cumsum_prophet'] = output_result.Error_prophet.cumsum() / output_result.y.cumsum() * 100

    output_result['Error_arima'] = np.subtract(output_result.y_ARIMA, output_result.y)
    output_result['Error_Cumsum_arima'] = output_result.Error_arima.cumsum() / output_result.y.cumsum() * 100

    return (mod_ARIMA, mod_prophet, output_result)
    #    print(output_result.head())
