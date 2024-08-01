import simplejson as js
import requests as req
import pandas as pd


# Collect raw data from NYC Covid Public Data
def collect_api_data(api_url='https://data.cityofnewyork.us/resource/rc75-m7u3.json?$limit=100000'):

    #collecting all data
    #accessing data from api
    response = req.get(api_url)

    # print(response)
    #JSON TO DataFrame
    df = pd.DataFrame(js.loads(response.content))

    # #subsetting data
    data_lake =   df[['date_of_interest', 'case_count', 'probable_case_count', 'hospitalized_count', 'death_count','bx_case_count',
        'bx_probable_case_count', 'bx_hospitalized_count', 'bx_death_count','bk_case_count', 'bk_probable_case_count',
        'bk_hospitalized_count', 'bk_death_count', 'mn_case_count', 'mn_probable_case_count', 'mn_hospitalized_count',
        'mn_death_count','qn_case_count', 'qn_probable_case_count', 'qn_hospitalized_count', 'qn_death_count',
        'si_case_count', 'si_probable_case_count', 'si_hospitalized_count', 'si_death_count']]
    data_lake['borough_case_count'] = data_lake.loc[:,['bx_probable_case_count','bk_probable_case_count','mn_probable_case_count',
                                                 'qn_probable_case_count','si_probable_case_count']].sum(axis=1, numeric_only=True)
    for record in data_lake.to_dict('records'):
        for key, value in record.items():
            if key == 'date_of_interest':
                print(value.split('T')[0])
            else:
                print(f"{key}:{value}")
        # break

    # return data_lake

collect_api_data()