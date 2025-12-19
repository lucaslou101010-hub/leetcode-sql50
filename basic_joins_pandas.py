import pandas as pd

def rising_temperature(weather: pd.DataFrame) -> pd.DataFrame:
    weather = weather.sort_values(by=['recordDate'])
    weather['prevTemperature'] = weather['temperature'].shift(1)
    weather['dateDiff'] = weather['recordDate'].diff()
    result = weather[
        (weather['prevTemperature'] < weather['temperature']) &
        (weather['dateDiff'] == pd.Timedelta(days=1))
    ]
    return result[['id']]

import pandas as pd

def get_average_time(activity: pd.DataFrame) -> pd.DataFrame:
    activity['weighted_timestamp'] = activity.apply(lambda x: x['timestamp'] if x['activity_type'] == 'end' else -x['timestamp'],axis=1)
    result = (
        activity
        .groupby(['machine_id'])
        .agg(
            num_process=('process_id','nunique'),
            weighted_sum=('weighted_timestamp','sum'),
        )
        .reset_index()
    )
    result['processing_time'] = (result['weighted_sum'] / result['num_process']).round(3)
    return result[['machine_id','processing_time']]