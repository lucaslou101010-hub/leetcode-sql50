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

import pandas as pd

def employee_bonus(employee: pd.DataFrame, bonus: pd.DataFrame) -> pd.DataFrame:
    df = (
        employee
        .merge(
            bonus,
            on=['empId'],
            how='left',
        )
    )

    result = df[
        (df['bonus'] < 1000) |
        (df['bonus'].isna())
    ]

    return result[['name','bonus']]

# Note2 'join' actually deals with index only in pandas!

import pandas as pd

def students_and_examinations(students: pd.DataFrame, subjects: pd.DataFrame, examinations: pd.DataFrame) -> pd.DataFrame:
    result = (
        students.merge(subjects,how='cross')
        .merge(
            examinations.groupby(['student_id','subject_name']).size().reset_index(name='attended_exams'),
            on=['student_id','subject_name'],
            how='left',
        )
        .fillna(0)
        .sort_values(by=['student_id','subject_name'])
    )
    return result

# This is a great specific question. The reason lies in what .size() actually returns. 
# When you perform a groupby().size(), pandas returns a Series, not a DataFrame. 
# This Series contains the counts, but because it's a Series, those counts don't have a column name—they are just "values" attached to an index.

import pandas as pd

def find_managers(employee: pd.DataFrame) -> pd.DataFrame:
    df = (
        employee
        .merge(
            employee.groupby(['managerId']).size().reset_index(name='direct_reports').rename(columns={'managerId':'id'}),
            how='left',
            on='id',
        )
    )
    results = df[df['direct_reports'] >= 5]
    return results[['name']]

def find_managers(employee: pd.DataFrame) -> pd.DataFrame:
    # 1. Get the counts (Series)
    counts = employee['managerId'].value_counts()
    
    # 2. Filter for IDs with >= 5 reports
    # This creates a small Index object of just the target IDs
    top_manager_ids = counts[counts >= 5].index
    
    # 3. Filter original DF using boolean indexing
    return employee[employee['id'].isin(top_manager_ids)][['name']]

# Why this is better:
# 1, No Merge Overhead: Merges are expensive ($O(N \log N)$ or hash-map based). 
# .isin() is a highly optimized vectorized lookup.Memory: 
# You don't create an intermediate dataframe with an extra column for every single employee.

import numpy as np
import pandas as pd

def confirmation_rate(signups: pd.DataFrame, confirmations: pd.DataFrame) -> pd.DataFrame:
    confirmations['numerator'] = (confirmations['action'] == 'confirmed').astype(int)
    df_confirm = (
        confirmations
        .groupby(['user_id'])
        .agg(
            num_request=('numerator','count'),
            num_confirm=('numerator','sum'),
        )
        # .fillna(0)
    )
    df_confirm['confirmation_rate'] = (df_confirm['num_confirm'] / df_confirm['num_request']).round(2)
    result = (
        signups
        .merge(
            df_confirm,
            on=['user_id'],
            how='left',
        )
        .fillna(0.)
    )
    return result[['user_id','confirmation_rate']].fillna(0.)
