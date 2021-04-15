from pandas.core.frame import DataFrame
import dateutil.parser as parser
import pandas as pd
import json
import numpy as np

#data def 57
def get_service_trend_time_month(data: 'dict[DataFrame]'):
    services = data[0]
    skeleton_month = data[3]

    trend:DataFrame = skeleton_month.merge(services, how ='left', on = 'calendaryearmonth')
    trend = trend.groupby(['calendaryearmonth','calendaryearmonth_start','calendaryearmonth_name'], as_index=False).agg(n = ('research_service_key', 'count'))
    return trend.to_json()

#data def 58
def get_service_trend_time_week(data: 'dict[DataFrame]'):
    services = data[0]
    skeleton_week = data[4]

    trend:DataFrame = skeleton_week.merge(services, how ='left', on = 'sunyearweek')
    trend = trend.groupby(['sunyearweek','sunyearweek_start'], as_index=False).agg(n = ('research_service_key', 'count'))
    return trend.to_json()

# data def 59
def get_service_trend_time_day(data: 'list[DataFrame]'):
    services = data[0]
    skeleton_day = data[5]

    trend:DataFrame = skeleton_day.merge(services, how='left', on='date')
    trend = trend.groupby(['date','date_label'], as_index=False).agg(n=('research_service_key','count'))
    return trend.to_json()

# data def 60
def get_service_trend_monthy_visits_avg(data:'list[DataFrame]'):
    services = data[0]
    skeleton_month = data[3]

    trend:DataFrame = skeleton_month.merge(services, how='left', on = 'calendaryearmonth')
    trend = trend.groupby(['research_family_key','calendaryearmonth','calendaryearmonth_start','calendaryearmonth_name'], as_index=False, dropna=False).agg(
        n_services= ('calendaryearmonth','count')
    )
    trend = trend.groupby(['calendaryearmonth','calendaryearmonth_start','calendaryearmonth_name'], as_index=False, dropna=False).agg(
        n_families=('research_family_key','count'),
        n_services=('n_services','sum')
    )
    trend['services_per_family'] = round(trend['n_services']/trend['n_families'], 2)
    return trend.to_json()

# data def 64
def get_service_trend_comparison(data: 'list[DataFrame]'):
    services = data[0]
    skeleton_day = data[5]

    max_calendaryearmonth = services['calendaryearmonth'].max()
    monthofyear = services[services['calendaryearmonth'] == max_calendaryearmonth]
    unique_month = monthofyear['monthofyear'].unique()[0]

    services = services[services['monthofyear'] == unique_month].groupby(['calendaryear', 'calendaryearmonth', 'date'], as_index = False).agg(n = ('date', 'count'))
    services = pd.merge(services, skeleton_day, how = 'inner', on = 'date')
    services['order'] = services.index + 1

    return services.to_json()

