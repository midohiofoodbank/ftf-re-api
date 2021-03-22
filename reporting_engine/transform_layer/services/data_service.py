from pandas.core.frame import DataFrame
import dateutil.parser as parser
import pandas as pd
from django.db import connections

import copy

SCOPE_HIERARCHY = "hierarchy"
SCOPE_GEOGRAPHY = "geography"

class Data_Service:
    __scope:str = None
    __fact_services:DataFrame = None
    ##  getter and setter for __fact_services based on the scope "hierarchy" or "geography"
    ##  Columns always in services:
    ##      research_service_key
    ##      service_status
    ##      service_id
    ##      research_family_key
    ##      research_member_key
    ##      served_children
    ##      served_adults
    ##      served_seniors
    ##      served_total
    ##      Additional columns depending on params:
    ##      hierarchy_id - if scope_type is "hierarchy"
    ##      dimgeo_id - if scope_type is "geography"
    @classmethod
    def fact_services(cls,params):
        if cls.__fact_services is None:
            cls.__fact_services = cls.__get_fact_services(params)
        return cls.__fact_services
    
    __base_services:DataFrame = None
    ## getter and setter for __base_services
    ##  Columns always in services:
    ##      research_service_key
    ##      research_family_key
    ##      service_id
    ##      service_name
    ##      service_category_code
    ##      service_category_name
    ##      served_total
    ##      loc_id
    @classmethod
    def base_services(cls,params):
        if cls.__base_services is None:
            cls.__base_services = cls.__get_base_services(params)
        return cls.__base_services

    __age_services:DataFrame = None
    ## getter and setter for __age_services
    ##  Columns always in services:
    ##      research_service_key
    ##      research_family_key
    ##      service_id
    ##      service_name
    ##      service_category_code
    ##      service_category_name
    ##      served_total
    ##      loc_id
    @classmethod
    def age_services(cls,params):
        if cls.__age_services is None:
            cls.__age_services = cls.__get_age_services(params)
        return cls.__age_services

    ## returns DataFrame for a specific data definition
    @classmethod
    def get_data_for_definition(cls, id, params):
        if( params != cls.__scope):
            cls.__fact_services = None
            cls.__base_services = None
            cls.__age_services = None
            cls.__scope = copy.deepcopy(params)
        func = cls.data_def_function_switcher.get(id, cls.get_data_def_error)
        return func(params)

    ## retrieves fact_services
    @classmethod
    def __get_fact_services(cls, params):
        conn = connections['source_db']

        table1 = ""
        left1 = right1 = ""

        if params["scope_type"] == "hierarchy":
            table1 = "dim_hierarchies"
            left1 = right1 = "hierarchy_id"
        elif params["scope_type"] == "geography":
            table1 = "dim_geos"
            left1 = "dimgeo_id"
            right1 = "id"

        control_type_field = params["control_type_field"]
        control_type_value = params["control_type_value"]
        scope_field = params["scope_field"]
        scope_value = params["scope_field_value"]
        start_date = cls.__date_str_to_int(params["startDate"])
        end_date = cls.__date_str_to_int(params["endDate"])

        query = f"""
        SELECT
            fs.research_service_key,
            fs.{left1},
            fs.service_status,
            fs.service_id,
            fs.research_family_key,
            fs.served_children,
            fs.served_adults,
            fs.served_seniors,
            fs.served_total,
            fsm.research_member_key
        FROM 
            fact_services AS fs
            LEFT JOIN {table1} AS t1 ON fs.{left1} = t1.{right1}
            LEFT JOIN dim_service_statuses ON fs.service_status = dim_service_statuses.status 
            LEFT JOIN fact_service_members AS fsm ON fs.research_service_key = fsm.research_service_key
        WHERE
            fs.service_status = 17 AND
            t1.{scope_field} = {scope_value} AND
            fs.date >= {start_date} AND fs.date <= {end_date}
        """

        query_control = f'SELECT id, {control_type_field} FROM dim_service_types'

        services = pd.read_sql(query, conn)
        service_types = pd.read_sql(query_control, conn)
        services = services.merge(service_types, how = 'left', left_on= 'service_id', right_on = 'id')
        services = services.query(f'{control_type_field} == {control_type_value}')
        return services

    @classmethod
    def __get_base_services(cls, params):
        conn = connections['source_db']

        control_type_field = params["control_type_field"]
        control_type_value = params["control_type_value"]
        scope_field = params["scope_field"]
        scope_value = params["scope_field_value"]
        start_date = cls.__date_str_to_int(params["startDate"])
        end_date = cls.__date_str_to_int(params["endDate"])

        query = f"""
        SELECT
            dm_fs.research_service_key,
            dm_fs.research_family_key,
            dm_fs.service_id,
            dim_service_types.name as service_name,
            dm_fs.service_category_code,
            dim_service_types.service_category_name,
            dm_fs.served_total,
            dm_fs.loc_id
        FROM
            dm_fact_services as dm_fs
            INNER JOIN dim_service_types ON dm_fs.service_id = dim_service_types.id
        WHERE
            dm_fs.service_status = 17 AND
            dm_fs.{control_type_field} = {control_type_value} AND
            dm_fs.{scope_field} = {scope_value} AND
            dm_fs.date >= {start_date} AND dm_fs.date <= {end_date}
        """
        return pd.read_sql(query, conn)

    ## retrieves age_services
    @classmethod
    def __get_age_services(cls, params):
        conn = connections['source_db']

        table1 = ""
        left1 = right1 = ""

        if params["scope_type"] == "hierarchy":
            table1 = "dim_hierarchies"
            left1 = right1 = "hierarchy_id"
        elif params["scope_type"] == "geography":
            table1 = "dim_geos"
            left1 = "dimgeo_id"
            right1 = "id"

        control_type_field = params["control_type_field"]
        control_type_value = params["control_type_value"]
        scope_field = params["scope_field"]
        scope_value = params["scope_field_value"]
        start_date = cls.__date_str_to_int(params["startDate"])
        end_date = cls.__date_str_to_int(params["endDate"])
        age_grouping_id = params["age_grouping_id"]

        service_query = f"""
        SELECT
            fs.research_service_key,
            fs.{left1},
            fs.service_status,
            fs.service_id,
            fs.research_family_key,
            fsm.research_member_key,
            members.current_age,
            members.gender,
            ages.age_grouping_id,
            ages.age_band_name_dash,
            ages.age,
            ages.start_age,
            ages.end_age
        FROM 
            fact_services AS fs
            LEFT JOIN {table1} AS t1 ON fs.{left1} = t1.{right1}
            LEFT JOIN dim_service_statuses ON fs.service_status = dim_service_statuses.status 
            LEFT JOIN fact_service_members AS fsm ON fs.research_service_key = fsm.research_service_key
            LEFT JOIN dim_members AS members ON fsm.research_member_key = members.research_member_key
            LEFT JOIN dim_ages AS ages ON members.current_age = ages.age
        WHERE
            fs.service_status = 17 AND
            t1.{scope_field} = {scope_value} AND
            fs.date >= {start_date} AND fs.date <= {end_date} AND
            ages.age_grouping_id = {age_grouping_id}
        """

        query_control = f"""SELECT id, {control_type_field} FROM dim_service_types"""

        services = pd.read_sql(service_query, conn)
        service_types = pd.read_sql(query_control, conn)
        services = services.merge(service_types, how = 'left', left_on= 'service_id', right_on = 'id')
        services = services.query(f'{control_type_field} == {control_type_value}')

        services = pd.read_sql(service_query, conn)
        return services

    @staticmethod
    def __date_str_to_int(date):
        dt = parser.parse(date,dayfirst = False)
        date_int = (10000*dt.year)+ (100 * dt.month) + dt.day 
        return date_int

    ## DataFrame to fulfill Data Definition 1
    ####    Returns: services
    ####        families - unduplicated families data table
    @staticmethod
    def __get_num_services(params):
        services = Data_Service.fact_services(params)
        return services.drop_duplicates(subset = 'research_service_key', inplace = False)
    
    ## DataFrame to fulfill Data Definition 2
    ####    Returns: services
    ####        families - unduplicated families data table
    @staticmethod
    def __get_undup_hh(params):
        services = Data_Service.fact_services(params)
        return services.drop_duplicates(subset = 'research_family_key', inplace = False)
    
    ## DataFrame to fulfill Data Definiton 3
    ####    Returns: services
    ####        inidividuals - unduplicated individuals data table
    @staticmethod
    def __get_undup_indv(params):
        services = Data_Service.fact_services(params)
        return services.drop_duplicates(subset = 'research_member_key', inplace = False)
    
    ## DataFrames to fulfill Data Definiton 4
    ####    Returns: (services, families)
    ####        services - fact service data table
    ####        families - unduplicated families data table
    @staticmethod
    def __fact_services_and_uhh(params):
        return Data_Service.__get_num_services(params), Data_Service.__get_undup_hh(params)
    
    ## DataFrame to fulfill Data Definitions 5, 14, 16, 17
    ####    Returns: services
    ####        services - fact service data table, filtered on served_children > 0
    @staticmethod
    def __get_wminor(params):
        services = Data_Service.fact_services(params).drop_duplicates(subset = 'research_service_key', inplace = False)
        return services[services['served_children']>0]
    
    ## DataFrame to fulfill Data Definition 6
    ####    Returns: services
    ####        services - fact service data table, filtered on served_children == 0
    @staticmethod
    def __get_wominor(params):
        services = Data_Service.fact_services(params).drop_duplicates(subset = 'research_service_key', inplace = False)
        return services[services['served_children']==0]
    
    ## DataFrame to fulfill Data Definitions 8, 18, 22
    ####    Returns: sen_hh_wminor
    ####        sen_hh_wminor - fact service data table, filtered on served_children > 0 and served_seniors > 0
    @staticmethod
    def __get_sen_wminor(params):
        seniors = Data_Service.__get_sen(params).drop_duplicates(subset = 'research_service_key', inplace = False)
        return seniors[seniors['served_children']>0]
    
    ## DataFrame to fulfill Data Definition 9
    ####    Returns: sen_hh_wominor
    ####        sen_hh_wominor - fact service data table, filtered on served_children == 0 and served_seniors > 0
    @staticmethod
    def __get_sen_wominor(params):
        seniors = Data_Service.__get_sen(params).drop_duplicates(subset = 'research_service_key', inplace = False)
        return seniors[seniors['served_children']==0]
    
    ## DataFrame to fulfill Data Definitions 10, 20
    ####    Returns: sen_hh
    ####        sen_hh - fact service data table, filtered on served_seniors > 0
    @staticmethod
    def __get_sen(params):
        services = Data_Service.fact_services(params).drop_duplicates(subset = 'research_service_key', inplace = False)
        return services[services['served_seniors']>0]
    
    ## DataFrame to fulfill Data Definition 11
    ####    Returns: adult_hh_wminor
    ####        adult_hh_wminor - fact service data table, filtered on served_children > 0 and served_adults > 0
    @staticmethod
    def __get_adult_wminor(params):
        adults = Data_Service.__get_adult(params).drop_duplicates(subset = 'research_service_key', inplace = False)
        return adults[adults['served_children']>0]
    
    ## DataFrame to fulfill Data Definition 12
    ####    Returns: adult_hh_wominor
    ####        adult_hh_wominor - fact service data table, filtered on served_children == 0 and served_adults > 0
    @staticmethod
    def __get_adult_wominor(params):
        adults = Data_Service.__get_adult(params).drop_duplicates(subset = 'research_service_key', inplace = False)
        return adults[adults['served_children']==0]
    
    ## DataFrame to fulfill Data Definition 13
    ####    Returns: adult_hh
    ####        adult_hh - fact service data table, filtered on served_adults > 0
    @staticmethod
    def __get_adult(params):
        services = Data_Service.fact_services(params).drop_duplicates(subset = 'research_service_key', inplace = False)
        return services[services['served_adults']>0]
    
    ## DataFrame to fulfill Data Definition 15
    ####    Returns: empty
    ####        empty - empty data table (no such thing as children wo minors)
    @staticmethod
    def __get_child_wominor(params):
        return DataFrame()
    
    ## DataFrame to fulfill Data Definition 21
    ####    Returns services_wosenior
    ####        services_wosenior - fact service data table, filtered on served_serniors == 0
    @staticmethod
    def __get_wosenior(params):
        services = Data_Service.fact_services(params).drop_duplicates(subset = 'research_service_key', inplace = False)
        return services[services['served_seniors']==0]

    ## DataFrame to fulfill Data Definition 23, 24, 25
    ####    Returns base_services
    @staticmethod
    def __get_service_summary(params):
        return Data_Service.base_services(params)

    ## DataFrame to fulfill Slide 67, 73
    ####    Returns age_services
    @staticmethod
    def __get_age_group_count(params):
        services = Data_Service.age_services(params)
        print(services)
        return Data_Service.age_services(params)

    ## error, none
    @staticmethod
    def get_data_def_error(params):
        return DataFrame()

    ## Data Defintion Switcher
    # usage:
    #   func = data_def_function_switcher.get(id)
    #   func()
    data_def_function_switcher = {
            1: __get_num_services.__func__,
            2: __get_undup_hh.__func__,
            3: __get_undup_indv.__func__,
            4: __fact_services_and_uhh.__func__,
            5: __get_wminor.__func__,
            6: __get_wominor.__func__,
            7: __get_num_services.__func__,
            8: __get_sen_wminor.__func__,
            9: __get_sen_wominor.__func__,
            10: __get_sen.__func__,
            11: __get_adult_wminor.__func__,
            12: __get_adult_wominor.__func__,
            13: __get_adult.__func__,
            14: __get_wminor.__func__,
            15: __get_child_wominor.__func__,
            16: __get_wminor.__func__,
            17: __get_wminor.__func__,
            18: __get_wominor.__func__,
            19: __get_num_services.__func__,
            20: __get_sen.__func__,
            21: __get_wosenior.__func__,
            22: __get_sen_wminor.__func__,
            23: __get_service_summary.__func__,
            24: __get_service_summary.__func__,
            25: __get_service_summary.__func__,
            67: __get_age_group_count.__func__,
            73: __get_age_group_count.__func__
        }

