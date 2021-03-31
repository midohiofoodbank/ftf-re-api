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

    __family_services:DataFrame = None
    ## getter and setter for __family_services
    @classmethod
    def family_services(cls, params):
        if cls.__family_services is None:
            cls.__family_services = cls.__get_family_services(params)
        return cls.__family_services

    __new_family_services:'list[DataFrame]' = None
    ## getter and setting for __new_family_services
    @classmethod
    def new_family_services(cls, params):
        if cls.__new_family_services is None:
            cls.__new_family_services = cls.__get_new_family_services(params)
        return cls.__new_family_services

    ## returns DataFrame for a specific data definition
    @classmethod
    def get_data_for_definition(cls, id, params):
        if( params != cls.__scope):
            cls.__fact_services = None
            cls.__base_services = None
            cls.__family_services = None
            cls.__new_family_services = None
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

        control_type_name = params["control_type_name"]
        control_query = cls.__get_control_query(control_type_name)
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
            INNER JOIN dim_service_types ON fs.service_id = dim_service_types.id
            LEFT JOIN {table1} AS t1 ON fs.{left1} = t1.{right1}
            LEFT JOIN dim_service_statuses ON fs.service_status = dim_service_statuses.status 
            LEFT JOIN fact_service_members AS fsm ON fs.research_service_key = fsm.research_service_key
        WHERE
            fs.service_status = 17
            AND {control_query}
            AND t1.{scope_field} = {scope_value}
            AND fs.date >= {start_date} AND fs.date <= {end_date}
        """

        return pd.read_sql(query, conn)

    @classmethod
    def __get_base_services(cls, params):
        conn = connections['source_db']

        extra_join = ""
        if params["scope_type"] == "hierarchy":
            table1 = "dim_hierarchies"
            left1 = right1 = "hierarchy_id"
        elif params["scope_type"] == "geography":
            table1 = "dim_geos"
            left1 = "dimgeo_id"
            right1 = "id"
            extra_join = """INNER JOIN dim_hierarchies ON fact_services.hierarchy_id = dim_hierarchies.loc_id"""

        control_type_name = params["control_type_name"]
        control_query = cls.__get_control_query(control_type_name)
        scope_field = params["scope_field"]
        scope_value = params["scope_field_value"]
        start_date = cls.__date_str_to_int(params["startDate"])
        end_date = cls.__date_str_to_int(params["endDate"])

        query = f"""
        SELECT
            fact_services.research_service_key,
            fact_services.research_family_key,
            fact_services.service_id,
            dim_service_types.name as service_name,
            dim_service_types.service_category_code,
            dim_service_types.service_category_name,
            fact_services.served_total,
            dim_hierarchies.loc_id
        FROM
            fact_services
            INNER JOIN dim_service_types ON fact_services.service_id = dim_service_types.id
            INNER JOIN {table1} ON fact_services.{left1} = {table1}.{right1}
            {extra_join if params["scope_type"] == "geography" else ""}
        WHERE
            fact_services.service_status = 17 
            AND {control_query}
            AND fact_services.date >= {start_date} AND fact_services.date <= {end_date}
            AND {table1}.{scope_field} = {scope_value}
        """
        return pd.read_sql(query, conn)

    @classmethod
    def __get_family_services(cls, params):
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

        control_type_name = params["control_type_name"]
        control_query = cls.__get_control_query(control_type_name)
        scope_field = params["scope_field"]
        scope_value = params["scope_field_value"]
        start_date = cls.__date_str_to_int(params["startDate"])
        end_date = cls.__date_str_to_int(params["endDate"])

        query = f"""
        SELECT
            fact_services.research_family_key,
            COUNT(fact_services.research_service_key) AS num_services,
            AVG(fact_services.served_total) AS avg_fam_size,
            SUM(fact_services.is_first_service_date) as timeframe_has_first_service_date,
            AVG(fact_services.days_since_first_service) AS avg_days_since_first_service,
            MAX(fact_services.days_since_first_service) AS max_days_since_first_service,
            dim_family_compositions.family_composition_type
        FROM 
            fact_services
            INNER JOIN dim_families ON fact_services.research_family_key = dim_families.research_family_key
            INNER JOIN dim_family_compositions ON dim_families.family_composition_type = dim_family_compositions.id
            INNER JOIN dim_service_types ON fact_services.service_id = dim_service_types.id
            INNER JOIN {table1}  ON fact_services.{left1} = {table1}.{right1}
        WHERE
            fact_services.service_status = 17 
            AND {control_query}
            AND fact_services.date >= {start_date} AND fact_services.date <= {end_date}
            AND {table1}.{scope_field} = {scope_value}
        GROUP BY
            fact_services.research_family_key,
            dim_family_compositions.family_composition_type
        """
        
        return pd.read_sql(query, conn)

    @classmethod
    def __get_new_family_services(cls, params):
        conn = connections['source_db']

        extra_join = ""
        if params["scope_type"] == "hierarchy":
            table1 = "dim_hierarchies"
            left1 = right1 = "hierarchy_id"
        elif params["scope_type"] == "geography":
            table1 = "dim_geos"
            left1 = "dimgeo_id"
            right1 = "id"
            extra_join = """INNER JOIN dim_hierarchies ON fs.hierarchy_id = dim_hierarchies.loc_id"""

        control_type_name = params["control_type_name"]
        control_query = cls.__get_control_query(control_type_name)
        scope_field = params["scope_field"]
        scope_value = params["scope_field_value"]
        start_date = cls.__date_str_to_int(params["startDate"])
        end_date = cls.__date_str_to_int(params["endDate"])

        services_query = f"""
        SELECT
            fs.research_service_key,
            fs.research_family_key,
            fs.service_id,
            dim_service_types.name as service_name,
            dim_service_types.service_category_code,
            dim_service_types.service_category_name,
            fs.served_total,
            dim_hierarchies.loc_id,
            fs.is_first_service_date
        FROM
            fact_services AS fs
            INNER JOIN dim_service_types ON fs.service_id = dim_service_types.id
            INNER JOIN {table1} ON fs.{left1} = {table1}.{right1}
            INNER JOIN dim_dates ON fs.date = dim_dates.date_key
            {extra_join if params["scope_type"] == "geography" else ""}
        WHERE
            fs.service_status = 17 
            AND {control_query}
            AND fs.date >= {start_date} AND fs.date <= {end_date}
            AND {table1}.{scope_field} = {scope_value}
        """

        families_query = f"""
            SELECT
                fs.research_family_key,
                COUNT( fs.research_service_key ) AS num_services,
                AVG( fs.served_total ) AS avg_fam_size,
                SUM( fs.is_first_service_date ) AS timeframe_has_first_service_date,
                AVG( fs.days_since_first_service ) AS avg_days_since_first_service,
                MAX( fs.days_since_first_service ) AS max_days_since_first_service,
                dim_family_compositions.family_composition_type,
                dim_families.datekey_first_service,
                dim_families.dummy_use_geo,
                dim_families.latitude_5,
                dim_families.longitude_5,
                dim_families.dimgeo_id,
                dim_geos.fips_state,
                dim_geos.fips_cnty,
                dim_geos.fips_zcta
            FROM
                fact_services AS fs
                INNER JOIN dim_families ON fs.research_family_key = dim_families.research_family_key
                INNER JOIN dim_family_compositions ON dim_families.family_composition_type = dim_family_compositions.id
                INNER JOIN dim_service_types ON fs.service_id = dim_service_types.id
                INNER JOIN dim_dates ON fs.date = dim_dates.date_key
                INNER JOIN {table1} ON fs.{left1} = {table1}.{right1}
                LEFT JOIN dim_geos ON dim_families.dimgeo_id = dim_geos.id
                {extra_join if params["scope_type"] == "geography" else ""}
            WHERE
                fs.service_status = 17
                AND {control_query}
                AND fs.date >= {start_date} AND fs.date <= {end_date}
                AND {table1}.{scope_field} = {scope_value}
            GROUP BY
                fs.research_family_key,
                dim_family_compositions.family_composition_type,
                dim_families.datekey_first_service,
                dim_families.dummy_use_geo,
                dim_families.latitude_5,
                dim_families.longitude_5,
                dim_families.dimgeo_id,
                dim_geos.fips_state,
                dim_geos.fips_cnty,
                dim_geos.fips_zcta
        """

        members_query = f"""
        SELECT
            fs_mem.research_member_key,
            COUNT( fs.research_service_key ) AS num_services,
            SUM( fs_mem.is_first_service_date ) AS timeframe_has_first_service_date,
            AVG( fs_mem.days_since_first_service ) AS avg_days_since_first_service,
            MAX( fs_mem.days_since_first_service ) AS max_days_since_first_service,
            dim_members.datekey_first_served,
            dim_members.gender,
            dim_members.current_age,
            dim_members.race_id,
            dim_members.ethnic_id,
            dim_members.immigrant_id,
            dim_members.language_id,
            dim_members.disability_id,
            dim_members.military_id,
            dim_members.healthcare_id,
            dim_members.education_id,
            dim_members.employment_id,
            dim_families.datekey_first_service AS dim_families_datekey_first_service,
            SUM( fs.is_first_service_date ) AS dim_families_timeframe_has_first_service_date
        FROM
            fact_services AS fs
            INNER JOIN dim_service_types ON fs.service_id = dim_service_types.id
            INNER JOIN {table1} ON fs.{left1} = {table1}.{right1}
            INNER JOIN dim_dates ON fs.date = dim_dates.date_key
            INNER JOIN fact_service_members AS fs_mem ON fs.research_service_key = fs_mem.research_service_key
            INNER JOIN dim_members ON fs_mem.research_member_key = dim_members.research_member_key
            INNER JOIN dim_families ON dim_members.research_family_key = dim_families.research_family_key
            {extra_join if params["scope_type"] == "geography" else ""}
        WHERE
            fs.service_status = 17
            AND {control_query}
            AND {table1}.{scope_field} = {scope_value}
            AND fs.date >= {start_date} AND fs.date <= {end_date}
        GROUP BY
            fs_mem.research_member_key
        """
        
        services = pd.read_sql(services_query, conn)
        families = pd.read_sql(families_query, conn)
        members = pd.read_sql(members_query, conn)
        
        ## TODO remove, for testing only
        print("SERVICES")
        print(services)
        print("FAMILIES")
        print(families)
        print("MEMBERS")
        print(members)

        return [services, families, members]


    @staticmethod
    def __get_control_query(control_type_name):
        if (control_type_name == "Prepack & Choice only"):
            return f"dim_service_types.service_category_code IN (10, 15)"
        elif (control_type_name == "Produce only"):
            return f"dim_service_types.service_category_code IN (20)"
        else:
            return f"dim_service_types.dummy_is_grocery_service = 1"

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

    ## DataFrame to fulfill Data Definition 26,27, 28, 29, 30, 31
    ## Returns family_services
    @staticmethod
    def __get_household_composition(params):
        return Data_Service.family_services(params)

    ## DataFram to fulfil DataDefinition 34
    @staticmethod
    def __get_new_members_to_old_families(params):
        return Data_Service.new_family_services(params)

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
            26: __get_household_composition.__func__,
            27: __get_household_composition.__func__,
            28: __get_household_composition.__func__,
            29: __get_household_composition.__func__,
            30: __get_household_composition.__func__,
            31: __get_household_composition.__func__,
            34: __get_new_members_to_old_families.__func__
        }
