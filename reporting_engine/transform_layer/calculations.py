import numpy as np
from .services.data_service import Data_Service as ds
import json

import numpy


BIG_NUM_NAMES = ["services_total", "undup_hh_total", "undup_indv_total", "services_per_uhh_avg"]
DEFAULT_CTRL = "Is Grocery Service"

class CalculationDispatcher:
    def __init__(self, request):

        # now on construction, it will automatically run parse request on the input request, so theres no extra in between step
        self.request = self.parse_request(request)
        data_list = request["ReportInfo"]
        self.params = request["Scope"]
        self.data_dict = CalculationDispatcher.__group_by_data_def(data_list)
        
    @staticmethod
    def __group_by_data_def(data_list):
        """Returns dict of data defs grouped by reportDictid and sorted by dataDefid
        
        data_dict is a dictionary that groups the data definitions in data_list by reportDictId
        and sorts the data definitions in each group by their dataDefId, highest to smallest.
        Does not modify data_list.
        data_dict = { 
            1: [{"reportDictId": 1, "dataDefId": 1 },   {"reportDictId": 1, "dataDefId": 2 }, ... ],
            2:  [{"reportDictId": 2, "dataDefId": 5 },   {"reportDictId": 2, "dataDefId": 6 }, ... ],
            3:  [{"reportDictId": 3, "dataDefId": 19 },   {"reportDictId": 3, "dataDefId": 20 }, ... ],
        }
        
        """

        data_dict = {}
        for item in data_list:
            entry_list = data_dict.get(item["reportDictId"])
            if entry_list is None:
                pos = item["reportDictId"]
                data_dict[pos] = [item]
            else:
                entry_list.append(item)

        for entry_list in data_dict.values():
            entry_list.sort(key = lambda e: e["dataDefId"])
        return data_dict
        
    
    #runs calculation on each data_def in data_dict
    #and appends the result of the calculation to the data_def
    #modifies: self.request
    #returns the modified data_defs as a list
    def run_calculations(self):
        for group in self.data_dict.values():
            for data_def in group:
                func = data_calc_function_switcher[data_def["dataDefId"]]
                result = func(data_def["dataDefId"], self.params)
                data_def["value"] = result

        return self.request["ReportInfo"]

    # static callable parse request
    @staticmethod
    def parse_request(input_dict):
        # Setting the scope type
        scope_field = input_dict["Scope"]["scope_field"]
        if scope_field.startswith("fip"):
            input_dict["Scope"]["scope_type"] = "geography"
        else:
            input_dict["Scope"]["scope_type"] = "hierarchy"
        
        # Setting the control type
        if "control_type_name" not in input_dict["Scope"]:
            input_dict["Scope"]["control_type_name"] = DEFAULT_CTRL

        return input_dict

#Big Numbers(Default Engine MVP)
def __get_total_hh_services(id, params):
    """Calculate number of households/individuals served (based on filter) DataDef 1, 2, 3, 5, 6, 7, 20, 21, & 22

    Arguments:
    id - data definiton id
    params - a dictionary of values to scope the queries

    Modifies:
    Nothing

    Returns: num_households
    num_households - number of households served for a specific filter based on id

    """
    return len(ds.get_data_for_definition(id, params))

#data def 4
def __get_services_per_uhh_avg(id, params):
    """Calculate number of services per family DataDef 4

    Arguments:
    id - data definiton id
    params - a dictionary of values to scope the queries

    Modifies:
    Nothing

    Returns: num_services_avg
    num_services_avg - average number of services per family

    """
    services, families = ds.get_data_for_definition(id, params)
    return len(services)/len(families)

## Ohio Addin
# data def 8, 9, 10
def __get_indv_senior(id, params):
    """Calculate number of seniors served DataDef 8, 9, & 10

    Arguments:
    id - data definiton id
    params - a dictionary of values to scope the queries

    Modifies:
    Nothing

    Returns: num_seniors
    num_seniors - number of seniors served

    """
    return ds.get_data_for_definition(id, params)['served_seniors'].sum()

# data def 11, 12, 13
def __get_indv_adult(id, params):
    """Calculate number of adults served DataDef 11, 12, & 13

    Arguments:
    id - data definiton id
    params - a dictionary of values to scope the queries

    Modifies:
    Nothing

    Returns: num_adults
    num_adults - number of adults served

    """
    return ds.get_data_for_definition(id, params)['served_adults'].sum()

# data def 14, 16
def __get_indv_child(id, params):
    """Calculate number of children served DataDef 14 & 16

    Arguments:
    id - data definiton id
    params - a dictionary of values to scope the queries

    Modifies:
    Nothing

    Returns: num_children
    num_children - number of children served

    """
    return ds.get_data_for_definition(id, params)['served_children'].sum()

# data def 15
def __get_indv_child_hh_wominor(id, params):
    return 0

# data def 17, 18, 19
def __get_indv_total(id, params):
    """Calculate number of people served DataDef 17, 18, & 19

    Arguments:
    id - data definiton id
    params - a dictionary of values to scope the queries

    Modifies:
    Nothing

    Returns: num_served
    num_served - number of people served

    """
    return ds.get_data_for_definition(id, params)['served_total'].sum()

# data def 23
def __get_services_summary(id, params):
    """Calculate number of people served DataDef 23

    Arguments:
    id - data definiton id
    params - a dictionary of values to scope the queries

    Modifies:
    Nothing

    Returns: num_served
    num_served - number of people served by service name

    """
    base_services = ds.get_data_for_definition(id, params).groupby(['service_name'])
    base_services = base_services.agg({'research_family_key': 'count', 'served_total': 'sum'})
    base_services = base_services.reset_index().rename(columns={'research_family_key':"Families Served", 'served_total': 'People Served'})
    return base_services.to_json()

# data def 24
def __get_services_category(id, params):
    """Calculate number of people served DataDef 24

    Arguments:
    id - data definiton id
    params - a dictionary of values to scope the queries

    Modifies:
    Nothing

    Returns: num_served
    num_served - number of people served by service category

    """
    base_services = ds.get_data_for_definition(id, params).groupby(['service_category_name'])
    base_services = base_services.agg({'research_family_key': 'count', 'served_total': 'sum'})
    base_services = base_services.reset_index().rename(columns={'research_family_key':"Families Served", 'served_total': 'People Served'})
    return base_services.to_json()

# data def 25
def __get_distribution_outlets(id, params):
    """Calculate number of people served DataDef 25

    Arguments:
    id - data definiton id
    params - a dictionary of values to scope the queries

    Modifies:
    Nothing

    Returns: sites_visited
    sites_visited - number of families that have made 1..n site visits

    """
    base_services = ds.get_data_for_definition(id, params)
    base_services = base_services.groupby('research_family_key')['loc_id'].nunique().reset_index().rename(columns={'loc_id': 'sites_visited'})
    base_services = base_services.groupby('sites_visited').agg(un_duplicated_families = ('sites_visited', 'count')).reset_index()
    base_services = base_services.sort_values(by = ['sites_visited'], ascending = [True])
    return base_services.to_json()

#data def 26/27 (return same data, outputted graph just has different y axis depending on def )
def __get_frequency_visits(id, params):
    families = ds.get_data_for_definition(id, params)
    families = families.groupby(['num_services'])
    families = families.agg({'research_family_key': 'count', 'num_services': 'sum'})
    largeSum = families.iloc[24:].sum()
    families.at[25, 'research_family_key'] = largeSum.iloc[0]
    families.at[25, 'num_services'] = largeSum.iloc[1]
    families = families.rename(columns={'research_family_key' :'n_families', 'num_services':'sum_services'})
    families = families.head(25)
    return families.to_json()

#data def 28
def __get_household_composition(id, params):
    families = ds.get_data_for_definition(id, params)
    
    families = families.groupby('family_composition_type').agg(num_families = ('family_composition_type', 'count')).reset_index()
    return families.to_json()

#data def 29
def __get_family_comp_key_insight(id, params):
    families = ds.get_data_for_definition(id, params)
    families = families.groupby('family_composition_type').agg(num_families = ('family_composition_type', 'count'))

    def choose_group(index_name):
        if index_name.find("child") >= 0 or index_name.find("senior") >= 0:
            return "has_child_senior"
        else:
            return "no_child_senior"
    families = families.groupby(by = choose_group).sum().reset_index()
    families = families.rename(columns = {"index":"family_composition_type"})


    #reset the index at the end

    return families.to_json()

#data def 30
def __get_household_size_distribution_1_to_10(id, params):
    """Calculate Families Breakdown DataDef 30

    Arguments:
    id - data definiton id
    params - a dictionary of values to scope the queries

    Modifies:
    Nothing

    Returns: num_families
    num_families - number of families per sizes 1 to 10

    """

    families = ds.get_data_for_definition(id, params)
    families.avg_fam_size = families.avg_fam_size.round()
    families['avg_fam_size_roll'] = np.where(families['avg_fam_size'] > 9, 10, families['avg_fam_size'])
    families['avg_fam_size_roll'] = np.where(families['avg_fam_size_roll'] == 0, 1, families['avg_fam_size_roll'])
    families = families.groupby('avg_fam_size_roll').agg(num_families = ('avg_fam_size_roll', 'count')).reset_index()

    conditions = [(families['avg_fam_size_roll'] < 4), (families['avg_fam_size_roll'] < 7), (families['avg_fam_size_roll'] >= 7)]
    choices = ['1 - 3', '4 - 6', '7+']

    families['classic_roll'] = np.select(conditions, choices)

    return families.to_json()

#data def 31
def __get_household_size_distribution_classic(id, params):
    families = ds.get_data_for_definition(id, params)

    families = families.groupby('avg_fam_size').count()

    """ for i in range(len(families)):
         """

    framework_dict = families.to_dict()
    framework_dict = framework_dict['research_family_key']

    return_dict = {
        '1 - 3':0,
        '4 - 6':0,
        '7+':0
    }

    for key in framework_dict:
        if key >= 0 and key < 3.5:
            return_dict['1 - 3'] = return_dict['1 - 3'] + framework_dict[key]
        elif key >= 3.5 and key < 6.5:
            return_dict['4 - 6'] = return_dict['4 - 6'] + framework_dict[key]
        elif key >= 6.5:
            return_dict['7+'] = return_dict['7+'] + framework_dict[key]

    return json.dumps(return_dict)

# slide 67
def __get_age_group_count(id, params):
    return ds.get_data_for_definition(id,params)

# slide 73
def __get_age_groups_at_least_one(id, params):
    return ds.get_data_for_definition(id,params)

## Data Defintion Switcher
# usage:
#   func = data_calc_function_switcher.get(id)
#   func()
data_calc_function_switcher = {
        1: __get_total_hh_services,
        2: __get_total_hh_services,
        3: __get_total_hh_services,
        4: __get_services_per_uhh_avg,
        5: __get_total_hh_services,
        6: __get_total_hh_services,
        7: __get_total_hh_services,
        8: __get_indv_senior,
        9: __get_indv_senior,
        10: __get_indv_senior,
        11: __get_indv_adult,
        12: __get_indv_adult,
        13: __get_indv_adult,
        14: __get_indv_child,
        15: __get_indv_child_hh_wominor,
        16: __get_indv_child,
        17: __get_indv_total,
        18: __get_indv_total,
        19: __get_indv_total,
        20: __get_total_hh_services,
        21: __get_total_hh_services,
        22: __get_total_hh_services,
        23: __get_services_summary,
        24: __get_services_category,
        25: __get_distribution_outlets,
        26: __get_frequency_visits,
        27: __get_frequency_visits,
        28: __get_household_composition,
        29: __get_family_comp_key_insight,
        30: __get_household_size_distribution_1_to_10,
        31: __get_household_size_distribution_classic,
        67: __get_age_group_count,
        73: __get_age_groups_at_least_one
    }
