from .services.data_service import Data_Service as ds

BIG_NUM_NAMES = ["services_total", "undup_hh_total", "undup_indv_total", "services_per_uhh_avg"]
DEFAULT_CTRL = "dummy_is_grocery_service"
DEFAULT_CTRL_VAL = "1"

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
        if "control_type_field" not in input_dict["Scope"]:
            input_dict["Scope"]["control_type_field"] = "dummy_is_grocery_service"

        # Setting the control type value
        if "control_type_value" not in input_dict["Scope"]:
            input_dict["Scope"]["control_type_value"] = 1

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

# slide 67
def __get_age_group_count(id, params):
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
        67: __get_age_group_count
    }
