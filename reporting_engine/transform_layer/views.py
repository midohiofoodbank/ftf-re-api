import time
from django.shortcuts import render
from django.http import HttpResponse
from django.shortcuts import render
from django.db import connections
from print_dict import print_dict, format_dict

from .calculations import CalculationDispatcher
from .services.data_service import Data_Service



def test_data_service(request, id):
    sample_dict = {
        "Scope": {
            "startDate":"01/01/2019",
            "endDate":"1/31/2019",
            "scope_field":"fb_id",
            "scope_field_value":21,
            "control_type_name":"Is Grocery Service"
        },
        "ReportInfo": [
            {
                "reportId":1,
                "reportDictId":1,
                "dataDefId":1,
                "name":"name_one",
                "dataDefType":"type1"
            },
            {
                "reportId":2,
                "reportDictId":2,
                "dataDefId":2,
                "name":"name_two",
                "dataDefType":"type1"
            },
            {
                "reportId":3,
                "reportDictId":3,
                "dataDefId":3,
                "name":"name_three",
                "dataDefType":"type1"
            }
        ]
    }
    params = CalculationDispatcher.parse_request(sample_dict)
    print_dict(params)

    data = Data_Service.get_data_for_definition(id, params)
    print(data)
    return HttpResponse(str(id) + "\t" + str(data))

def get_report_big_numbers(request):
    input_dict = {
        "Scope": {
            "startDate":"01/01/2019",
            "endDate":"12/31/2019",
            "scope_field":"fb_id",
            "scope_field_value":21,
            "control_type_name":"Is Grocery Service"
        },
        "ReportInfo": [
            {
                "reportId":1,
                "reportDictId":1,
                "dataDefId":1,
                "name":"services_total",
                "dataDefType":"type1"
            },
            {
                "reportId":2,
                "reportDictId":2,
                "dataDefId":2,
                "name":"undup_hh_total",
                "dataDefType":"type1"
            },
            {
                "reportId":3,
                "reportDictId":3,
                "dataDefId":3,
                "name":"undup_indv_total",
                "dataDefType":"type1"
            },
            {
                "reportId":4,
                "reportDictId":4,
                "dataDefId":4,
                "name":"services_per_uhh_avg",
                "dataDefType":"type1"
            }
        ]
    }

    # params = parse_request(input_dict)
    cd = CalculationDispatcher(input_dict)
    cd.run_calculations()

    context = { 'report_output': format_dict(cd.request) }
    print_dict(input_dict)
    return render(request, 'transformapi/get-report.html', context)

def get_report_ohio(request):
    input_dict = {
        "Scope": {
            "startDate":"01/01/2019",
            "endDate":"12/31/2019",
            "scope_field":"fb_id",
            "scope_field_value":21,
            "control_type_name":"Is Grocery Service"
        },
        "ReportInfo": [
            {
                "reportId":1,
                "reportDictId":2,
                "dataDefId":5,
                "name":"hh_wminor",
                "dataDefType":"type1"
            },
            {
                "reportId":1,
                "reportDictId":2,
                "dataDefId":6,
                "name":"hh_wominor",
                "dataDefType":"type1"
            },
            {
                "reportId":1,
                "reportDictId":2,
                "dataDefId":7,
                "name":"hh_total",
                "dataDefType":"type1"
            },
            {
                "reportId":1,
                "reportDictId":2,
                "dataDefId":8,
                "name":"indv_sen_hh_wminor",
                "dataDefType":"type1"
            },
            {
                "reportId":1,
                "reportDictId":2,
                "dataDefId":9,
                "name":"indv_sen_hh_wominor",
                "dataDefType":"type1"
            },
            {
                "reportId":1,
                "reportDictId":2,
                "dataDefId":10,
                "name":"indv_sen_total",
                "dataDefType":"type1"
            },
            {
                "reportId":1,
                "reportDictId":2,
                "dataDefId":11,
                "name":"indv_adult_hh_wminor",
                "dataDefType":"type1"
            },
            {
                "reportId":1,
                "reportDictId":2,
                "dataDefId":12,
                "name":"indv_adult_hh_wominor",
                "dataDefType":"type1"
            },
            {
                "reportId":1,
                "reportDictId":2,
                "dataDefId":13,
                "name":"indv_adult_total",
                "dataDefType":"type1"
            },
            {
                "reportId":1,
                "reportDictId":2,
                "dataDefId":14,
                "name":"indv_child_hh_wminor",
                "dataDefType":"type1"
            },
            {
                "reportId":1,
                "reportDictId":2,
                "dataDefId":15,
                "name":"indv_child_hh_wominor",
                "dataDefType":"type1"
            },
            {
                "reportId":1,
                "reportDictId":2,
                "dataDefId":16,
                "name":"indv_child_total",
                "dataDefType":"type1"
            },
            {
                "reportId":1,
                "reportDictId":2,
                "dataDefId":17,
                "name":"indv_total_hh_wminor",
                "dataDefType":"type1"
            },
            {
                "reportId":1,
                "reportDictId":2,
                "dataDefId":18,
                "name":"indv_total_hh_wominor",
                "dataDefType":"type1"
            },
            {
                "reportId":1,
                "reportDictId":2,
                "dataDefId":19,
                "name":"indv_total",
                "dataDefType":"type1"
            }
        ]
    }

    # params = parse_request(input_dict)
    cd = CalculationDispatcher(input_dict)
    cd.run_calculations()

    context = { 'report_output': format_dict(cd.request) }
    print_dict(input_dict)
    return render(request, 'transformapi/get-report.html', context)

def get_report_mofc(request):
    input_dict = {
        "Scope": {
            "startDate":"01/01/2019",
            "endDate":"12/31/2019",
            "scope_field":"fb_id",
            "scope_field_value":21,
            "control_type_name":"Is Grocery Service"
        },
        "ReportInfo": [
            {
                "reportId":1,
                "reportDictId":3,
                "dataDefId":20,
                "name":"hh_wsenior",
                "dataDefType":"type1"
            },
            {
                "reportId":1,
                "reportDictId":3,
                "dataDefId":21,
                "name":"hh_wosenior",
                "dataDefType":"type1"
            },
            {
                "reportId":1,
                "reportDictId":3,
                "dataDefId":22,
                "name":"hh_grandparent",
                "dataDefType":"type1"
            }
        ]
    }

    # params = parse_request(input_dict)
    cd = CalculationDispatcher(input_dict)
    cd.run_calculations()

    context = { 'report_output': format_dict(cd.request) }
    print_dict(input_dict)
    return render(request, 'transformapi/get-report.html', context)

def get_demo1_mofc(request):
    input_dict = {
        "Scope": {
            "startDate":"01/01/2019",
            "endDate":"12/31/2019",
            "scope_field":"fb_id",
            "scope_field_value":21,
            "control_type_name":"Is Grocery Service"
        },
        "ReportInfo": []
    }

    data_def_names = [
        "services_total",
        "undup_hh_total",
        "undup_indv_total",
        "services_per_uhh_avg",
        "hh_wminor",
        "hh_wominor",
        "hh_total",
        "indv_sen_hh_wminor",
        "indv_sen_hh_wominor",
        "indv_sen_total",
        "indv_adult_hh_wminor",
        "indv_adult_hh_wominor",
        "indv_adult_total",
        "indv_child_hh_wminor",
        "indv_child_hh_wominor",
        "indv_child_total",
        "indv_total_hh_wminor",
        "indv_total_hh_wominor",
        "indv_total",
        "hh_wsenior",
        "hh_wosenior",
        "hh_grandparent",
        "service_summary_service",
        "service_summary_category",
        "distribution_outlets"
    ]
    
    num_defs = len(Data_Service.data_def_function_switcher)
    for i in range(1, num_defs + 1):
        data_def = {
            "reportId":1,
            "reportDictId":1,
            "dataDefId":i,
            "name": data_def_names[i-1],
            "dataDefType":"type1"
        }
        input_dict["ReportInfo"].append(data_def)
    

    # params = parse_request(input_dict)
    cd = CalculationDispatcher(input_dict)
    cd.run_calculations()

    context = { 'report_output': format_dict(cd.request)}
    print_dict(input_dict)
    return render(request, 'transformapi/get-report.html', context)

def get_demo1_franklin(request):
    input_dict = {
        "Scope": {
            "startDate":"01/01/2019",
            "endDate":"12/31/2019",
            "scope_type": "geography",
            "scope_field":"fips_cnty",
            "scope_field_value":39049,
            "control_type_name":"Is Grocery Service"
        },
        "ReportInfo": []
    }

    data_def_names = [
        "services_total",
        "undup_hh_total",
        "undup_indv_total",
        "services_per_uhh_avg",
        "hh_wminor",
        "hh_wominor",
        "hh_total",
        "indv_sen_hh_wminor",
        "indv_sen_hh_wominor",
        "indv_sen_total",
        "indv_adult_hh_wminor",
        "indv_adult_hh_wominor",
        "indv_adult_total",
        "indv_child_hh_wminor",
        "indv_child_hh_wominor",
        "indv_child_total",
        "indv_total_hh_wminor",
        "indv_total_hh_wominor",
        "indv_total",
        "hh_wsenior",
        "hh_wosenior",
        "hh_grandparent",
        "service_summary_service",
        "service_summary_category",
        "distribution_outlets"
    ]

    num_defs = len(Data_Service.data_def_function_switcher)
    for i in range(1, num_defs + 1):
        data_def = {
            "reportId":1,
            "reportDictId":1,
            "dataDefId":i,
            "name": data_def_names[i-1],
            "dataDefType":"type1"
        }
        input_dict["ReportInfo"].append(data_def)
    

    # params = parse_request(input_dict)
    cd = CalculationDispatcher(input_dict)
    cd.run_calculations()

    context = { 'report_output': format_dict(cd.request)}
    print_dict(input_dict)
    return render(request, 'transformapi/get-report.html', context)



def get_all_defs_typical(request):
    input_dict = {
        "Scope": {
            "startDate":"01/01/2019",
            "endDate":"12/31/2019",
            "scope_field":"loc_id",
            "scope_field_value":1,
            "control_type_name":"Is Grocery Service"
        },
        "ReportInfo": []
    }

    data_def_names = [
        "services_total",
        "undup_hh_total",
        "undup_indv_total",
        "services_per_uhh_avg",
        "hh_wminor",
        "hh_wominor",
        "hh_total",
        "indv_sen_hh_wminor",
        "indv_sen_hh_wominor",
        "indv_sen_total",
        "indv_adult_hh_wminor",
        "indv_adult_hh_wominor",
        "indv_adult_total",
        "indv_child_hh_wminor",
        "indv_child_hh_wominor",
        "indv_child_total",
        "indv_total_hh_wminor",
        "indv_total_hh_wominor",
        "indv_total",
        "hh_wsenior",
        "hh_wosenior",
        "hh_grandparent",
        "service_summary_service",
        "service_summary_category",
        "distribution_outlets",
        "fam_frequency_of_visits",
        "fam_service_distribution",
        "fam_household_composition",
        "fam_family_composition_key_insight",
        "fam_household_size_distribution_1_10",
        "fam_household_size_distribution_classic"
    ]
    num_defs = len(Data_Service.data_def_function_switcher)
    for i in range(1, num_defs + 1):
        data_def = {
            "reportId":1,
            "reportDictId":1,
            "dataDefId":i,
            "name": data_def_names[i-1],
            "dataDefType":"type1"
        }
        input_dict["ReportInfo"].append(data_def)
    

    start_time = time.time()
    cd = CalculationDispatcher(input_dict)
    cd.run_calculations()

    context = { 'report_output': format_dict(cd.request)}
    print_dict(input_dict)
    print(str(time.time() - start_time), ' seconds to run all queries')
    return render(request, 'transformapi/get-report.html', context)


def get_report_services(request):
    input_dict = {
        "Scope": {
            "startDate":"01/01/2020",
            "endDate":"12/31/2020",
            "scope_field":"fb_id",
            "scope_field_value":21,
            "control_type_name":"Is Grocery Service"
        },
        "ReportInfo": [
            {
                "reportId":1,
                "reportDictId":1,
                "dataDefId":23,
                "name": "service_summary_service",
                "dataDefType":3
            },
             {
                "reportId":1,
                "reportDictId":1,
                "dataDefId":24,
                "name": "service_summary_category",
                "dataDefType":3
            },
            {
                "reportId":1,
                "reportDictId":1,
                "dataDefId":25,
                "name": "distribution_outlets",
                "dataDefType":3
            },

        ]
    }

    cd = CalculationDispatcher(input_dict)
    cd.run_calculations()

    context = { 'report_output': format_dict(cd.request)}
    print_dict(cd.request)
    return render(request, 'transformapi/get-report.html', context)

def get_family_breakdown(request):
    input_dict = {
        "Scope": {
            "startDate":"01/01/2020",
            "endDate":"12/31/2020",
            "scope_field":"fb_id",
            "scope_field_value":21,
            "control_type_name":"Is Grocery Service"
        },
        "ReportInfo": [
            {
                "reportId":1,
                "reportDictId":1,
                "dataDefId":26,
                "name": "frequency_visits",
                "dataDefType":3
            },
            {
                "reportId":1,
                "reportDictId":1,
                "dataDefId":28,
                "name": "household_composition",
                "dataDefType":3
            },
            {
                "reportId":1,
                "reportDictId":1,
                "dataDefId":29,
                "name": "family_composition_key_insight",
                "dataDefType":3
            },
            {
                "reportId":1,
                "reportDictId":1,
                "dataDefId":30,
                "name": "household_size_distribution_1_to_10",
                "dataDefType":3
            },
            {
                "reportId":1,
                "reportDictId":1,
                "dataDefId":31,
                "name": "household_composition",
                "dataDefType":3
            }
        ]
    }

    cd = CalculationDispatcher(input_dict)
    cd.run_calculations()

    context = { 'report_output': format_dict(cd.request)}
    print_dict(cd.request)
    return render(request, 'transformapi/get-report.html', context)

def test_data_def_3_large(request):
    input_dict = {
        "Scope": {
            "startDate":"01/01/2019",
            "endDate":"12/31/2019",
            "scope_field":"fb_id",
            "scope_field_value":21,
            "control_type_name":"Is Grocery Service",
            "control_type_value":1
        },
        "ReportInfo": [
            {
                "reportId":3,
                "reportDictId":3,
                "dataDefId":3,
                "name":"undup_indv_total",
                "dataDefType":"type1"
            }
        ]
    }

    # params = parse_request(input_dict)
    start_time = time.time()
    cd = CalculationDispatcher(input_dict)
    cd.run_calculations()

    context = { 'report_output': format_dict(cd.request)}
    print_dict(input_dict)
    print(str(time.time() - start_time), ' seconds to run query')
    return render(request, 'transformapi/get-report.html', context)

def test_data_def_3_typical(request):
    input_dict = {
        "Scope": {
            "startDate":"01/01/2019",
            "endDate":"12/31/2019",
            "scope_field":"loc_id",
            "scope_field_value":1,
            "control_type_name":"Is Grocery Service"
        },
        "ReportInfo": [
            {
                "reportId":3,
                "reportDictId":3,
                "dataDefId":3,
                "name":"undup_indv_total",
                "dataDefType":"type1"
            }
        ]
    }

    # params = parse_request(input_dict)
    start_time = time.time()
    cd = CalculationDispatcher(input_dict)
    cd.run_calculations()

    context = { 'report_output': format_dict(cd.request)}
    print_dict(input_dict)
    print(str(time.time() - start_time), ' seconds to run query')
    return render(request, 'transformapi/get-report.html', context)


def get_age_groups(request):
    input_dict = {
        "Scope": {
            "startDate":"01/01/2020",
            "endDate":"01/31/2020",
            "scope_field":"fb_id",
            "scope_field_value":21,
            "control_type_name":"Is Grocery Service",
            "age_grouping_id":1 # using age_grouping_id to use to subset dim_ages
        },
        "ReportInfo": [
            {
                "reportId":1,
                "reportDictId":1,
                "dataDefId":67,
                "name": "age_group_count",
                "dataDefType":3
            },
            {
                "reportId":1,
                "reportDictId":1,
                "dataDefId":71,
                "name": "age_group_and_gender_count",
                "dataDefType":3
            },
            {
                "reportId":1,
                "reportDictId":1,
                "dataDefId":76,
                "name": "gender_disparity_male_total",
                "dataDefType":3
            }
        ]
    }

    start_time = time.time()
    cd = CalculationDispatcher(input_dict)
    cd.run_calculations()

    context = { 'report_output': format_dict(cd.request)}
    print(str(time.time() - start_time), ' seconds to run query')
    return render(request, 'transformapi/get-report.html', context)

def get_age_groups_at_least_one(request):
    input_dict = {
        "Scope": {
            "startDate":"01/01/2020",
            "endDate":"01/31/2020",
            "scope_field":"fb_id",
            "scope_field_value":21,
            "control_type_name":"Is Grocery Service",
            "age_grouping_id":1 # using age_grouping_id to use to subset dim_ages
        },
        "ReportInfo": [
            {
                "reportId":1,
                "reportDictId":1,
                "dataDefId":73,
                "name": "age_group_count",
                "dataDefType":3
            }
        ]
    }

    start_time = time.time()
    cd = CalculationDispatcher(input_dict)
    cd.run_calculations()

    context = { 'report_output': format_dict(cd.request)}
    print(str(time.time() - start_time), ' seconds to run query')
    return render(request, 'transformapi/get-report.html', context)