import time
from django.shortcuts import render
from django.http import HttpResponse
from django.shortcuts import render
from django.db import connections
from print_dict import print_dict, format_dict
from .calculations import CalculationDispatcher
from .services.data_service import DataService

def test_data_service(request, id):
    sample_dict = {
        "Meta": {
            "startDate":"4/01/2020",
            "endDate":"3/31/2021",
            "scope_field":"fips_tract",
            "scope_field_value":21187970300,
            "control_type_name":"Is Grocery Service",
            "control_type_value":1
        },
        "ReportInfo": [
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":id,
                "name":"test",
                "dataDefType":"type1"
            }
        ]
    }
    cd = CalculationDispatcher(sample_dict)
    cd.run_calculations()

    context = { 'report_output': format_dict(cd.request) }
    return render(request, 'transformapi/get-report.html', context)


def get_report_big_numbers(request):
    input_dict = {
        "Meta": {
            "startDate":"01/01/2019",
            "endDate":"12/31/2019",
            "scope_field":"fb_id",
            "scope_field_value":21,
            "control_type_name":"Is Grocery Service",
            "control_type_value":1
        },
        "ReportInfo": [
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":1,
                "name":"services_total",
                "dataDefType":"type1"
            },
            {
                "reportScheduleId":2,
                "reportDictId":2,
                "dataDefId":2,
                "name":"undup_hh_total",
                "dataDefType":"type1"
            },
            {
                "reportScheduleId":3,
                "reportDictId":3,
                "dataDefId":3,
                "name":"undup_indv_total",
                "dataDefType":"type1"
            },
            {
                "reportScheduleId":4,
                "reportDictId":4,
                "dataDefId":4,
                "name":"services_per_uhh_avg",
                "dataDefType":"type1"
            }
        ]
    }

    cd = CalculationDispatcher(input_dict)
    cd.run_calculations()

    context = { 'report_output': format_dict(cd.request) }
    print_dict(input_dict)
    return render(request, 'transformapi/get-report.html', context)

def get_report_ohio(request):
    input_dict = {
        "Meta": {
            "startDate":"01/01/2019",
            "endDate":"12/31/2019",
            "scope_field":"fb_id",
            "scope_field_value":21,
            "control_type_name":"Is Grocery Service",
            "control_type_value":1
        },
        "ReportInfo": [
            {
                "reportScheduleId":1,
                "reportDictId":2,
                "dataDefId":5,
                "name":"hh_wminor",
                "dataDefType":"type1"
            },
            {
                "reportScheduleId":1,
                "reportDictId":2,
                "dataDefId":6,
                "name":"hh_wominor",
                "dataDefType":"type1"
            },
            {
                "reportScheduleId":1,
                "reportDictId":2,
                "dataDefId":7,
                "name":"hh_total",
                "dataDefType":"type1"
            },
            {
                "reportScheduleId":1,
                "reportDictId":2,
                "dataDefId":8,
                "name":"indv_sen_hh_wminor",
                "dataDefType":"type1"
            },
            {
                "reportScheduleId":1,
                "reportDictId":2,
                "dataDefId":9,
                "name":"indv_sen_hh_wominor",
                "dataDefType":"type1"
            },
            {
                "reportScheduleId":1,
                "reportDictId":2,
                "dataDefId":10,
                "name":"indv_sen_total",
                "dataDefType":"type1"
            },
            {
                "reportScheduleId":1,
                "reportDictId":2,
                "dataDefId":11,
                "name":"indv_adult_hh_wminor",
                "dataDefType":"type1"
            },
            {
                "reportScheduleId":1,
                "reportDictId":2,
                "dataDefId":12,
                "name":"indv_adult_hh_wominor",
                "dataDefType":"type1"
            },
            {
                "reportScheduleId":1,
                "reportDictId":2,
                "dataDefId":13,
                "name":"indv_adult_total",
                "dataDefType":"type1"
            },
            {
                "reportScheduleId":1,
                "reportDictId":2,
                "dataDefId":14,
                "name":"indv_child_hh_wminor",
                "dataDefType":"type1"
            },
            {
                "reportScheduleId":1,
                "reportDictId":2,
                "dataDefId":15,
                "name":"indv_child_hh_wominor",
                "dataDefType":"type1"
            },
            {
                "reportScheduleId":1,
                "reportDictId":2,
                "dataDefId":16,
                "name":"indv_child_total",
                "dataDefType":"type1"
            },
            {
                "reportScheduleId":1,
                "reportDictId":2,
                "dataDefId":17,
                "name":"indv_total_hh_wminor",
                "dataDefType":"type1"
            },
            {
                "reportScheduleId":1,
                "reportDictId":2,
                "dataDefId":18,
                "name":"indv_total_hh_wominor",
                "dataDefType":"type1"
            },
            {
                "reportScheduleId":1,
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
        "Meta": {
            "startDate":"01/01/2019",
            "endDate":"12/31/2019",
            "scope_field":"fb_id",
            "scope_field_value":21,
            "control_type_name":"Is Grocery Service",
            "control_type_value":1
        },
        "ReportInfo": [
            {
                "reportScheduleId":1,
                "reportDictId":3,
                "dataDefId":20,
                "name":"hh_wsenior",
                "dataDefType":"type1"
            },
            {
                "reportScheduleId":1,
                "reportDictId":3,
                "dataDefId":21,
                "name":"hh_wosenior",
                "dataDefType":"type1"
            },
            {
                "reportScheduleId":1,
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
        "Meta": {
            "startDate":"01/01/2019",
            "endDate":"12/31/2019",
            "scope_field":"fb_id",
            "scope_field_value":21,
            "control_type_name":"Is Grocery Service",
            "control_type_value":1
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
    
    num_defs = len(data_def_names)
    for i in range(1, num_defs + 1):
        data_def = {
            "reportScheduleId":1,
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
        "Meta": {
            "startDate":"01/01/2020",
            "endDate":"12/31/2020",
            "scope_field":"loc_id",
            "scope_field_value":1,
            "control_type_name":"Is Grocery Service",
            "control_type_value":1
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
        "fam_household_size_distribution_classic",
        "newfam_undup_hh_total",
        "newmem_allfams_undup_indv_total",
        "newmem_oldfam_undup_indv_total",
        "new_fam_services_total",
        "global_first_services_total",
        "new_fam_frequency_of_visits",
        "new_fam_service_distribution",
        "new_fam_household_composition",
        "new_fam_family_composition_key_insight",
        "new_fam_household_size_distribution_1_10",
        "new_fam_household_size_distribution_classic",
        "relationship_length_families_mean",
        "relationship_length_families_distribution",
        "relationship_length_indv_mean",
        "relationship_length_indv_distribution",
        "dimgeo_coverage",
        "geographic_breakdown_fam_state",
        "geographic_breakdown_fam_county",
        "geographic_breakdown_fam_zcta",
        "services_flow_event_fips",
        "distance_traveled",
        "direction_traveled",
        "windrose",
        "sites_visited_distribution",
        "dummy_trip_coverage",
        "service_trend_time_month",
        "service_trend_time_week",
        "service_trend_time_day",
        "service_trend_monthly_visits_avg",
        "service_trend_monthly_people_dup",
        "service_trend_monthly_served_group_dup",
        "service_trend_service_category",
        "service_trend_comparison",
        "service_summary_dow",
        "service_summary_hod",
        "service_summary_dowhod",
        "service_trend_event",
        "hoh_gender_summary",
        "hoh_single_adult_w_children",
        "skipped_generation",
        "demo_indv_gender",
        "demo_indv_age_groups",
        "hh_has_age_groups",
        "population_pyramid",
        "demo_indv_race",
        "demo_indv_ethnic",
        "demo_indv_military",
        "demo_indv_education",
        "demo_indv_employment",
        "demo_indv_health_insurance"
    ]

    num_defs = len(data_def_names)
    for i in range(1, num_defs + 1):
        data_def = {
            "reportScheduleId":1,
            "reportDictId":1,
            "dataDefId":i,
            "name": data_def_names[i-1],
            "dataDefType":"type1"
        }
        input_dict["ReportInfo"].append(data_def)
    

    start_time = time.time()
    cd = CalculationDispatcher(input_dict)
    cd.run_calculations()
    print(str(time.time() - start_time), ' seconds to run all queries')
    context = { 'report_output': format_dict(cd.request)}
    return render(request, 'transformapi/get-report.html', context)

def get_fact_services(request):
    input_dict = {
        "Meta": {
            "startDate":"01/01/2020",
            "endDate":"12/31/2020",
            "scope_field":"loc_id",
            "scope_field_value":1,
            "control_type_name":"Is Grocery Service",
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
    ]

    num_defs = len(data_def_names)
    for i in range(1, num_defs + 1):
        data_def = {
            "reportScheduleId":1,
            "reportDictId":1,
            "dataDefId":i,
            "name": data_def_names[i-1],
            "dataDefType":"type1"
        }
        input_dict["ReportInfo"].append(data_def)
    
    start_time = time.time()
    cd = CalculationDispatcher(input_dict)
    cd.run_calculations()
    print(str(time.time() - start_time), ' seconds to run fact services queries')

    context = { 'report_output': format_dict(cd.request)}
    print_dict(input_dict)
    return render(request, 'transformapi/get-report.html', context)

def get_report_services(request):
    input_dict = {
        "Meta": {
            "startDate":"01/01/2020",
            "endDate":"12/31/2020",
            "scope_field":"loc_id",
            "scope_field_value":1,
            "control_type_name":"Is Grocery Service",
        },
        "ReportInfo": [
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":23,
                "name": "service_summary_service",
                "dataDefType":3
            },
             {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":24,
                "name": "service_summary_category",
                "dataDefType":3
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":25,
                "name": "distribution_outlets",
                "dataDefType":3
            },

        ]
    }

    start_time = time.time()
    cd = CalculationDispatcher(input_dict)
    cd.run_calculations()
    print(str(time.time() - start_time), ' seconds to run service types queries')

    context = { 'report_output': format_dict(cd.request)}
    print_dict(cd.request)
    return render(request, 'transformapi/get-report.html', context)

def get_family_breakdown(request):
    input_dict = {
        "Meta": {
            "startDate":"01/01/2020",
            "endDate":"12/31/2020",
            "scope_field":"loc_id",
            "scope_field_value":1,
            "control_type_name":"Is Grocery Service",
        },
        "ReportInfo": [
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":26,
                "name": "frequency_visits",
                "dataDefType":3
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":28,
                "name": "household_composition",
                "dataDefType":3
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":29,
                "name": "family_composition_key_insight",
                "dataDefType":3
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":30,
                "name": "household_size_distribution_1_to_10",
                "dataDefType":3
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":31,
                "name": "household_composition",
                "dataDefType":3
            }
        ]
    }

    start_time = time.time()
    cd = CalculationDispatcher(input_dict)
    cd.run_calculations()
    print(str(time.time() - start_time), ' seconds to run families queries')
    context = { 'report_output': format_dict(cd.request)}
    print_dict(cd.request)
    
    return render(request, 'transformapi/get-report.html', context)


def get_new_families(request):
    input_dict = {
        "Meta": {
            "startDate":"01/01/2020",
            "endDate":"12/31/2020",
            "scope_field":"loc_id",
            "scope_field_value":1,
            "control_type_name":"Is Grocery Service",
        },
        "ReportInfo": [
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":32,
                "name": "newfam_undup_fam_total",
                "dataDefType":1
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":33,
                "name": "newmem_undup_indv_total",
                "dataDefType":1
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":34,
                "name": "newmem_oldfam_undup_indv_total",
                "dataDefType":1
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":35,
                "name": "new_fam_services_total",
                "dataDefType":1
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":36,
                "name": "global_first_services_total",
                "dataDefType":1
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":37,
                "name": "newfam_freq_visits",
                "dataDefType":3
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":38,
                "name": "new_fam_service_distribution",
                "dataDefType":3
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":39,
                "name": "new_fam_household_composition",
                "dataDefType":3
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":40,
                "name": "new_fam_composition_key_insight",
                "dataDefType":3
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":41,
                "name": "newfam_hh_size_dist_1_to_10",
                "dataDefType":3
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":42,
                "name": "newfam_hh_size_dist_classic",
                "dataDefType":3  
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":43,
                "name": "relationship_length_fam_mean",
                "dataDefType":3  
            },
             {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":44,
                "name": "newfam_dist_length_of_relationship",
                "dataDefType":3  
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":45,
                "name": "relationship_length_indv_mean",
                "dataDefType":3  
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":46,
                "name": "new_fam_dist_of_length_of_relationships_for_individuals",
                "dataDefType":3
            }
        ]
    }

    start_time = time.time()
    cd = CalculationDispatcher(input_dict)
    cd.run_calculations()
    print(str(time.time() - start_time), ' seconds to run new families queries')
    context = { 'report_output': format_dict(cd.request)}
    print_dict(cd.request)
   
    return render(request, 'transformapi/get-report.html', context)

def get_geography(request):
    input_dict = {
        "Meta": {
            "startDate":"01/01/2020",
            "endDate":"12/31/2020",
            "scope_field":"loc_id",
            "scope_field_value":6,
            "control_type_name":"Is Grocery Service",
        },
        "ReportInfo": [
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":47,
                "name": "dimgeo_coverage",
                "dataDefType":3
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":48,
                "name": "geographic_breakdown_fam_state",
                "dataDefType":2
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":49,
                "name": "geographic_breakdown_fam_cnty",
                "dataDefType":2
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":50,
                "name": "geographic_breakdown_fam_zcta",
                "dataDefType":2
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":51,
                "name": "services_flow_event_fips",
                "dataDefType":2
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":52,
                "name": "distance_traveled",
                "dataDefType":2
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":53,
                "name": "direction_traveled",
                "dataDefType":1
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":54,
                "name": "windrose",
                "dataDefType":1
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":55,
                "name": "sites_visited_distribution",
                "dataDefType":1
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":56,
                "name": "dummy_trip_coverage",
                "dataDefType":1
            }
        ]
    }

    start_time = time.time()
    cd = CalculationDispatcher(input_dict)
    cd.run_calculations()
    print(str(time.time() - start_time), ' seconds to run geography queries')
    context = { 'report_output': format_dict(cd.request)}
    print_dict(cd.request)
   
    return render(request, 'transformapi/get-report.html', context)

def test_geos_no_dummy_trip(request):
    sample_dict = {
        "Meta": {
            "startDate":"4/01/2020",
            "endDate":"4/12/2021",
            "scope_field":"fips_zcta",
            "scope_field_value":40359,
            "control_type_name":"Is Grocery Service",
            "control_type_value":1
        },
        "ReportInfo": [
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":52,
                "name":"name_one",
                "dataDefType":"type1"
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":53,
                "name":"name_one",
                "dataDefType":"type1"
            },
                        {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":54,
                "name":"name_one",
                "dataDefType":"type1"
            }
        ]
    }
    cd = CalculationDispatcher(sample_dict)
    cd.run_calculations()

    context = { 'report_output': format_dict(cd.request) }
    print_dict(sample_dict)
    return render(request, 'transformapi/get-report.html', context)

def get_service_trends(request):
    sample_dict = {
        "Meta": {
            "startDate":"1/01/2020",
            "endDate":"12/31/2020",
            "scope_field":"loc_id",
            "scope_field_value":6,
            "control_type_name":"Is Grocery Service",
            "control_type_value":1
        },
        "ReportInfo": [
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":57,
                "name":"service_trends_time_month",
                "dataDefType":"type2"
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":58,
                "name":"service_trends_time_week",
                "dataDefType":"type2"
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":59,
                "name":"service_trends_time_day",
                "dataDefType":"type2"
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":60,
                "name":"service_trend_monthly_visits_avg",
                "dataDefType":"type2"
            },
             {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":61,
                "name":"service_trend_monthly_people",
                "dataDefType":"type2"
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":62,
                "name":"service_trend_group_people",
                "dataDefType":"type2"
            },
             {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":63,
                "name":"service_trend_service_category",
                "dataDefType":"type2"
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":64,
                "name":"service_trend_comparison",
                "dataDefType":"type2"
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":65,
                "name":"service_summary_dow",
                "dataDefType":"type2"
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":66,
                "name":"service_summary_hod",
                "dataDefType":"type2"
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":67,
                "name":"service_summary_dowhod",
                "dataDefType":"type2"
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":68,
                "name":"service_trend_event",
                "dataDefType":"type2"
            }
        ]
    }   

    cd = CalculationDispatcher(sample_dict)
    cd.run_calculations()

    context = { 'report_output': format_dict(cd.request) }
    print_dict(sample_dict)
    return render(request, 'transformapi/get-report.html', context)

def get_family_members(request):
    sample_dict = {
        "Meta": {
            "startDate":"1/1/2020",
            "endDate":"3/31/2021",
            "scope_field":"loc_id",
            "scope_field_value":6,
            "control_type_name":"Is Grocery Service",
            "control_type_value":1
        },
        "ReportInfo": [
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":69,
                "name":"gender_summary",
                "dataDefType":"type2"
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":70,
                "name":"single_adult_w_children",
                "dataDefType":"type2"
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":71,
                "name":"skipped_generation",
                "dataDefType":"type2"
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":72,
                "name":"demo_indv_gender",
                "dataDefType":"type2"
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":73,
                "name":"demo_indv_age_groups",
                "dataDefType":"type2"
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":74,
                "name":"hh_has_age_groups",
                "dataDefType":"type2"
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":75,
                "name":"population_pyramid",
                "dataDefType":"type2"
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":76,
                "name":"demo_indv_race",
                "dataDefType":"type2"
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":77,
                "name":"demo_indv_ethnic",
                "dataDefType":"type2"
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":78,
                "name":"demo_indv_military",
                "dataDefType":"type2"
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":79,
                "name":"demo_indv_education",
                "dataDefType":"type2"
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":80,
                "name":"demo_indv_employment",
                "dataDefType":"type2"
            },
            {
                "reportScheduleId":1,
                "reportDictId":1,
                "dataDefId":81,
                "name":"demo_indv_health_insurance",
                "dataDefType":"type2"
            }
        ]
    }   

    cd = CalculationDispatcher(sample_dict)
    cd.run_calculations()

    context = { 'report_output': format_dict(cd.request) }
    print_dict(sample_dict)
    return render(request, 'transformapi/get-report.html', context)


