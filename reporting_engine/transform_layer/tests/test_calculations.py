from django.test import TestCase
import transform_layer.calculations as calc
from transform_layer.services.data_service import Data_Service as ds
from django.db import connections
import pandas

import unittest
import csv
import os 
import sys
import json


sample_scope_1 = {
    "startDate":"2019-01-01",
    "endDate":"2019-12-31",
    "scope_type": "hierarchy",
    "scope_field":"fb_id",
    "scope_field_value":21,
    "control_type_name":"Is Grocery Service"
}


sample_scope_2 = {
    "startDate":"2019-01-01",
    "endDate":"2019-12-31",
    "scope_type": "geography",
    "scope_field":"fips_cnty",
    "scope_field_value":39049,
    "control_type_name":"Is Grocery Service"
}

base_services_scope = {
    "startDate":"01/01/2020",
    "endDate":"12/31/2020",
    "scope_type": "hierarchy",
    "scope_field":"fb_id",
    "scope_field_value":21,
    "control_type_name":"Is Grocery Service"
}

json_test_results = {
    "result_label": ["service_summary_service","service_category_service", "distribution_outlets"],
    "mofc_value": [
        '{"service_name":{"0":"Backpack w\\/ Food","1":"CSFP Box","2":"Disaster Box","3":"Emergency - Prepack - 1 day","4":"Emergency - Prepack - 10 day","5":"Emergency - Prepack - '
        '2 day","6":"Emergency - Prepack - 3 day","7":"Food - Box - 1 day","8":"Food - Box - 2 day","9":"Food - Holiday Box","10":"Food - Senior Box","11":"Healthy Box","12":"Home '
        'Delivered Food","13":"Mobile Pantry - Choice","14":"Mobile Pantry - Senior","15":"Pantry - Choice - 10 day","16":"Pantry - Choice - 14 day","17":"Pantry - Choice - '
        '1day","18":"Pantry - Choice - 3 day","19":"Pantry - Choice - 4 day","20":"Pantry - Choice - 5 day","21":"Pantry - Choice - 6 day","22":"Pantry - Choice - 7 '
        'day","23":"Pantry - Choice - 8 day","24":"Pantry - Choice - 9 day","25":"Pantry - Prepack - 14 day","26":"Pantry - Prepack - 3 day","27":"Pantry - Prepack - 3 day - '
        'Vegetarian","28":"Pantry - Prepack - 4 day","29":"Pantry - Prepack - 5 day","30":"Pantry - Prepack - 6 day","31":"Pantry - Prepack - 7 day","32":"Pantry - Prepack - 8 '
        'day","33":"Pantry - Prepack - 9 day","34":"Pantry - Prepack\\/Choice - 3 day","35":"Pantry - Prepack\\/Choice - 4 day","36":"Pantry - Prepack\\/Choice - 5 '
        'day","37":"Pantry - Special Dietary Needs - 3 day","38":"Prepack - Perishables only","39":"Produce & Protein","40":"Produce \\/ Mobile Market","41":"Produce '
        'Market","42":"Rx - Produce","43":"Snack Bag","44":"Special - Perishables only","45":"TEFAP"},"Families '
        'Served":{"0":173,"1":6149,"2":73,"3":599,"4":4,"5":239,"6":497,"7":190,"8":368,"9":13373,"10":2173,"11":1,"12":1773,"13":29458,"14":1545,"15":1332,"16":2197,"17":65336,"18":303571,"19":35831,"20":59866,"21":6790,"22":16919,"23":2049,"24":1048,"25":3444,"26":97550,"27":2,"28":11237,"29":28965,"30":2338,"31":98123,"32":5,"33":1878,"34":596,"35":66,"36":865,"37":1,"38":5473,"39":27111,"40":191074,"41":120216,"42":1440,"43":381,"44":25638,"45":4},"People '
        'Served":{"0":750,"1":11712,"2":272,"3":1482,"4":9,"5":895,"6":1268,"7":616,"8":1537,"9":47112,"10":3084,"11":5,"12":4765,"13":79066,"14":2309,"15":3479,"16":5849,"17":227452,"18":965629,"19":115220,"20":190225,"21":21308,"22":59012,"23":6062,"24":2649,"25":8215,"26":290223,"27":9,"28":36120,"29":105112,"30":7995,"31":340291,"32":14,"33":5996,"34":1601,"35":199,"36":2386,"37":4,"38":19428,"39":95428,"40":546296,"41":370530,"42":4833,"43":689,"44":84918,"45":19}}',
        '{"service_category_name":{"0":"CSFP","1":"Choice Pantry","2":"Prepack Pantry","3":"Produce"},"Families ''Served":{"0":6149,"1":553054,"2":264917,"3":343841},"People '
        'Served":{"0":11712,"1":1773692,"2":860664,"3":1026005}}',
        '{"sites_visited":{"0":1,"1":2,"2":3,"3":4,"4":5,"5":6,"6":7,"7":8,"8":9,"9":10,"10":11,"11":12,"12":13,"13":14,"14":15,"15":16,"16":17,"17":18,"18":19,"19":20,"20":22,"21":23,"22":24,"23":28,"24":29},"un_duplicated_families":{"0":132981,"1":37029,"2":14403,"3":6363,"4":3072,"5":1646,"6":939,"7":489,"8":313,"9":201,"10":125,"11":79,"12":53,"13":35,"14":31,"15":22,"16":11,"17":7,"18":2,"19":5,"20":2,"21":4,"22":2,"23":1,"24":1}}'],
}

#reads int_test_results.csv and returns a dictionary of expected test results for data definitions 1-22.
def read_expected_int():
    expected = {}
    __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
    with open(os.path.join(__location__,'./int_test_results.csv'), newline = '') as csvfile:
        rownum = 0
        reader = csv.reader(csvfile, dialect = 'excel')
        for row in reader:
            if rownum != 0:
                expected[row[0]] = {
                    "mofc_value" : float(row[1]) if row[1].find(".") != -1 else int(row[1]),
                    "franklin_value" : float(row[2]) if row[2].find(".") != -1 else int(row[2])
                }
            rownum += 1
    return expected

def read_expected_json():
    expected = {}

    for i in range(len(json_test_results['result_label'])):
        expected[json_test_results['result_label'][i]] = {
            "mofc_value": json_test_results["mofc_value"][i]
        }

    return expected

EXPECTED_INT_RESULTS = read_expected_int()
EXPECTED_JSON_RESULTS = read_expected_json()


class CalculationsTestCase(unittest.TestCase):
    

    
    def test_get_services_total(self):
        #how to avoid repeatedly making database requests in calculation tests?
        #might want to instantiate the data service object
        #might want to pass a connection to mock database in data_service.get_fact_service
        #make data service a singleton
        func = calc.data_calc_function_switcher[1]
        result = func(1,sample_scope_2)
        self.assertEqual(result, EXPECTED_INT_RESULTS["services_total"]["franklin_value"])
    
    def test_get_undup_hh_total(self):
        func = calc.data_calc_function_switcher[2]
        result = func(2,sample_scope_2)
        self.assertEqual(result, EXPECTED_INT_RESULTS["undup_hh_total"]["franklin_value"])
    
    def test_get_undup_indv_total(self):
        func = calc.data_calc_function_switcher[3]
        result = func(3,sample_scope_2)
        self.assertEqual(result, EXPECTED_INT_RESULTS["undup_indv_total"]["franklin_value"])
    def test_get_services_per_uhh_avg(self):
        func = calc.data_calc_function_switcher[4]
        result = func(4,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_INT_RESULTS["services_per_uhh_avg"]["franklin_value"])
    
    #Ohio Addin
    def test_get_hh_wminor(self):
        func = calc.data_calc_function_switcher[5]
        result = func(5,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_INT_RESULTS["hh_wminor"]["franklin_value"])
        

    def test_get_hh_wominor(self):
        func = calc.data_calc_function_switcher[6]
        result = func(6,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_INT_RESULTS["hh_wominor"]["franklin_value"])
        
    def test_get_hh_total(self):
        func = calc.data_calc_function_switcher[7]
        result = func(7,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_INT_RESULTS["hh_total"]["franklin_value"])
        
    def test_get_indv_sen_hh_wminor(self):
        func = calc.data_calc_function_switcher[8]
        result = func(8,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_INT_RESULTS["indv_sen_hh_wminor"]["franklin_value"])
        
    def test_get_indv_sen_hh_wominor(self):
        func = calc.data_calc_function_switcher[9]
        result = func(9,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_INT_RESULTS["indv_sen_hh_wominor"]["franklin_value"])
        
    def test_get_indv_sen_total(self):
        func = calc.data_calc_function_switcher[10]
        result = func(10,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_INT_RESULTS["indv_sen_total"]["franklin_value"])
        
    def test_get_indv_adult_hh_wminor(self):
        func = calc.data_calc_function_switcher[11]
        result = func(11,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_INT_RESULTS["indv_adult_hh_wminor"]["franklin_value"])
        
    def test_get_indv_adult_hh_wominor(self):
        func = calc.data_calc_function_switcher[12]
        result = func(12,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_INT_RESULTS["indv_adult_hh_wominor"]["franklin_value"])
        
    def test_get_indv_adult_total(self):
        func = calc.data_calc_function_switcher[13]
        result = func(13,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_INT_RESULTS["indv_adult_total"]["franklin_value"])
        
    def test_get_indv_child_hh_wminor(self):
        func = calc.data_calc_function_switcher[14]
        result = func(14,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_INT_RESULTS["indv_child_hh_wminor"]["franklin_value"])
        
    def test_get_indv_child_hh_wominor(self):
        func = calc.data_calc_function_switcher[15]
        result = func(15,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_INT_RESULTS["indv_child_hh_wominor"]["franklin_value"])
        
    def test_get_indv_child_total(self):
        func = calc.data_calc_function_switcher[16]
        result = func(16,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_INT_RESULTS["indv_child_total"]["franklin_value"])
        
    def test_get_indv_total_hh_wminor(self):
        func = calc.data_calc_function_switcher[17]
        result = func(17,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_INT_RESULTS["indv_total_hh_wminor"]["franklin_value"])
        
    def test_get_indv_total_hh_wominor(self):
        func = calc.data_calc_function_switcher[18]
        result = func(18,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_INT_RESULTS["indv_total_hh_wominor"]["franklin_value"])
        
    def test_get_indv_total(self):
        func = calc.data_calc_function_switcher[19]
        result = func(19,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_INT_RESULTS["indv_total"]["franklin_value"])
        
    # #MOFC addin
    def test_get_hh_wsenior(self):
        func = calc.data_calc_function_switcher[20]
        result = func(20,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_INT_RESULTS["hh_wsenior"]["franklin_value"])
        
    def test_get_hh_wosenior(self):
        func = calc.data_calc_function_switcher[21]
        result = func(21,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_INT_RESULTS["hh_wosenior"]["franklin_value"])
        
    def test_get_hh_grandparent(self):
        func = calc.data_calc_function_switcher[22]
        result = func(22,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_INT_RESULTS["hh_grandparent"]["franklin_value"])
    
    # Base services
    def test_get_services_summary(self):
        func = calc.data_calc_function_switcher[23]
        result = func(23, base_services_scope)
        self.maxDiff = None
        self.assertEqual(json.loads(result), json.loads(EXPECTED_JSON_RESULTS["service_summary_service"]["mofc_value"]))
        
    def test_get_services_category(self):
        func = calc.data_calc_function_switcher[24]
        result = func(24, base_services_scope)
        self.maxDiff = None
        self.assertEqual(json.loads(result), json.loads(EXPECTED_JSON_RESULTS["service_category_service"]["mofc_value"]))

    def test_get_distribution_outlets(self):
        func = calc.data_calc_function_switcher[25]
        result = func(25, base_services_scope)
        self.maxDiff = None
        self.assertEqual(json.loads(result), json.loads(EXPECTED_JSON_RESULTS["distribution_outlets"]["mofc_value"]))
    
    def test_get_frequency_visits(self):
        expected = {
            "n_families": {
                "1":75530,"2":29221,"3":17037,"4":11730,"5":8713,"6":6931,"7":5698,"8":4898,"9":4115,"10":3470,"11":3181,"12":2867,"13":2342,"14":1889,
                "15":1691,"16":1554,"17":1339,"18":1244,"19":1101,"20":1021,"21":921,"22":797,"23":747,"24":694,"25":8863
            },
            "sum_services" :{
                "1":75530,"2":58442,"3":51111,"4":46920,"5":43565,"6":41586,"7":39886,"8":39184,"9":37035,"10":34700,"11":34991,"12":34404,"13":30446,
                "14":26446,"15":25365,"16":24864,"17":22763,"18":22392,"19":20919,"20":20420,"21":19341,"22":17534,"23":17181,"24":16656,"25":366004
            }
        }
        func = calc.data_calc_function_switcher[26]
        result = func(26, base_services_scope)
        resultDict = json.loads(result)
        self.assertDictEqual(resultDict, expected)

        
    def test_get_household_composition(self):
        expected = {
            "family_composition_type": {
                "0":"adults_and_children",
                "1":"adults_and_seniors",
                "2":"adults_only",
                "3":"adults_seniors_and_children",
                "4":"children_and_seniors",
                "5":"children_only",
                "6":"seniors_only"
                },
            "num_families":
            {
                "0":74123,
                "1":17882,
                "2":58365,
                "3":13913,
                "4":2492,
                "5":434,
                "6":30385
            }
        }
        func = calc.data_calc_function_switcher[28]
        result = func(28, base_services_scope)
        resultDict = json.loads(result)
        self.assertDictEqual(resultDict, expected)

    def test_get_family_comp_key_insight(self):

        # expected = {
        #     "family_composition_type": ["has_child_senior"," no_child_senior"],
        #     "num_families": [139229, 58365]
        # }
        expected = {
            "family_composition_type": {
                "0":"has_child_senior",
                "1":"no_child_senior"
                },
            "num_families":
            {
                "0":139229,
                "1":58365,
            }
        }
        func = calc.data_calc_function_switcher[29]
        result = func(29, base_services_scope)
        resultDict = json.loads(result)
        self.assertDictEqual(resultDict, expected)

    def test_get_household_size_distribution_1_to_10(self):
        expected = {
            "avg_fam_size_roll": {
                "0": 1.0, "1": 2.0, "2": 3.0, "3": 4.0, "4": 5.0, "5": 6.0, "6": 7.0, "7": 8.0, "8": 9.0, "9": 10.0
            },
            "num_families":{
                "0": 56174, "1": 38967, "2": 29398, "3": 28081, "4": 20341, "5": 12336, "6": 6103, "7": 3144, "8": 1493, "9": 1557
            },
            "classic_roll": {
                "0": "1 - 3", "1": "1 - 3", "2": "1 - 3", "3": "4 - 6", "4": "4 - 6", "5": "4 - 6", "6": "7+", "7": "7+", "8": "7+", "9": "7+"
            }
        }
        func = calc.data_calc_function_switcher[30]
        result = func(30, base_services_scope)
        resultDict = json.loads(result)
        self.assertDictEqual(resultDict, expected)

    def test_get_household_size_distribution_classic(self):
        expected = {
            '1 - 3':124539,
            '4 - 6':60758,
            '7+':12297
        }
        func = calc.data_calc_function_switcher[31]
        result = func(31, base_services_scope)
        resultDict = json.loads(result)
        self.assertDictEqual(resultDict, expected)


if __name__ == '__main__':
    unittest.main()