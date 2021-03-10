from django.test import TestCase
import transform_layer.calculations as calc
from django.db import connections
import unittest
import csv
import os 
import sys


sample_scope_1 = { "Scope": {
            "startDate":"01/01/2019",
            "endDate":"12/31/2019",
            "scope_type": "hierarchy",
            "scope_field":"fb_id",
            "scope_field_value":21,
            "control_type_field":"dummy_is_grocery_service",
            "control_type_value":1
        }
}


sample_scope_2 = { "Scope": {
            "startDate":"01/01/2019",
            "endDate":"12/31/2019",
            "scope_type": "geography",
            "scope_field":"fips_cnty",
            "scope_field_value":39049,
            "control_type_field":"dummy_is_grocery_service",
            "control_type_value":1
        }
}



def read_expected():
    expected = {}
    __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
    with open(os.path.join(__location__,'./transform_test_results.csv'), newline = '') as csvfile:
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


EXPECTED_RESULTS = read_expected()


class CalculationsTestCase(unittest.TestCase):
    # @classmethod
    # def setUpClass(cls):
    #     cls.fact_services_scope1 = ds.fact_services(sample_scope_2)
    #     #ds.__fact_services = None
    #     #cls.fact_services_scope2 = ds.fact_services(sample_scope_2)
    #     #ds.__fact_services = None


    def test_get_services_total(self):
        #how to avoid repeatedly making database requests in calculation tests?
        #might want to instantiate the data service object
        #might want to pass a connection to mock database in data_service.get_fact_service
        #make data service a singleton
        func = calc.data_calc_function_switcher[1]
        result = func(1,sample_scope_2)
        self.assertEqual(result, EXPECTED_RESULTS["services_total"]["franklin_value"])
    
    def test_get_undup_hh_total(self):
        func = calc.data_calc_function_switcher[2]
        result = func(2,sample_scope_2)
        self.assertEqual(result, EXPECTED_RESULTS["undup_hh_total"]["franklin_value"])
    
    def test_get_undup_indv_total(self):
        func = calc.data_calc_function_switcher[3]
        result = func(3,sample_scope_2)
        self.assertEqual(result, EXPECTED_RESULTS["undup_indv_total"]["franklin_value"])
    def test_get_services_per_uhh_avg(self):
        func = calc.data_calc_function_switcher[4]
        result = func(4,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_RESULTS["services_per_uhh_avg"]["franklin_value"])
    
    #Ohio Addin
    def test_get_hh_wminor(self):
        func = calc.data_calc_function_switcher[5]
        result = func(5,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_RESULTS["hh_wminor"]["franklin_value"])
        

    def test_get_hh_wominor(self):
        func = calc.data_calc_function_switcher[6]
        result = func(6,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_RESULTS["hh_wominor"]["franklin_value"])
        
    def test_get_hh_total(self):
        func = calc.data_calc_function_switcher[7]
        result = func(7,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_RESULTS["hh_total"]["franklin_value"])
        
    def test_get_indv_sen_hh_wminor(self):
        func = calc.data_calc_function_switcher[8]
        result = func(8,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_RESULTS["indv_sen_hh_wminor"]["franklin_value"])
        
    def test_get_indv_sen_hh_wominor(self):
        func = calc.data_calc_function_switcher[9]
        result = func(9,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_RESULTS["indv_sen_hh_wominor"]["franklin_value"])
        
    def test_get_indv_sen_total(self):
        func = calc.data_calc_function_switcher[10]
        result = func(10,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_RESULTS["indv_sen_total"]["franklin_value"])
        
    def test_get_indv_adult_hh_wminor(self):
        func = calc.data_calc_function_switcher[11]
        result = func(11,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_RESULTS["indv_adult_hh_wminor"]["franklin_value"])
        
    def test_get_indv_adult_hh_wominor(self):
        func = calc.data_calc_function_switcher[12]
        result = func(12,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_RESULTS["indv_adult_hh_wominor"]["franklin_value"])
        
    def test_get_indv_adult_total(self):
        func = calc.data_calc_function_switcher[13]
        result = func(13,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_RESULTS["indv_adult_total"]["franklin_value"])
        
    def test_get_indv_child_hh_wminor(self):
        func = calc.data_calc_function_switcher[14]
        result = func(14,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_RESULTS["indv_child_hh_wminor"]["franklin_value"])
        
    def test_get_indv_child_hh_wominor(self):
        func = calc.data_calc_function_switcher[15]
        result = func(15,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_RESULTS["indv_child_hh_wominor"]["franklin_value"])
        
    def test_get_indv_child_total(self):
        func = calc.data_calc_function_switcher[16]
        result = func(16,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_RESULTS["indv_child_total"]["franklin_value"])
        
    def test_get_indv_total_hh_wminor(self):
        func = calc.data_calc_function_switcher[17]
        result = func(17,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_RESULTS["indv_total_hh_wminor"]["franklin_value"])
        
    def test_get_indv_total_hh_wominor(self):
        func = calc.data_calc_function_switcher[18]
        result = func(18,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_RESULTS["indv_total_hh_wominor"]["franklin_value"])
        
    def test_get_indv_total(self):
        func = calc.data_calc_function_switcher[19]
        result = func(19,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_RESULTS["indv_total"]["franklin_value"])
        
    # #MOFC addin
    def test_get_hh_wsenior(self):
        func = calc.data_calc_function_switcher[20]
        result = func(20,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_RESULTS["hh_wsenior"]["franklin_value"])
        
    def test_get_hh_wosenior(self):
        func = calc.data_calc_function_switcher[21]
        result = func(21,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_RESULTS["hh_wosenior"]["franklin_value"])
        
    def test_get_hh_grandparent(self):
        func = calc.data_calc_function_switcher[22]
        result = func(22,sample_scope_2)
        self.assertAlmostEqual(result, EXPECTED_RESULTS["hh_grandparent"]["franklin_value"])
        


if __name__ == '__main__':
    unittest.main()