from django.test import TestCase
from django.db import connections
from numpy import int64
import numpy
import pandas
from pandas.testing import assert_frame_equal, assert_series_equal

from transform_layer.services.data_service import DataService, KEY_SERVICE, SKEY_HOD_DOW
import transform_layer.calculations as calc

import json
import math
import unittest
import os
import pyreadr
from unittest.case import expectedFailure

REL_TOL = .01

base_scope = {
    "startDate":"01/01/2020",
    "endDate":"12/31/2020",
    "scope_type": "hierarchy",
    "scope_field":"loc_id",
    "scope_field_value":6,
    "control_type_name":"Is Grocery Service"
}

base_scope2 = {
    "startDate":"01/01/2020",
    "endDate":"3/31/2021",
    "scope_type": "hierarchy",
    "scope_field":"loc_id",
    "scope_field_value":6,
    "control_type_name":"Is Grocery Service"
}

__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

TEST_DATA_SERVICE = DataService(base_scope)
TEST_DATA_SERVICE_2 = DataService(base_scope2)
class CalculationsTestCase(unittest.TestCase):
    
    #test data def 57
    def test_get_service_trend_time_month(self):
        expected = pandas.read_csv(
            os.path.join(__location__, './expected_results/test_calc_service_trends/service_trend_time_month.csv'),
            skipinitialspace= True
        ).fillna('0')
        expected['n'] = expected['n'].astype(int64)

        data = TEST_DATA_SERVICE.get_data_for_definition(57)
        func = calc.data_calc_function_switcher[57]
        result = func(data)
        resultFrame = pandas.read_json(result)
        assert_frame_equal(resultFrame, expected, rtol = REL_TOL)

    #test data def 58
    def test_get_service_trend_time_week(self):
        expected = pandas.read_csv(
            os.path.join(__location__, './expected_results/test_calc_service_trends/service_trend_time_week.csv'),
            skipinitialspace= True
        ).fillna('0')
        expected['n'] = expected['n'].astype(int64)

        data = TEST_DATA_SERVICE.get_data_for_definition(58)
        func = calc.data_calc_function_switcher[58]
        result = func(data)
        resultFrame = pandas.read_json(result)
        assert_frame_equal(resultFrame, expected, rtol = REL_TOL)

    #test data def 59
    def test_get_service_trend_time_day(self):
        expected = pandas.read_csv(
            os.path.join(__location__, './expected_results/test_calc_service_trends/service_trend_time_day.csv')
        ).fillna('0')
        expected['n'] = expected['n'].astype(int64)

        data = TEST_DATA_SERVICE.get_data_for_definition(59)
        func = calc.data_calc_function_switcher[59]
        result = func(data)
        resultFrame = pandas.read_json(result)
        assert_frame_equal(resultFrame, expected, rtol = REL_TOL)

    #test data def 60
    def test_get_service_trend_monthly_visits_avg(self):
        expected = pandas.read_csv(
            os.path.join(__location__, './expected_results/test_calc_service_trends/service_trend_monthly_visits_avg.csv')
        ).fillna(0)
        data = TEST_DATA_SERVICE.get_data_for_definition(60)
        func = calc.data_calc_function_switcher[60]
        result = func(data)
        resultFrame = pandas.read_json(result)
        self.assertTrue(len(resultFrame) == len(expected))
        assert_frame_equal(resultFrame, expected, rtol = REL_TOL)

    #test data def 61
    def test_get_service_trend_monthly_people_dup(self):
        expected = pandas.read_csv(
            os.path.join(__location__, './expected_results/test_calc_service_trends/service_trend_monthly_people.csv')
        ).fillna(0)
        data = data = TEST_DATA_SERVICE_2.get_data_for_definition(61)
        func = calc.data_calc_function_switcher[61]
        result = func(data)
        resultFrame = pandas.read_json(result)
        self.assertTrue(len(resultFrame) == len(expected))
        assert_frame_equal(resultFrame, expected, rtol = REL_TOL)

    #test data def 62
    def test_get_service_trend_monthly_group_dup(self):
        expected = pandas.read_csv(
            os.path.join(__location__, './expected_results/test_calc_service_trends/service_trend_monthly_served_group.csv')
        ).fillna(0)
        data = data = TEST_DATA_SERVICE_2.get_data_for_definition(62)
        func = calc.data_calc_function_switcher[62]
        result = func(data)
        resultFrame = pandas.read_json(result)
        self.assertTrue(len(resultFrame) == len(expected))
        assert_frame_equal(resultFrame, expected, rtol = REL_TOL)

    
    #test data def 63
    def test_get_service_trend_service_category(self):
        expected = pandas.read_csv(
            os.path.join(__location__, './expected_results/test_calc_service_trends/service_trend_service_category.csv')
        ).fillna(0)
        data = data = TEST_DATA_SERVICE_2.get_data_for_definition(63)
        func = calc.data_calc_function_switcher[63]
        result = func(data)
        resultFrame = pandas.read_json(result)
        self.assertTrue(len(resultFrame) == len(expected))
        assert_frame_equal(resultFrame, expected, rtol = REL_TOL)

    #test data def 64
    def test_get_service_trend_comparison(self):
        expected = pandas.read_csv(
            os.path.join(__location__, './expected_results/test_calc_service_trends/results_service_trend_comparison.csv'),
            index_col = 0
        )

        DS = TEST_DATA_SERVICE_2
        data = DS.get_data_for_definition(64)
        func = calc.data_calc_function_switcher[64]
        result = func(data)
        resultFrame = pandas.read_json(result)
        assert_frame_equal(resultFrame, expected, rtol = REL_TOL)

    # test data def 65
    def test_get_service_summary_dow(self):
        expected = pandas.read_csv(
            os.path.join(__location__, './expected_results/test_calc_service_trends/results_service_summary_dow.csv'),
            index_col = 0
        ).fillna(0)
        scope = {
            "startDate":"01/01/2020",
            "endDate":"03/31/2021",
            "scope_type": "hierarchy",
            "scope_field":"loc_id",
            "scope_field_value":6,
            "control_type_name":"Is Grocery Service"
        }
        DS = TEST_DATA_SERVICE_2
        data = DS.get_data_for_definition(65)
        func = calc.data_calc_function_switcher[65]
        result = func(data)
        resultFrame = pandas.read_json(result)
        expected.n_services = expected.n_services.astype(int64)
        self.assertTrue(len(resultFrame) == len(expected))
        assert_frame_equal(resultFrame, expected, rtol = REL_TOL)

    #test data def 66
    def test_get_service_summary_hod(self):
        expected = pandas.read_csv(
            os.path.join(__location__, './expected_results/test_calc_service_trends/service_summary_hod.csv'),
        ).fillna(0).reset_index().drop(columns = 'index')
        expected['n_services'] = expected['n_services'].astype(int64)

        data = TEST_DATA_SERVICE_2.get_data_for_definition(66)
        func = calc.data_calc_function_switcher[66]
        result = func(data)
        resultFrame = pandas.read_json(result)
        assert_frame_equal(resultFrame, expected, rtol = REL_TOL)

    #test data def 67
    def test_get_service_summary_dowhod(self):
        expected = pandas.read_csv(
            os.path.join(__location__, './expected_results/test_calc_service_trends/service_trend_summary_dowhod.csv'),
            index_col = 0
        ).fillna(0).reset_index().drop(columns = 'index')
        expected = expected.astype({'n_services':'int64'})

        data = TEST_DATA_SERVICE_2.get_data_for_definition(67)
        func = calc.data_calc_function_switcher[67]
        result = func(data)
        resultFrame = pandas.read_json(result)
        assert_frame_equal(resultFrame, expected, rtol = REL_TOL)

    def test_get_service_summary_dowhod_no_times(self):
        folder_path = os.path.join(__location__, 'test_data','test_calc_service_trends', 'edge_cases','no_time_data')
        services = pandas.read_parquet(path = os.path.join(folder_path,'services.parquet'), engine = 'pyarrow')
        skeleton_dowhod = pandas.read_parquet(path = os.path.join(folder_path,'skeleton_dowhod.parquet'), engine = 'pyarrow')
        data = {
            KEY_SERVICE : services,
            SKEY_HOD_DOW : skeleton_dowhod
        }
        func = calc.data_calc_function_switcher[67]
        result = func(data)
        resultFrame = pandas.read_json(result)
        self.assertTrue(numpy.count_nonzero(resultFrame['n_services'].to_numpy()) == 0)

    
    #test data def 68
    def test_get_service_trend_event(self):
        expected = pandas.read_csv(
            os.path.join(__location__, './expected_results/test_calc_service_trends/service_trend_event.csv'),
            index_col = 0
        ).fillna(0).reset_index().drop(columns = 'index')
        #pandas seems to cast quoted numbers into floats
        #need to cast back to int 

        expected = expected.astype({'n_services':'int64'})
        data = TEST_DATA_SERVICE_2.get_data_for_definition(68)
        func = calc.data_calc_function_switcher[68]
        result = func(data)
        resultFrame = pandas.read_json(result)
        self.assertTrue(len(resultFrame) == len(expected))
        assert_frame_equal(resultFrame, expected, rtol = REL_TOL)