"""
models.py

This file contains definitions for tables in the reports database. These definitions
are used by Django to create migrations. These migrations are then used by Django to
create or modify tables in the mySQL database whose connection is defined in ../.env.

Each class defines a table. The name of the class is the name used to access the table
via python. Each class has a Meta subclass. This is used to define the name of the table
in the mySQL data base. The name defined in Meta matches the names used in the ERD provided
my MOFC. The class name is a singular PascalCase version of this name.

To access a table, use `from api.models import <ClassName1>, <ClassName2>, ...`.

Written by Joy Lin, Nick Biederman, Alli Hornyak, and Emily Robinson
"""

from django.db import models
import datetime

class TimeframeType(models.Model):
	"""Defines TimeframeType table (named time_frame_types in mysql database)"""
	name = models.CharField(max_length = 255, blank = True)
	dim_dates_reference = models.TextField(blank = True)
	current_start_date = models.DateField(null = True, blank = True)
	current_end_date = models.DateField(null = True, blank = True)
	class Meta:
		db_table = 'timeframe_types'
	def __str__(self):
		return self.name

class ControlType(models.Model):
	"""Defines ControlType table (control_types in mysql database)"""
	name = models.CharField(max_length = 255, blank = True)
	notes = models.CharField(max_length = 255, blank = True)
	class Meta:
		db_table = 'control_types'
	def __str__(self):
		return self.name

class DataDefinitionType(models.Model):
	"""Defines DataDefinitionType table (data_definition_types in mysql database)"""
	name = models.CharField(max_length = 20, blank = True)
	class Meta:
		db_table = 'data_definition_types'
	def __str__(self):
		return self.name

class DataDefinition(models.Model):
	"""Defines DataDefinition table (data_definitions in mysql database)"""
	name = models.CharField(max_length = 255, blank = True)
	definition_public = models.CharField(max_length = 255, blank = True)
	calculation_notes = models.CharField(max_length = 255, blank = True)
	interpretation_notes = models.CharField(max_length = 300, blank = True)
	data_definition_type = models.ForeignKey(DataDefinitionType, on_delete = models.CASCADE, null = True, blank = True)
	class Meta:
		db_table = 'data_definitions'
	def __str__(self):
		return self.name

class ReportScope(models.Model):
	"""Defines ReportScope table (report_scopes in mysql database)"""
	type = models.CharField(max_length = 255, blank = True)
	name = models.CharField(max_length = 255, blank = True)
	field_reference = models.CharField(max_length = 255, blank = True)
	class Meta:
		db_table = 'report_scopes'
	def __str__(self):
		return self.name

class ReportingDictionary(models.Model):
	"""Defines ReportingDictionary table (reporting_dictionaries in mysql database)"""
	name = models.CharField(max_length = 255, blank = True)
	definition = models.CharField(max_length = 255, blank = True)
	class Meta:
		db_table = 'reporting_dictionaries'
	def __str__(self):
		return self.name

class ReportingDictionarySection(models.Model):
	"""Defines ReportingDictionarySection table (reporting_dictionary_sections in mysql database)"""
	name = models.CharField(max_length = 255, blank = True)
	reporting_dictionary = models.ForeignKey(ReportingDictionary, on_delete = models.CASCADE, null = True, blank = True)
	class Meta:
		db_table = 'reporting_dictionary_sections'
	def __str__(self):
		return self.name

class ReportingDictionaryDefinition(models.Model):
	"""Defines ReportingDictionaryDefinition table (reporting_dictionary_definitions in mysql database)"""
	report_dictionary = models.ForeignKey(ReportingDictionary, on_delete = models.CASCADE, null = True, blank = True)
	data_definition = models.ForeignKey(DataDefinition, on_delete = models.CASCADE, null = True, blank = True)
	section = models.ForeignKey(ReportingDictionarySection, on_delete = models.CASCADE)
	class Meta:
		unique_together = (('id', 'section_id'), )
		db_table = 'reporting_dictionary_definitions'
	def __str__(self):
		return str(self.id)

class RunType(models.Model):
	"""Defines RunType table (run_types in mysql database)"""
	name = models.CharField(max_length = 255, blank = True)
	class Meta:
		db_table = 'run_types'
	def __str__(self):
		return self.name

class ReportScheduleAddin(models.Model):
	reporting_dictionary = models.ForeignKey(ReportingDictionary, on_delete=models.CASCADE, null = True, blank = True)
	class Meta:
		db_table = 'report_schedule_addins'
	def __str__(self):
		return str(self.id)

class ReportSchedule(models.Model):
	"""Defines ReportSchedule table (report_schedules in mysql database)"""
	run_type = models.ForeignKey(RunType, on_delete = models.CASCADE)
	timeframe_type = models.ForeignKey(TimeframeType, on_delete = models.CASCADE)
	report_scope = models.ForeignKey(ReportScope, on_delete = models.CASCADE)
	report_scope_value = models.PositiveIntegerField()
	control_type = models.ForeignKey(ControlType, on_delete = models.CASCADE)
	reporting_dictionary = models.ForeignKey(ReportingDictionary, on_delete = models.CASCADE)
	control_age_group_id = models.IntegerField()
	date_scheduled = models.DateField(default = datetime.date.today)
	date_custom_start = models.DateField(null = True, blank = True)
	date_custom_end = models.DateField(null = True, blank = True)
	addin_reports = models.ManyToManyField(ReportScheduleAddin, through = 'ReportScheduleAddinReport', blank = True)
	class Meta:
		db_table = 'report_schedules'
	def __str__(self):
		return str(self.id)

class ReportScheduleAddinReport(models.Model):
	report_schedule = models.ForeignKey(ReportSchedule, on_delete=models.CASCADE)
	report_schedule_addin = models.ForeignKey(ReportScheduleAddin, on_delete=models.CASCADE)
	class Meta:
		db_table = 'report_schedule_addin_reports'
	def __str__(self):
		return str(self.id)

class Report(models.Model):
	"""Defines Report table (reports in mysql database)"""
	report_schedule = models.ForeignKey(ReportSchedule, on_delete = models.CASCADE)
	start_date = models.DateField(null = True, blank = True)
	end_date = models.DateField(null = True, blank = True)
	date_completed = models.DateField(null = True, blank = True)
	class Meta:
		db_table = 'reports'
	def __str__(self):
		return str(self.id)

class AddinManager(models.Model):
	"""Defines AddinManager table (addin_manager in mysql database)"""
	name = models.CharField(max_length = 255, null = True, blank = True)
	reporting_dictionary = models.ForeignKey(ReportingDictionary, on_delete = models.CASCADE, null = True, blank = True)
	report_scope = models.ForeignKey(ReportScope, on_delete = models.CASCADE, null = True, blank = True)
	report_scope_value = models.PositiveIntegerField(null = True, blank = True)
	control_type = models.ForeignKey(ControlType, on_delete = models.CASCADE, null = True, blank = True)
	class Meta:
		db_table = 'addin_manager'
	def __str__(self):
		return self.name

class ReportDataFloat(models.Model):
	"""Defines ReportDataFloat table (report_data_float in mysql database)"""
	report = models.ForeignKey(Report, on_delete = models.CASCADE, null = True, blank = True)
	data_definition = models.ForeignKey(DataDefinition, on_delete = models.CASCADE, null = True, blank = True)
	float_value = models.FloatField(null = True, blank = True)
	class Meta:
		db_table = 'report_data_float'
	def __str__(self):
		return str(self.id)

class ReportDataInt(models.Model):
	"""Defines ReportDataInt table (report_data_int in mysql database)"""
	report = models.ForeignKey(Report, on_delete = models.CASCADE, null = True, blank = True)
	data_definition = models.ForeignKey(DataDefinition, on_delete = models.CASCADE, null = True, blank = True)
	int_value =  models.PositiveIntegerField(null = True, blank = True)
	class Meta:
		db_table = 'report_data_int'
	def __str__(self):
		return str(self.id)

class ReportDataJson(models.Model):
	"""Defines ReportDataJson table (report_data_json in mysql database)"""
	report = models.ForeignKey(Report, on_delete = models.CASCADE)
	data_definition = models.ForeignKey(DataDefinition, on_delete = models.CASCADE)
	json_object = models.JSONField(blank = True)
	class Meta:
		unique_together = (('id', 'report_id', 'data_definition_id'), )
		db_table = 'report_data_json'
	def __str__(self):
		return str(self.id)
