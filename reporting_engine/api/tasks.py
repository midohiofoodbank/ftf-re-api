from celery import shared_task
from api.utils import get_data_definitions
from transform_layer.calculations import CalculationDispatcher
from api.models import ReportSchedule, Report, ReportDataInt, ReportDataFloat
from datetime import date

# test to ensure tasks/Celery are working
@shared_task
def test():
    print("hi")

# given a report schedule, generates a report and then saves it to the database
def generate_report_and_save(schedule):
	# get data definitions for current schedule and perform necessary calculations to generate the report
	data_def_dict = get_data_definitions(schedule.id)

	cd = CalculationDispatcher(data_def_dict)
	cd.run_calculations()

	# save the generated report to the database
	save_report(schedule, data_def_dict)


# generates recurring reports if they are due based on recurrence parameter
@shared_task
def periodic_report_generation(recurrence):
	for schedule in ReportSchedule.objects.all():
    		if(schedule.timeframe_type.recurrence == recurrence):
    				generate_report_and_save(schedule)

### This section of code is written in case celery is unable to pass a parameter as shown in periodic_report_generation. 
### If we are able to pass the recurrence parameter this will block of code will be obselete.
### Otherwise, the periodic_report_generation function will be obselete and this block will be used instead.
# # generates recurring reports if they are monthly
# @shared_task
# def periodic_report_generation_monthly():
# 	for schedule in ReportSchedule.objects.all():
#     		if(schedule.timeframe_type.recurrence == "monthly"):
#     				generate_report_and_save(schedule)

# # generates recurring reports if they are weekly
# @shared_task
# def periodic_report_generation_weekly():
# 	for schedule in ReportSchedule.objects.all():
#     		if(schedule.timeframe_type.recurrence == "weekly"):
#     				generate_report_and_save(schedule)

# # generates recurring reports if they are daily
# @shared_task
# def periodic_report_generation_daily():
# 	for schedule in ReportSchedule.objects.all():
#     		if(schedule.timeframe_type.recurrence == "daily"):
#     				generate_report_and_save(schedule)

# generates (and saves) a one time report if it has been requested
@shared_task
def one_time_report_generation(schedule):
    generate_report_and_save(schedule)

# saves the given calculated report to the database
def save_report(schedule, results):
    # New report to the report
    dateCompleted = date.today().strftime('%Y-%m-%d')
    new_report = Report(report_schedule = schedule, start_date = results['Scope']['startDate'], end_date = results['Scope']['endDate'], date_completed = dateCompleted)
    new_report.save()

    # New rows to report_data_int/report_data_float
    for values in results['ReportInfo']:
        if(values['dataDefType'] == 'integer'):
            new_data_int = ReportDataInt(report = new_report, data_definition_id = values['dataDefId'], int_value = values['value'])
            new_data_int.save()
        elif(values['dataDefType'] == 'float'):
            new_data_float = ReportDataFloat(report = new_report, data_definition_id = values['dataDefId'], float_value = values['value'])
            new_data_float.save()

# used for testing purposes
mock_dict = {
	'Scope':  {
		'startDate': '2019-01-01',
		'endDate': '2019-12-31',
		'scope_type': 'hierarchy',
		'scope_field': 'fb_id',
		'scope_field_value': 21,
		'control_type_field': 'dummy_is_grocery_service',
		'control_type_value': 1
	},
	'ReportInfo': [
		{
			'reportId': 1,
			'reportDictId': 1, 
			'dataDefId': 1,
			'dataDefType': 'integer',
			'name': 'services_total',
			'value': 1040876
		}, 
		{
			'reportId': 1,
			'reportDictId': 1,
			'dataDefId': 2,
			'dataDefType': 'integer',
			'name': 'undup_hh_total',
			'value': 161114
		},
        {
			'reportId': 1,
			'reportDictId': 1,
			'dataDefId': 2,
			'dataDefType': 'float',
			'name': 'undup_hh_total',
			'value': 1.507
		}
    ]
}
