"""
addins.py

This file contains the function addin_helper, which helps manage repoert addins, a main function for
testing, and a loadTests() function to load data into report_schedules prior to testing.

This script cannot be executed directly from the command line. The code must be copied and pasted
into Django's shell or it must be imported into another script. 


Written by Joy Lin, Nick Biederman, Alli Hornyak, and Emily Robinson
"""

from api.models import ReportSchedule, ReportScope, AddinManager, ReportScheduleAddin
import json
from django.core.serializers.json import DjangoJSONEncoder
from pandas.core.frame import DataFrame
import dateutil.parser as parser
import pandas as pd
from django.db import connections




#get object if exist in AddinManager else return none
def get_or_none(**kwargs):
    try:
        obj = AddinManager.objects.get(**kwargs).reporting_dictionary_id
    except AddinManager.DoesNotExist:  #if nothing matches return None
        obj = None
    return obj

def addin_helper(report_schedule_row):
    report_scope_id = report_schedule_row.report_scope_id
    report_scope_value = report_schedule_row.report_scope_value
    if(report_scope_id<9):
        field_reference=ReportScope.objects.get(pk=report_scope_id).field_reference #getting the field reference
        query=("SELECT DISTINCT dh.fb_id, dh.state_id FROM dim_hierarchies dh WHERE %s = %s" % (field_reference, report_scope_value))
        conn=connections['source_db']
        dim_hier=pd.read_sql(query, conn)
        dim_hier=dim_hier.fillna(0)#replace Nan value with 0 (throws error with Nan)
        fb_id = set(dim_hier['fb_id'].values.tolist())#remove dubplicates using set
        state_id = set(dim_hier['state_id'].values.tolist())
        if(report_scope_id!=6):#only need state_report if report_scope is 6
            for f in fb_id:
                fb_addin = get_or_none(report_scope_id=5,report_scope_value=f)
                if fb_addin is not None:
                    #get obj if exists otherwise create obj, created returns true if object is created
                    obj, created = ReportScheduleAddin.objects.get_or_create(reporting_dictionary_id=fb_addin)
                    report_schedule_row.addin_reports.add(obj)#add obj to addin_reports
        for s in state_id:
            state_addin = get_or_none(report_scope_id=6,report_scope_value=s)
            if state_addin is not None:
                 obj, created = ReportScheduleAddin.objects.get_or_create(reporting_dictionary_id=state_addin)
                 report_schedule_row.addin_reports.add(obj)
        report_schedule_row.save()#update report_schedule_row
        
        


def main():
    """Runs addin_helper on all objects in ReportSchedules. Only runs if called
       manually or if script is caled from command line."""
    for rs in ReportSchedule.objects.all():
        print("_________")        
        addin_helper(rs)
        print(rs.addin_reports.all())        

if __name__ == "__main__":
    """Entry Point if script is called from command line"""
    main()



def loadTests():
    """Loads test data into ReportSchedule table. This should be run once, and should note be run again unless the report_schedules table is flushed. This function
       must be called manually from the shell."""
    #normal test
    ReportSchedule(run_type_id=2, timeframe_type_id=2, report_scope_id=3, report_scope_value= 1,control_type_id=1, reporting_dictionary_id=1, control_age_group_id=1, date_scheduled="2021-02-25").save()
    #report scope id == 9 nothing should be set
    ReportSchedule(run_type_id=2, timeframe_type_id=2, report_scope_id=9, report_scope_value= 2,control_type_id=1, reporting_dictionary_id=1, control_age_group_id=1, date_scheduled="2021-02-25").save()
    #report scope id > 9 nothing should be set
    ReportSchedule(run_type_id=2, timeframe_type_id=2, report_scope_id=13, report_scope_value= 2,control_type_id=1, reporting_dictionary_id=1, control_age_group_id=1, date_scheduled="2021-02-25").save()
    # report scope id == 6 ohio
    ReportSchedule(run_type_id=2, timeframe_type_id=2, report_scope_id=6, report_scope_value= 39,control_type_id=1, reporting_dictionary_id=1, control_age_group_id=1, date_scheduled="2021-02-25").save()
    # report scope id == 6 virgina
    ReportSchedule(run_type_id=2, timeframe_type_id=2, report_scope_id=6, report_scope_value= 51,control_type_id=1, reporting_dictionary_id=1, control_age_group_id=1, date_scheduled="2021-02-25").save()
    # report scope id == 5 MOFC
    ReportSchedule(run_type_id=2, timeframe_type_id=2, report_scope_id=5, report_scope_value= 21,control_type_id=1, reporting_dictionary_id=1, control_age_group_id=1, date_scheduled="2021-02-25").save()
    #only has one report
    ReportSchedule(run_type_id=2, timeframe_type_id=2, report_scope_id=2, report_scope_value= 12,control_type_id=1, reporting_dictionary_id=1, control_age_group_id=1, date_scheduled="2021-02-25").save()
