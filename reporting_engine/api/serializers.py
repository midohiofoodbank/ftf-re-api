from rest_framework import serializers
from .models import Report, ReportSchedule, RunType, TimeframeType, ReportScope, ControlType, ReportingDictionary


class ReportSerializer(serializers.ModelSerializer):
    class Meta:
        model = Report
        depth = 2
        fields = ['report_schedule', 'start_date',
                  'end_date', 'date_completed']


class ReportScheduleSerializer(serializers.ModelSerializer):

    #checks if report schedule exists in report_schedules
    def check_uniqueness(self):
        #for all the current schedules in ReportSchedule check for uniqueness, if report is unique return 0 otherwise return report.id of original
        for schedule in ReportSchedule.objects.all():
      	    #if two report schedules match in every field they are duplicates and we return the id of the original report schedule
            if (schedule.run_type.id == self.data['run_type'] and 
			    schedule.timeframe_type.id == self.data['timeframe_type'] and
			    schedule.report_scope.id == self.data['report_scope'] and
			    schedule.report_scope_value == self.data['report_scope_value'] and
			    schedule.control_type.id == self.data['control_type'] and
			    schedule.reporting_dictionary.id == self.data['reporting_dictionary'] and
			    schedule.control_age_group_id == self.data['control_age_group_id']):
			    #if a report schedule is a "One Time" report we must also check the custom start/end dates before returning a value
                if(self.data['run_type'] == 1):
                    if (schedule.date_custom_start.id == self.data['date_custom_start'] and schedule.date_custom_end.id == self.data['date_custom_end']):
                        return ReportScheduleSerializer(schedule)
                    else:
                        return None
                else:
                    return ReportScheduleSerializer(schedule)
        return None

    class Meta:
        model = ReportSchedule
        fields = '__all__'

class RunTypeSerializer(serializers.ModelSerializer):
    class Meta:
        model = RunType
        fields = '__all__'


class TimeframeTypeSerializer(serializers.ModelSerializer):
    class Meta:
        model = TimeframeType
        fields = '__all__'


class ReportScopeSerializer(serializers.ModelSerializer):
    class Meta:
        model = ReportScope
        fields = '__all__'


class ControlTypeSerializer(serializers.ModelSerializer):
    class Meta:
        model = ControlType
        fields = '__all__'


class ReportingDictionarySerializer(serializers.ModelSerializer):
    class Meta:
        model = ReportingDictionary
        fields = '__all__'
