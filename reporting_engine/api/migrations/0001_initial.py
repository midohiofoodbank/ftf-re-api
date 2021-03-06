
# Generated by Django 3.1.6 on 2021-03-23 18:57

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='ControlType',
            fields=[
                ('id', models.AutoField(auto_created=True,
                                        primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(blank=True, max_length=255)),
                ('notes', models.CharField(blank=True, max_length=255)),
            ],
            options={
                'db_table': 'control_types',
            },
        ),
        migrations.CreateModel(
            name='DataDefinition',
            fields=[
                ('id', models.AutoField(auto_created=True,
                                        primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(blank=True, max_length=255)),
                ('definition_public', models.CharField(blank=True, max_length=255)),
                ('calculation_notes', models.CharField(blank=True, max_length=255)),
                ('interpretation_notes', models.CharField(
                    blank=True, max_length=255)),
            ],
            options={
                'db_table': 'data_definitions',
            },
        ),
        migrations.CreateModel(
            name='DataDefinitionType',
            fields=[
                ('id', models.AutoField(auto_created=True,
                                        primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(blank=True, max_length=20)),
            ],
            options={
                'db_table': 'data_definition_types',
            },
        ),
        migrations.CreateModel(
            name='Report',
            fields=[
                ('id', models.AutoField(auto_created=True,
                                        primary_key=True, serialize=False, verbose_name='ID')),
                ('start_date', models.DateField(blank=True, null=True)),
                ('end_date', models.DateField(blank=True, null=True)),
                ('date_completed', models.DateField(blank=True, null=True)),
            ],
            options={
                'db_table': 'reports',
            },
        ),
        migrations.CreateModel(
            name='ReportingDictionary',
            fields=[
                ('id', models.AutoField(auto_created=True,
                                        primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(blank=True, max_length=255)),
                ('definition', models.CharField(blank=True, max_length=255)),
            ],
            options={
                'db_table': 'reporting_dictionaries',
            },
        ),
        migrations.CreateModel(
            name='ReportScope',
            fields=[
                ('id', models.AutoField(auto_created=True,
                                        primary_key=True, serialize=False, verbose_name='ID')),
                ('type', models.CharField(blank=True, max_length=255)),
                ('name', models.CharField(blank=True, max_length=255)),
                ('field_reference', models.CharField(blank=True, max_length=255)),
            ],
            options={
                'db_table': 'report_scopes',
            },
        ),
        migrations.CreateModel(
            name='RunType',
            fields=[
                ('id', models.AutoField(auto_created=True,
                                        primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(blank=True, max_length=255)),
            ],
            options={
                'db_table': 'run_types',
            },
        ),
        migrations.CreateModel(
            name='TimeframeType',
            fields=[
                ('id', models.AutoField(auto_created=True,
                                        primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(blank=True, max_length=255)),
                ('dim_dates_reference', models.TextField(blank=True)),
                ('current_start_date', models.DateField(blank=True, null=True)),
                ('current_end_date', models.DateField(blank=True, null=True)),
                ('recurrence_type', models.CharField(blank=True, max_length=255)),
            ],
            options={
                'db_table': 'timeframe_types',
            },
        ),
        migrations.CreateModel(
            name='ReportSchedule',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('report_scope_value', models.CharField(max_length=255)),
                ('control_age_group_id', models.IntegerField()),
                ('date_scheduled', models.DateField()),
                ('date_custom_start', models.DateField(blank=True, null=True)),
                ('date_custom_end', models.DateField(blank=True, null=True)),
                ('addin_foodbank_report', models.ForeignKey(blank=True, null=True,
                                                            on_delete=django.db.models.deletion.CASCADE, related_name='addin_foodbank', to='api.reportingdictionary')),
                ('addin_state_report', models.ForeignKey(blank=True, null=True,
                                                         on_delete=django.db.models.deletion.CASCADE, related_name='addin_state', to='api.reportingdictionary')),
                ('control_type', models.ForeignKey(
                    on_delete=django.db.models.deletion.CASCADE, to='api.controltype')),
                ('report_scope', models.ForeignKey(
                    on_delete=django.db.models.deletion.CASCADE, to='api.reportscope')),
                ('reporting_dictionary', models.ForeignKey(
                    on_delete=django.db.models.deletion.CASCADE, to='api.reportingdictionary')),
                ('run_type', models.ForeignKey(
                    on_delete=django.db.models.deletion.CASCADE, to='api.runtype')),
                ('timeframe_type', models.ForeignKey(
                    on_delete=django.db.models.deletion.CASCADE, to='api.timeframetype')),
            ],
            options={
                'db_table': 'report_schedules',
            },
        ),
        migrations.CreateModel(
            name='ReportingDictionarySection',
            fields=[
                ('id', models.AutoField(auto_created=True,
                                        primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(blank=True, max_length=255)),
                ('reporting_dictionary', models.ForeignKey(blank=True, null=True,
                                                           on_delete=django.db.models.deletion.CASCADE, to='api.reportingdictionary')),
            ],
            options={
                'db_table': 'reporting_dictionary_sections',
            },
        ),
        migrations.CreateModel(
            name='ReportDataInt',
            fields=[
                ('id', models.AutoField(auto_created=True,
                                        primary_key=True, serialize=False, verbose_name='ID')),
                ('int_value', models.PositiveIntegerField(blank=True, null=True)),
                ('data_definition', models.ForeignKey(blank=True, null=True,
                                                      on_delete=django.db.models.deletion.CASCADE, to='api.datadefinition')),
                ('report', models.ForeignKey(blank=True, null=True,
                                             on_delete=django.db.models.deletion.CASCADE, to='api.report')),
            ],
            options={
                'db_table': 'report_data_int',
            },
        ),
        migrations.CreateModel(
            name='ReportDataFloat',
            fields=[
                ('id', models.AutoField(auto_created=True,
                                        primary_key=True, serialize=False, verbose_name='ID')),
                ('float_value', models.FloatField(blank=True, null=True)),
                ('data_definition', models.ForeignKey(blank=True, null=True,
                                                      on_delete=django.db.models.deletion.CASCADE, to='api.datadefinition')),
                ('report', models.ForeignKey(blank=True, null=True,
                                             on_delete=django.db.models.deletion.CASCADE, to='api.report')),
            ],
            options={
                'db_table': 'report_data_float',
            },
        ),
        migrations.AddField(
            model_name='report',
            name='report_schedule',
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE, to='api.reportschedule'),
        ),
        migrations.AddField(
            model_name='datadefinition',
            name='data_definition_type',
            field=models.ForeignKey(
                blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to='api.datadefinitiontype'),
        ),
        migrations.CreateModel(
            name='AddinManager',
            fields=[
                ('id', models.AutoField(auto_created=True,
                                        primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(blank=True, max_length=255, null=True)),
                ('report_scope_value', models.PositiveIntegerField(
                    blank=True, null=True)),
                ('control_type', models.ForeignKey(blank=True, null=True,
                                                   on_delete=django.db.models.deletion.CASCADE, to='api.controltype')),
                ('report_scope', models.ForeignKey(blank=True, null=True,
                                                   on_delete=django.db.models.deletion.CASCADE, to='api.reportscope')),
                ('reporting_dictionary', models.ForeignKey(blank=True, null=True,
                                                           on_delete=django.db.models.deletion.CASCADE, to='api.reportingdictionary')),
            ],
            options={
                'db_table': 'addin_manager',
            },
        ),
        migrations.CreateModel(
            name='ReportingDictionaryDefinition',
            fields=[
                ('id', models.AutoField(auto_created=True,
                                        primary_key=True, serialize=False, verbose_name='ID')),
                ('data_definition', models.ForeignKey(blank=True, null=True,
                                                      on_delete=django.db.models.deletion.CASCADE, to='api.datadefinition')),
                ('report_dictionary', models.ForeignKey(blank=True, null=True,
                                                        on_delete=django.db.models.deletion.CASCADE, to='api.reportingdictionary')),
                ('section', models.ForeignKey(
                    on_delete=django.db.models.deletion.CASCADE, to='api.reportingdictionarysection')),
            ],
            options={
                'db_table': 'reporting_dictionary_definitions',
                'unique_together': {('id', 'section_id')},
            },
        ),
        migrations.CreateModel(
            name='ReportDataJson',
            fields=[
                ('id', models.AutoField(auto_created=True,
                                        primary_key=True, serialize=False, verbose_name='ID')),
                ('json_object', models.JSONField(blank=True)),
                ('data_definition', models.ForeignKey(
                    on_delete=django.db.models.deletion.CASCADE, to='api.datadefinition')),
                ('report', models.ForeignKey(
                    on_delete=django.db.models.deletion.CASCADE, to='api.report')),
            ],
            options={
                'db_table': 'report_data_json',
                'unique_together': {('id', 'report_id', 'data_definition_id')},
            },
        ),
    ]
