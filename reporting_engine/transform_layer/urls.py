from django.urls import path

from . import views

urlpatterns = [
    path('data/<int:id>/', views.test_data_service, name='data'),
    path('get-all/typical', views.get_all_defs_typical, name='get-all-defs-typical'),
    path('get-report-big-numbers/', views.get_report_big_numbers, name='get-report-big-numbers'),
    path('get-report-ohio/', views.get_report_ohio, name='get-report-ohio'),
    path('get-report-mofc/', views.get_report_mofc, name='get-report-mofc'),
    path('get-report-services', views.get_report_services, name='get-report-services'),
    path('get-family-breakdown', views.get_family_breakdown, name = 'get-family-breakdown'),
    path('demo1/franklin', views.get_demo1_franklin, name='get-demo1-franklin'),
    path('demo1/mofc', views.get_demo1_mofc, name='get-demo1-mofc'),
    path('demo2/def3-large', views.test_data_def_3_large, name = 'test-data-def-3-large'),
    path('demo2/def3-typical', views.test_data_def_3_typical, name = 'test-data-def-3-typical'),
    path('get-age-groups', views.get_age_groups, name='get-age-groups')
]