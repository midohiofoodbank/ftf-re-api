from django.urls import path

from . import views

urlpatterns = [
    path('data/<int:id>/', views.test_data_service, name='data'),
    path('get-all-typical', views.get_all_defs_typical, name='get-all-defs-typical'),
    path('get-fact-services', views.get_fact_services, name = 'get-fact-services'),
    path('get-report-services', views.get_report_services, name='get-report-services'),
    path('get-family-breakdown', views.get_family_breakdown, name = 'get-family-breakdown'),
    path('get-new-families', views.get_new_families, name="get-new-families"),
    path('get-geography', views.get_geography, name="get-geography"),
    path('get-service-trends', views.get_service_trends, name = 'get-service-trends'),
    path('get-family-members', views.get_family_members, name = 'get-family-members'),
    path('get-report-big-numbers', views.get_report_big_numbers, name='get-report-big-numbers'),
    path('get-report-ohio', views.get_report_ohio, name='get-report-ohio'),
    path('get-report-mofc', views.get_report_mofc, name='get-report-mofc'),
    path('demo1/mofc', views.get_demo1_mofc, name='get-demo1-mofc'),
    path('test/geos-no-dummy-trip', views.test_geos_no_dummy_trip, name = 'test_geos_no_dummy_trip')

]