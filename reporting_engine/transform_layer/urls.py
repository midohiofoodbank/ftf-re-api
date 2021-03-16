from django.urls import path

from . import views

urlpatterns = [
    path('about/', views.test_endpoint_2, name='about'),
    path('data/<int:id>/', views.test_data_service, name='data'),
    path('get-report-big-numbers/', views.get_report_big_numbers, name='get-report-big-numbers'),
    path('get-report-ohio/', views.get_report_ohio, name='get-report-ohio'),
    path('get-report-mofc/', views.get_report_mofc, name='get-report-mofc'),
    path('demo1/mofc', views.get_demo1_mofc, name='get-demo1-mofc'),
    path('demo1/franklin', views.get_demo1_franklin, name='get-demo1-franklin'),
    path('demo1/typical', views.get_demo1_typical, name='get-demo1-typical'),
    path('get-report-services', views.get_report_services, name='get-report-services'),
    path('get-age-groups', views.get_age_groups, name='get-age-groups'),
    path('get-household-comp', views.get_family_household_comp, name = 'get-household-comp')
]