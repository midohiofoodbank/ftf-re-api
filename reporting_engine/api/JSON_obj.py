import json
import psycopg2
import MySQLdb
import mysql.connector


try:
    conn=mysql.connector.connect(host='localhost', database='API', user='XueyangLi', password='Wozhixihuanni48')
    
    if conn.is_connected():
        db_Info = conn.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = conn.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)
except Error as e:
    print("Error while connecting to MySQL", e)
print('\n')
def query_db(query, one=False):
    cur = conn.cursor()
    cur.execute(query)
    r = [dict((cur.description[i][0], value) \
               for i, value in enumerate(row)) for row in cur.fetchall()]
               
    return (r[0] if r else None) if one else r

ID=input('Please enter the report ID: ')
data_existQuery='SELECT no_data FROM reports WHERE ID='+ID+';'
data_exist=query_db(data_existQuery)[0]['no_data']
json_dataExist=json.dumps(query_db(data_existQuery))

SchedueleQuery='SELECT report_schedule_id FROM reports WHERE ID='+ID+';'
Scheduele=query_db(SchedueleQuery)[0]['report_schedule_id']
jsonScheduleID=json.dumps(query_db(SchedueleQuery))

dateCompQuery='SELECT date_completed FROM reports WHERE ID='+ID+';'
dateComp=query_db(dateCompQuery)[0]['date_completed']
jsonDayComp=json.dumps(query_db(dateCompQuery))

RunTypeQuery='SELECT run_type_id FROM report_schedules WHERE ID='+ID+';'
RunType=query_db(RunTypeQuery)[0]['run_type_id']
jsonRunType=json.dumps(query_db(RunTypeQuery))

meta='Meta:\n' + '[{"report_id": '+ ID +'}]\n'+jsonScheduleID+'\n'+json_dataExist+'\n'+jsonRunType+'\n'+'[{"_all_other_report_parameters_": "enumerate them"}] \n'+jsonDayComp
print (meta)

if data_exist==0:
    query='SELECT * FROM addin_ohio WHERE ID='+ID+';'
    my_query = query_db(query)

    query2='SELECT * FROM addin_mid_ohio_foodbank WHERE ID='+ID+';'
    my_query2 = query_db(query2)

    big_num='SELECT * FROM big_numbers WHERE ID='+ID+';'
    big_query=query_db(big_num)

    service='SELECT * FROM services WHERE ID='+ID+';'
    ser_query=query_db(service)

    families='SELECT * FROM families WHERE ID='+ID+';'
    fam_query=query_db(families)

    new_families='SELECT * FROM new_families WHERE ID='+ID+';'
    newF_query=query_db(new_families)

    geographic_origin='SELECT * FROM geographic_origin WHERE ID='+ID+';'
    geographic_origin_query=query_db(geographic_origin)

    family_members='SELECT * FROM family_members WHERE ID='+ID+';'
    family_members_query=query_db(family_members)

    trends='SELECT * FROM trends WHERE ID='+ID+';'
    trends_query=query_db(trends)

    json_output1 = json.dumps(my_query)
    json_output2 = json.dumps(my_query2)
    json_outputBIG=json.dumps(big_query) 
    json_outputSER= json.dumps(ser_query) 
    json_fam=json.dumps(fam_query)
    json_newF=json.dumps(newF_query)
    json_geo=json.dumps(geographic_origin_query)
    json_faMem=json.dumps(family_members_query)
    json_tren=json.dumps(trends_query) 
    result='addin_ohio: \n'+json_output1+'\n'+'addin_mid_ohio_foodbank: \n'+json_output2+'\n'+'big_numbers: \n'+json_outputBIG+'\n'+'services: \n'+json_outputSER
    result=result+'families: \n'+json_fam+'\n'+'new_families: \n'+json_newF+'\n'+'geographic_origin: \n'+json_geo+'\n'
    result=result+'family_members: \n'+json_faMem+'\n'+'trends: \n'+json_tren
    print("data: \n"+result)
else:
    print ('"data": {}')
