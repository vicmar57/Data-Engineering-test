# -*- coding: utf-8 -*-
"""
Created on Mon Jun  8 12:45:54 2020

@author: vicma
"""

import requests
import json
from datetime import datetime


def connect_to_db_init_tables(): #get credentials and try to connect to DB
    import mysql.connector
    from mysql.connector import errorcode
    import sys
    
    try:
        with open('SQLdbCred.properties') as file:
            DBcred = json.load(file)
        
        cnx = mysql.connector.connect(user= DBcred["user"], 
                                      password= DBcred["password"],
                                      host= DBcred["host"],
                                      database= DBcred["database"])
    except mysql.connector.Error as err:
      if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
        sys.exit()
      elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
        sys.exit()
      else:
        print(err)
        sys.exit()
    else:
        print('connected to remote db')
        #init required DB tables
        cursor = cnx.cursor()
        cursor.execute("DROP TABLE IF EXISTS city_stats_Victor_Martinov")
        cursor.execute("DROP TABLE IF EXISTS orbital_data_Victor_Martinov")
        cursor.execute("""CREATE TABLE IF NOT EXISTS orbital_data_Victor_Martinov (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            city VARCHAR(255) NOT NULL,
                            pass_Tstamp_UTC TIMESTAMP )
                            """)   
        cursor.execute(""" CREATE TABLE IF NOT EXISTS city_stats_Victor_Martinov (
                            id INT AUTO_INCREMENT UNIQUE PRIMARY KEY,
                            city VARCHAR(255) UNIQUE NOT NULL,
                            avg_passes FLOAT NOT NULL )
                            """)

        return cnx, cursor
    
    
def insert_to_db(to_insert, insert_query): #insert to_insert via insert_query
    cursor = cnx.cursor()
    cursor.executemany(insert_query, to_insert)
    cnx.commit()
    print(cursor.rowcount, "Records inserted successfully into table")
    cursor.close()


def get_from_db(query): #query db with select statement and retrieve results + field names if needed
    cursor = cnx.cursor()
    cursor.execute(query)
    field_names = [i[0] for i in cursor.description]

    res = []
    for record in cursor:
        res.append(record) #(record[0], float(record[1]))
    
    cursor.close()
    return res, field_names


def parse_response(city_name, response): #parse response from ISS API call 
    response_list = json.loads(response)["response"]
    parsed_data = []
    for entry in response_list:
        parsed_data.append( (city_name, datetime.utcfromtimestamp(entry["risetime"])) )
    
    return parsed_data


def main():
    #get data for each city
    for city in city_details:
        params = {'lon': city['longitude'], 'lat': city['latitude'], 'n': passes}
        url = "http://api.open-notify.org/iss-pass.json"
        r = requests.get(url = url, params = params)
        
        response = parse_response(city['city_name'], r.text)
        
        #insert API call parsed results into DB
        mySql_insert_query = """ INSERT INTO orbital_data_Victor_Martinov (city, pass_Tstamp_UTC) 
                       VALUES (%s, %s)"""
        insert_to_db(response, mySql_insert_query)
    #execute Stored Procedure, get avg ISS passes per city        
    cursor.callproc('avg_ISS_pass_Victor_Martinov')
    avgs  = [] 
    for result in cursor.stored_results():
        avgs = result.fetchall()
    
    avgs_insert_query = """INSERT INTO city_stats_Victor_Martinov (city, avg_passes) 
                        VALUES (%s, %s) 
                        ON DUPLICATE KEY UPDATE 
                        avg_passes = VALUES(avg_passes) """
    insert_to_db(avgs, avgs_insert_query)
    
    #combine city_stats tables together, and with my own one to form single, full city_stats table. Then save to output CSV
    combining_query = """ 
                        select stats.city, population, max_temperature, min_temperature, update_date, avg_passes
                            from(
                                select * from city_stats_haifa
                                union all
                                select * from city_stats_eilat
                                union all
                                select * from city_stats_tel_aviv
                                union all
                                select * from city_stats_beer_sheva
                                ) as stats
                        LEFT JOIN city_stats_Victor_Martinov as vm_stats
                        ON vm_stats.city = stats.city 
                    """
    final_combined_table , columns = get_from_db(combining_query)
    
    cnx.close()
    
    import csv
    with open('finalCSV.csv','w') as out:
        writer = csv.writer(out, lineterminator='\n')
        writer.writerow(columns)
        writer.writerows(final_combined_table)
        
if __name__ == "__main__":
    cnx, cursor = connect_to_db_init_tables()
    
    #get city configs (names, longitude, latitude)
    with open('city_details.json') as json_file:
        city_confs = json.load(json_file)
        passes = city_confs['passes'] #50
        city_details = city_confs['city_details']
    
    main()
        
        
# stored procedure created
# CREATE PROCEDURE avg_ISS_pass_Victor_Martinov()
# BEGIN
#     select city, avg (day_cnt) as avg_passes from (
#       select city, DATE(pass_Tstamp_UTC) as date , count(id) as day_cnt
#       from orbital_data_Victor_Martinov
#       group by city, date
#       ) as temp
#     group by city;
# END;



# feedback to improve on:
# 1. split to smaller funcs, write more generic code, get rid of import to func, handle error handling whenever needed