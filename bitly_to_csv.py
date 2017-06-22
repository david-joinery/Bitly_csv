#imports
import sys
import os
import datetime
from datetime import date
import config
from bitly_api_python import bitly_api
from bitly_api_python.bitly_api import Connection, Error, BitlyError
import time
from time import sleep
from datetime import timedelta
import csv
import json
from urllib.request import urlopen
import pdb
import operator

#final variables
DAYS_BACK = 30

#load in date/time info
load_date = date.today()
diff = datetime.timedelta(-DAYS_BACK)
start_dt = load_date + diff
end_dt = load_date

tzOffset = "America/New_York"


#connect to bitly_api
conn_bitly = bitly_api.Connection(access_token = 'c6352ab7c958bbfdd0817670d25ac81a7292f264')

#helper functions

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

#helper function to return the creation date of a link  
def get_creation_date(link):
    raw_info = conn_bitly.info(link)
    creation_time = raw_info[0]['created_at']
    creation_date = time.gmtime(creation_time)[0], time.gmtime(creation_time)[1], time.gmtime(creation_time)[2]
    formatted_date =datetime.date(creation_date[0],creation_date[1],creation_date[2])
    return formatted_date

#helper function to calculate the amount of days it has been since the link was created
def calculate_cohort_day(start_date,current_date):
    cohort_day = current_date - start_date
    return cohort_day.days


def sortcsvbymanyfields(csvfilename, themanyfieldscolumnnumbers):
    with open(csvfilename, 'r') as f:
        readit = csv.reader(f)
        thedata = list(readit)
    thedata.sort(key=operator.itemgetter(*themanyfieldscolumnnumbers))
    with open(csvfilename, 'w') as f:
        writeit = csv.writer(f)
        writeit.writerows(thedata)

        

#collect the popular links
pop_links = conn_bitly.user_popular_links()


#create csv and write header row
with open("bitly_metrics_pst.csv",'w',newline='') as csvfile:
    fieldnames = ['Date Created', 'Click Date','Cohort Day', 'Total Clicks','Clicks Today', 'Link','Hash','Admin/Lister' ]
    wr = csv.writer(csvfile,fieldnames)
    wr.writerow(fieldnames)


    #print date range
    print ("###### Starting at " + str(datetime.datetime.now()))
    print ("###### for dates " + str(end_dt) + " to " + str(start_dt))

    #loop through days from start date to end date
    while end_dt >= start_dt:
        
        #convert start_dt to epoch timestamp
        ts = int(time.mktime(end_dt.timetuple()))
        
        print ("* " + time.strftime("%m/%d/%y", time.localtime(ts)))
        date = end_dt
        current_day = date.timetuple()[2]
        current_month = date.timetuple()[1]
        #loop through the links collected
        for pop_link in pop_links:
        
        
            #separate link info into individual variables
            link = pop_link['link']
            clicks = conn_bitly.link_clicks(link)
            link_hash = pop_link['hash']
            day_created = get_creation_date(link_hash)
            cohort_day = calculate_cohort_day(day_created, date)
            print(cohort_day)

            #check if clicks occured on specified date
            clicks_by_day = conn_bitly.link_clicks(link, units = 30, rollup = False)
            
            #loop through days
            clicks_today = 0
            for day in clicks_by_day:
                click_time = time.gmtime(day['dt'])
                click_day = click_time[2]
                click_month = click_time[1]
                if (int(current_month) == int(click_month) and int(current_day) == int(click_day)):
                    #print('works')
                    clicks_today = day['clicks']
                
            #pdb.set_trace()
            long_url = conn_bitly.expand(link_hash)
            admin_test = long_url[0]['long_url']
            if admin_test.find('admin') == -1 :
                rowData = ([day_created, date, cohort_day, clicks, clicks_today, link, link_hash, 'Lister'])
            #write the link information(date loaded, short link, hash, number of clicks) to the csv
            else:
                rowData = ([day_created, date, cohort_day, clicks, clicks_today, link, link_hash, 'Admin'])
            wr.writerow(rowData)
            
        #iterate to next day
        end_dt += datetime.timedelta(days=-1)


#close the csv file
csvfile.close()

#sort the file

#finished
print ("###### Finished at " + str(datetime.datetime.now()))









