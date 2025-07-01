

import requests
import csv
from io import StringIO
from datetime import datetime, timedelta
import time
import concurrent.futures
import pandas as pd
from collections import defaultdict
from dotenv import load_dotenv
import os


API_EMPLOYEE_TIMESHEET = f"https://wbthecreativejewellerylimited.sage.hr/api/timesheets/clock-in"
API_CREATE_WORKDAY = "https://wbthecreativejewellerylimited.sage.hr/api/timesheets/workdays"
API_LIST_ACTIVE_EMPLOYEES = "https://wbthecreativejewellerylimited.sage.hr/api/employees"
API_COMPLIANCE_GENIE = "https://api.be-safetech.com/te3kx2bj6u2y3ru/api/ExportUsersCheckInCheckOutDetails?APIID=96f272d8-4ea2-4306-8142-d92cf1fa3933&APIKEY=28be8402-a90b-4472-a723-01a630eb7416"
API_KEY = os.getenv("API_KEY")

# number of all clockin records to send in Sage API
countAll = 0

# number of completed or processed records
countCompleted = 0
 
def writeToTxtFile(text):

    # Open a file named 'last_date.txt' in write mode
    with open('last_date.txt', 'w') as f:
        # Write some text to the file
        f.write(text)

def getPrevDate():
    try:
        # Get the current date and time
        current_date_time = datetime.now()

        # Subtract one day from the current date and time
        one_day_delta = timedelta(days=2)
        previous_date_time = current_date_time - one_day_delta

        # Format the previous date and time as a string in the specified format
        formatted_previous_date_time = previous_date_time.strftime("%m/%d/%Y %H:%M:%S")

        return formatted_previous_date_time
    except Exception as e:
        print("Error: Getting previous date.", e)
    
def get_last_date():

    try:
        # Open the text file in read mode
        file_path = "last_date.txt"
        with open(file_path, 'r') as f:
            # Read the first line
            last_date = f.readline()

        if last_date:
            return last_date
        else:
            return getPrevDate()
    except Exception as e:
        #print("Error: Getting last date in Compliance Gemini.", e)
        return getPrevDate()
    

def is_greater_date(input_date_string, compare_date_string):
    # Convert input date string to datetime object
    input_date = datetime.strptime(input_date_string, '%Y/%m/%d')
    
    # Convert compare date string to datetime object
    compare_date = datetime.strptime(compare_date_string, '%Y/%m/%d')
    
    # Compare the two datetime objects
    return input_date >= compare_date 
    #return input_date == compare_date 

def fetchData(api_url, employees):
    try:
        headers = {
            "content-Type": "text/csv",
            "content-encoding": "gzip"
        }
                
        # Make a GET request the Companies Genie API 
        response = requests.get(api_url, headers=headers)
        
        data_list = []
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the CSV data
            csv_data = response.content.decode('utf-8')
            
            csv_reader = csv.reader(StringIO(csv_data))
            # Skip the header row if needed
            # None is use to prevent exception , it sets default value to None
            next(csv_reader, None)

            # get the latest record of checkin date
            # use the second row 
            second_row = next(csv_reader)
            last_date = second_row[5]
            
            #REMOVE THIS IN PRODUCTION
            #last_date = "05/24/2024 13:20:45"
            
            writeToTxtFile(last_date)

            # Process each row of CSV data
            for row in csv_reader:

                strCheckInDate = str(row[5])
                strCheckOutDate = str(row[6])
                strFirstName = str(row[10])
                strLastName = str(row[12])
             
                if is_greater_date(getDateStrFormat_YYYYMMDD(strCheckInDate),
                                   getDateStrFormat_YYYYMMDD(get_last_date())):    
                    
                    #print("strCheckInDate:",strCheckInDate)
                    # EMPLOYEE_ID
                    # to get employee ID, we need to compare the name from the both APIs
                    employee_id = getEmployeeIDByName(employees, strFirstName,strLastName)
                   
                    #YYYY/MM/DD 
                    date = getDateStrFormat_YYYYMMDD(strCheckInDate)
                    #CLOCKIN YYYY/MM/DD HH:MM
                    clockin = convertDateStrToFormat_YYYYMMDD_HH_MM(strCheckInDate)

                    #CLOCKOUT YYYY/MM/DD HH:MM
                    clockout = convertDateStrToFormat_YYYYMMDD_HH_MM(strCheckOutDate)  

                    # SAGE HR rounds the time to nearest multiples of 5
                    # we need to calculate this so we have accurate calculations for breaks
                    clockin = roundTimeToNearestMultiple5(clockin)
                    clockout = roundTimeToNearestMultiple5(clockout)
                    
                    #if employee_id == "4300308": 
                    if employee_id:    
                        item_dict = { 
                                'date' : str(date), 
                                'id': str(employee_id),
                                'clock_in': str(clockin),
                                'clock_out': str(clockout)
                            }

                        # instead of posting one by one, we collate a list then process it by multi threading 
                        data_list.append(item_dict)

                else:
                    break
                
            return data_list
        else:
            # If the request was not successful, raise an exception
            response.raise_for_status()
    except requests.exceptions.HTTPError as http_err:
        # Handle HTTP errors (e.g., 404, 500, etc.)
        print("HTTP error occurred:", http_err)
    except requests.exceptions.RequestException as req_err:
        # Handle other request errors (e.g., network errors)
        print("Request error occurred:", req_err)


def getDateStrFormat_YYYYMMDD(date_string):
    try:
        # Parse the string into a datetime object
        date_object = datetime.strptime(date_string, "%m/%d/%Y %H:%M:%S")

        # Format the date as YYYY/MM/DD
        formatted_date = date_object.strftime("%Y/%m/%d")

        return formatted_date
    except Exception as e:
        print(f"Cannot convert date {date_string} to YYYY/MM/DD.", e)

def convertDateStrToFormat_YYYYMMDD_HH_MM(date_string):

    try:
        # Parse the string into a datetime object
        date_object = datetime.strptime(date_string, "%m/%d/%Y %H:%M:%S")

        # Format the date as YYYY/MM/DD HH:MM
        formatted_date = date_object.strftime("%Y/%m/%d %H:%M")

        return formatted_date
    except Exception as e:
        print(f"Cannot convert date {date_string} to YYYY/MM/DD HH:MM.", e)




def fetch_all_employees():

    try:

        headers = {
            "content-Type": "application/json",
            "accept": "application/json",
            "X-Auth-Token": API_KEY
        }
              
        # store employees data and store in a dict
        # use to get the names that we cross reference in the api 
        all_employees = []
        current_page = 1
        total_pages = None
        
        while True:
            # Make a request to fetch the current page of employee data
            response = requests.get(url = f'{API_LIST_ACTIVE_EMPLOYEES}?page={current_page}', headers=headers)
            
            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                
                data = response.json()

                for employee in data['data']:
                    employee_data = {
                        'id': employee['id'],
                        'first_name': employee['first_name'],
                        'last_name': employee['last_name']
                    }
                    all_employees.append(employee_data)

                # Check if there are more pages
                meta = data.get('meta', {})
                total_pages = meta.get('total_pages')
                next_page = meta.get('next_page')
                
                if next_page is None or current_page >= total_pages:
                    break  # No more pages to fetch
                
                current_page += 1
            else:
                # If the request was not successful, raise an exception
                response.raise_for_status()

        return all_employees

    except requests.exceptions.HTTPError as http_err:
        # Handle HTTP errors (e.g., 404, 500, etc.)
        print("HTTP error occurred:", http_err)
    except requests.exceptions.RequestException as req_err:
        # Handle other request errors (e.g., network errors)
        print("Request error occurred:", req_err)

def getEmployeeIDByName(employees, first_name, last_name):
    try:

        for employee in employees:
            if employee['first_name'] == first_name and employee['last_name'] == last_name:
                return str(employee['id'])
        
            #print("Employeed ID not found in this name" + first_name + " " + last_name)
        # If no matching employee is found, return None
        #return None
    except Exception as e:
        print("Error getting employee ID. \n", e)


def getCountByIdAndDate(employees, employee_id, date):       
    try:
            
        for employee in employees:
            if employee['id'] == employee_id and employee['date'] == date:
                return str(employee['count'])
            
        # If no matching employee is found, return None
        return None
    except Exception as e:
        print("Error getting employee ID. \n" + e)

def postTimeRecordInSageHR(data):
    try:
        headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'X-Auth-Token': API_KEY
        }

        if data['count'] == 1:
            isOverride = "true"
        else:
            isOverride = ""
        # Define the payload for the POST request
        payload = {
            # Define the data to be sent to the other API
            "override": isOverride, # this should not be true because we have muliple entries

            "clocked_time": {
                data["date"]: {
                    data["id"]: [
                        {
                            "clock_in": data["clock_in"],
                            "clock_out": data["clock_out"]
                        }
                    ]
                }
            }
        }





        # Make a POST request to the other API
        response = requests.post(url=API_EMPLOYEE_TIMESHEET, json=payload, headers=headers)
        response.raise_for_status()
        return response.status_code
    
    except requests.exceptions.HTTPError as http_err:
        # Handle HTTP errors (e.g., 404, 500, etc.)
        print("HTTP error occurred:", http_err)
    except requests.exceptions.RequestException as req_err:
        # Handle other request errors (e.g., network errors)
        print("Request error occurred:", req_err)


# def post_multiple_records(records):
#     with concurrent.futures.ThreadPoolExecutor() as executor:
#         futures = [executor.submit(postTimeRecordInSageHR, record) for record in records]
        
#         for future in concurrent.futures.as_completed(futures):
#             try:
#                 result = future.result()
#                 if result in {200, 201}:
#                     print("Data sent to SAGE HR API successfully")
#                 else:
#                     print("Error posting data in sage API")
#             except Exception as exc:
#                 print('post_multiple_records raised an exception:', exc)



def post_multiple_records(records):
    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = list(executor.map(postTimeRecordInSageHR, records))
            
            for result in results:
                if result in {200, 201}:
                    global countCompleted
                    countCompleted += 1
                    print(str(countCompleted) + " of " + str(countAll) + " has been saved to SAGE HR successfully")
                else:
                    print("Error posting data in sage API")
    except Exception as e:
        print("Error in post_multiple_records.", e)

def roundTimeToNearestMultiple5(dateString):
    try:
        # Parse the datetime from the line
        dt = datetime.strptime(dateString, "%Y/%m/%d %H:%M")
        
        # Round down the minutes to the nearest multiple of 5
        rounded_minutes = dt.minute - (dt.minute % 5)
        
        # Create a new datetime object with rounded minutes
        rounded_dt = dt.replace(minute=rounded_minutes)
        
        # Format the datetime object back into string
        rounded_string = rounded_dt.strftime("%Y/%m/%d %H:%M")
        
        return rounded_string

    except Exception as e:
        print("Error: Rounding off the time to multiples of 5.", e)
        return dateString


def addCountForEachClockInInDay(records):

    # Create a defaultdict to store the count of records for each employee ID and date
    record_count = defaultdict(int)

    # Count the number of records for each employee ID and date
    for record in records:
        key = (record['id'], record['date'])
        record_count[key] += 1

    # Update each record with the count
    for record in records:
        key = (record['id'], record['date'])
        record['count'] = record_count[key]
        record_count[key] -= 1

    return records


def add_30_minutes(date_string):
    # Parse the date string into a datetime object
    dt = datetime.strptime(date_string, "%Y/%m/%d %H:%M")
    
    # Add 30 minutes to the datetime object
    dt += timedelta(minutes=30)
    
    # Format the datetime object back into a string
    new_date_string = dt.strftime("%Y/%m/%d %H:%M")
    
    return new_date_string

def add_4hrs(date_string):
    # Parse the date string into a datetime object
    dt = datetime.strptime(date_string, "%Y/%m/%d %H:%M")
    
    # Add 3.5 hours to the datetime object
    dt += timedelta(hours=4)
    
    # Format the datetime object back into a string
    new_date_string = dt.strftime("%Y/%m/%d %H:%M")
    
    return new_date_string


def getFirstClockOut(records, id, date):

    try:
        # Iterate through the list of dictionaries
        for record in records:
            # get the first record by id and date
            if record['count'] == 1 and record['id'] == id and record['date'] == date:
                return record['clock_out']

    except Exception as e:
        print("Error: Getting first clock_out", e)

def GetLatestRecord(records,id, date):

    try:
        # Filter records by id and date
        filtered_data = [record for record in records if record['date'] == date and record['id'] == id]

        if filtered_data:
            # Find the record with the highest count
            record_with_highest_count = max(filtered_data, key=lambda x: x['count'])
            
            return record_with_highest_count
        else:
            print("No records found for the specified date and id.")

    except Exception as e:
        print("Error: Getting last count", e)
    

def getLatestClockOutAndCount(records, id, date):

    try:

        lastRecord = GetLatestRecord(records,id, date)

        return lastRecord['clock_out'], lastRecord['count']

    except Exception as e:
        print("Error: Getting first clock_out", e)

def has2ndClockin(records, id, date):
    try:
        count_list = []
        # Iterate through the list of dictionaries
        for record in records:
            # check if has count 2 by id and date
            if record['id'] == id and record['date'] == date:
                count_list.append(record['count'])

        second_entry = 2 
        if second_entry in count_list:
            return True

        return False
        
    except Exception as e:
        print("Error: Getting first clock_out", e)

def addOffsetToClockin(records):

    try:

        new_records = []
        # Iterate through the list of dictionaries
        for record in records:

            latestClockOut, latestCount = getLatestClockOutAndCount(records, record['id'], record['date'])
            

            # Check if the count is equal to 2
            if record['count'] == 2:

                firstClockOut = getFirstClockOut(records, record['id'], record['date'])
                # Add 30 minutes to the 'clock_in' time
                if firstClockOut:
                    record['clock_in'] = add_30_minutes(firstClockOut)
                
                # handles if has more than 3 entries 
                
                if latestClockOut:
                    record['clock_out'] = latestClockOut

                continue

            #if not has2ndClockin(records, record['id'], record['date']):
            if latestCount == 1:
                #if clockout is greater than 3.5 hrs , then put a break
                #create a new record of clockin 
                if time_difference_greater_than_4_5_hours(clock_in = record['clock_in'], 
                                                          clock_out = latestClockOut):
                    #print("time_difference_greater_than_4_5_hours")
                    original_clock_out = record['clock_out']
                    record['clock_out'] = add_4hrs(record['clock_in'])

                    new_clockin = add_30_minutes(record['clock_out'])
                    new_clockout = original_clock_out

                    # we split the clockin of employee who does not leave the building
                    # in order to put artificial breaks of 30 mins

                    # Computation Sample
                    # First Clockin = 8:10 
                    # First Clockout = First Clockin + 3.5 hrs 
                    # Break = 30 mins 

                    # Second Clockin = First Clockout + break 
                    # Second Clockout = 16:10

                    new_record = {
                        'id': record['id'],
                        'date': record['date'],
                        'clock_in': new_clockin,
                        'clock_out': new_clockout,
                        'count': 2,
                    }
                    
                    # we add this to the timesheet record
                    new_records.append(new_record)


        return records + new_records
    
    except Exception as e:
        print("Error: adding offset to clockin.", e)
        return records

def time_difference_greater_than_4_5_hours(clock_in, clock_out):
    try:
        # Convert strings to datetime objects
        format_str = '%Y/%m/%d %H:%M'
        clock_in_time = datetime.strptime(clock_in, format_str)
        clock_out_time = datetime.strptime(clock_out, format_str)

        # Calculate the time difference
        time_difference = clock_out_time - clock_in_time

        # Convert time difference to hours
        time_difference_hours = time_difference.total_seconds() / 3600

        # Check if time difference is greater than 3.5 hours
        if time_difference_hours >= 4.5:
            return True
        else:
            return False

    except Exception as e:
        print("Error: Calculating time_difference_greater_than_4_5_hours.", e)


def removeRecordHasCount3Above(record):

    # Remove records with count more than 3
    filtered_data = [record for record in record if record['count'] <= 3]

    return filtered_data

def filter_records_by_count(records, count_value):
    return [record for record in records if record['count'] == count_value]

def main():

    # Record the start time
    start_time = time.time()
    print("Program has started...")
    print("Last successful processed date is:", get_last_date())

    # Get all employees in SAGE HR
    # We use the names as key to get update records
    all_employees = fetch_all_employees()

    #print(getEmployeeIDByName(all_employees, "James", "Trevett"))
    # Fetch the data in Genie API and update timesheet based on their names
    print("Fetching timesheet in Compliance Genie...")
    timeSheetDataList = fetchData(api_url = API_COMPLIANCE_GENIE, employees = all_employees)
    #print("timeSheetDataList:" , timeSheetDataList)
    # add count to timesheet to determine first and second entry in a day
    print("Preparing data to integrate in Sage HR...")
    print("This may take few minutes...")
    timeSheetWithCount = addCountForEachClockInInDay(timeSheetDataList)
    #print("TimeSheetWithCount:", timeSheetWithCount)

    # manipulate clockin and clockouts to insert breaks
    modifiedTimeSheet = addOffsetToClockin(timeSheetWithCount)
    #print("ModifiedTimeSheet:", modifiedTimeSheet)
    
    # We reverse the list so first checkin time will be posted in SAGE 
    reversedTimeSheet = modifiedTimeSheet[::-1]

    # remove records that has count more than 2
    filtered_record = removeRecordHasCount3Above(reversedTimeSheet)
    #print("FILETERED RECORD:", filtered_record)
    global countAll
    # Filter records with count = 1
    filtered_records_1 = filter_records_by_count(filtered_record, 1)
    countAll += len(filtered_records_1)
    #print("Record 1:", filtered_records_1)
    
    # Filter records with count = 2
    filtered_records_2 = filter_records_by_count(filtered_record, 2)
    countAll += len(filtered_records_2)
    #print(filtered_record_2)

    if bool(filtered_records_1):
        # Post each record to SAGE HR
        post_multiple_records(records=filtered_records_1)
    
    if bool(filtered_records_2):
        # Post each record to SAGE HR
        post_multiple_records(records=filtered_records_2)

    print("Program has ended.")

    # Record the end time
    end_time = time.time()

    # Calculate the elapsed time
    elapsed_time = end_time - start_time

    # Print the elapsed time
    print("Time taken to process the program:", elapsed_time, "seconds")


if __name__ == "__main__":
    main()