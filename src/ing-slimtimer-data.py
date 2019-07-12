import calendar
import datetime
import json
import os
import requests

def buildDateRange(start_month, start_year, end_month, end_year):
    '''
    Build user-defined date ranges.
    '''
    years = range(start_year, end_year + 1)
    months = range(1, 13)

    dates = []

    for year in years:
        for month in months:
            if year == min(years):
                if month >= start_month:
                    dates.append([month, year])
            elif year < max(years):
                dates.append([month,year])
            elif year == max(years):
                if month <= end_month:
                    dates.append([month,year])

    return dates

def slimtimerAPI(dates, api_key, user_name, password, user, access, output):
    '''
    Build request for SlimTimer API, make request, output data.
    '''
    #Design API Call.
    headers = {'Accept': 'application/{}'.format(output),
               'Content-Type': 'application/{}'.format(output)}

    call_url = 'http://slimtimer.com/users/{}/time_entries'.format(user)

    #For each month/year, pull tasks.
    for month, year in dates[0:1]:
        #Set date range for API call. Ranges are monthly.
        month_range = calendar.monthrange(int(year),int(month))
        month_start = str(1).zfill(2)
        month_end = str(month_range[1])
        print('Created date range as {}/{}/{} to {}/{}/{}'.format(month,month_start,year,month,month_end,year))

        #Create API parameter argument.
        params = {'api_key': api_key,
                  'access_token': access,
                  'offset': '0',
                  'range_start': '{}-{}-{} 00:00:00'.format(year, month, month_start),
                  'range_end': '{}-{}-{} 23:59:59'.format(year, month, month_end)}

        #Make request.
        print('Making API Call at {}'.format(datetime.datetime.now().strftime("%H:%M:%S")))
        request = requests.get(call_url, params=params, headers=headers)
        print('Finished API Call with {} at {}.'.format(request, datetime.datetime.now().strftime("%H:%M:%S")))

        #Output.
        outputFile(request=request, month=month, year=year, day_start=month_start, day_end=month_end)

def outputFile(request, month, year, day_start, day_end):
    '''
    Output API results as XML.
    '''
    data_folder = os.path.join(os.getcwd(),'../data/slimtimer-data')
    file_name = '{}_{}_{}-{}_{}_{}.xml'.format(year, month, day_start, year, month, day_end)
    file_path = os.path.join(data_folder,file_name)

    with open(file_path, 'w') as w:
        w.write(request.text)

    print("Output data at {}".format(datetime.datetime.now().strftime("%H:%M:%S")))

def main():
    #Slimtimer API log-in information.
    file_path = os.path.join(os.getcwd(),'../api_info.txt')
    api_info = json.load(open(file_path, 'r'))

    #Create date ranges.
    date_range = buildDateRange(start_month=7, start_year=2014, end_month=7, end_year=2019)

    #Call SlimTimer API and output data.
    slimtimerAPI(date_range,
                 api_key=api_info['api_key'], user_name=api_info['user_name'], password=api_info['password'],
                 user=api_info['user'], access=api_info['access'], output='xml')

if __name__ == '__main__':
    main()
