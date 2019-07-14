import datetime
import os
import pandas as pd

def setSkiprows(file_name, sheet_name):
    '''
    Return an integer to set skiprows argument for the read_excel function.
    '''
    if file_name == 'webta_20140713_20160625.xlsx':
        skiprows = 1
    elif file_name == 'webta_20160626_20171028.xlsx' and int(sheet_name[5:]) < 33:
        skiprows = 1
    else:
        skiprows = 2

    return skiprows

def createDateRange(file_name, sheet_name):
    '''
    Return a dataframe with all of the date ranges covered in the timesheet
    being worked on. All timesheets cover a 14-day period, 10 business days.
    '''
    file_start_date = pd.Timestamp(file_name.split("_")[1])
    elapsed = int(sheet_name[5:])

    if file_name == "webta_20140713_20160625.xlsx":
        if elapsed > 29:
            elapsed -= 1
        else:
            elapsed = elapsed

    start_day = (elapsed-1)*14
    end_day = (elapsed)*14

    date_range_start = file_start_date + datetime.timedelta(days=start_day)
    date_range_end = file_start_date + datetime.timedelta(days=end_day)

    time_range = []

    for i in range(start_day,end_day):
        date = file_start_date + datetime.timedelta(days=i)
        date_month = str(date.month).zfill(2)
        date_day = str(date.day).zfill(2)
        date_year = str(date.year)

        time_range.append([date_month, date_day, date_year])

    time_range_df = pd.DataFrame(time_range, columns=['month','day','year'])

    return time_range_df

def setDateTimeInOut(row):
    '''
    Return three values, a date formatted as "YYYY-MM-DD", and
    two time values (clocked in and out) formatted as "HH:MM:SS".
    '''
    d_str = '{} {} {}'.format(row['month'], row['day'], row['year'])

    d_ts = pd.Timestamp(d_str).strftime('%Y-%m-%d')
    tin_ts = pd.Timestamp(row['Time In']).strftime('%H:%M:%S')
    tout_ts = pd.Timestamp(row['Time Out']).strftime('%H:%M:%S')

    return d_ts, tin_ts, tout_ts

def wrangleData(time_sheet_df, time_range_df):
    '''
    Return dataframe of wrangled timesheet data.
    '''
    #Handle missing values.
    df = time_sheet_df.dropna(axis=0, how='all')

    df = df.loc[df['Transaction'].isna() == False]

    df[['Date','Shift Total','Daily Total']] = df[['Date','Shift Total','Daily Total']].shift(+1)

    df = df.loc[(df['Date'].isna() == False) &
                (df['Date'] != 'Date')]

    #Create month and day columns.
    df['md']  = df['Date'].str.split(pat=" ", n=2, expand=True)[1]
    df[['month', 'day']] = df['md'].str.split("/", 2, expand=True)
    df['month'] = df['month'].str.zfill(2)
    df['day'] = df['day'].str.zfill(2)

    #Create year column.
    df = df.merge(right=time_range_df, on=['month','day'])

    #Create formatted date, time-in, and time-out columns.
    df[['date','time-in','time-out']] = df.apply(setDateTimeInOut, axis=1, result_type='expand')

    #Rename columns.
    rename = {'Shift Total':'shift_total',
              'Meal':'lunch',
              'Daily Total':'daily_total',
              'Transaction':'transaction'}

    df.rename(columns=rename, inplace=True)

    #Subset columns.
    df = df[['date','time-in','time-out','shift_total',
             'lunch','daily_total','transaction']]

    return df

def main():
    '''
    Driver function.
    '''
    #Set folder path.
    folder_path = "../data/time-data"

    #Define final pandas dataframe.
    df_app = pd.DataFrame(columns=['date','time-in','time-out','shift_total','lunch','daily_total','transaction'])

    #Work over every workbook and spreadsheet within the workbook.
    for file in os.listdir(folder_path):
        file_path = os.path.join(folder_path,file)

        #Create excel reader and determine sheet names.
        xlsx = pd.ExcelFile(file_path)
        sheets = xlsx.sheet_names

        for sheet in sheets:
            #Create time range dataframe from the file.
            time_range_df = createDateRange(file_name=file, sheet_name=sheet)

            #Import timesheet.
            print(file, sheet)
            skiprows = setSkiprows(file_name=file, sheet_name=sheet)
            df_ing = pd.read_excel(xlsx, sheet_name=sheet, skiprows=skiprows)

            #Wrangle timesheet.
            df_wng = wrangleData(time_sheet_df=df_ing, time_range_df=time_range_df)

            #Append timesheets.
            df_app = df_app.append(other=df_wng, ignore_index=True)

    #Output timesheets to comma-separated file.
    df_app.to_csv(path_or_buf='../data/timesheet.csv', sep=',', index=False)

if __name__ == '__main__':
    main()
