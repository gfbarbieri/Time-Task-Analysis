import pandas as pd
import sqlite3

def importData(file_name):
    #Set file path.
    file_path = '../data/{}'.format(file_name)

    #Import data.
    xlsx = pd.ExcelFile(file_path)
    sheets = xlsx.sheet_names

    df_ing = pd.read_excel(xlsx, sheet_name=sheets[0])

    return df_ing

def setDatetime(row, time_var):
    date = row['date'].strftime('%Y-%m-%d')
    time = row[time_var].strftime('%H:%M:%S')

    ts = '{} {}'.format(date, time)

    return ts

def wrangleData(df, type=None):

    #Handle missing values.
    df = df.loc[df['time-in'].isna() == False]

    #Set datetime variable.
    df['arrival_time'] = df[['date','time-in']].apply(setDatetime, axis=1, args=('time-in',))

    #Set datetime variable for output time, if available.
    #otherwise, entry_type and description columns.
    if type == 'curr':
        df['departure_time'] = df[['date','time-out']].apply(setDatetime, axis=1, args=('time-out',))

        df_in = df[['arrival_time']].copy()
        df_in.loc[:, 'entry_type'] = 'Arrival'
        df_in.loc[:, 'description'] = ""
        df_in.rename(columns={'arrival_time':'datetimes'}, inplace=True)

        df_out = df[['departure_time']].copy()
        df_out.loc[:, 'entry_type'] = 'Departure'
        df_out.loc[:, 'description'] = ""
        df_out.rename(columns={'departure_time':'datetimes'}, inplace=True)

        df = pd.concat([df_in, df_out], sort=False)
        df.reset_index(drop=True, inplace=True)
    else:
        df.loc[:, 'entry_type'] = 'Arrival'
        df.loc[:, 'description'] = ""
        df.rename(columns={'arrival_time':'datetimes'}, inplace=True)
        df.drop(columns=['date','time-in'], inplace=True)

    return df

def createDB(data, table_name):
    #Open connection and create cursor.
    conn = sqlite3.connect('../data/arrival_departure_hist.db')
    c = conn.cursor()

    #If table already exists, drop it before writing to it.
    c.execute("drop table if exists {}".format(table_name))

    #Write to the table.
    data.to_sql(table_name, conn)

    #Commit and close connection.
    conn.commit()
    conn.close()

def main():
    #Import
    df_ing_hist = importData(file_name='20140714_20180331.xlsx')
    df_ing_curr = importData(file_name='20190623_20190629.xlsx')

    #Wrangle.
    df_wng_hist = wrangleData(df_ing_hist)
    df_wng_curr = wrangleData(df_ing_curr, type='curr')

    #Merge.
    df_final = pd.concat([df_wng_hist, df_wng_curr], sort=False).reset_index(drop=True)

    #Output.
    createDB(data=df_final, table_name='arrive_depart_timea')

if __name__ == '__main__':
    main()
