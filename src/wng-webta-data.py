import pandas as pd
import sqlite3

def setDateTime(row, sheet):
    sheet_num = int(sheet[5:])
    dow, md  = row['Date'].split(" ", 2)
    month, day = md.split("/", 2)

    if sheet_num < 20:
        year = 2018
    elif sheet_num == 20:
        if int(month) == 1:
            year = 2019
        else:
            year = 2018
    elif sheet_num > 20:
        year = 2019

    dt_in = '{}-{}-{} {}'.format(year, month, day, row['Time In'])
    dt_out = '{}-{}-{} {}'.format(year, month, day, row['Time Out'])

    ts_date = pd.Timestamp(dt_in).strftime('%Y-%m-%d')
    ts_in = pd.Timestamp(dt_in).strftime('%Y-%m-%d %H:%M:%S')
    ts_out = pd.Timestamp(dt_out).strftime('%Y-%m-%d %H:%M:%S')

    return ts_date, ts_in, ts_out

def wrangleData(df, sheet):
    #Handle missing values.
    df.dropna(axis=0, how='all', inplace=True)

    df = df.loc[df['Transaction'].isna() == False]

    df[['Date','Shift Total','Daily Total']] = df[['Date','Shift Total','Daily Total']].shift(+1)

    df = df.loc[(df['Date'].isna() == False) &
                (df['Date'] != 'Date')]

    #Handle datetime.
    df[['Date','time_in','time_out']] = df.apply(setDateTime, axis=1, result_type='expand', args=(sheet,))

    #Handle transactions.
    df = df.loc[(df['Transaction'] == '01 - Regular Base Pay') |
                (df['Transaction'] == '29 - Credit Hours Worked-Regular Time')]

    df['time_out_max'] = df.groupby(by=['Date'])['time_out'].transform(max)

    df = df.loc[(df['Transaction'] == '01 - Regular Base Pay')]

    #Finalize columns and format
    df_in = df[['time_in']].copy()
    df_in.loc[:, 'entry_type'] = 'Arrival'
    df_in.loc[:, 'description'] = ""
    df_in.rename(columns={'time_in':'datetimes'}, inplace=True)

    df_out = df[['time_out_max']].copy()
    df_out.loc[:, 'entry_type'] = 'Departure'
    df_out.loc[:, 'description'] = ""
    df_out.rename(columns={'time_out_max':'datetimes'}, inplace=True)

    df_final = pd.concat([df_in, df_out], sort=False)
    df_final.reset_index(drop=True, inplace=True)

    return df_final

def createDB(data, table_name):
    '''
    Write dataframes to database tables.
    '''
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
    #Set file path.
    file_name = "20180401_20190622.xlsx"
    file_path = '../data/{}'.format(file_name)

    #Determine total number of sheets.
    xlsx = pd.ExcelFile(file_path)
    sheets = xlsx.sheet_names

    #Define final pandas dataframe.
    df_app = pd.DataFrame(columns=['datetimes','entry_type','description'])

    #Work over every sheet.
    for sheet in sheets:
        print(sheet)

        #Import.
        df_ing = pd.read_excel(xlsx, sheet_name=sheet, skiprows=2)

        #Wrangle.
        df_wng = wrangleData(df_ing, sheet)

        #Append.
        df_app = df_app.append(other=df_wng)

    #Output.
    createDB(data=df_app, table_name='arrive_depart_webta')

if __name__ == '__main__':
    main()
