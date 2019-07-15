import pandas as pd
import os

def createDataFrames(full_df):
    '''
    '''
    names = ['Year, Pay Period', 'Earnings and Deductions', 'Year-to-Date Leave Status',
             'Agency Contributions to Employee Benefits this Pay Period']

    indexes = full_df.index[full_df['Gregory Barbieri'].isin(names)].tolist()

    for item, index in enumerate(indexes):
        start = index

        if item < len(indexes)-1:
            end=indexes[item+1]

        if item == 0:
            pay_period_info = full_df.iloc[start:end].reset_index(drop=True)
        elif item == 1:
            earnings = full_df.iloc[start:end].reset_index(drop=True)
        elif item == 2:
            leave = full_df.iloc[start:end].reset_index(drop=True)
        elif item == 3:
            agency_contributions = full_df.iloc[start:].reset_index(drop=True)

    return pay_period_info, earnings, leave, agency_contributions

def createRenameDict(df, re_cols):
    '''
    '''
    columns = df.columns.tolist()

    rename_dict = {}

    for index, column in enumerate(columns):
        rename_dict[column] = re_cols[index]

    return rename_dict

def wranglePPData(pp_df):
    '''
    '''
    rename_cols = ['pay_period','agency','pay_plan','salary',
                   'scd','na_col_1','na_col_2','na_col_3']

    rename_dict = createRenameDict(pp_df, rename_cols)

    pp_df.rename(columns=rename_dict, inplace=True)

    pp_df = pp_df.loc[pp_df['pay_period'].isnull() == False]
    pp_df['pay_period_range'] = pp_df['pay_period'].shift(-1)
    pp_df = pp_df.loc[(pp_df['agency'].isnull() == False) &
                      (pp_df['salary'] != 'Salary')]

    pp_df['pay_period'] = pp_df['pay_period'].str.replace(u'\xa0', u' ').str.replace(' ','')
    pp_df = pp_df.drop(columns=['na_col_1','na_col_2','na_col_3']).reset_index(drop=True)

    return pp_df

def wrangleEarningsData(earn_df, pp_df):
    '''
    '''
    rename_cols = ['code','description','hours_pp','hours_ytd',
                   'amount_pp','amount_ytd','na_col_1','na_col_2']

    rename_dict = createRenameDict(earn_df, rename_cols)

    earn_df.rename(columns=rename_dict, inplace=True)

    earn_df = earn_df.loc[(earn_df['code'] != '**') &
                          (earn_df['code'].isnull() == False) &
                          (earn_df['code'] != 'Code') &
                          (earn_df['description'].isnull() == False)]

    earn_df['pay_period'] = pp_df['pay_period'][0]
    earn_df = earn_df.drop(columns=['na_col_1','na_col_2']).reset_index(drop=True)

    return earn_df

def wrangleLeaveData(leave_df, pp_df):
    '''
    '''
    rename_cols = ['type','na_col_1','accured','used',
                   'balance','projected_uol','na_col_2','na_col_3']

    rename_dict = createRenameDict(leave_df, rename_cols)
    leave_df.rename(columns=rename_dict, inplace=True)
    leave_df = leave_df.loc[(leave_df['type'].isnull() == False) &
                            (leave_df['type'] != 'Type') &
                            (leave_df['accured'].isnull() == False)]

    leave_df['pay_period'] = pp_df['pay_period'][0]

    leave_df = leave_df.drop(columns=['na_col_1','na_col_2','na_col_3']).reset_index(drop=True)

    return leave_df

def wrangleAgencyContributionData(agency_contrib_df, pp_df):
    rename_cols = ['contribution','amount','na_col_1','na_col_2',
                   'na_col_3','na_col_4','na_col_5','na_col_6']

    rename_dict = createRenameDict(agency_contrib_df, rename_cols)

    agency_contrib_df.rename(columns=rename_dict, inplace=True)

    agency_contrib_df = agency_contrib_df.loc[agency_contrib_df['amount'].isnull() == False]
    agency_contrib_df['contribution'] = agency_contrib_df['contribution'].str.replace("*","")
    agency_contrib_df['pay_period'] = pp_df['pay_period'][0]
    agency_contrib_df = agency_contrib_df.drop(columns=['na_col_1','na_col_2','na_col_3','na_col_4',
                                                        'na_col_5','na_col_6']).reset_index(drop=True)
    return agency_contrib_df

def main():
    '''
    '''
    #Set folder path.
    folder_path = "../data/income-data"

    pp_df_final = pd.DataFrame(columns=['pay_period','agency','pay_plan','salary',
                                        'scd','pay_period_range'])
    earn_df_final = pd.DataFrame(columns=['code','description','hours_pp','hours_ytd',
                                          'amount_pp','amount_ytd','pay_period'])
    leave_df_final = pd.DataFrame(columns=['type','na_col_1','accured','used',
                                           'balance','projected_uol','pay_period'])
    agency_df_final = pd.DataFrame(columns=['contribution','amount','pay_period'])

    #Work over every workbook and spreadsheet within the workbook.
    for file in os.listdir(folder_path):
        #Import
        print(file)
        file_path = os.path.join(folder_path,file)
        csv_df = pd.read_csv(file_path, encoding='Latin-1')

        pay_period_df, earnings_df, leave_df, agency_contributions_df = createDataFrames(csv_df)

        #Wrangle
        wng_pp_df = wranglePPData(pp_df=pay_period_df)
        wng_earn_df = wrangleEarningsData(earn_df=earnings_df, pp_df=wng_pp_df)
        wng_leave_df = wrangleLeaveData(leave_df=leave_df, pp_df=wng_pp_df)
        wng_agency_df = wrangleAgencyContributionData(agency_contrib_df=agency_contributions_df, pp_df=wng_pp_df)

        #Merge
        pp_df_final = pp_df_final.append(other=wng_pp_df, ignore_index=True)
        earn_df_final = earn_df_final.append(other=wng_earn_df, ignore_index=True)
        leave_df_final = leave_df_final.append(other=wng_leave_df, ignore_index=True)
        agency_df_final = agency_df_final.append(other=wng_agency_df, ignore_index=True)

    #Output
    pp_df_final.to_csv(path_or_buf='../data/pay_period_info.csv', sep=',', index=False, encoding='Latin-1')
    earn_df_final.to_csv(path_or_buf='../data/earnings_data.csv', sep=',', index=False, encoding='Latin-1')
    leave_df_final.to_csv(path_or_buf='../data/leave_usage_accural_data.csv', sep=',', index=False, encoding='Latin-1')
    agency_df_final.to_csv(path_or_buf='../data/agency_contributions_data.csv', sep=',', index=False, encoding='Latin-1')

if __name__ == '__main__':
    main()
