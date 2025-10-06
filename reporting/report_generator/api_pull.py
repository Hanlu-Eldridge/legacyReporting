import pandas as pd
import requests



# palceholder for datasets, to be replaced with Snowflake holdings table, transaction table and security master table
#  . .\.venv\Scripts\Activate.ps1
# dir = "C:/Users/XiaHanlu/workspace/legacyReporting/local_reading/panagram"
# date = '04-30-2025'
def process_api(url,readObject) -> pd.DataFrame:
    """
    Fetch positions data from Panarama API and return it as a DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing the 'positions' data.
    """
    try:
        response = requests.get(url,verify=False)
        response.raise_for_status()  # Raise error if request failed
        data = response.json()

        if readObject in data:
            df_api = pd.DataFrame(data[readObject])
        else:
            raise ValueError(f"Key {readObject} not found in response JSON.")

        return df_api

    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
    except ValueError as ve:
        print(f"Data processing error: {ve}")

    return pd.DataFrame() # Return empty DataFrame on failure


def process_raw_data(date: str, inputdir: str):
    target_portfolio_list = ['SBL_103_103',
                             'SBL_104_104',
                             'SBL_105_105',
                             'SBL_107_107',
                             'SBL_111_111',
                             'SBL_404_404']

    choices_port = ['44G8/813/IG CLO FWH', '44G9/814/ABS FWH', '44H1/815/Atypical FWH', '44I4/816/HY CLOs FWH', 'P44I5/817/HY CLOs OC', '44J6/821/IG CLO OC','44J7/822/ABS OC', '44J5/823/Atypical OC']

    formatted_date = date.strftime("%Y-%m-%d")
    holding_url = f"https://panarama.p-gram.com:7211/DataAPI/Positions/get/date/{formatted_date}"
    df_holding = process_api(holding_url, 'positions')
    print(df_holding.columns.tolist())
    df_holding = df_holding[df_holding['portfolio'].isin(target_portfolio_list)]

    beg_date = (date - pd.DateOffset(months=2)).replace(day=1) + pd.offsets.MonthEnd(0)
    formatted_start = beg_date.strftime("%Y-%m-%d")
    transaction_url = f"https://panarama.p-gram.com:7211/DataAPI/BasisTransactions/get/date/from/{formatted_start}/to/{formatted_date}"
    df_transaction = process_api(transaction_url, 'basisTransactions')
    df_transaction = df_transaction[df_transaction['profitCenterCode'].isin(target_portfolio_list)]
    #['principal', 'securityDesc', 'settleDate', 'securityId', 'accountingDate', 'accountingPeriodId', 'companyAbbr', 'custodianAbbr', 'securityMasterId', 'productType', 'ccyKeyCode', 'quantity', 'isCanceled', 'lastUpdateDateTime', 'totalAmount', 'txnNum', 'payDay', 'transactionType', 'productClass', 'tradeDate', 'originalFace', 'couponRate', 'maturityDate', 'profitCenterCode', 'isSettled', 'currType', 'basisTradeId', 'basePrincipal', 'tradeTotalAmount', 'lotName', 'costProceeds', 'tradeCostProceeds', 'lotAccruedInterest', 'tradeAmount', 'tradePrice', 'pmtScheduleDesc', 'assetEventType', 'brokerAbbr', 'basis', 'originalCost', 'tradeYield', 'tradeAccruedInterest', 'tradeFXRate']

    print(df_transaction.columns.tolist())


    df_trader_info = pd.read_excel(inputdir + '/df_trader_info.xlsx', sheet_name='Sheet1', index_col=None, header=0)

    # creating transactions table
    df_transaction = df_transaction[['profitCenterCode', 'transactionType', 'securityId', 'securityDesc', 'tradeDate', 'settleDate', 'maturityDate','quantity','tradePrice','costProceeds','couponRate','Portfolio_Name']]

    return df_transaction


def generate_input_from_jared(date: str, inputdir: str, outputdir: str):
    formatted_date = date.strftime("%Y-%m-%d")
    holding_url = f"https://panarama.p-gram.com:7211/DataAPI/Positions/get/date/{formatted_date}"
    df_holding = process_api(holding_url, 'positions')
    print(df_holding.columns.tolist())
    df_trader_info = pd.read_csv(inputdir + '/df_trader_info.csv', index_col=None, header=0)

    df_holding['Par Sub'] = 0
    df_holding = df_holding[['securityId','wal','nextPaymentDate','nextCallDate','Par Sub','dm','marketPrice','portfolioManager','issueDate','sp','moody','fitch','kbra','issuerName']].drop_duplicates()


    df_sec_master = pd.merge(df_trader_info, df_holding, how='left', left_on='Cusips', right_on='securityId')
    df_sec_master = df_sec_master.rename(columns={
        'wal': 'WAL',
        'nextPaymentDate': 'Next Payment Date',
        'nextCallDate': 'Non-Call Date',
        'dm':'Implied DM',
        'marketPrice': 'Market Price',
        'portfolioManager': 'Manager',
        'issueDate': 'Issue Date',
        'sp': 'SP Rating',
        'moody': 'Moody Rating',
        'fitch': 'Fitch Rating',
        'kbra': 'KBRA Rating',
        'issuerName': 'Issuer Name'
    })

    df_sec_master[['SP Rating', 'Moody Rating', 'Fitch Rating', 'KBRA Rating']] = df_sec_master[
        ['SP Rating', 'Moody Rating', 'Fitch Rating', 'KBRA Rating']].fillna('NR')
    df_sec_master['Manager'] = df_sec_master['Manager'].fillna('Unknown')
    df_sec_master['Manager'] = df_sec_master['Manager'].str.upper()
    #df_sec_master = df_sec_master.drop(columns=['securityId'], inplace=True)


    report_path = outputdir + "/Input_From_Jared_" + date.strftime("%Y%m%d") + ".csv"
    df_sec_master.to_csv(report_path, index=False)

    return