import json
from csv import DictReader
import requests
import time
from bs4 import BeautifulSoup
import pandas as pd

BALANCE_SHEET_URL = 'https://www.reuters.com/companies/api/getFetchCompanyFinancials/{TICKER}'


def get_item(item_list, item, new_key=''):
    if new_key == '':
        new_key = item

    try:
        item_list = item_list[item]
    except KeyError:
        print('Cannot find key: ' + str(item))
        return {'error': 'cannot find key', 'key': new_key}

    result = {new_key: {}}
    for instance in item_list:
        date = instance['date']
        try:
            value = float(instance['value']) * 1000000
        except ValueError:
            value = instance['value']
        result[new_key][date] = value

    return result


def get_IS_item(json, item, new_key=''):
    try:
        item_list = json['income']['interim']
        result = get_item(item_list, item, new_key)
    except KeyError:
        return "Not Found"
    return result


def get_BS_item(json, item, new_key=''):
    item_list = json['balance_sheet']['interim']
    result = get_item(item_list, item, new_key)
    return result


def get_CF_item(json, item, new_key=''):
    try:
        item_list = json['cash_flow']['interim']
        result = get_item(item_list, item, new_key)
    except KeyError:
        return "Not Found"
    return result


def get_standardized_statements(ticker='6414.T'):
    request_url = BALANCE_SHEET_URL.replace('{TICKER}', ticker)
    response = requests.get(request_url)
    json = response.json()

    try:
        if 'rcom_service_message' in json:
            return

        if 'retry_in' in json and 'market_data' not in json:
            time.sleep(json['retry_in'] + 0.5)
            json = requests.get(request_url).json()

        if 'market_data' in json and json['market_data'] == {}:
            time.sleep(5)
            json = requests.get(request_url).json()
            if 'market_data' in json and json['market_data'] == {}:
                raise KeyError

        statements = json['market_data']['financial_statements']
    except KeyError:
        print('KeyError!')
        count = 0
        while count < 10:
            try:
                count += 1
                time.sleep(1)
                json = requests.get(request_url).json()
                if 'rcom_service_message' in json:
                    return
                statements = json['market_data']['financial_statements']
            except KeyError:
                pass

        if count >= 10:
            # raise KeyError
            pass

    # Revenues
    revenue_data = get_IS_item(statements, 'Total Revenue', 'revenues')
    # COGS
    cogs_data = get_IS_item(statements, 'Cost of Revenue, Total', 'cost_of_revenues')
    # Gross Profit
    gross_profit_data = get_IS_item(statements, 'Gross Profit', 'gross_profit')
    # Selling, General, & Admin Expenses
    sga_data = get_IS_item(statements, 'Selling/General/Admin. Expenses, Total', 'sga_expenses')
    # R&D Expenses
    rd_data = get_IS_item(statements, 'Research & Development', 'rd_expenses')
    # Operating Expenses
    operating_expenses_data = get_IS_item(statements, 'Total Operating Expense', 'total_operating_expenses')
    # Depreciation and Amortization
    # da_data = get_IS_item(statements, 'Depreciation/Amortization', 'depreciation_amortization')
    # Operating Income
    operating_income_data = get_IS_item(statements, 'Operating Income', 'operating_income')
    # Interest Income (Expense)
    interest_income_data = get_IS_item(statements, 'Interest Inc.(Exp.),Net-Non-Op., Total', 'interest_income_expense')
    # Tax Expense
    tax_expense_data = get_IS_item(statements, 'Provision for Income Taxes', 'income_tax_expense')
    # Net Income
    net_income_data = get_IS_item(statements, 'Net Income', 'net_income')
    # Shares outstanding
    shares_outstanding_data = get_IS_item(statements, 'Diluted Weighted Average Shares', 'shares_outstanding')
    # Cash, Equivalents, and Short-term Investments
    cash_eq_data = get_BS_item(statements, 'Cash and Short Term Investments', 'cash_and_equivalents')
    # Receivables
    receivables_data = get_BS_item(statements, 'Total Receivables, Net', 'receivables')
    # Inventory
    inventory_data = get_BS_item(statements, 'Total Inventory', 'inventory')
    # Other Current Assets
    other_current_assets_data = get_BS_item(statements, 'Other Current Assets, Total', 'other_current_assets')
    # Total Current Assets
    total_current_assets_data = get_BS_item(statements, 'Total Current Assets', 'total_current_assets')
    # Net PP&E
    ppe_data = get_BS_item(statements, 'Property/Plant/Equipment, Total - Net', 'net_ppe')
    # Intangibles
    intangibles_data = get_BS_item(statements, 'Intangibles, Net', 'net_intangibles')
    # Long-term Investments
    long_term_investments_data = get_BS_item(statements, 'Long Term Investments', 'long_term_investments')
    # Other Long-Term Assets
    other_long_term_assets_data = get_BS_item(statements, 'Other Long Term Assets, Total', 'other_long_term_assets')
    # Total Assets
    total_assets_data = get_BS_item(statements, 'Total Assets', 'total_assets')
    # Accounts Payable
    payables_data = get_BS_item(statements, 'Accounts Payable', 'accounts_payable')
    # Accrued Expenses
    accrued_exp_data = get_BS_item(statements, 'Accrued Expenses', 'accrued_expenses')
    # Short-term Borrowings
    short_term_borrowings_data = get_BS_item(statements, 'Notes Payable/Short Term Debt', 'short_term_borrowings')
    # Current portion of long-term debt
    current_lt_debt_data = get_BS_item(statements, 'Current Port. of LT Debt/Capital Leases', 'current_portion_lt_debt')
    # Other Current Liabilities
    other_current_liabilities_data = get_BS_item(statements, 'Other Current liabilities, Total',
                                                 'other_current_liabilities')
    # Total Current Liabilities
    total_current_liabilities_data = get_BS_item(statements, 'Total Current Liabilities', 'total_current_liabilities')
    # Long-term Debt
    total_debt_data = get_BS_item(statements, 'Total Debt', 'total_debt')
    # Other long-term Liabilities
    other_liabilities_data = get_BS_item(statements, 'Other Liabilities, Total', 'other_liabilities_total')
    # Total Liabilities
    total_liabilities_data = get_BS_item(statements, 'Total Liabilities', 'total_liabilities')

    if 'cash_flow' in statements:
        # Net Income
        net_income_cf_data = get_CF_item(statements, 'Net Income/Starting Line', 'net_income_starting_line')
        # Depreciation
        depreciation_data = get_CF_item(statements, 'Depreciation/Depletion', 'depreciation_and_depletion')
        # Non-Cash Items
        non_cash_items_data = get_CF_item(statements, 'Non-Cash Items', 'amortization_and_noncash_items')
        # Cash Taxes Paid
        cash_taxes_paid_data = get_CF_item(statements, 'Cash Taxes Paid', 'cash_taxes_paid')
        # Cash Interest Paid
        cash_interest_paid_data = get_CF_item(statements, 'Cash Interest Paid', 'cash_interest_paid')
        # Changes in Working Capital
        working_capital_changes_data = get_CF_item(statements, 'Changes in Working Capital',
                                                   'changes_in_working_capital')
        # Cash from Operating Activities
        cash_from_operations_data = get_CF_item(statements, 'Cash from Operating Activities', 'cash_from_operations')
        # Capital Expenditures
        capital_expenditures_data = get_CF_item(statements, 'Capital Expenditures', 'capital_expenditures')
        # Other Investing Activities
        other_investing_data = get_CF_item(statements, 'Other Investing Cash Flow Items, Total',
                                           'other_cash_from_investing')
        # Cash from Investing Activities
        cash_from_investing_data = get_CF_item(statements, 'Cash from Investing Activities', 'cash_from_investing')
        # Cash Dividends Paid
        cash_dividends_paid_data = get_CF_item(statements, 'Total Cash Dividends Paid', 'cash_dividends_paid')
        # Issuance (Repurchase) of Stock
        issuance_repurchase_stock_data = get_CF_item(statements, 'Issuance (Retirement) of Stock, Net',
                                                     'issuance_repurchase_stock')
        # Issuance (Repayment) of Debt
        issuance_repayment_debt_data = get_CF_item(statements, 'Issuance (Retirement) of Debt, Net',
                                                   'issuance_repayment_debt')
        # Cash from Financing Activities
        cash_from_financing_data = get_CF_item(statements, 'Cash from Financing Activities', 'cash_from_financing')
        # Net Change in Cash
        net_change_in_cash_data = get_CF_item(statements, 'Net Change in Cash', 'net_change_in_cash')

    financial_statements_data_array = [
        revenue_data,
        cogs_data,
        gross_profit_data,
        sga_data,
        rd_data,
        operating_expenses_data,
        # da_data,
        operating_income_data,
        interest_income_data,
        tax_expense_data,
        net_income_data,
        shares_outstanding_data,
        cash_eq_data,
        receivables_data,
        inventory_data,
        other_current_assets_data,
        total_current_assets_data,
        ppe_data,
        intangibles_data,
        long_term_investments_data,
        other_long_term_assets_data,
        total_assets_data,
        payables_data,
        accrued_exp_data,
        short_term_borrowings_data,
        current_lt_debt_data,
        other_current_liabilities_data,
        total_current_liabilities_data,
        total_debt_data,
        other_liabilities_data,
        total_liabilities_data,
    ]

    if 'cash_flow' in statements:
        cf_items = [
            net_income_cf_data,
            depreciation_data,
            non_cash_items_data,
            cash_taxes_paid_data,
            cash_interest_paid_data,
            working_capital_changes_data,
            cash_from_operations_data,
            capital_expenditures_data,
            other_investing_data,
            cash_from_investing_data,
            cash_dividends_paid_data,
            issuance_repurchase_stock_data,
            issuance_repayment_debt_data,
            cash_from_financing_data,
            net_change_in_cash_data,
        ]

        for entry in cf_items:
            financial_statements_data_array.append(entry)

    print('Processing data...')
    standardized_financial_data = {}
    bad_entries = []
    for entry in financial_statements_data_array:
        if 'error' in entry.keys():
            bad_entries.append(entry['key'])
            continue

        key = list(entry.keys())[0]
        # print(key)
        for date in entry[key]:
            value = entry[key][date]
            if date not in standardized_financial_data.keys():
                standardized_financial_data[date] = {}
            standardized_financial_data[date][key] = value

    for entry in bad_entries:
        for date in list(standardized_financial_data.keys()):
            standardized_financial_data[date][entry] = 0

    # do some sanity checks
    for date in standardized_financial_data.keys():
        report = standardized_financial_data[date]
        # calculate equity
        if 'total_assets' in report:
            print(report['total_assets'])
            print(report)
            report['equity'] = report['total_assets'] - report['total_liabilities']

    return standardized_financial_data


def get_description(ticker):
    base_url = f'https://www.reuters.com/companies/api/getFetchCompanyProfile/{ticker}'
    for i in range(0, 10):
        try:
            resp = requests.get(base_url, headers={'Host': 'www.reuters.com', 'Connection': 'keep-alive'}).json()
            about = resp['market_data']['about']
            return about
        except KeyError:
            time.sleep(1)

    print('Error fetching description!')
    return 'N/A'


def get_ttm_eps(ticker):
    base_url = f'https://www.reuters.com/companies/api/getFetchCompanyProfile/{ticker}'
    for i in range(0, 3):
        try:
            resp = requests.get(base_url).json()
            ttm_eps = float(resp['market_data']['eps_excl_extra_ttm'])
            return ttm_eps
        except:
            pass
    return 0


def get_price(ticker):
    base_url = f'https://www.reuters.com/companies/api/getFetchCompanyProfile/{ticker}'
    for i in range(0, 3):
        try:
            resp = requests.get(base_url).json()
            last_price = float(resp['market_data']['last'])
            return last_price
        except:
            pass

    return -1


def get_exchange(ticker):
    base_url = f'https://www.reuters.com/companies/api/getFetchCompanyProfile/{ticker}'
    for i in range(0, 3):
        try:
            resp = requests.get(base_url).json()
            exchange = (resp['market_data']['fundamental_exchange_name'])
            return exchange
        except:
            pass

    return -1


# determine Reuters code
def get_ric(stock_data):
    reuters_url = f'https://www.reuters.com/finance/stocks/lookup?searchType=any&search={stock_data}'
    resp = requests.get(reuters_url).content

    soup = BeautifulSoup(resp, 'html.parser')
    first_tr = soup.find('tr', {'class': 'stripe'})

    if first_tr is None:
        return "None"

    ric = first_tr.find_all('td')[1].text

    return ric


column_names = [
    'Price',
    'Description',
    'EPS',
    'Exchange',
    'Ric',
    'Statement'
]

if __name__ == '__main__':
    temp_store = {col: [] for col in column_names}
    with open('../tickers/Master_CSV.csv', 'r') as read_obj:
        csv_reader = DictReader(read_obj)
        file_name = "Reuters" + ".csv"
        counter = 0
        for row in csv_reader:
            if get_ric(row['Name']) != "None":
                n = get_ric(row['Name'])
                temp_store['Price'].append(get_price(n))
                temp_store['Description'].append(get_description(n))
                temp_store['EPS'].append(get_ttm_eps(n))
                temp_store['Exchange'].append(get_exchange(n))
                temp_store['Ric'].append(get_ric(n))
                temp_store['Statement'].append(get_standardized_statements(n))

            counter += 1
            if counter == 10:
                break

df = pd.DataFrame(temp_store)
df.to_csv("reuters_output.csv")
