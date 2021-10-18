import requests
import pandas as pd
import glob
import re

column_names = [
    'Symbol',
    'Name',
    'Exchange',
    'Country',
    'Sector',
    'Revenue',
    'RevenueYoY',
    'LastClose',
    'PE'
]


def natural_sort_key(s):
    regex_sorting = re.compile('([0-9]+)')
    return [int(text) if text.isdigit() else text.lower()
            for text in re.split(regex_sorting, s)]


def compose_search_url(url_head, auth_token, ticker_formulas, file_index, search_config):
    return url_head + auth_token + ticker_formulas + str(file_index) + search_config


# URL Parsing
def prepare_search_configuration(file_index):
    url_head = "https://stock.screener.co/screenerinclude.php?token="
    ticker_formulas = "&asset=stocks&type=calcStocks&conditions=Total%20Current%20Assets(A)%20-%20Total%20Liabilities(" \
                      "A)|%3E|Market%20capitalization|4|0&fields=Symbol|Company%20Name|Exchange%20Traded%20On|Country%20Located" \
                      "%20In|Sector|Revenue-most%20recent%20fiscal%20year|Revenue%20Change-year%20over%20year|Price-closing" \
                      "%20or%20last%20bid|P/E%20including%20extraordinary%20items-TTM&orderVar=Symbol&orderDir=ASC&start= "
    search_config = "&limit=50&csv=1&markets="

    # token
    auth_token = requests.get(
        "https://stock.screener.co/screenerinclude.php?user=screenerproject&pass=ScreenerProject").text
    return compose_search_url(url_head, auth_token, ticker_formulas, file_index, search_config)


def create_ticker_csv():
    file_index = 0
    csv_index = 0

    column_names, df = set_start_columns(csv_index, file_index)
    while not pd.isnull(df.iloc[1, 0]):
        file_index += 50
        csv_index += 1
        search_url = prepare_search_configuration(file_index)
        search_response = requests.get(search_url)
        file_name = "ALL_CSV/" + "token-csv-" + str(csv_index) + ".csv"
        print(search_response.content)
        req = requests.get(search_url)
        url_content = req.content
        csv_file = open(file_name, 'wb')
        csv_file.write(url_content)
        csv_file.close()
        df = pd.read_csv(file_name, names=column_names)
        print("File Download " + str(csv_index))


def set_start_columns(csv_index, file_index):
    # Download first file
    # Real_URL
    print("HEre")
    search_request = prepare_search_configuration(file_index)
    # Real_Request
    file_name = "ALL_CSV/" + "token-csv-" + str(csv_index) + ".csv"
    req = requests.get(search_request)
    url_content = req.content
    csv_file = open(file_name, 'wb')
    csv_file.write(url_content)
    csv_file.close()
    print("File Download " + str(csv_index))

    df = pd.read_csv(file_name, names=column_names)
    return column_names, df


def merge_csv():
    print("Preparing to Combine")
    extension = 'csv'
    all_filenames = [i for i in glob.glob('ALL_CSV/*.{}'.format(extension))]
    all_filenames.sort(key=natural_sort_key)
    # all_filenames=all_filenames[::-1]
    # print(all_filenames)
    combined_csv = pd.concat([pd.read_csv(f, names=column_names)[1:] for f in all_filenames])
    combined_csv.to_csv("Master_CSV.csv", index=True, encoding='utf-8-sig')


if __name__ == '__main__':
    create_ticker_csv()
    merge_csv()
