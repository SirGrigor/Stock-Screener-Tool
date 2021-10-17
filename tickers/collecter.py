import requests
import pandas as pd
import glob
import re


def natural_sort_key(s):
    regex_sorting = re.compile('([0-9]+)')
    return [int(text) if text.isdigit() else text.lower()
            for text in re.split(regex_sorting, s)]


# URL Parsing
API_URL_Start = "https://stock.screener.co/screenerinclude.php?token="
API_URL_2 = "&asset=stocks&type=calcStocks&conditions=Total%20Current%20Assets(A)%20-%20Total%20Liabilities(" \
            "A)|%3E|Market%20capitalization|4|0&fields=Symbol|Company%20Name|Exchange%20Traded%20On|Country%20Located" \
            "%20In|Sector|Revenue-most%20recent%20fiscal%20year|Revenue%20Change-year%20over%20year|Price-closing" \
            "%20or%20last%20bid|P/E%20including%20extraordinary%20items-TTM&orderVar=Symbol&orderDir=ASC&start= "
API_URL_3 = "&limit=50&csv=1&markets="

# token
File_Index = 0
CSV_Index = 0
token = requests.get("https://stock.screener.co/screenerinclude.php?user=screenerproject&pass=ScreenerProject").text
Real_URL = API_URL_Start + token + API_URL_2 + str(File_Index) + API_URL_3
Real_Request = requests.get(Real_URL)
print(Real_Request.content)
# Download first file
File_Name = "ALL_CSV/" + "token-csv-" + str(CSV_Index) + ".csv"
req = requests.get(Real_URL)
url_content = req.content
csv_file = open(File_Name, 'wb')
csv_file.write(url_content)
csv_file.close()
print("File Download " + str(CSV_Index))

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

df = pd.read_csv(File_Name, names=column_names)
while (pd.isnull(df.iloc[1, 0]) == False):
    File_Index += 50
    CSV_Index += 1
    Real_URL = API_URL_Start + token + API_URL_2 + str(File_Index) + API_URL_3
    Real_Request = requests.get(Real_URL)
    File_Name = "ALL_CSV/" + "token-csv-" + str(CSV_Index) + ".csv"
    req = requests.get(Real_URL)
    url_content = req.content
    csv_file = open(File_Name, 'wb')
    csv_file.write(url_content)
    csv_file.close()
    df = pd.read_csv(File_Name, names=column_names)
    print("File Download " + str(CSV_Index))

print("Preparing to Combine")
extension = 'csv'
all_filenames = [i for i in glob.glob('ALL_CSV/*.{}'.format(extension))]
all_filenames.sort(key=natural_sort_key)
# all_filenames=all_filenames[::-1]
# print(all_filenames)

combined_csv = pd.concat([pd.read_csv(f, names=column_names)[1:] for f in all_filenames])
combined_csv.to_csv("Master_CSV.csv", index=True, encoding='utf-8-sig')

print("Download Complete")
