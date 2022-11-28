# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import os
from os import walk
from bs4 import BeautifulSoup
import re
import pandas as pd

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

dir_path = os.path.join('.','AWR')
#print current directory // pwd
print(os.getcwd())
print_hi(dir_path)
awr_paths = []
for (dir_path, dir_names, file_names) in walk(dir_path):
    file_paths = [os.path.join(dir_path, file_name) for file_name in file_names]
    awr_paths.extend(file_paths)

output = []
for awr_path in awr_paths:
    print(awr_path)
    with open(awr_path) as f:
        soup = BeautifulSoup(f)

    row = {}
    row['AWR file'] = awr_path

    table = soup.find(string=re.compile('DB Name')).findParent('table')
    df = pd.read_html(str(table))[0]
    row['DB Name'] = str(df['DB Name'][0])

    table = soup.find(string=re.compile('Instance')).findParent('table')
    df = pd.read_html(str(table))[0]
    row['Instance Name'] = str(df['Instance'][0])

    table = soup.find(string=re.compile('Host')).findParent('table')
    df = pd.read_html(str(table))[0]
    try:
        row['Host Name'] = str(df['Host Name'][0])
    except:
        row['Host Name'] = str(df['Host'][0])

    table = soup.find(string=re.compile('Elapsed:')).findParent('table')
    df = pd.read_html(str(table))[0]
    string = str(df[df.iloc[:, 0] == 'Elapsed:']['Snap Time'])
    string = re.compile('\d*,?\d*\.\d*').search(string).group(0)
    string = string.replace(',', '')
    row['Elapsed Time (mins)'] = float(string)

    string = str(df[df.iloc[:, 0] == 'DB Time:']['Snap Time'])
    string = re.compile('\d*,?\d*\.\d*').search(string).group(0)
    string = string.replace(',', '')
    row['DB Time (mins)'] = float(string)

    chapter = soup.find(string=re.compile('Wait Classes by Total Wait Time'))
    if chapter:
        table = chapter.find_next('table')
        df = pd.read_html(str(table))[0]
        element = df[df.iloc[:, 0] == 'DB CPU']['Total Wait Time (sec)']
        element = element.replace({"K": "*1e3", "M": "*1e6", "G": "*1e9", ",": ""}, regex=True).map(pd.eval).astype(
            float)
        row['DB CPU (s)'] = float(element)
    else:
        chapter = soup.find(re.compile("h\d+|p"), string=re.compile('Service Statistics'))
        table = chapter.find_next('table')
        df = pd.read_html(str(table))[0]
        column = df['DB CPU (s)']
        row['DB CPU (s)'] = float(column.sum())

    chapter = soup.find(string=re.compile('NUM_CPUS'))
    table = chapter.findParent('table')
    df = pd.read_html(str(table))[0]
    string = str(df[df.iloc[:, 0] == 'NUM_CPUS'].iloc[0, 1])
    string = string.replace(',', '')
    row['CPUs'] = float(string)

    string = str(df[df.iloc[:, 0] == 'NUM_CPU_CORES'].iloc[0, 1])
    string = string.replace(',', '')
    row['Cores'] = float(string)

    string = str(df[df.iloc[:, 0] == 'PHYSICAL_MEMORY_BYTES'].iloc[0, 1])
    string = string.replace(',', '')
    row['Memory (GB)'] = float(string) / (1024 ** 3)


    chapter = soup.find(string=re.compile('Instance CPU'))
    if chapter:
        table = chapter.find_next('table')
        df = pd.read_html(str(table))[0]
        row['%Busy CPU'] = float(df['%Busy CPU'])
    else:
        print('   error: "Instance CPU" not found')

    text = soup.find(string=re.compile('SGA use \(MB\):'))
    if text:
        table = text.findParent('table')
        df = pd.read_html(str(table))[0]
        row['SGA use (MB)'] = float(df[df.iloc[:, 0] == 'SGA use (MB):']['Begin'])
        row['PGA use (MB)'] = float(df[df.iloc[:, 0] == 'PGA use (MB):']['Begin'])
    else:
        chapter = soup.find(re.compile("h\d+|p"), string=('SGA Memory Summary'))
        table = chapter.find_next('table')
        df = pd.read_html(str(table))[0]
        row['SGA use (MB)'] = df['Begin Size (Bytes)'].sum() / 1024 / 1024

        chapter = soup.find(re.compile("h\d+|p"), string=('PGA Aggr Target Stats'))
        table = chapter.find_next('table')
        df = pd.read_html(str(table))[0]
        row['PGA use (MB)'] = float(df['PGA Mem Alloc(M)'][0])

    chapter = soup.find(re.compile("h\d+|p"), string='Instance Activity Stats')
    table = chapter.find_next('table')
    df = pd.read_html(str(table))[0]

    element = df[df.iloc[:, 0] == 'physical read total bytes']['per Second'].map(pd.eval)
    row['Read Throughput (MB/s)'] = float(element) / 1024 / 1024

    element = df[df.iloc[:, 0] == 'physical write total bytes']['per Second'].map(pd.eval)
    row['Write Throughput (MB/s)'] = float(element) / 1000 / 1000

    element = df[df.iloc[:, 0] == 'physical read total IO requests']['per Second'].map(pd.eval)
    row['Read IOPS'] = float(element)

    element = df[df.iloc[:, 0] == 'physical write total IO requests']['per Second'].map(pd.eval)
    row['Write IOPS'] = float(element)

    output.append(row)

output = pd.DataFrame(output)
print('Here!')
output['Total Throughput (MB/s)'] = output['Read Throughput (MB/s)'] + output['Write Throughput (MB/s)']
output['Total IOPS'] = output['Read IOPS'] + output['Write IOPS']
print('guher')
output['%DB Time of Elapsed Time'] = output['DB Time (mins)'] / output['Elapsed Time (mins)']
output['CPU total capacity (s)'] = output['Elapsed Time (mins)'] * 60 * output['CPUs']
output['%DB CPU of server capacity'] = output['DB CPU (s)'] / output['CPU total capacity (s)']
output['ORA use (GB)'] = (output['SGA use (MB)'] + output['PGA use (MB)']) / 1024
output['source CPU HT factor'] = output['CPUs'] / output['Cores']

output.to_excel(os.path.join('.', 'output', 'awr_data.xlsx'))
print('Done!')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
