#%% import libraries
import os
from os import walk
from bs4 import BeautifulSoup
import re
import pandas as pd


#%% list the AWR files that are located in the ./AWR/ subfolder
dir_path = os.path.join('.','AWR')
awr_paths = []
for (dir_path, dir_names, file_names) in walk(dir_path):
    file_paths = [os.path.join(dir_path, file_name) for file_name in file_names]
    awr_paths.extend(file_paths)

#%% for each file in the ./AWR/ subfolder, read the data and add it to the output list

# additional features to implement:
# - extract more data on exadata - which data?
# - extract info about CDB - which data? what if CDB is not referenced in some old reports?
# - extract cpu_count - where is located this info?
output = []
for awr_path in awr_paths:
    print(awr_path)
    with open(awr_path) as f:
        soup = BeautifulSoup(f, features="lxml")

    row = {}
    row['AWR file'] = awr_path

    table = soup.find(string='DB Name').findParent('table')
    df = pd.read_html(str(table))[0]
    row['DB Name'] = str(df['DB Name'][0])
    row['Release'] = str(df['Release'][0])
    row['RAC'] = str(df['RAC'][0])

    table = soup.find(string='Instance').findParent('table')
    df = pd.read_html(str(table))[0]
    row['Instance Name'] = str(df['Instance'][0])

    string = soup.find(string='Platform')
    if string:
        table = string.findParent('table')
        df = pd.read_html(str(table))[0]
        row['Platform'] = str(df['Platform'][0])
    else:
        print('   error: "Platform" not found')
 
    table = soup.find(string=re.compile('Host( Name)?')).findParent('table')
    df = pd.read_html(str(table))[0]
    try:
        row['Host Name'] = str(df['Host Name'][0])
    except:
        row['Host Name'] = str(df['Host'][0])

    table = soup.find(string=re.compile('Elapsed:')).findParent('table')
    df = pd.read_html(str(table))[0]
    df = df.set_index(df.columns[0])

    string = str(df.loc['Elapsed:']['Snap Time'])
    string = re.compile('\d*,?\d*\.\d*').search(string).group(0)
    string = string.replace(',','')
    row['Elapsed Time (mins)'] = float(string)

    string = str(df.loc['DB Time:']['Snap Time'])
    string = re.compile('\d*,?\d*\.\d*').search(string).group(0)
    string = string.replace(',','')
    row['DB Time (mins)'] = float(string)

    chapter = soup.find(re.compile("h\d+|p"), string='Exadata Storage Server Model')
    if chapter:
        row['Exadata'] = 'YES'
        table = chapter.find_next('table')
        df = pd.read_html(str(table))[0]
        df = pd.read_html(str(table))[0]
        row['Exadata Model'] = str(df['Model'][0])
    else:
        row['Exadata'] = 'NO'

    chapter = soup.find(string='Wait Classes by Total Wait Time')
    if chapter:
        table = chapter.find_next('table')
        df = pd.read_html(str(table))[0]
        
        element = df[df.iloc[:,0] == 'DB CPU']['Total Wait Time (sec)']
        element = element.replace({"K":"*1e3", "M":"*1e6", "G":"*1e9", ",":""}, regex=True).map(pd.eval).astype(float)
        row['DB CPU (s)'] = float(element)
    else:
        print('"   Wait Classes by Total Wait Time" not found, used alternative section "Service Statistics"')
        chapter = soup.find(re.compile("h\d+|p"), string='Service Statistics')
        table = chapter.find_next('table')
        df = pd.read_html(str(table))[0]
        column = df['DB CPU (s)']
        row['DB CPU (s)'] = float(column.sum())
    
    chapter = soup.find(re.compile("h\d+|p"), string='Operating System Statistics')
    table = chapter.find_next('table')
    df = pd.read_html(str(table))[0]
    df = df.set_index('Statistic')

    string = str(df.loc['NUM_CPUS'][0])
    string = string.replace(',','')
    row['CPUs'] = float(string)
        
    string = str(df.loc['NUM_CPU_CORES'][0])
    string = string.replace(',','')
    row['Cores'] = float(string)

    string = str(df.loc['PHYSICAL_MEMORY_BYTES'][0])
    string = string.replace(',','')
    row['Memory (GB)'] = float(string)/(1024**3)

    string = str(df.loc['BUSY_TIME'][0])
    string = string.replace(',','')
    row['BUSY_TIME'] = float(string)

    string = str(df.loc['IDLE_TIME'][0])
    string = string.replace(',','')
    row['IDLE_TIME'] = float(string)


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
        df = df.set_index(df.columns[0])
        row['SGA use (MB)'] = float(df.loc['SGA use (MB):']['Begin'])
        row['PGA use (MB)'] = float(df.loc['PGA use (MB):']['Begin'])
    else:
        chapter = soup.find(re.compile("h\d+|p"), string=('SGA Memory Summary'))
        table = chapter.find_next('table')
        df = pd.read_html(str(table))[0]
        row['SGA use (MB)'] = df['Begin Size (Bytes)'].sum()/1024/1024

        chapter = soup.find(re.compile("h\d+|p"), string=('PGA Aggr Target Stats'))
        table = chapter.find_next('table')
        df = pd.read_html(str(table))[0]
        row['PGA use (MB)'] = float(df['PGA Mem Alloc(M)'][0])




    chapter = soup.find(re.compile("h\d+|p"), string='Instance Activity Stats')
    table = chapter.find_next('table')
    df = pd.read_html(str(table))[0]
    df = df.set_index(df.columns[0])

    element = df.loc['physical read total bytes']['per Second']
    row['Read Throughput (MB/s)'] = float(element)/1024/1024

    element = df.loc['physical write total bytes']['per Second']
    row['Write Throughput (MB/s)'] = float(element)/1000/1000

    element = df.loc['physical read total IO requests']['per Second']
    row['Read IOPS'] = float(element)

    element = df.loc['physical write total IO requests']['per Second']
    row['Write IOPS'] = float(element)	

    output.append(row)


#%% convert the output list in Pandas DF and compute additional columns
output = pd.DataFrame(output)
output['Total Throughput (MB/s)'] = output['Read Throughput (MB/s)'] + output['Write Throughput (MB/s)']
output['Total IOPS'] = output['Read IOPS'] + output['Write IOPS']
output['%DB Time of Elapsed Time'] = output['DB Time (mins)'] / output['Elapsed Time (mins)']
output['CPU total capacity (s)'] = output['Elapsed Time (mins)'] * 60 * output['CPUs']
output['%DB CPU of server capacity'] = output['DB CPU (s)'] / output['CPU total capacity (s)']
output['ORA use (GB)'] = (output['SGA use (MB)'] + output['PGA use (MB)'])/1024
output['source CPU HT factor'] = output['CPUs'] / output['Cores']
output['%idle CPU'] = output['IDLE_TIME'] / (output['BUSY_TIME'] + output['IDLE_TIME']) * 100


#%% write the output dataframe to excel in the output folder
output.to_excel(os.path.join('.', 'output', 'awr_data.xlsx'))

