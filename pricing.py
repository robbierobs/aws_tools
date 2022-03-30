from tabulate import tabulate
from io import StringIO
import csv
import getopt
import sys
from IPython.display import display, HTML
import pandas as pd
import numpy as np
import webbrowser
import os
import datetime
from pretty_html_table import build_table


def get_table_html(df):

    """
    From https://stackoverflow.com/a/49687866/2007153

    Get a Jupyter like html of pandas dataframe

    """

    styles = [
        #table properties
        dict(selector=" ",
             props=[("margin","0"),
                    ("font-family",'"Helvetica", "Arial", sans-serif'),
                    ("border-collapse", "collapse"),
                    ("border","none"),
                    # ("border", "2px solid #ccf")
                       ]),

        #background shading
        dict(selector="tbody tr:nth-child(even)",
             props=[("background-color", "#fff")]),
        dict(selector="tbody tr:nth-child(odd)",
             props=[("background-color", "#eee")]),

        #cell spacing
        dict(selector="td",
             props=[("padding", ".5em")]),

        #header cell properties
        dict(selector="th",
             props=[("font-size", "100%"),
                    ("text-align", "center")]),


    ]
    return (df.style.set_table_styles(styles)).render()

class FargatePricing():
    def __init__(self):
        ''' No Upfront Costs '''
        self.on_demand_cpu = 0.04048
        self.on_demand_mem = 0.004445
        self.one_year_cpu = 0.032384
        self.one_year_mem = 0.003556
        self.three_year_cpu = 0.022264
        self.three_year_mem = 0.00244475
        self.monthly = 730

class ECS_Service():
    def __init__(self):
        self.service_name = '-'

        self.current_count = 0
        self.new_count = 0

        self.current_cpu = 0
        self.current_mem = 0
        self.new_cpu = 0
        self.new_mem = 0

        self.count_diff = 0
        self.cpu_diff = 0
        self.mem_diff = 0
        self.total_cpu_diff = 0
        self.total_mem_diff = 0

        self.current_mem_total = 0
        self.current_cpu_total = 0
        self.new_mem_total = 0
        self.new_cpu_total = 0

    def calculate_totals(self):
        self.new_mem_total = int(self.new_mem) * int(self.new_count)
        self.new_cpu_total = int(self.new_cpu) * int(self.new_count)
        self.current_cpu_total = int(self.current_cpu) * int(self.current_count)
        self.current_mem_total = int(self.current_mem) * int(self.current_count)


        totals = {
            'new_mem': self.new_mem_total,
            'new_cpu': self.new_cpu_total,
            'current_mem': self.current_mem_total,
            'current_cpu': self.current_cpu_total,
        }

        return totals

    def calculate_diffs(self):
        self.count_diff = int(self.new_count) - int(self.current_count)
        self.cpu_diff = int(self.new_cpu) - int(self.current_cpu)
        self.mem_diff = int(self.new_mem) - int(self.current_mem)
        self.total_cpu_diff = int(self.new_cpu_total) - int(self.current_cpu_total)
        self.total_mem_diff = int(self.new_mem_total) - int(self.current_mem_total)

        diffs = {
            'count': self.count_diff,
            'cpu': self.cpu_diff,
            'mem': self.mem_diff,
            'total_cpu': self.total_cpu_diff,
            'total_mem': self.total_mem_diff,
        }
        return diffs


# def create_dataframe(dataframe, names):
#     df = pd.DataFrame(
#         dataframe, index=list(names),
#         # columns=pd.MultiIndex.from_product([['Decision Tree', 'Regression', 'Random'],['Tumour', 'Non-Tumour']], names=['Model:', 'Predicted:'])
#     )
#     return df

def import_csv(csvfile):
    def header_validation(headers):
        validity = True
        try:
            if header[0] != 'Service Name' or\
               header[1] != 'Current Count' or\
               header[2] != 'New Count' or\
               header[3] != 'Current vCPU Value' or\
               header[4] != 'New vCPU Value' or\
               header[5] != 'Current Memory (in MB)' or\
               header[6] != 'New Memory (in MB)':
                validity = False
        except Exception as e:
            print(e)
            return False
        return validity

    def row_processor(row):
        service_name = row[0]
        current_count = row[1]
        new_count = row[2]
        current_cpu = row[3]
        new_cpu = row[4]
        current_mem = row[5]
        new_mem = row[6]

        e = ECS_Service()
        e.service_name = service_name
        e.current_count = current_count
        e.new_count = new_count
        e.current_cpu = current_cpu
        e.new_cpu = new_cpu
        e.current_mem = current_mem
        e.new_mem = new_mem

        totals = e.calculate_totals()

        e.new_mem_total = totals['new_mem']
        e.new_cpu_total = totals['new_cpu']
        e.current_mem_total = totals['current_mem']
        e.current_cpu_total = totals['current_cpu']

        diffs = e.calculate_diffs()

        e.count_diff = diffs['count']
        e.cpu_diff = diffs['cpu']
        e.mem_diff = diffs['mem']
        e.total_cpu_diff = diffs['total_cpu']
        e.total_mem_diff = diffs['total_mem']

        return e


    with open(csvfile, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        header = next(reader)
        valid_headers = header_validation(header)
        if not valid_headers:
            return False

        rows = []
        for row in reader:
            rows.append(row)

        services = []
        for row in rows:
            service = row_processor(row)
            services.append(service)
        return services

def process_services(services):

    headers = [
        'Current Count',
        'New Count',
        'Count Diff',
        'Current CPU',
        'New CPU',
        'CPU Diff',
        'Current Memory',
        'New Memory',
        'Memory Diff',
        'Current Total CPU',
        'New Total CPU',
        'Current Total Memory',
        'New Total Memory',
        'Total CPU Diff',
        'Total Memory Diff'
    ]

    dataframe = {}
    key_list = list(headers)
    names = []
    changes = []

    for header in headers:
        dataframe[header] = []

    for service in services:

        change = {
            'name': service.service_name,
            'data': [
                service.current_count,
                service.new_count,
                service.count_diff,
                service.current_cpu,
                service.new_cpu,
                service.cpu_diff,
                service.current_mem,
                service.new_mem,
                service.mem_diff,
                service.current_cpu_total,
                service.new_cpu_total,
                service.current_mem_total,
                service.new_mem_total,
                service.total_cpu_diff,
                service.total_mem_diff,
            ]
        }

        changes.append(change)

    for change in changes:
        name = change['name']
        names.append(name)
        for index, data in enumerate(change['data']):
            key = key_list[index]
            dataframe[key].append(data)

    df = pd.DataFrame(
        dataframe, index=list(names),
    )
    return df


def process_pricing(services):

    fargate = FargatePricing()

    headers = [
        'Current Total CPU',
        'Current CPU Price',
        'New Total CPU',
        'New CPU Price',
        'CPU Price Diff',
        'Current Total Memory',
        'Current Mem Price',
        'New Total Memory',
        'New Mem Price',
        'Mem Price Diff',
        'Total Price Diff'
    ]

    dataframe = {}
    key_list = list(headers)
    names = []
    changes = []

    for header in headers:
        dataframe[header] = []

    for service in services:
        current_cpu_price = (service.current_cpu_total / 1024) * (fargate.one_year_cpu * fargate.monthly)
        new_cpu_price = (service.new_cpu_total / 1024) * (fargate.one_year_cpu * fargate.monthly)
        cpu_diff = new_cpu_price - current_cpu_price
        current_mem_price = (service.current_mem_total / 1024) * (fargate.one_year_mem * fargate.monthly)
        new_mem_price = (service.new_mem_total / 1024) * (fargate.one_year_mem * fargate.monthly)
        total_cpu_diff = (service.total_cpu_diff / 1024) * (fargate.one_year_cpu * fargate.monthly)
        total_mem_diff = (service.total_mem_diff / 1024) * (fargate.one_year_mem * fargate.monthly)
        total_price_diff = total_cpu_diff + total_mem_diff

        change = {
            'name': service.service_name,
            'data': [
                service.current_cpu_total,
                current_cpu_price,
                service.new_cpu_total,
                new_cpu_price,
                cpu_diff,
                service.current_mem_total,
                current_mem_price,
                service.new_mem_total,
                new_mem_price,
                total_mem_diff,
                total_price_diff

            ]
        }

        changes.append(change)

    for change in changes:
        name = change['name']
        names.append(name)
        for index, data in enumerate(change['data']):
            key = key_list[index]
            dataframe[key].append(data)

    df = pd.DataFrame(
        dataframe, index=list(names),
    )
    df.loc['Total'] = df.sum()
    df = df.round(2)
    return df


def display_df_html(df):
    df = get_table_html(df)
    filename = 'pandas.html'
    with open(filename, 'w') as f:
        f.write(df)
    file_location = os.path.abspath(f'./%s' % (filename))
    webbrowser.open('file://' + file_location)


def define_service():
    new_cpu = 0
    new_mem = 0
    service_name = input("Set service name: ")
    service_action = input("Do you want to change or remove? (remove/change): ")
    service_action = service_action.strip().lower()
    current_count = int(input("How many tasks are currently running as a baseline: "))
    if service_action in ('r', 'remove'):
        new_count = 0
    else:
        new_count = int(input("Set new service count baseline: "))
        if not new_count:
            new_count = current_count
    current_cpu = int(input("Set current vCPU value: "))
    current_mem = int(input("Set current Memory value: "))
    if service_action in ('change', 'c'):
        service_action = 'change'
        new_cpu = int(input("Set new vCPU value: "))
        new_mem = int(input("Set new Memory value: "))
        # new_count = int(input("Set new service count baseline: "))
        if not new_count:
            new_count = current_count
    if service_action in ('r', 'remove'):
        new_count = 0

    service_definition = {
        'name': service_name,
        'current_count': current_count,
        'new_count': new_count,
        'action': service_action,
        'current_cpu': current_cpu,
        'current_mem': current_mem,
        'new_cpu': new_cpu,
        'new_mem': new_mem
        }
    return service_definition


def manual():
    services = []
    changes = []
    while True:
        service = define_service()
        services.append(service)

        add_more = input("Add another service? (y/n): ")
        add_more = add_more.strip().lower()

        if add_more in ('y', 'yes'):
            continue
        break

    current_total_count = 0
    current_total_cpu_usage = 0
    current_total_mem_usage = 0
    new_total_count = 0
    new_total_cpu_usage = 0
    new_total_mem_usage = 0

    for service in services:
        action = service.get('action')
        current_cpu = service.get('current_cpu')
        current_mem = service.get('current_mem')
        new_cpu = service.get('new_cpu')
        new_mem = service.get('new_mem')
        name = service.get('name')
        current_count = service.get('current_count')
        new_count = service.get('new_count')

        if action == 'change':

            current_cpu_total = current_cpu * current_count
            current_mem_total = current_mem * current_count
            new_cpu_total = new_cpu * new_count
            new_mem_total = new_mem * new_count
            cpu_diff = new_cpu - current_cpu
            mem_diff = new_mem - current_mem

            current_total_count += current_count
            current_total_cpu_usage += current_cpu_total
            current_total_mem_usage += current_mem_total

            new_total_count += new_count
            new_total_cpu_usage += new_cpu_total
            new_total_mem_usage += new_mem_total

            service_cpu_diff = new_cpu_total - current_cpu_total
            service_mem_diff = new_mem_total - current_mem_total
            count_diff = new_count - current_count

            change = {
                'name': name,
                'data': [
                    current_count,
                    new_count,
                    count_diff,
                    current_cpu,
                    new_cpu,
                    cpu_diff,
                    current_mem,
                    new_mem,
                    mem_diff,
                    current_cpu_total,
                    new_cpu_total,
                    current_mem_total,
                    new_mem_total,
                    service_cpu_diff,
                    service_mem_diff,
                ]
            }

            changes.append(change)

    total_cpu_diff = new_total_cpu_usage - current_total_cpu_usage
    total_mem_diff = new_total_mem_usage - current_total_mem_usage
    total_count_diff = new_total_count - current_total_count

    headers = [
        # 'Name',
        'Current Count',
        'New Count',
        'Count Diff',
        'Current CPU',
        'New CPU',
        'CPU Diff',
        'Current Memory',
        'New Memory',
        'Memory Diff',
        'Current Total CPU',
        'New Total CPU',
        'Current Total Memory',
        'New Total Memory',
        'Total CPU Diff',
        'Total Memory Diff'
    ]

    dataframe = {}

    for header in headers:
        dataframe[header] = []

    key_list = list(dataframe)
    names = []

    for change in changes:
        name = change['name']
        names.append(name)
        for index, data in enumerate(change['data']):
            key = key_list[index]
            dataframe[key].append(data)

    df = pd.DataFrame(
        dataframe, index=list(names),
        # columns=pd.MultiIndex.from_product([['Decision Tree', 'Regression', 'Random'],['Tumour', 'Non-Tumour']], names=['Model:', 'Predicted:'])
    )
    df.style
    print(tabulate(df, headers='keys', tablefmt='pretty'))

def run_csv(inputfile):
    services = import_csv(inputfile)
    service_df = process_services(services)
    pricing_df = process_pricing(services)

    print(tabulate(service_df, headers='keys', tablefmt='pretty'))
    print(tabulate(pricing_df, headers='keys', tablefmt='pretty'))

    display_df_html(pricing_df)


def main(argv):

    inputfile = ''
    try:
        opts, args = getopt.getopt(argv, "hmi:", ["ifile="])
    except getopt.GetoptError:
        print('pricing.py -i <csvfile.csv>')
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print('pricing -i <file>.csv')
            sys.exit()
        elif opt in ("-i", "--ifile"):
            inputfile = arg
            run_csv(inputfile)
        elif opt == '-m':
            manual()



if __name__ == '__main__':
    main(sys.argv[1:])
