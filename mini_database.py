'''This module has features that you can make simple database for your programms.'''

# import requirements
import csv
import os
from typing import Callable


# check that database folder is exists.
if not os.path.exists(f'{os.getcwd()}/database'):
    os.mkdir(f'{os.getcwd()}/database')



class Database:

    def __init__(self, table_name: str) -> None:
        self.table_name = table_name
        self.address = f'{os.getcwd()}/database/{self.table_name}.csv'

    
    def add_column(self, columns_data: list[str]) -> None:
        
        if os.path.exists(self.address):
            raise FileExistsError(f'{self.address} is already exists.')

        with open(self.address, 'w', newline= '') as table_csv:
            columns_data.insert(0, 'id')
            csv.writer(table_csv).writerow(columns_data)
        


    def add_one_row(self, row_data: list[str]) -> None:

        num_id = 0
        with open(self.address, 'r+', newline= '') as table_csv:
            for _ in csv.reader(table_csv):
                num_id += 1

            row_data.insert(0, num_id)
            csv.writer(table_csv).writerows([row_data])

    
    def delete(self, column_data: dict) -> None:
        collected = []

        with open(self.address, 'r') as table_csv:

            for item in csv.DictReader(table_csv):
                mark = 0

                for value in column_data.values():
                    if value in item.values():
                        mark += 1
                
                if mark != len(column_data):
                    collected.append(list(item.values())[1:])
            
            columns = list(item.keys())[1:]
        os.remove(self.address)

        self.add_column(columns)
        for row in collected:
            self.add_one_row(row)

    
    def get(self, data: dict, multiple: bool = False) -> list:
        
        results = []

        with open(self.address, 'r') as table_csv:

            for item in csv.DictReader(table_csv):
                mark = 0

                for value in data.values():
                    if value in item.values():
                        mark += 1
                
                if mark == len(data.values()):
                    results.append(item)

                    if not multiple:
                       return results
        
        return results
    


    def filter_statement(self, columns: list[str], orders: list[Callable]) -> list:
        
        results = []

        with open(self.address, 'r') as table_csv:

            if len(columns) == 1:

                for item in csv.DictReader(table_csv):
                    if bool(orders[0](item[columns[0]])):
                       results.append(item)

            
            else:
                for item in csv.DictReader(table_csv):
                    mark = 0
                    for ind, column in enumerate(columns):
                        if bool(orders[ind](item[column])):
                            mark += 1
                    
                    if mark == len(columns):
                        results.append(item)
        
        return results
    


class Connect(Database):
    
    def __init__(self, table_name_fount: str, table_name_related: str, connected_column: str, related_name: str) -> None:
        self.table_name_fount = table_name_fount
        self.table_name_related = table_name_related
        self.connected_column = connected_column
        self.related_name = related_name

        self.address_fount = f'{os.getcwd()}/database/{self.table_name_fount}.csv'
        self.address_related = f'{os.getcwd()}/database/{self.table_name_related}.csv'
    
    
    def get_from(self, data_from_fount: dict, multiple: bool = False) -> list:
        
        results = []

        with open(self.address_related, 'r') as related_table:

            for item in csv.DictReader(related_table):

                if item[self.related_name] == data_from_fount[self.connected_column]:
                    results.append(item)

                    if not multiple:
                        break
        
        return results




    








