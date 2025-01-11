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
        '''This method add column in the csv file.'''
        
        if os.path.exists(self.address):
            raise FileExistsError(f'{self.address} is already exists.')

        with open(self.address, 'w', newline= '') as table_csv:
            columns_data.insert(0, 'id')
            csv.writer(table_csv).writerow(columns_data)
        


    def add_one_row(self, row_data: list[str]) -> None:
        '''This method add one row to csv file.'''

        num_id = 0
        with open(self.address, 'r+', newline= '') as table_csv:
            for _ in csv.reader(table_csv):
                num_id += 1

            row_data.insert(0, num_id)
            csv.writer(table_csv).writerows([row_data])

    
    def delete(self, row_data: dict) -> None:
        '''This method delete one row from csv file'''
        collected = []

        with open(self.address, 'r') as table_csv:

            for item in csv.DictReader(table_csv):
                mark = 0

                for value in row_data.values():
                    if value in item.values():
                        mark += 1
                
                if mark != len(row_data):
                    collected.append(list(item.values())[1:])
            
            columns = list(item.keys())[1:]
        os.remove(self.address)

        self.add_column(columns)
        for row in collected:
            self.add_one_row(row)

    
    def get(self, data: dict, multiple: bool = False) -> list[dict]:
        '''This method get one row from csv file.'''
        
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
        '''This method retun list of dicts that filter with func you gave it.'''
        
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
    

    def count_row(self) -> int:
        row_number = -1
        with open(self.address, 'r') as csv_file:
            for _ in csv.reader(csv_file):
                row_number += 1
        
        return row_number
    

    def sum_column(self, field_name: str) -> float:
        total_number = 0
        with open(self.address, 'r') as csv_file:
            for item in csv.DictReader(csv_file):
                total_number += float(item.get(field_name))
        
        return total_number
    

    def average_filed(self, field_name: str) -> float:
        return self.sum_column(field_name) / self.count_row()
    

    def min_field(self, field_name: str, _order= 'min') -> float:
        number = self.average_filed(field_name)

        with open(self.address, 'r') as csv_file:

            for item in csv.DictReader(csv_file):
                
                if _order == 'min':
                   if float(item.get(field_name)) < number:
                      number = float(item.get(field_name))
                
                elif _order == 'max':
                    if float(item.get(field_name)) > number:
                      number = float(item.get(field_name))
                
        return number
    

    def max_field(self, field_name: str) -> float:
        return self.min_field(field_name, 'max')




    


class Connect:
    
    def __init__(self, table_name_fount: str, table_name_related: str, connected_column: str, related_name: str) -> None:
        self.table_name_fount = table_name_fount
        self.table_name_related = table_name_related
        self.connected_column = connected_column
        self.related_name = related_name

        self.address_fount = f'{os.getcwd()}/database/{self.table_name_fount}.csv'
        self.address_related = f'{os.getcwd()}/database/{self.table_name_related}.csv'
    
    
    def get_from(self, data_from_fount: dict, multiple: bool = False, address = 'related', column = 'related') -> list[dict]:
        '''This method get related data that connected to fount row you gave.'''

        results = []
        path = self.address_related if address == 'related' else self.address_fount
        giver_column = self.related_name if column == 'related' else self.connected_column
        fount_column = self.connected_column if column == 'related' else self.related_name

        with open(path, 'r') as related_table:

            for item in csv.DictReader(related_table):

                if item[giver_column] == data_from_fount[fount_column]:
                    results.append(item)

                    if not multiple:
                        break
        
        return results
    

    def get_from_columns(self, data_from_fount: dict, multiple: bool = False, columns: list[str]|list = [], table: str = 'related') -> list[dict]:
        '''This method return only related column data you gave.'''

        new_results = []

        iterable = self.get_from(data_from_fount, multiple) if table == 'related' else self.get_from(data_from_fount, multiple, 'founted', 'founted')
        
        for item in iterable:
            data = {}
            for column in columns:
                data[column] = item.get(column)
            new_results.append(data)
        
        return new_results
    

    def get_fount(self, data_from_related: dict, multiple: bool = False) -> list:
        return self.get_from(data_from_related, multiple, 'founted', 'founted')
    

    def get_fount_columns(self, data_from_related: dict, multiple: bool = False, columns: list[str] = []) -> list[dict]:
        return self.get_from_columns(data_from_related, multiple, columns, 'founted')

    
    def join_tables(self, file_name: str) -> None:

        if os.path.exists((address := f'{os.getcwd()}/database/{file_name}.csv')):
            raise FileExistsError(f'{address} is already exists.')
        
        with open(address, 'w', newline= '') as joined_file:

            joind_columns = []
            joind_rows = []

            with open(self.address_fount, 'r') as fount_file, open(self.address_related, 'r') as related_file:
                

                for ind, item in enumerate(csv.DictReader(fount_file)):
                    if not ind:
                        new = list(item.keys())
                        new.extend(list(self.get_from(item)[0])[1:])
                        joind_columns.append(new)
                    joind_rows.append([*list(item.values()), *list(self.get_from(item)[0].values())])
                
                file = csv.writer(joined_file)
                file.writerow(joind_columns[0])

                for item in joind_rows:
                    file.writerows([item])
    



    



    








