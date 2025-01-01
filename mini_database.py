'''This module has features that you can make simple database for your programms.'''

# import requirements
import csv
import os


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







