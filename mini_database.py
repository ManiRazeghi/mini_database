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
    
    
    def add_column(self, columns_data: list[str]) -> None:
        
        if os.path.exists(address := f'{os.getcwd()}/database/{self.table_name}.csv'):
            raise FileExistsError(f'{address} is already exists.')

        with open(address, 'w', newline= '') as table_csv:
            csv.writer(table_csv).writerow(columns_data)




