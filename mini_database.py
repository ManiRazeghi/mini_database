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
            table_csv.seek(len(row_data))

    






