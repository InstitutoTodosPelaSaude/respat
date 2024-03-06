import  jpype
import  asposecells
from pathlib import Path


class Converter:
    def __init__(self, tmp_dir):
        jpype.startJVM()
        self.tmp_dir = tmp_dir

    def convert(self, input_file):
        from asposecells.api import Workbook
        workbook = Workbook(input_file)
        filename = input_file.split('/')[-1].split('.')[0]

        # Create a directory to store CSV files
        save_dir = f'{self.tmp_dir}/{filename}'
        Path(save_dir).mkdir(parents=True, exist_ok=True)

        # Get the names of all worksheets in the workbook
        worksheet_names = list()
        worksheets = workbook.getWorksheets()
        for i in range(worksheets.getCount()):
            worksheet = worksheets.get(i)
            worksheet_name = worksheet.getName()
            worksheet_names.append(worksheet_name)

        # Save each worksheet as CSV
        for worksheet_name in worksheet_names:
            workbook.save(f'{save_dir}/{filename}_{worksheet_name}.csv', asposecells.api.SaveFormat.CSV)
            workbook.getWorksheets().removeAt(worksheet_name)

        return save_dir

    def __del__(self):
        jpype.shutdownJVM()