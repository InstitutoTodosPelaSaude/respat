# Last update: 2023-07-04

import argparse
import pandas as pd

if __name__ == '__main__':

    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Split an Excel file into separate tab files. It reads the input Excel file, iterates through each sheet, and saves each sheet as a separate TSV file. Provide the input file name using the --input argument.')
    parser.add_argument('--input', help='Input Excel file name')
    args = parser.parse_args()

    print('Processing spreadsheet...')
    def save_tabs_as_files(input_file):
        # Read the input Excel file
        xls = pd.ExcelFile(input_file)

        # Iterate through each sheet/tab in the Excel file
        for sheet_name in xls.sheet_names:
            # Read the sheet/tab as a DataFrame
            df = xls.parse(sheet_name)

            # Create the output file name by appending the sheet name as a suffix
            output_file = f"{input_file.split('.')[0]}_{sheet_name}.tsv"

            # Save the DataFrame as a new Excel file
            # df.to_excel(output_file, index=False)
            df.to_csv(output_file, sep='\t', index=False)

            print(f"\t - Saved sheet '{sheet_name}' as '{output_file}'.")


    # Call the function to save tabs as files
    if args.input:
        save_tabs_as_files(args.input)
    else:
        print("Please provide the input file name using the --input argument.")
