import pandas as pd
from pathlib import Path

def concat_csv_files(input_dir, output_file):
    # Get all CSV files
    csv_files = list(Path(input_dir).glob('*.csv'))

    # Concat CSV files
    df = pd.concat((pd.read_csv(f, skipfooter=1, engine='python') for f in csv_files))

    # Save output file
    df.to_csv(output_file, index=False)
