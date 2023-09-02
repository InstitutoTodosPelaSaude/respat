import os
import shutil
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Copies files from a source directory to multiple destination directories based on a predefined list of files and their respective destination subdirectories.')
    parser.add_argument('--src_dir', default='results', help='Source directory')
    parser.add_argument('--dest_dir_d', default='data', help='First destination directory')
    parser.add_argument('--dest_dir_p', default='figures/python', help='Second destination directory for Python figures')
    parser.add_argument('--dest_dir_f', default='figures/flourish', help='Third destination directory for Flourish figures')
    
    args = parser.parse_args()
    
    src_dir = args.src_dir
    dest_dir_d = args.dest_dir_d
    dest_dir_p = args.dest_dir_p
    dest_dir_f = args.dest_dir_f

    # Create the output directories if they don't exist
    os.makedirs(dest_dir_p, exist_ok=True)
    os.makedirs(dest_dir_f, exist_ok=True)

## Copy combined.tsv to dest_dir_d
src = os.path.join(src_dir, "combined.tsv")
dest = os.path.join(dest_dir_d, "combined_cache.tsv")
if os.path.exists(src):
    shutil.copy(src, dest)
else:
    print(f"File {src} does not exist")

## List of files to copy
files = [
    ("demography/combined_matrix_agegroup.tsv", "pyramid", "pyramid"),
    ("country/combined_matrix_country_posneg_full_weeks.tsv", "barplot", "barplot"),
    ("country/combined_matrix_country_posneg_panel_weeks.tsv", "barplot", "barplot"),
    ("country/combined_matrix_country_posneg_allpat_weeks.tsv", "barplot", "barplot"),
    ("country/combined_matrix_country_posrate_full_weeks.tsv", "lineplot", "lineplot"),
    ("state/combined_matrix_state_posrate_full_weeks.tsv", "heatmap", "heatmap"),
    ("demography/matrix_agegroups_weeks_SC2_posrate.tsv", "heatmap", "heatmap"),
    ("demography/matrix_agegroups_weeks_FLUA_posrate.tsv", "heatmap", "heatmap"),
    ("demography/matrix_agegroups_weeks_FLUB_posrate.tsv", "heatmap", "heatmap"),
    ("demography/matrix_agegroups_weeks_VSR_posrate.tsv", "heatmap", "heatmap")
]

## Copy the files
for f, dest_p_sub, dest_f_sub in files:
    src = os.path.join(src_dir, f)
    dest_p = os.path.join(dest_dir_p, dest_p_sub)
    dest_f = os.path.join(dest_dir_f, dest_f_sub)

    if os.path.exists(src):
        shutil.copy(src, dest_p)
        shutil.copy(src, dest_f)
    else:
        print(f"File {src} does not exist")
