# Respiratory analysis
# Wildcards setting
LOCATIONS = [
	"country",
	"region",
	"state"]
	#, "locations"]

SAMPLES = [
	"FLUA",
	"FLUB", 
	"VSR", 
	"SC2", 
	'META', 
	'RINO', 
	'PARA', 
	'ADENO', 
	'COVS', 
	'ENTERO', 
	'BOCA', 
	'BAC']


rule all:
	message:
		"""
		Execute all rules at terminal commands in a logical order.
		"""

	shell:
		"""
		snakemake --cores all test_results_go
		snakemake --cores all combine_posneg_go
		snakemake --cores all total_tests_go
		snakemake --cores all posrate_go
		snakemake --cores all posneg_allpat
		
		snakemake --cores all demog_go
		snakemake --cores all combine_demog
		snakemake --cores all ttpd
		snakemake --cores all demogposrate_go

		snakemake --cores all copy_files
		"""

		
rule arguments:
	message:
		"""
		Define parameters for the pipeline including the structure of the folders, files and names.
		"""

	params:
		datadir = "data",
		rename_file = "data/rename_columns.xlsx",
		correction_file = "data/fix_values.xlsx",
		cache = "data/combined0.tsv",  ## first time data/combined0.tsv
		shapefile = "config/ibge_2020_shp/bra_admbnda_adm2_ibge_2020.shp",
		coordinates = "config/cache_coordinates.tsv",
		age_groups = "config/demo_bins.txt",
		date_column = "date_testing",

		geography = "config/tabela_municipio_macsaud_estado_regiao.tsv",
		population = "config/municipio_faixasetarias_ibgeTCU.tsv",
		index_column = "division_exposure",

		start_date = "2021-11-01",
		end_date = "2023-07-29" ## update last epidemiological week here

arguments = rules.arguments.params

rule files:
	message:
		"""
		Define the names for output files generated and to be used along the pipeline.
		"""
	input:
		expand(["results/{geo}/matrix_{sample}_{geo}_posneg.tsv", "results/{geo}/combined_matrix_{geo}_posneg.tsv", "results/{geo}/combined_matrix_{geo}_posneg_weeks.tsv", "results/{geo}/combined_matrix_{geo}_posneg.tsv", "results/{geo}/combined_matrix_{geo}_totaltests.tsv", "results/{geo}/combined_matrix_{geo}_posrate.tsv", "results/demography/matrix_{sample}_agegroups.tsv"], sample=SAMPLES, geo=LOCATIONS),
		combined1 = "results/combined1_labs.tsv", #labs
		combined2 = "results/combined2_age.tsv", #age
		combined = "results/combined.tsv", #geocols

		merged = "results/demography/combined_matrix_agegroup.tsv",
		#caserate = "results/demography/combined_matrix_agegroup_100k.tsv",
		week_matrix = "results/demography/matrix_agegroups_posneg_weeks.tsv",
		allpat_matrix = "results/country/combined_matrix_country_posneg_allpat_weeks.tsv",

rule reformat_hlagyn:
	message:
		"""
		Given the input data from the HLAGyn laboratory, we combine multiple files, create unique hashes for each row, manipulate and clean column variables, apply 'if' statements for specific cases, incorporate age and sex information, eliminate duplicates, and finally, save the output in a relational structure.
		"""
	input:
		rename = arguments.rename_file,
		correction = arguments.correction_file,
		cache = arguments.cache 
	params:
		datadir = arguments.datadir
	output:
		matrix = "results/combined_hlagyn.tsv",
	shell:
		"""
		python scripts/reformat_hlagyn.py \
			--datadir {params.datadir} \
			--rename {input.rename} \
			--correction {input.correction} \
			--cache {input.cache} \
			--output {output.matrix}
		"""

rule reformat_dasa:
	message:
		"""
		Given the input data from the DASA laboratory, we combine multiple files, create unique hashes for each row, manipulate and clean column variables, apply 'if' statements for specific cases, incorporate age and sex information, eliminate duplicates, and finally, save the output in a relational structure.
		"""
	input:
		rename = arguments.rename_file,
		correction = arguments.correction_file,
		cache = rules.reformat_hlagyn.output.matrix # previous lab
	params:
		datadir = arguments.datadir
	output:
		matrix = "results/combined_dasa.tsv",
	shell:
		"""
		python scripts/reformat_dasa.py \
			--datadir {params.datadir} \
			--rename {input.rename} \
			--correction {input.correction} \
			--cache {input.cache} \
			--output {output.matrix}
		"""


rule reformat_db:
	message:
		"""
		Given the input data from the DB Molecular laboratory, we combine multiple files, create unique hashes for each row, manipulate and clean column variables, apply 'if' statements for specific cases, incorporate age and sex information, eliminate duplicates, and finally, save the output in a relational structure.
		"""
	input:
		rename = arguments.rename_file,
		correction = arguments.correction_file,
		cache = rules.reformat_dasa.output.matrix # previous lab
	params:
		datadir = arguments.datadir
	output:
		matrix = "results/combined_db.tsv", #rules.files.input.combined1,
	shell:
		"""
		python scripts/reformat_db.py \
			--datadir {params.datadir} \
			--rename {input.rename} \
			--correction {input.correction} \
			--cache {input.cache} \
			--output {output.matrix}
		"""


rule reformat_sabin:
	message:
		"""
		Given the input data from the SABIN laboratory, we combine multiple files, create unique hashes for each row, manipulate and clean column variables, apply 'if' statements for specific cases, incorporate age and sex information, eliminate duplicates, and finally, save the output in a relational structure.
		"""
	input:
		rename = arguments.rename_file,
		correction = arguments.correction_file,
		cache = rules.reformat_db.output.matrix
	params:
		datadir = arguments.datadir
	output:
		matrix = "results/combined_sabin.tsv", #rules.files.input.combined1,
	shell:
		"""
		python scripts/reformat_sabin.py \
			--datadir {params.datadir} \
			--rename {input.rename} \
			--correction {input.correction} \
			--cache {input.cache} \
			--output {output.matrix}
		"""


rule reformat_fleury:
	message:
		"""
		Given the input data from the Fleury laboratory, we combine multiple files, create unique hashes for each row, manipulate and clean column variables, apply 'if' statements for specific cases, incorporate age and sex information, eliminate duplicates, and finally, save the output in a relational structure.
		"""
	input:
		rename = arguments.rename_file,
		correction = arguments.correction_file,
		cache = rules.reformat_sabin.output.matrix # last lab
	params:
		datadir = arguments.datadir
	output:
		matrix = "results/combined_fleury.tsv", #rules.files.input.combined1,
	shell:
		"""
		python scripts/reformat_fleury.py \
			--datadir {params.datadir} \
			--rename {input.rename} \
			--correction {input.correction} \
			--cache {input.cache} \
			--output {output.matrix}
		"""


rule reformat_einstein:
	message:
		"""
		Given the input data from the Hospital Israelita Albert Einstein, we combine multiple files, create unique hashes for each row, manipulate and clean column variables, apply 'if' statements for specific cases, incorporate age and sex information, eliminate duplicates, and finally, save the output in a relational structure.
		"""
	input:
		rename = arguments.rename_file,
		correction = arguments.correction_file,
		cache = rules.reformat_fleury.output.matrix # last lab
	params:
		datadir = arguments.datadir
	output:
		matrix = rules.files.input.combined1, # new combined1_lab or temp("results/combined_eintein.tsv")
	shell:
		"""
		python scripts/reformat_einstein.py \
			--datadir {params.datadir} \
			--rename {input.rename} \
			--correction {input.correction} \
			--cache {input.cache} \
			--output {output.matrix}
		"""

# rule reformat_hilab:
# 	message:
# 		"""
# 		Given the input data from the Hilab laboratory, we combine multiple files, create unique hashes for each row, manipulate and clean column variables, apply 'if' statements for specific cases, incorporate age and sex information, eliminate duplicates, and finally, save the output in a relational structure.
# 		"""
# 	input:
# 		rename = arguments.rename_file,
# 		correction = arguments.correction_file,
# 		cache = rules.reformat_einstein.output.matrix # last lab
# 	params:
# 		datadir = arguments.datadir
# 	output:
# 		matrix = rules.files.input.combined1, # new combined1_lab or temp("results/combined_hilab.tsv")
# 	shell:
# 		"""
# 		python scripts/reformat_einstein.py \
# 			--datadir {params.datadir} \
# 			--rename {input.rename} \
# 			--correction {input.correction} \
# 			--cache {input.cache} \
# 			--output {output.matrix}
# 		"""

rule agegroups:
	message:
		"""
		Use 'combined.tsv', the resultant metadata file after all laboratory reformats, as an input. Then, we employ the 'groupbyrange' script to introduce a new column for age groups. This script uses the age range definitions provided in the 'config/demo_bins.txt' file. The parameters set within this script allow us to define the new column name and thresholds. The output is a new, combined file that includes the added age groups.
		"""
	input:
		metadata =  rules.files.input.combined1,
		bins = arguments.age_groups,
	params:
		column = "age",
		group = "age_group",
		lowest = "0",
		highest = "200", #Noé
	output:
		matrix = rules.files.input.combined2, # new combined2_age
	shell:
		"""
		python scripts/groupbyrange.py \
			--input {input.metadata} \
			--column {params.column} \
			--bins {input.bins} \
			--group {params.group} \
			--lowest {params.lowest} \
			--highest {params.highest} \
			--output {output.matrix}
		"""

# rule geomatch:
# 	message:
# 		"""
# 		Using as input file Match location names with geographic shapefile polygons
# 		"""
# 	input:
# 		input_file = rules.agegroups.output.matrix,
# 		coordinates = arguments.coordinates,
# 		shapefile = arguments.shapefile,
# 	params:
# 		geo_columns = "state, location",
# 		add_geo = "country:Brazil",
# 		lat = "lat",
# 		long = "long",
# 		check_match = "ADM2_PT",
# 		target = "state, state_code, ADM2_PT, ADM2_PCODE",
# 	output:
# 		matrix = "results/combined_testdata3.tsv"
# 	shell:
# 		"""
# 		python scripts/name2shape.py \
# 			--input {input.input_file} \
# 			--shapefile \"{input.shapefile}\" \
# 			--geo-columns \"{params.geo_columns}\" \
# 			--add-geo {params.add_geo} \
# 			--lat {params.lat} \
# 			--long {params.long} \
# 			--cache {input.coordinates} \
# 			--check-match {params.check_match} \
# 			--target \"{params.target}\" \
# 			--output {output.matrix}
# 		"""


rule geocols:
	message:
		"""
		Using the output matrix from the 'agegroups' rule, we augment it by incorporating additional geographic columns. These columns are specified in the 'arguments.geography' input. Our focus targets are 'country', 'region', and 'state_code', located respectively at the 5th, 6th, and 8th positions. We introduce these new elements into the 'state' index column. To perform this 'add' operation, we employ the 'reformat_dataframe.py' script, indicating 'columns' as the operational mode. Consequently, we generate an updated combined file, which is subsequently stored as a refreshed 'combined_cache', primed for the next stage of processing.
		"""
	input:
		file = rules.agegroups.output.matrix,
		newcols = arguments.geography,
	params:
		target = "country#5, region#6, state_code#8",
		index = "state",
		action = "add",
		mode = "columns"
	output:
		matrix = rules.files.input.combined, # new combined_cache
	shell:
		"""
		python scripts/reformat_dataframe.py \
			--input1 {input.file} \
			--input2 {input.newcols} \
			--index {params.index} \
			--action {params.action} \
			--mode {params.mode} \
			--targets "{params.target}" \
			--output {output.matrix}
		"""


rule test_results_go:
	message:
		"""
		Processes matrices of test results, catering to a variety of samples and geographic locations. To fine-tune this processing in accordance with specific samples and locations, we utilize the 'set_index_results' function. This function carefully configures critical parameters such as target variables, indexes, supplemental columns, a suite of filters, and additional column information. These parameters, working in synergy, facilitate the generation and customization of distinct test result matrices, thereby streamlining our analysis.
		"""
	input:
		expand("results/{geo}/matrix_{sample}_{geo}_posneg_panel.tsv", sample=SAMPLES, geo=LOCATIONS),

index_results = {
"country": ["country lab_id test_kit", "\'\'"], #0 #1
"region": ["region lab_id test_kit", "country"], #0 #1
"state": ["state lab_id test_kit", "state_code country"] #0 #1
}

def set_index_results(spl, loc):
	yvar = tests[spl] + ' ' + index_results[loc][0]
	index = loc #index_results[loc][0] change
	index2 = loc + " pathogen test_result"
	extra_cols = index_results[loc][1]
	filter1 = "~" + tests[spl] + ":NT"
	filter2 = "test_kit:test_4, test_kit:test_21, test_kit:test_24"
	filter3 = "~test_kit:covid_antigen, ~test_kit:flu_antigen, ~test_kit:vsr_antigen"
	add_col = "pathogen:" + spl
	test_col = tests[spl]
	return([yvar, index, extra_cols, filter1, filter2, filter3, add_col, test_col, index2])

rule test_results:
	message:
		"""
		Takes respiratory pathogen data and transforms it into multiple, detailed test results matrices, according to the sample type and geographical location. This is achieved by leveraging the parameters defined by the 'test_results_go' rule, effectively tailoring the data handling process. 
		The operations encompass sorting and filtering the data, adding extra columns and specifying the date range, which results in matrices of all tests, total positive and negative results, panel tests, and nucleic acid tests. The Python scripts 'rows2matrix.py' and 'collapser.py' handle the data transformation, while the 'sed' command adjusts the column naming to fit the analytical requirements.
		"""
	message:
		"""
		Compile data of respiratory pathogens
		"""
	input:
		input_file = rules.geocols.output.matrix
	params:
		xvar = arguments.date_column,
		xtype = "time",
		format = "integer",
		
		yvar = lambda wildcards: set_index_results(wildcards.sample, wildcards.geo)[0],
		index = lambda wildcards: set_index_results(wildcards.sample, wildcards.geo)[1],
		extra_columns = lambda wildcards: set_index_results(wildcards.sample, wildcards.geo)[2],
		filter1 = lambda wildcards: set_index_results(wildcards.sample, wildcards.geo)[3],
		filter2 = lambda wildcards: set_index_results(wildcards.sample, wildcards.geo)[4],
		filter3 = lambda wildcards: set_index_results(wildcards.sample, wildcards.geo)[5],
		id_col = lambda wildcards: set_index_results(wildcards.sample, wildcards.geo)[6],
		test_col = lambda wildcards: set_index_results(wildcards.sample, wildcards.geo)[7],
		index2 = lambda wildcards: set_index_results(wildcards.sample, wildcards.geo)[8],
		
		sortby = "lab_id test_kit",
		ignore = "test_kit lab_id",
		sortby2 = "pathogen",

		start_date = arguments.start_date,
		end_date = arguments.end_date,

	output:
		posneg_labtests = "results/{geo}/matrix_{sample}_{geo}_posneg_labtests.tsv", # todos os testes
		posneg_full = "results/{geo}/matrix_{sample}_{geo}_posneg_full.tsv", # positivos e negativos totais
		posneg_panel = "results/{geo}/matrix_{sample}_{geo}_posneg_panel.tsv", # apenas paineis
		posneg_nucleic = "results/{geo}/matrix_{sample}_{geo}_posneg_nucleic.tsv", # apenas testes de ácido nucleico
	shell:
		"""
		python scripts/rows2matrix.py \
			--input {input.input_file} \
			--xvar {params.xvar} \
			--xtype {params.xtype} \
			--format {params.format} \
			--yvar {params.yvar} \
			--unique-id {params.index} \
			--extra-columns {params.extra_columns} \
			--new-columns "{params.id_col}" \
			--filters \"{params.filter1}\" \
			--start-date {params.start_date} \
			--end-date {params.end_date} \
			--sortby {params.sortby} \
			--output {output.posneg_labtests}
		
		sed -i '' 's/{params.test_col}/test_result/' {output.posneg_labtests}

		python scripts/collapser.py \
			--input {output.posneg_labtests} \
			--index {params.index2} \
			--unique-id {params.index} \
			--extra-columns {params.extra_columns} \
			--ignore {params.ignore} \
			--sortby {params.sortby2} \
			--output {output.posneg_full}

		python scripts/collapser.py \
			--input {output.posneg_labtests} \
			--index {params.index2} \
			--unique-id {params.index} \
			--extra-columns {params.extra_columns} \
			--filters \"{params.filter2}\" \
			--ignore {params.ignore} \
			--sortby {params.sortby2} \
			--output {output.posneg_panel}

		python scripts/collapser.py \
			--input {output.posneg_labtests} \
			--index {params.index2} \
			--unique-id {params.index} \
			--extra-columns {params.extra_columns} \
			--filters \"{params.filter3}\" \
			--ignore {params.ignore} \
			--sortby {params.sortby2} \
			--output {output.posneg_nucleic}
		"""

		# Linux
		#sed -i 's/{params.unique_id}/test_result/' {output.age_matrix}


rule combine_posneg_go:
	message:
		"""
		The combine_posneg_go rule triggers output names for the combine_posneg rule which aggregates and merges all full, panel, and nucleic tests results data for each geographic location, and summarizes it on a weekly basis.
		"""
	input:
		expand("results/{geo}", geo=LOCATIONS),
		expand("results/{geo}/combined_matrix_{geo}_posneg_full.tsv", geo=LOCATIONS),
		expand("results/{geo}/combined_matrix_{geo}_posneg_full_weeks.tsv", geo=LOCATIONS),

rule combine_posneg:
	message:
		"""
		Streamline and aggregate various test result datasets on a weekly basis. Utilizing the multi_merger.py script, diverse test results are harmonized into single files per location, with empty entries replaced by zeroes. These files are organized by geographic location, pathogen, and test result. The aggregator.py script further processes these datasets, providing a time-aggregated summary on a weekly basis. The end product is a set of six distinct weekly files for each geographic location, encompassing full, panel, and nucleic test results. The time frame for aggregation can be adjusted from weekly to monthly, as required, by changing the unit parameter.
		"""
	params:
		path = "results/{geo}",
		regex1 = "*_full.tsv",
		regex2 = "*_panel.tsv",
		regex3 = "*_nucleic.tsv",
		filler = "0",
		unit = "week", # change here for MONTH
		format = "integer",
		sortby = "{geo} pathogen test_result",
	output:
		merged_full = "results/{geo}/combined_matrix_{geo}_posneg_full.tsv",
		merged_full_weeks = "results/{geo}/combined_matrix_{geo}_posneg_full_weeks.tsv",
		merged_panel = "results/{geo}/combined_matrix_{geo}_posneg_panel.tsv",
		merged_panel_weeks = "results/{geo}/combined_matrix_{geo}_posneg_panel_weeks.tsv",
		merged_nucleic = "results/{geo}/combined_matrix_{geo}_posneg_nucleic.tsv",
		merged_nucleic_weeks = "results/{geo}/combined_matrix_{geo}_posneg_nucleic_weeks.tsv",
	shell:
		"""
		python scripts/multi_merger.py \
			--path {params.path} \
			--regex \"{params.regex1}\" \
			--fillna {params.filler} \
			--sortby {params.sortby} \
			--output {output.merged_full}
		
		python scripts/aggregator.py \
			--input {output.merged_full} \
			--unit {params.unit} \
			--format {params.format} \
			--output {output.merged_full_weeks}
		
		python scripts/multi_merger.py \
			--path {params.path} \
			--regex \"{params.regex2}\" \
			--fillna {params.filler} \
			--sortby {params.sortby} \
			--output {output.merged_panel}
		
		python scripts/aggregator.py \
			--input {output.merged_panel} \
			--unit {params.unit} \
			--format {params.format} \
			--output {output.merged_panel_weeks}

		python scripts/multi_merger.py \
			--path {params.path} \
			--regex \"{params.regex3}\" \
			--fillna {params.filler} \
			--sortby {params.sortby} \
			--output {output.merged_nucleic}
		
		python scripts/aggregator.py \
			--input {output.merged_nucleic} \
			--unit {params.unit} \
			--format {params.format} \
			--output {output.merged_nucleic_weeks}
		"""
# 		cp results/country/combined_matrix_country_posneg.tsv figures/barplot


rule posneg_allpat:
	message:
		"""
		Combines counts of positive and negative tests across all pathogens. Using the collapser.py script, it processes the weekly aggregated full test results data on a country level. It excludes specific pathogen details during this operation, focusing instead on the test results and the corresponding country data. The final output, allpat_matrix, represents a consolidated view of total testing activities and their outcomes.
		"""
	input:
		input = "results/country/combined_matrix_country_posneg_full_weeks.tsv"
	params:
		index = "test_result",
		extracol = "country",
		ignore = "pathogen",
		format = "integer",
	output:
		allpat_matrix = rules.files.input.allpat_matrix,
	shell:
		"""
		python scripts/collapser.py \
			--input {input.input} \
			--index {params.index} \
			--unique-id {params.index} \
			--extra-columns {params.extracol} \
			--ignore {params.ignore} \
			--format {params.format} \
			--output {output.allpat_matrix}
		"""


rule total_tests_go:
	message:
		"""
		Create logical groups of output files for total_test rule, the index_totals dictionary defines the data handling setup for different geographical levels - 'country', 'region', and 'state'. The index_totals dictionary defines the data handling setup for different geographical levels - 'country', 'region', and 'state'. The set_index_totals(loc) function derives a distinct set of parameters—index, unique_id, and extra_cols—from the index_totals dictionary for each geographic location. These parameters are essential for indexing and uniquely identifying data, as well as for preserving necessary additional data columns.
		"""
	input:
		expand("results/{geo}/combined_matrix_{geo}_posneg_full_weeks.tsv", geo=LOCATIONS),
		expand("results/{geo}/combined_matrix_{geo}_totaltests_full_weeks.tsv", geo=LOCATIONS),

index_totals = {
"country": ["pathogen country", "country", "\'\'"],
"region": ["pathogen region", "region", "country"],
"state": ["pathogen state", "state", "state_code country"]
}

def set_index_totals(loc):
	index = index_totals[loc][0]
	unique_id = index_totals[loc][1]
	extra_cols = index_totals[loc][2]
	return([index, unique_id, extra_cols])


rule total_tests:
	message:
		"""
		Calculates the total tests performed per pathogen for each geographical level. Using location-specific parameters extracted by the 'set_index_totals' function, it processes both 'nucleic' and 'full' test data weekly. The function filters out non-test results, and the time unit for aggregation can be adjusted if needed. The rule generates two sets of output: the total number of 'nucleic' and 'full' tests for each location, contributing to the larger data analysis pipeline.
		"""
	input:
		file1 = "results/{geo}/combined_matrix_{geo}_posneg_nucleic_weeks.tsv",
		file2 = "results/{geo}/combined_matrix_{geo}_posneg_full_weeks.tsv",
	params:
		format = "integer",
		index = lambda wildcards: set_index_totals(wildcards.geo)[0],
		unique_id = lambda wildcards: set_index_totals(wildcards.geo)[1],
		extra_columns = lambda wildcards: set_index_totals(wildcards.geo)[2],
		filters = "~test_result:NT",
		unit = "week", #change for month
		ignore = "test_result" #keep one outpute of results
	output:
		output1 = "results/{geo}/combined_matrix_{geo}_totaltests_nucleic_weeks.tsv",
		output2 = "results/{geo}/combined_matrix_{geo}_totaltests_full_weeks.tsv",
	shell:
		"""
		python scripts/collapser.py \
			--input {input.file1} \
			--index {params.index} \
			--unique-id {params.unique_id} \
			--extra-columns {params.extra_columns} \
			--ignore {params.ignore} \
			--format {params.format} \
			--filter \"{params.filters}\" \
			--output {output.output1}

		python scripts/collapser.py \
			--input {input.file2} \
			--index {params.index} \
			--unique-id {params.unique_id} \
			--extra-columns {params.extra_columns} \
			--ignore {params.ignore} \
			--format {params.format} \
			--filter \"{params.filters}\" \
			--output {output.output2}
		"""


rule posrate_go:
	message: 
		"""
		Generates required file paths for the computation of positive test rates. It leverages the expand function to produce a series of files, each tailored to a specific geographic location. Employs a dictionary, indexes, which pairs geographic areas to their corresponding identifiers.
		"""
	input:
		expand("results/{geo}/combined_matrix_{geo}_posrate_full_weeks.tsv", geo=LOCATIONS)

indexes = {
"country": ["pathogen country"],
"region": ["pathogen region"],
"state": ["pathogen state"]
}

def getIndex(loc):
	id = indexes[loc][0]
	return(id)

rule posrate:
	message:
		"""
		Calculates test positivity rates from 'combine_posneg' (numerator) and 'total_tests' (denominator), applying distinct indices and filters. The Python scripts use parameters such as data index, positive-result filter, and a minimum threshold. 'matrix_operations.py' then processes the test data, delivering weekly positivity rates per test type.
		"""
	input:
		file1 = rules.combine_posneg.output.merged_nucleic_weeks, # numerator, nucleic only
		file2 = rules.total_tests.output.output1, # denominator, nucleic only
		
		file3 = rules.combine_posneg.output.merged_full_weeks, # numerator, all test types
		file4 = rules.total_tests.output.output2, # denominator, all test types
	params:
		index1 = lambda wildcards: getIndex(wildcards.geo),
		index2 = lambda wildcards: getIndex(wildcards.geo),
		filter = 'test_result:Pos',
		min_denominator = 50
	output:
		output1 = "results/{geo}/combined_matrix_{geo}_posrate_nucleic_weeks.tsv",
		output2 = "results/{geo}/combined_matrix_{geo}_posrate_full_weeks.tsv"
	shell:
		"""
		python scripts/matrix_operations.py \
			--input1 {input.file1} \
			--input2 {input.file2} \
			--index1 {params.index1} \
			--index2 {params.index2} \
			--min-denominator {params.min_denominator} \
			--filter1 {params.filter} \
			--output {output.output1}

		python scripts/matrix_operations.py \
			--input1 {input.file3} \
			--input2 {input.file4} \
			--index1 {params.index1} \
			--index2 {params.index2} \
			--min-denominator {params.min_denominator} \
			--filter1 {params.filter} \
			--output {output.output2}
		"""


rule demog_go:
	message:
		"""
		Generates file paths for demographic analysis over different sample types, facilitated by 'expand'. It utilizes a 'tests' dictionary mapping pathogens to their test result identifiers. The 'set_groups' function enhances this by setting a dependent variable ('yvar'), an identifier column ('id_col'), specific conditions ('filter'), and additional columns ('add_col') — all custom-defined per pathogen, allowing precise demographic analysis.
		"""
	input:
		expand("results/demography/matrix_{sample}_agegroups.tsv", sample=SAMPLES),

tests = {
	"SC2": "SC2_test_result",
	"FLUA": "FLUA_test_result",
	"FLUB": "FLUB_test_result",
	"VSR": "VSR_test_result",
	"META": "META_test_result",
	"RINO": "RINO_test_result",
	"PARA": "PARA_test_result",
	"ADENO": "ADENO_test_result",
	"BOCA": "BOCA_test_result",
	"COVS": "COVS_test_result",
	"SC2": "SC2_test_result",
	"ENTERO": "ENTERO_test_result",
	"BAC": "BAC_test_result"
}

def set_groups(spl):
	yvar = tests[spl] + ' epiweek' #[0]
	id_col = tests[spl] #[1]
	filter = "sex:F, sex:M, test_kit:test_4, test_kit:test_21, test_kit:test_24, ~test_kit:'', ~age_group:''" #[2] gráfico de pirâmide somente kit

	add_col = "pathogen:" + spl + ", name:Brasil" #[3] 
	return([yvar, id_col, filter, add_col])

rule demog:
	message:
		"""
		Compiles age and gender demographic data for each sample type, using 'set_groups' to assign test-specific parameters. The 'xvar' identifies 'age_group' as the independent variable; 'format' sets the data type as integer; 'start_date' and 'end_date' define the data range. These parameters, used in 'rows2matrix.py', shape the metadata into a matrix, further refined to the given date range. The output: a tailored demographic matrix with the unique identifier renamed to 'test_result'.
		"""
	input:
		metadata = rules.geocols.output.matrix,
		bins = arguments.age_groups,
	params:
		xvar = "age_group",
		format = "integer",
		yvar = lambda wildcards: set_groups(wildcards.sample)[0],
		unique_id = lambda wildcards: set_groups(wildcards.sample)[1],
		filters = lambda wildcards: set_groups(wildcards.sample)[2],
		id_col = lambda wildcards: set_groups(wildcards.sample)[3],
		start_date = arguments.start_date,
		end_date = arguments.end_date,
	output:
		age_matrix = "results/demography/matrix_{sample}_agegroups.tsv",
	shell:
		"""
		python scripts/rows2matrix.py \
			--input {input.metadata} \
			--xvar {params.xvar} \
			--format {params.format} \
			--yvar {params.yvar} \
			--new-columns \"{params.id_col}\" \
			--filter \"{params.filters}\" \
			--unique-id {params.unique_id} \
			--start-date {params.start_date} \
			--end-date {params.end_date} \
			--output {output.age_matrix}
			
		sed -i '' 's/{params.unique_id}/test_result/' {output.age_matrix}
		"""
		## Linux
		## sed -i 's/{params.unique_id}/test_result/' {output.age_matrix}


rule combine_demog:
	message:
		"""
		Combines demographic results across different pathogen samples. Parameters specify the data directory 'path_demog', a regex pattern to match required files, a filler value for missing data, a filter to exclude non-tested results, and a sorting preference by 'epiweek'. These parameters feed into the 'multi_merger.py' script, producing a combined demographic matrix that is then copied into the 'figures/pyramid' directory.
		"""
	params:
		path_demog = "results/demography",
		regex = "*_agegroups.tsv",
		filler = "0",
		filters = "~test_result:NT",
		sortby = "epiweek",
	output:
		merged = rules.files.input.merged,
	shell:
		"""
		python scripts/multi_merger.py \
			--path {params.path_demog} \
			--regex {params.regex} \
			--fillna {params.filler} \
			--filters {params.filters} \
			--sortby {params.sortby} \
			--output {output.merged} 

		cp results/demography/combined_matrix_agegroup.tsv figures/pyramid
		"""

rule ttpd: #total_tests_panel_demog
	message:
		"""
		Computes total tests for demographic flow panels. It takes in a combined age group matrix as input and sets parameters for index, unique ID, columns to ignore, sorting preferences, and a filter for four specific pathogens with positive results. These parameters are used in the 'collapser.py' script twice with varying indices, generating weekly and overall positivity results. The script 'matrix_operations.py' then calculates the frequency of positive cases, producing three output files—weekly positives, overall positives, and positivity frequency. This rule enables in-depth demographic analysis for a panel of pathogens.
		"""
	input:
		combi_ages = rules.combine_demog.output.merged, #combined_matrix_agegroup.tsv 
	params:
		## filter for pathogens of interest VSR SC2 FLUA FLUB
		## percentual of positivy among the panel tests
		index = "epiweek",
		unique_id = "epiweek", 
		ignore = "name pathogen test_result",
		index2 = "epiweek pathogen",
		ignore2 = "name test_result",
		## filter positives pathogens of interest VSR SC2 FLUA FLUB
		filter_testpanelpos = "test_result:Pos, pathogen:VSR, pathogen:SC2, pathogen:FLUA ,pathogen:FLUB",
		sortby = "epiweek",
		sortby2 = "epiweek pathogen",
		format = "integer",
	output:
		totaltestpanel_agegroups_posweek = "results/demography/combined_matrix_totaltestpanel_agegroups_posweek.tsv", #denominator
		totaltestpanel_agegroups_pos = "results/demography/combined_matrix_totaltestpanel_agegroups_pos.tsv", #numerator
		totaltestpanel_agegroups_freq = "results/demography/combined_matrix_totaltestpanel_agegroups_freq.tsv", #freq - adding --rate 100.000 output will be incidence 
	shell:
		"""
		python scripts/collapser.py \
			--input {input.combi_ages} \
			--index {params.index} \
			--unique-id {params.unique_id} \
			--ignore {params.ignore} \
			--filter \"{params.filter_testpanelpos}\" \
			--sortby {params.sortby} \
			--format {params.format} \
			--output {output.totaltestpanel_agegroups_posweek}

		python scripts/collapser.py \
			--input {input.combi_ages} \
			--index {params.index2} \
			--unique-id {params.unique_id} \
			--ignore {params.ignore2} \
			--filter \"{params.filter_testpanelpos}\" \
			--sortby {params.sortby2} \
			--format {params.format} \
			--output {output.totaltestpanel_agegroups_pos}
		
		python scripts/matrix_operations.py \
			--input1 {output.totaltestpanel_agegroups_pos} \
			--input2 {output.totaltestpanel_agegroups_posweek} \
			--index1 {params.index2} \
			--index2 {params.unique_id} \
			--output {output.totaltestpanel_agegroups_freq}
		"""


rule demogposrate_go:
	message:
		"""
		Generates required file paths for the computation of positive test rates. It leverages the expand function to produce a series of files, each tailored to a specific pathogens and age_groups. Employs a dictionary, indexes, which pairs test results by age and pathogen to their corresponding identifiers.
		"""
	input:
		expand("results/demography/matrix_agegroups_weeks_{sample}_posneg.tsv", sample=SAMPLES),

def set_groups2(spl):
	yvar = tests[spl] + ' age_group'
	filter1 = "~" + tests[spl] + ":NT"
	filter2 = "~" + tests[spl] + ":NT, ~" + tests[spl] + ":Neg"
	return([yvar, filter1, filter2]) #, filter_tests]) # add

rule posrate_agegroup:
	message:
		"""
		Calculates the positivity rate for all pathogens by age group. The 'params' section defines parameters such as 'xvar' for the independent variable, 'format' for the data format, 'unique_id' for the identifier column, and 'min_denominator' for the minimum limit. 'Rows2matrix.py' script transforms raw data into weekly matrices per pathogen. 'Collapser.py' collapses these matrices into total test results. Finally, 'matrix_operations.py' computes positivity rates, based on the parameters from 'set_groups2' function. Outputs include weekly matrices, total tests, and positivity rates per pathogen.
		"""
	input:
		metadata = rules.geocols.output.matrix,
	params:
		format = "integer",
		xvar = "epiweek",
		yvar = lambda wildcards: set_groups2(wildcards.sample)[0], # include other pathogens
		unique_id = "age_group",
		extra = "country",
		min_denominator = 50,
		filter1 = lambda wildcards: set_groups2(wildcards.sample)[1],
		filter2 = lambda wildcards: set_groups2(wildcards.sample)[2],
	output:
		week_matrix = "results/demography/matrix_agegroups_weeks_{sample}_posneg.tsv",
		alltests = "results/demography/matrix_agegroups_weeks_{sample}_alltests.tsv",
		posrate = "results/demography/matrix_agegroups_weeks_{sample}_posrate.tsv",
	shell:
		"""
		python scripts/rows2matrix.py \
			--input {input.metadata} \
			--xvar {params.xvar} \
			--format {params.format} \
			--yvar {params.yvar} \
			--filters \"{params.filter1}\" \
			--extra-columns {params.extra} \
			--unique-id {params.unique_id} \
			--output {output.week_matrix}

		python scripts/collapser.py \
			--input {output.week_matrix} \
			--index {params.unique_id} \
			--unique-id {params.unique_id} \
			--extra-columns {params.extra} \
			--output {output.alltests} \

		python scripts/matrix_operations.py \
			--input1 {output.week_matrix} \
			--input2 {output.alltests} \
			--index1 {params.yvar} \
			--index2 {params.unique_id} \
			--filter1 \"{params.filter2}\" \
			--min-denominator {params.min_denominator} \
			--output {output.posrate}
		"""


rule copy_files:
	message:
		"""
		Copy files for plotting
		"""
	shell:
		"""
		cp results/combined.tsv data/combined_cache.tsv

		cp results/demography/combined_matrix_agegroup.tsv figures/python/pyramid

		cp results/country/combined_matrix_country_posneg_full_weeks.tsv figures/python/barplot
		cp results/country/combined_matrix_country_posneg_panel_weeks.tsv figures/python/barplot
		cp results/country/combined_matrix_country_posneg_allpat_weeks.tsv figures/python/barplot

		cp results/country/combined_matrix_country_posrate_full_weeks.tsv figures/python/lineplot

		cp results/state/combined_matrix_state_posrate_full_weeks.tsv figures/python/heatmap
		cp results/demography/matrix_agegroups_weeks_SC2_posrate.tsv figures/python/heatmap
		cp results/demography/matrix_agegroups_weeks_FLUA_posrate.tsv figures/python/heatmap
		cp results/demography/matrix_agegroups_weeks_FLUB_posrate.tsv figures/python/heatmap
		cp results/demography/matrix_agegroups_weeks_VSR_posrate.tsv figures/python/heatmap

	"""
		# cp results/demography/combined_matrix_agegroup_100k.tsv figures/pyramid


#rule xxx:
#	message:
#		"""
#		
#		"""
#	input:
#		metadata = arguments.
#	params:
#		index = arguments.,
#		date = arguments.
#	output:
#		matrix = "results/"
#	shell:
#		"""
#		python scripts/ \
#			--metadata {input.} \
#			--index-column {params.} \
#			--extra-columns {params.} \
#			--date-column {params.} \
#			--output {output.}
#		"""


rule remove_figs:
	message: "Removing figures"
	shell:
		"""
		rm figures/*/*/matrix*
		rm figures/*/*/combined*
		rm figures/*/*/*.pdf
		"""


rule clean:
	message: "Removing directories: {params}"
	params:
		"results"
	shell:
		"""
		rm -rfv {params}
		"""

