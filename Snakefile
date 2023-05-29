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
	shell:
		"""
		snakemake --cores all test_results_go
		snakemake --cores all combine_posneg_go
		snakemake --cores all total_tests_go
		snakemake --cores all posrate_go
		
		snakemake --cores all demog_go
		snakemake --cores all combine_demog

		snakemake --cores all ttpd
		snakemake --cores all demogposrate_go

		snakemake --cores all posneg_allpat
		snakemake --cores all copy_files
		"""

		
rule arguments:
	params:
		datadir = "data",
		rename_file = "data/rename_columns.xlsx",
		correction_file = "data/fix_values.xlsx",
		cache = "data/combined0.tsv", #"data/combined_fleury_all.tsv", #first time data/combined0.tsv
		shapefile = "config/ibge_2020_shp/bra_admbnda_adm2_ibge_2020.shp",
		coordinates = "config/cache_coordinates.tsv",
		age_groups = "config/demo_bins.txt",
		date_column = "date_testing",

		geography = "config/tabela_municipio_macsaud_estado_regiao.tsv",
		population = "config/municipio_faixasetarias_ibgeTCU.tsv",
		index_column = "division_exposure",

		start_date = "2022-01-02",
		end_date = "2023-05-27" #atualizar data aqui

arguments = rules.arguments.params

rule files:
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
		Combine data from HLAGyn
		"""
	input:
		rename = arguments.rename_file,
		correction = arguments.correction_file,
		cache = arguments.cache 
	params:
		datadir = arguments.datadir
	output:
		matrix = temp("results/combined_hla.tsv"),
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
		Combine data from Dasa
		"""
	input:
		rename = arguments.rename_file,
		correction = arguments.correction_file,
		cache = rules.reformat_hlagyn.output.matrix # previous lab
	params:
		datadir = arguments.datadir
	output:
		matrix = temp("results/combined_dasa.tsv"),
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
		Combine data from DB Molecular
		"""
	input:
		rename = arguments.rename_file,
		correction = arguments.correction_file,
		cache = rules.reformat_dasa.output.matrix # previous lab
	params:
		datadir = arguments.datadir
	output:
		matrix = temp("results/combined_db.tsv"), #rules.files.input.combined1,
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
		Combine data from SABIN
		"""
	input:
		rename = arguments.rename_file,
		correction = arguments.correction_file,
		cache = rules.reformat_db.output.matrix
	params:
		datadir = arguments.datadir
	output:
		matrix = temp("results/combined_sabin.tsv"), #rules.files.input.combined1,
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
		Combine data from Fleury
		"""
	input:
		rename = arguments.rename_file,
		correction = arguments.correction_file,
		cache = rules.reformat_sabin.output.matrix # last lab
	params:
		datadir = arguments.datadir
	output:
		matrix = rules.files.input.combined1, # new combined1_lab or temp("results/combined_fleury.tsv")
	shell:
		"""
		python scripts/reformat_fleury.py \
			--datadir {params.datadir} \
			--rename {input.rename} \
			--correction {input.correction} \
			--cache {input.cache} \
			--output {output.matrix}
		"""


rule agegroups:
	message:
		"""
		Add column with age groups
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
# 		Match location names with geographic shapefile polygons
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
		Add extra geographic columns
		"""
	input:
		file = rules.agegroups.output.matrix,
		newcols = arguments.geography,
	params:
		target = "country#5, region#6, state_code#8", # reorder rename_columns
		index = "state", # config/tabela_municipio_macsaud_estado_regiao 
		action = "add",
		mode = "columns"
	output:
		matrix = rules.files.input.combined, # new combined_cache
	priority: 10
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

rule demog_go:
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
	filter = "sex:F, sex:M, ~age_group:'', test_kit:test_4, test_kit:test_21, test_kit:test_24, ~test_kit:''" #[2] gráfico de pirâmide somente kit

	add_col = "pathogen:" + spl + ", name:Brasil" #[3] 
	return([yvar, id_col, filter, add_col])

rule demog:
	message:
		"""
		Aggregate ages per age group and sex
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
			
		echo {params.unique_id}
		sed -i '' 's/{params.unique_id}/test_result/' {output.age_matrix}
		"""
		# Linux
		#sed -i 's/{params.unique_id}/test_result/' {output.age_matrix}



rule demogposrate_go:
	input:
		expand("results/demography/matrix_agegroups_weeks_{sample}_posneg.tsv", sample=SAMPLES),
		expand("results/demography/matrix_agegroups_weeks_{sample}_alltests.tsv", sample=SAMPLES),
		expand("results/demography/matrix_agegroups_weeks_{sample}_posrate.tsv", sample=SAMPLES),


def set_groups2(spl):
	yvar = tests[spl] + ' age_group'
	filters = "~" + tests[spl] + ":NT" # filter specific test ->  + ", test_kit:test_4"
	#filter_tests = '~testkit:covid, ~testkit:covidantigen, ~testkit:thermo" 
	#return([yvar])
	return([yvar, filters]) #, filter_tests]) # add


rule posrate_agegroup:
	message:
		"""
		Positive rate for all pathogens, by age group
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
		filter = lambda wildcards: set_groups2(wildcards.sample)[1],
	output:
		week_matrix = "results/demography/matrix_agegroups_weeks_{sample}_posneg.tsv", #rules.files.input.week_matrix,
		alltests = "results/demography/matrix_agegroups_weeks_{sample}_alltests.tsv", #rules.files.input.allcovid,
		posrate = "results/demography/matrix_agegroups_weeks_{sample}_posrate.tsv", #rules.files.input.posrate,
	shell: #old normdata script 3
		"""
		python scripts/rows2matrix.py \
			--input {input.metadata} \
			--xvar {params.xvar} \
			--format {params.format} \
			--yvar {params.yvar} \
			--extra-columns {params.extra} \
			--unique-id {params.unique_id} \
			--output {output.week_matrix}

		python scripts/collapser.py \
			--input {output.week_matrix} \
			--index {params.unique_id} \
			--unique-id {params.unique_id} \
			--extra-columns {params.extra} \
			--filter \"{params.filter}\" \
			--output {output.alltests} \

		python scripts/matrix_operations.py \
			--input1 {output.week_matrix} \
			--input2 {output.alltests} \
			--index1 {params.yvar} \
			--index2 {params.unique_id} \
			--min-denominator {params.min_denominator} \
			--output {output.posrate}
		"""



rule combine_demog:
	message:
		"""
		Combine demographic results
		"""
	input:
		population = arguments.population
	params:
		path_demog = "results/demography",
		regex = "*_agegroups.tsv",
		filler = "0",
		sortby = "epiweek",
		
		index1 = "name pathogen test_result epiweek",
		index2 = "name",
		rate = "100000",
		filter = "test_result:Pos",
	output:
		merged = rules.files.input.merged,
		#caserate = rules.files.input.caserate,
	shell: #old normdata script 2
		"""
		python scripts/multi_merger.py \
			--path {params.path_demog} \
			--regex {params.regex} \
			--fillna {params.filler} \
			--sortby {params.sortby} \
			--output {output.merged} 

		
		cp results/demography/combined_matrix_agegroup.tsv figures/pyramid
		"""


rule ttpd: #total_tests_panel_demog
	message:
		"""
		Total tests for panels demog flow
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
		filter_testpanelpos =  "test_result:Pos, pathogen:VSR, pathogen:SC2, pathogen:FLUA ,pathogen:FLUB",
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



rule test_results_go:
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
	add_col = "pathogen:" + spl
	test_col = tests[spl]
	return([yvar, index, extra_cols, filter1, filter2, add_col, test_col, index2])

rule test_results:
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
		id_col = lambda wildcards: set_index_results(wildcards.sample, wildcards.geo)[5],
		test_col = lambda wildcards: set_index_results(wildcards.sample, wildcards.geo)[6],
		index2 = lambda wildcards: set_index_results(wildcards.sample, wildcards.geo)[7],
		
		sortby = "lab_id test_kit",
		ignore = "test_kit lab_id",
		sortby2 = "pathogen",

		start_date = arguments.start_date,
		end_date = arguments.end_date,

	output:
		posneg_labtests = "results/{geo}/matrix_{sample}_{geo}_posneg_labtests.tsv", # todos os testes
		posneg_full = "results/{geo}/matrix_{sample}_{geo}_posneg_full.tsv", # positivos e negativos totais
		posneg_panel = "results/{geo}/matrix_{sample}_{geo}_posneg_panel.tsv", # apenas painel
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

		"""
		# Linux
		#sed -i 's/{params.unique_id}/test_result/' {output.age_matrix}



rule combine_posneg_go:
	input:
		expand("results/{geo}", geo=LOCATIONS),
		expand("results/{geo}/combined_matrix_{geo}_posneg.tsv", geo=LOCATIONS),
		expand("results/{geo}/combined_matrix_{geo}_posneg_weeks.tsv", geo=LOCATIONS),

rule combine_posneg:
	message:
		"""
		Combine positive and negative test results
		"""
	params:
		path = "results/{geo}",
		regex = "*_full.tsv",
		filler = "0",
		unit = "week", # change here for MONTH
		format = "integer",
		sortby = "{geo} pathogen test_result",
	output:
		merged = "results/{geo}/combined_matrix_{geo}_posneg.tsv",
		merged_weeks = "results/{geo}/combined_matrix_{geo}_posneg_weeks.tsv"
	shell:
		"""
		python scripts/multi_merger.py \
			--path {params.path} \
			--regex \"{params.regex}\" \
			--fillna {params.filler} \
			--sortby {params.sortby} \
			--output {output.merged}
		
		python scripts/aggregator.py \
			--input {output.merged} \
			--unit {params.unit} \
			--format {params.format} \
			--output {output.merged_weeks}
		
# 		cp results/country/combined_matrix_country_posneg.tsv figures/barplot
		"""


rule total_tests_go:
	input:
		expand("results/{geo}/combined_matrix_{geo}_posneg.tsv", geo=LOCATIONS),
		expand("results/{geo}/combined_matrix_{geo}_totaltests.tsv", geo=LOCATIONS),

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
		Get total tests performed per pathogen
		"""
	input:
		file = "results/{geo}/combined_matrix_{geo}_posneg.tsv"
	params:
		format = "integer",
		index = lambda wildcards: set_index_totals(wildcards.geo)[0],
		unique_id = lambda wildcards: set_index_totals(wildcards.geo)[1],
		extra_columns = lambda wildcards: set_index_totals(wildcards.geo)[2],
		filters = "~test_result:NT",
		unit = "week", #change for month
		ignore = "test_result" #keep one outpute of results
	output:
		output1 = "results/{geo}/combined_matrix_{geo}_totaltests.tsv",
		output2 = "results/{geo}/combined_matrix_{geo}_totaltests_weeks.tsv",
	shell:
		"""
		python scripts/collapser.py \
			--input {input.file} \
			--index {params.index} \
			--unique-id {params.unique_id} \
			--extra-columns {params.extra_columns} \
			--ignore {params.ignore} \
			--format {params.format} \
			--filter \"{params.filters}\" \
			--output {output.output1}


		python scripts/aggregator.py \
			--input {output.output1} \
			--unit {params.unit} \
			--format {params.format} \
			--output {output.output2}

		"""
		

rule posrate_go:
	input:
		expand("results/{geo}/combined_matrix_{geo}_posrate.tsv", geo=LOCATIONS)

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
		Test positive rates
		"""
	input:
		file1 = rules.combine_posneg.output.merged,
		file2 = rules.total_tests.output.output1,
		
		file3 = rules.combine_posneg.output.merged_weeks,
		file4 = rules.total_tests.output.output2,
	params:
		index1 = lambda wildcards: getIndex(wildcards.geo),
		index2 = lambda wildcards: getIndex(wildcards.geo),
		filter = 'test_result:Pos',
		min_denominator = 50
	output:
		output_days = "results/{geo}/combined_matrix_{geo}_posrate.tsv",
		output_weeks = "results/{geo}/combined_matrix_{geo}_posrate_weeks.tsv"
	shell: #atualizar para matrix_operations
		"""
		python scripts/matrix_operations.py \
			--input1 {input.file1} \
			--input2 {input.file2} \
			--index1 {params.index1} \
			--index2 {params.index2} \
			--min-denominator {params.min_denominator} \
			--filter1 {params.filter} \
			--output {output.output_days}

		python scripts/matrix_operations.py \
			--input1 {input.file3} \
			--input2 {input.file4} \
			--index1 {params.index1} \
			--index2 {params.index2} \
			--min-denominator {params.min_denominator} \
			--filter1 {params.filter} \
			--output {output.output_weeks}
		"""



rule posneg_allpat:
	message:
		"""
		Combine counts of positive and negative tests for all pathogens
		"""
	input:
		input = "results/country/combined_matrix_country_posneg_weeks.tsv"
	params:
		index = "test_result",
		extracol = "country",
		ignore = "pathogen",
		format = "integer",
		filter_test = "~test_result:Não Detectado",
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
			--filter \"{params.filter_test}\" \
			--format {params.format} \
			--output {output.allpat_matrix}
		"""
	


rule copy_files:
	message:
		"""
		Copy files for plotting
		"""
	shell:
		"""
		cp results/combined.tsv data/combined_cache.tsv

		cp results/demography/combined_matrix_agegroup.tsv figures/pyramid

		cp results/country/combined_matrix_country_posneg_weeks.tsv figures/barplot
		cp results/country/combined_matrix_country_posneg_allpat_weeks.tsv figures/barplot

		cp results/country/combined_matrix_country_posrate_weeks.tsv figures/lineplot
		cp results/state/combined_matrix_state_posrate_weeks.tsv figures/heatmap
		cp results/demography/matrix_agegroups_weeks_SC2_posrate.tsv figures/heatmap
		cp results/demography/matrix_agegroups_weeks_FLUA_posrate.tsv figures/heatmap
		cp results/demography/matrix_agegroups_weeks_VSR_posrate.tsv figures/heatmap
		
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
#
#

rule remove_figs:
	message: "Removing figures"
	shell:
		"""
		rm figures/*/matrix*
		rm figures/*/combined*
		rm figures/*/*.pdf
		rm figures/*/*.eps
		"""


rule clean:
	message: "Removing directories: {params}"
	params:
		"results"
	shell:
		"""
		rm -rfv {params}
		"""

