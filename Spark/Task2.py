from pyspark import SparkContext
import sys

# map patients data to filter cancer only patients
def get_cancer_patients(record):
    try:
        patient_id, age, gender, postcode, diseases_list, drug_response = record.strip().split(",")
        diseases = diseases_list.strip().split(" ")
        for disease in diseases:
            if(disease=='breast-cancer' or disease=='prostate-cancer' or disease=='pancreatic-cancer' or disease=='leukemia' or disease=='lymphoma'):
                return [(patient_id, disease.strip())]
        return []
    except:
        return []

# map any gene to filter only patients with active gene		
def get_active_gene(record):
    try:
        patient_id, geneid, expression_value = record.split(",")
        #print(expression_value,geneid)
        expression_value = float(expression_value)
        geneid=int(geneid)
        if expression_value > 1250000:
            return [(patient_id, geneid)]
        else:
            return []
    except:
        return []

# aggregate by key combiner, add active gene to a set
def gene_combiner(gene_set, active_gene):
	gene_set.add(active_gene) 
	return gene_set
	
# aggregate by key reducer, merge set of genes together
def gene_reducer(gene_set1, gene_set2):
	gene_set1.update(gene_set2)
	return gene_set1
	
# return list of 1 frequent item set	
def get_1_fis(record):
	out = []
	for gene in record:
		out.append((frozenset([gene]), 1))
	return out
	
# use cross product to get combination of k+1 candidates, and filter only set which has length = k + 1
def get_candicate_k_fis(rdd, k):
	return rdd.cartesian(rdd).map(lambda x: x[0][0].union(x[1][0])).filter(lambda x: len(x) == k).distinct()
	
# count the support for a candidate if it is a subset of broadcasted patient genes set
def count_candidate_records(x):
	ctr = 0
	for record in broadcast_patients_genes.value:
		if x.issubset(record):
			ctr += 1
	return (x, ctr)
	
# filter any candidates with minimum support
def filter_candidate(candidate, patient_gene, min_occurrence):
	#test = candidate.cartesian(patient_gene).map(lambda x: (x[0], (1 if x[0].issubset(x[1]) else 0)) ).reduceByKey(lambda a, b: a + b)
	test = candidate.map(count_candidate_records)
	return test.filter(lambda x: x[1] >= min_occurrence) 
	
# range partition 1-5, descending order
def custom_partition(x):
	val = (x - min_occurrence) * min_support
	if val <= min_support:
		return 4
	elif val <= 2 * min_support:
		return 3
	elif val <= 3 * min_support:
		return 2
	elif val <= 4 * min_support:
		return 1
	else:
		return 0	
	
# format the output before saving to file
def prepare_output(record):
	out = str(record[0])
	for x in sorted(record[1]):
		out += '\t' + str(x)
	return out
	
if __name__ == "__main__":
	# init the context
	sc = SparkContext(appName="Task 2")
	args = sys.argv
	# required at least 3 arguments: patient metadata file, sample file, output dir
	if len(args) < 4:
		print("Error more input arguments required.")
		exit()
		
	input_patient = args[1]
	input_sample = args[2]
	output = args[3]
	output = output if output[-1] == '/' else (output + '/')
	
	# set default min support to 0.3 if the input given is invalid
	min_support = 0.3
	if len(args) >= 5:
		try:
			min_support = float(args[4])
			if min_support < 0.1 and min_support > 1:
				min_support = 0.3
		except:
			min_support = 0.3
		
	# set default max itemset to 2 if the input given is invalid	
	max_itemset = 2
	if len(args) >= 6:
		try:
			max_itemset = int(args[5])
			if max_itemset < 2:
				max_itemset = 2
		except:
			max_itemset = 2
			
	# read input files	
	patient_data = sc.textFile(input_patient)
	sample_data = sc.textFile(input_sample)
	
	# join cancer patients and patients with active genes, return set of active genes per cancer patients
	cancer_patients = patient_data.flatMap(get_cancer_patients)
	active_genes = sample_data.flatMap(get_active_gene).aggregateByKey(set(), gene_combiner, gene_reducer)
	cancer_patients_active_genes = active_genes.join(cancer_patients).map(lambda x: x[1][0]).cache()
	
	# calculate min occurrence
	ctr = cancer_patients_active_genes.count()
	min_occurrence  = min_support * ctr
	
	# get 1 frequent itemset  
	fis = cancer_patients_active_genes.flatMap(get_1_fis).reduceByKey(lambda a, b: a + b).filter(lambda x: x[1] >= min_occurrence)
	
	# broadcast cancer patients with active genes records
	patients_genes = []
	for record in cancer_patients_active_genes.toLocalIterator():
		patients_genes.append(record)
	broadcast_patients_genes = sc.broadcast(patients_genes)
	
	# init value for looping
	k_fis = fis
	k = 1
	while ctr > 0 and k < max_itemset: #
		k += 1
		# generate k + 1 candidates of frequent itemset
		k_plus_1_fis_candidate = get_candicate_k_fis(k_fis, k)
		# filter only candidates which have minimum occurrence to be k + 1 frequent itemset
		k_fis = filter_candidate(k_plus_1_fis_candidate, None, min_occurrence) #cancer_patients_active_genes
		
		# calculate the k + 1 frequent item set records
		ctr = k_fis.count()
		if (ctr > 0):
			# add the result to list of frequent itemset
			fis = fis.union(k_fis)
	
	# repartition, sort and format the output before saving to file
	fis.map(lambda x: (x[1], x[0])).repartitionAndSortWithinPartitions(1, lambda x: 0, False).map(prepare_output).saveAsTextFile(output + '2')
	# terminate
	sc.stop()
	
	

