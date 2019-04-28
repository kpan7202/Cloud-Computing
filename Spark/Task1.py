from pyspark import SparkContext
import sys


# map patients data to filter cancer only patients
def get_cancer_patients(record):
    try:
        temp = []
        patient_id, age, gender, postcode, diseases_list, drug_response = record.strip().split(",")
        diseases = diseases_list.strip().split(" ")
        for disease in diseases:
            if (disease == 'breast-cancer' or disease == 'prostate-cancer' or disease == 'pancreatic-cancer' or disease == 'leukemia' or disease == 'lymphoma'):
                temp.append((patient_id, disease.strip()))
        return temp
    except:
        return []


# map  gene 42 to filter only patients with active gene
def get_active_gene(record):
    try:
        patient_id, geneid, expression_value = record.split(",")
        expression_value = float(expression_value)
        geneid = int(geneid)
        if expression_value > 1250000 and geneid == 42:
            return [(patient_id, 1)]
        else:
            return []
    except:
        return []


# aggregate by key combiner, add count
def disease_combiner(accumulated_count, current_count):
    return accumulated_count + current_count


# aggregate by key reducer, merge disease together
def disease_reducer(disease_count1, disease_count2):
    return disease_count1 + disease_count2


# format the output before saving to file
def prepare_output(record):
    return record[0] + "\t" + str(record[1])


if __name__ == "__main__":
    # init the context
    sc = SparkContext(appName="Task 1")
    args = sys.argv
    # required at least 3 arguments: patient metadata file, sample file, output dir
    if len(args) < 3:
        print("Error more input arguments required.")
        exit()


    input_patient = args[1]
    input_sample = args[2]
    output = args[3]
    output = output if output[-1] == '/' else (output + '/')

    # read input files
    patient_data = sc.textFile(input_patient)
    sample_data = sc.textFile(input_sample)

    # join cancer patients and patients with active genes, return set of active genes 42 per cancer patients
    cancer_patients = patient_data.flatMap(
        get_cancer_patients)
    active_genes = sample_data.flatMap(get_active_gene)
    diseases_category_summation = cancer_patients.join(active_genes).values().aggregateByKey((0), disease_combiner,
                                                                                             disease_reducer,
                                                                                             1).sortByKey().sortBy(
        lambda a: a[1],
        False).map(prepare_output).saveAsTextFile(output+ '1')

    sc.stop()
