import sys
from pyspark import SparkContext
from itertools import chain, combinations

def pair_support_to_gene(record):
    try:
        results = []
        temp=[]
        temp_record = record.strip().split("\t")
        support = temp_record[0]
        # remove support
        temp_record.pop(0)
        for gene in temp_record:
            temp.append(gene)
        subsets = generate_all_subset(temp)

        for x in subsets:
            if 0 < len(x) < len(temp):
                results.append((frozenset(x), (frozenset(set(temp)-set(x)), support)))
        # return R,S-R,support
        return results
    except:
        return []


# generate all possible subset
def generate_all_subset(record):
    xs = list(record)
    return list(chain.from_iterable(combinations(xs, n) for n in range(len(xs) + 1)))


# map frequent item to set
def map_to_set(record):
    try:
        temp=[]
        temp_record = record.strip().split("\t")
        support = temp_record[0]
        temp_record.pop(0)
        for gene in temp_record:
            temp.append(gene)
        return [(frozenset(temp),support)]
    except:
        return []

# map to rule
def map_to_rule(record):
	#lhs=str(list(record[0]))
	#rhs=str(list(record[1][0][0]))
	number_subset=float(record[1][0][1])
	number_fis=int(record[1][1])
	conf=number_subset/number_fis
	if conf >= min_confidence:
		lhs = record[0]
		rhs = record[1][0][0]	
		return [(lhs,(rhs,conf))]
	else:
		return []

# format the output before saving to file
def prepare_output(line):
    #return line[0]+"\t"+line[1]+"\t"+str(line[2])
	out = ""
	for i in sorted(line[1][0]):
		out += i + " "
	out  = out[:-1] + "\t"
	
	for i in sorted(line[1][1]):
		out += i + " "
	out  = out[:-1] + "\t" + str(line[0])
	return out

if __name__ == "__main__":
    # init the context
    sc = SparkContext(appName="Task 3")
    args = sys.argv
    # required at least 2 arguments: frequent item set file, output dir , min confidence
    if len(args) < 3:
        print("Error more input arguments required.")
        exit()

    input_fis = args[1]
    output = args[2]
    output = output if output[-1] == '/' else (output + '/')

    # set default min support to 0.3 if the input given is invalid
    min_confidence = 0.6
    if len(args) >= 4:
        try:
            min_confidence = float(args[3])
            if min_confidence < 0.1 or min_confidence > 1:
                min_confidence = 0.6
        except:
            min_confidence = 0.6

    # read input files R
    fis_data = sc.textFile(input_fis)

    # generate all possible subsets S-R
    subsets = fis_data.flatMap(pair_support_to_gene)
    # generate R set
    fis=fis_data.flatMap(map_to_set)
    # join R and S-R together to create rule and the filter > min confidence
    rules = subsets.join(fis).flatMap(map_to_rule)#.map(map_to_rule).filter(lambda x: x[2] >= min_confidence)

    # sort and format the output before saving to file
    #rules.sortBy(lambda x: x[2], False).map(prepare_output).saveAsTextFile(output+ '3')
    rules.map(lambda x: (x[1][1], (x[0], x[1][0]))).repartitionAndSortWithinPartitions(1, lambda x: 0, False).map(prepare_output).saveAsTextFile(output + '3')
    sc.stop()