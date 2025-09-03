from pyspark import SparkContext

sc = SparkContext("local", "RDD Transformation Exercises")

num_rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10], numSlices=4)

string_rdd = sc.parallelize(["one", "two", "three", "four", "five", "six"], numSlices=4)

pair_rdd = sc.parallelize([('a',1),('b', 2),('c', 3)])
pair_rdd_word = sc.parallelize([('a',"one"),('b', "two"),('c', "three")])

# -------------------- MAP Exercises --------------------

# Exercise 1: Square Numbers

square_rdd = num_rdd.map(lambda x : x*x)

print(square_rdd.collect())


# Exercise 2: Add 10 to Each Number

add_rdd = num_rdd.map((lambda x : x+ 10))

print(add_rdd.collect())


# Exercise 3: Convert Strings to Uppercase

upper_rdd = string_rdd.map(lambda x : x.upper())
print(upper_rdd.collect())

# Exercise 4: Get Length of Each Word
len_rdd = string_rdd.map(lambda x : len(x))
print(len_rdd.collect())

# Exercise 5: Add Prefix to Each String
pre_rdd = string_rdd.map(lambda x : "num_" + x)
print(pre_rdd.collect())

# -------------------- MAPPARTITIONS Exercises --------------------

# Exercise 1: Sum of Each Partition
sum_part_rdd = num_rdd.mapPartitions(lambda x : [sum(x)])
print(sum_part_rdd.collect())

# Exercise 2: Multiply Each Number by 2 in Each Partition
double_part_rdd = num_rdd.mapPartitions(lambda it: [ n * 2 for n in it ])
print(double_part_rdd.collect())

# Exercise 3: Convert Strings to Uppercase in Each Partition
upper_part_rdd = string_rdd.mapPartitions(lambda it : (n.upper() for n in it))
print(upper_part_rdd.collect())

# Exercise 4: Count Elements in Each Partition
print(num_rdd.getNumPartitions())
count_part_rdd = num_rdd.mapPartitions(lambda x : [len(list(x))])
print(count_part_rdd.collect())
print("MapPartitions Ex4:", num_rdd.mapPartitions(lambda it: [len(list(it))]).collect())

# Exercise 5: Append Partition Sum to Each Element (Note: not practical this way, just demo)
app_part_sum_rdd = num_rdd.mapPartitions(lambda x : [ (each,sum([each])) for each in x  ])
print(app_part_sum_rdd.collect())

# -------------------- MAPVALUES Exercises --------------------

# Exercise 1: Double the Values
print(pair_rdd.mapValues(lambda x : x*2).collect())

# Exercise 2: Convert Values to Uppercase
print(pair_rdd_word.mapValues(lambda x: x.upper()).collect())

# Exercise 3: Append Length to String Values
print(pair_rdd_word.mapValues(lambda x: (x, len(x))).collect())

# Exercise 4: Create List from Integer Value
print(pair_rdd.mapValues(lambda x: list(range(x))).collect())

# Exercise 5: Reverse String Values


################################## MapPartitionsWithIndex ####################################

# ------------------ Exercise 1 ------------------
# Tag each element with its partition index

# ------------------ Exercise 2 ------------------
# Count number of elements in each partition

# ------------------ Exercise 3 ------------------
# Add partition index to each element (if numeric)

# ------------------ Exercise 4 ------------------
# Return only the first element of each partition

# ------------------ Exercise 5 ------------------
# Label each element as 'even' or 'odd' based on partition index


# -------------------- GLOM Exercises --------------------

# Exercise 1: View Partitions

# Exercise 2: Count Items per Partition

# Exercise 3: Max of Each Partition

# Exercise 4: Concatenate String Elements in Each Partition

# Exercise 5: Sort Each Partition
