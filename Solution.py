"""

Created on March 25, 2019
@author: Pulkit Ghai

"""


from pyspark.rdd import RDD
from pyspark.sql import Row
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from math import sqrt
from pretty_print_dict import pretty_print_dict as ppd
from pretty_print_bands import pretty_print_bands as ppb
import random
import random
import os

all_states = ["ab", "ak", "ar", "az", "ca", "co", "ct", "de", "dc",
              "fl", "ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la",
              "me", "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv",
              "nh", "nj", "nm", "ny", "nc", "nd", "oh", "ok", "or", "pa",
              "pr", "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", "vi",
              "wa", "wv", "wi", "wy", "al", "bc", "mb", "nb", "lb", "nf",
              "nt", "ns", "nu", "on", "qc", "sk", "yt", "dengl", "fraspm"]


def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark



def toCSVLineRDD(rdd):
    """This function is used by toCSVLine to convert an RDD into a CSV string

    """
    a = rdd.map(lambda row: ",".join([str(elt) for elt in row]))\
           .reduce(lambda x, y: "\n".join([x, y]))
    return a + "\n"


def toCSVLine(data):
    """This function convert an RDD or a DataFrame into a CSV string

    """
    if isinstance(data, RDD):
        return toCSVLineRDD(data)
    elif isinstance(data, DataFrame):
        return toCSVLineRDD(data.rdd)
    return None


def data_preparation(data_file, key, state):
    """Our implementation of LSH will be based on RDDs. As in the clustering
    part, we will represent each state in the dataset as a dictionary of
    boolean values with an extra key to store the state name.
    We call this dictionary 'state dictionary'.

    Task 1 : Write a script that
             1) Creates an RDD in which every element is a state dictionary
                with the following keys and values:

                    Key     |         Value
                ---------------------------------------------
                    name    | abbreviation of the state
                    <plant> | 1 if <plant> occurs, 0 otherwise

             2) Returns the value associated with key
                <key> in the dictionary corresponding to state <state>


    Keyword arguments:
    data_file -- csv file of plant name/states tuples (e.g. ./data/plants.data)
    key -- plant name
    state -- state abbreviation (see: all_states)
    """
    alls = sorted(all_states)
    spark = init_spark()
    line = spark.read.text(data_file).rdd
    data = line.map(lambda row: row.value.split(","))
    li = data.keys().collect()
    lis = sorted(li)

    def create(x):
        plant = x[0]
        list = []
        for elm in x[1:]:
            tup = (elm, plant)
            list.append(tup)
        return list

    data = data.flatMap(lambda x: create(x))
    data = data.filter(lambda x: x[0] != "pe" and x[0] != "gl")
    data = data.reduceByKey(lambda a, b: a+b)
    data = data.mapValues(lambda x: [1 if lis[i] in x else 0
                                              for i in range(0, len(lis))]).cache().collect()


    data1 = sorted(data)

    index = alls.index(state)
    tempList = data1[index]
    val = lis.index(key)
    result = tempList[1][val]
    # print (tempList[0], tempList[1][val])
    
    return (result)


def primes(n, c):
    """To create signatures we need hash functions (see next task). To create
    hash functions,we need prime numbers.

    Task 2: Write a script that returns the list of n consecutive prime numbers
    greater or equal to c. A simple way to test if an integer x is prime is to
    check that x is not a multiple of any integer lower or equal than sqrt(x).

    Keyword arguments:
    n -- integer representing the number of consecutive prime numbers
    c -- minimum prime number value
    """
    list_of_primes =[]
    temp = c
    x=0
    flag = False
    while x < n :
          for i in range(2, int(sqrt(temp)+1)):
              if (temp % i) == 0:
                  flag = False
                  break
              else:
                  flag = True
          if(flag ==  True):
              list_of_primes.append(temp)
              temp += 1
              x+=1
          else:
              temp+=1

    return (list_of_primes)


def getAB(s,m):
    random.seed(s)
    a = random.randint(1,m)
    b = random.randint(1,m)
    return [a,b]

def hash_plants(s, m, p, x):
    """We will generate hash functions of the form h(x) = (ax+b) % p, where a
    and b are random numbers and p is a prime number.

    Task 3: Write a function that takes a pair of integers (m, p) and returns
    a hash function h(x)=(ax+b)%p where a and b are random integers chosen
    uniformly between 1 and m, using Python's random.randint. Write a script
    that:
        1. initializes the random seed from <seed>,
        2. generates a hash function h from <m> and <p>,
        3. returns the value of h(x).

    Keyword arguments:
    s -- value to initialize random seed from
    m -- maximum value of random integers
    p -- prime number
    x -- value to be hashed
    """
    AB =getAB(s,m)
    a= AB[0]
    b= AB[1]

    return (((a*x)+b)%p)


def hash_list(s, m, n, i, x):
    """We will generate "good" hash functions using the generator in 3 and
    the prime numbers in 2.

    Task 4: Write a script that:
        1) creates a list of <n> hash functions where the ith hash function is
           obtained using the generator in 3, defining <p> as the ith prime
           number larger than <m> (<p> being obtained as in 1),
        2) prints the value of h_i(x), where h_i is the ith hash function in
           the list (starting at 0). The random seed must be initialized from
           <seed>.

    Keyword arguments:
    s -- seed to intialize random number generator
    m -- max value of hash random integers
    n -- number of hash functions to generate
    i -- index of hash function to use
    x -- value to hash
    """
    random.seed(s)
    list_of_primes= primes(n,m)
    print(list_of_primes)
    list_of_hashes =[]
    for k in range(n):
        a= random.randint(1,m)
        b = random.randint(1,m)
        list_of_hashes.append(((a*x)+b)%list_of_primes[k])

    # print(list_of_hashes)
    return (list_of_hashes[i])


def signatures(datafile, seed, n, state):
    """We will now compute the min-hash signature matrix of the states.

    Task 5: Write a function that takes build a signature of size n for a
            given state.

    1. Create the RDD of state dictionaries as in data_preparation.
    2. Generate `n` hash functions as done before. Use the number of line in
       datafile for the value of m.
    3. Sort the plant dictionary by key (alphabetical order) such that the
       ordering corresponds to a row index (starting at 0).
       Note: the plant dictionary, by default, contains the state name.
       Disregard this key-value pair when assigning indices to the plants.
    4. Build the signature array of size `n` where signature[i] is the minimum
       value of the i-th hash function applied to the index of every plant that
       appears in the given state.


    Apply this function to the RDD of dictionary states to create a signature
    "matrix", in fact an RDD containing state signatures represented as
    dictionaries. Write a script that returns the string output of the RDD
    element corresponding to state '' using function pretty_print_dict
    (provided in answers).

    The random seed used to generate the hash function must be initialized from
    <seed>, as previously.

    ***Note: Dask may be used instead of Spark.

    Keyword arguments:
    datafile -- the input filename
    seed -- seed to initialize random int generator
    n -- number of hash functions to generate
    state -- state abbreviation
    """
    alls = sorted(all_states)
    spark = init_spark()
    line = spark.sparkContext.textFile(datafile, minPartitions=8)
    data = line.map(lambda x: x.split(','))
    li = data.keys().collect()
    lis = sorted(li)

    def create(x):
        plant = x[0]
        list = []
        for elm in x[1:]:
            tup = (elm, plant)
            list.append(tup)
        return list

    data = data.flatMap(lambda x: create(x))
    data = data.filter(lambda x: x[0] != "pe" and x[0] != "gl")
    data = data.reduceByKey(lambda a, b: a + b)
    data = data.mapValues(lambda x: [1 if lis[i] in x else 0
                                     for i in range(0, len(lis))]).collect()
    # data = data.mapValues(lambda x: )
    data1 = sorted(data)
    """ generate the n hash functions"""

    index = alls.index(state)

    st = data1[index]
    # print(st)

    m = line.count()
    Signature = []
    random.seed(seed)

    list_of_primes = primes(n, m)


    for r in list_of_primes:
        min = 9999999999999999999999999999
        a = random.randint(1, m)
        b = random.randint(1, m)
        for k in range(len(st[1])):

            if st[1][k] == 1:
                hash = ((a * k) + b) % r
                if hash < min:
                    min = hash
        Signature.append(min)
    s_dict = {i: Signature[i] for i in range(len(Signature))}

    return s_dict
    
  
def hash_band(datafile, seed, state, n, bb, n_r):
    """We will now hash the signature matrix in bands. All signature vectors,
    that is, state signatures contained in the RDD computed in the previous
    question, can be hashed independently. Here we compute the hash of a band
    of a signature vector.

    Task: Write a script that, given the signature dictionary of state <state>
    computed from <n> hash functions (as defined in the previous task),
    a particular band <b> and a number of rows <n_r>:

    1. Generate the signature dictionary for <state>.
    2. Select the sub-dictionary of the signature with indexes between
       [b*n_r, (b+1)*n_r[.
    3. Turn this sub-dictionary into a string.
    4. Hash the string using the hash built-in function of python.

    The random seed must be initialized from <seed>, as previously.

    Keyword arguments:
    datafile --  the input filename
    seed -- seed to initialize random int generator
    state -- state to filter by
    n -- number of hash functions to generate
    b -- the band index
    n_r -- the number of rows
    """
    alls = sorted(all_states)
    spark = init_spark()
    line = spark.sparkContext.textFile(datafile, minPartitions=8)
    data = line.map(lambda x: x.split(','))
    li = data.keys().collect()
    lis = sorted(li)

    def create(x):
        plant = x[0]
        list = []
        for elm in x[1:]:
            tup = (elm, plant)
            list.append(tup)
        return list

    data = data.flatMap(lambda x: create(x))
    data = data.filter(lambda x: x[0] != "pe" and x[0] != "gl")
    data = data.reduceByKey(lambda a, b: a + b)
    data = data.mapValues(lambda x: [1 if lis[i] in x else 0
                                     for i in range(0, len(lis))]).collect()
    # data = data.mapValues(lambda x: )
    data1 = sorted(data)
    """ generate the n hash functions"""

    index = alls.index(state)

    st = data1[index]
   

    m = line.count()
    Signature = list()
    random.seed(seed)

    list_of_primes = primes(n, m)
#     print(list_of_primes)

    for r in list_of_primes:
        min = 9999999999999999999999999999
        a = random.randint(1, m)
        b = random.randint(1, m)
        for k in range(len(st[1])):

            if st[1][k] == 1:
                hashy = ((a * k) + b) % r
                if hashy < min:
                    min = hashy
        Signature.append(min)
    q = bb * n_r
    p = (bb + 1) * n_r
    slice_dict = {i: Signature[i] for i in range(q, p)}
    result = hash(str(slice_dict))
 
    return result

def signature_matrix(final_list,primes,seed,m):
   all_signature_list = []
   temp_cnt = 0
   for state in final_list:
       signature_values = []
       random.seed(seed)
       for p in primes:
           min = 99999999999999
           a = random.randint(1, m)
           b = random.randint(1, m)
           x=0
           while x < len(state[1]):
               if state[1][x] == 0:
                   temp_cnt=+1
               else:
                   hash_value = ((a * x) + b) % p
                   if hash_value < min:
                       min = hash_value
               x += 1
           signature_values.append(min)
       tup = (state[0], signature_values)
       all_signature_list.append(tup)
       # print(temp_cnt)
   return all_signature_list

def hash_bands(data_file, seed, n_b, n_r):
    """We will now hash the complete signature matrix

    Task: Write a script that, given an RDD of state signature dictionaries
    constructed from n=<n_b>*<n_r> hash functions (as in 5), a number of bands
    <n_b> and a number of rows <n_r>:

    1. maps each RDD element (using flatMap) to a list of ((b, hash),
       state_name) tuples where hash is the hash of the signature vector of
       state state_name in band b as defined in 6. Note: it is not a triple, it
       is a pair.
    2. groups the resulting RDD by key: states that hash to the same bucket for
       band b will appear together.
    3. returns the string output of the buckets with more than 2 elements
       using the function in pretty_print_bands.py.

    That's it, you have printed the similar items, in O(n)!

    Keyword arguments:
    datafile -- the input filename
    seed -- the seed to initialize the random int generator
    n_b -- the number of bands
    n_r -- the number of rows in a given band
    """
    spark = init_spark()
    line = spark.sparkContext.textFile(data_file, minPartitions=8)
    # data = line.map(lambda x: x.split(','))
    data = spark.createDataFrame(line, StringType())
    data = data.select('value').rdd.map(lambda x: x[0].split(',')).map(lambda x: (x[0], x[1:]))
    li = data.keys().collect()
    lis = sorted(li)

    # # table = spark.sparkContext.textFile(data_file, minPartitions=8)
    # # table = spark.createDataFrame(table, StringType())
    # table = table.select('value').rdd.map(lambda x: x[0].split(',')).map(lambda x: (x[0], x[1:]))
    # table_key = table.keys().collect()
    # sorted_table_key = sorted(table_key)
    n = n_b * n_r

    def f(rdd):
        return rdd

    data = data.flatMapValues(f)
    data = data.map(lambda x: (x[1], [x[0]]))
    # data = data.flatMap(lambda x: create(x))
    data = data.filter(lambda x: x[0] != "pe" and x[0] != "gl")
    data = data.reduceByKey(lambda a, b: a + b)

    def indexes(x):
        indexList = []
        for i in range(len(x)):
            if x[i] == 1:
                indexList.append(i)
        return indexList
    data = data.mapValues(lambda x: [1 if lis[i] in x else 0
                                     for i in range(0, len(lis))]).collect()

    data1 = sorted(data)
    """ generate the n hash functions"""
    m = line.count()
    list_of_primes = primes(n, m)
    SignatureMatrix = signature_matrix(data1,list_of_primes,seed,m)

    def createbands(band):
        list_of_bands = []
        i = 0
        while i < n_b:
            q = i * n_r
            p = (i + 1) * n_r
            slice_dict = {i: band[1][i] for i in range(q, p)}
            tupp = ((i, (hash(str(slice_dict)))), band[0])
            list_of_bands.append(tupp)
            i += 1

        return list_of_bands

    rdd1 = spark.sparkContext.parallelize(SignatureMatrix)

    rdd1 = rdd1.flatMap(lambda x: createbands(x))

    rdd1 = rdd1.groupByKey().mapValues(list)
#     print(ppb(rdd1))
    return ppb(rdd1)
  
#Please note that copying the complete code may put you in trouble of universitie's code of conduct.
