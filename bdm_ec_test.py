from pyspark import SparkContext
from pyspark.sql.session import SparkSession
import csv
import time

def processTweets(pid, records):

    with open('drug_sched2.txt') as file:
        sched2 = file.read().splitlines()

    with open('drug_illegal.txt') as file:
        term2 = file.read().splitlines()

    full_list = sched2 + term2

    reader = csv.reader(records, delimiter='|')
    for row in reader:
        if len(row) == 7:
            try:
                if any(ele in row[5] for ele in full_list) :
                    yield(1,1) 
            except:
                continue

if __name__=='__main__':
    sc = SparkContext()
    spark = SparkSession(sc)

    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
    print("***START***")
    print(current_time)
    tweets = sc.textFile('hdfs:///tmp/bdm/tweets-100m.csv')
    result = tweets.mapPartitionsWithIndex(processTweets)\
            .reduceByKey(lambda x,y: x+y)
    print(result.take(1))

    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
    print(current_time)
    print("***END***")

    