from pyspark import SparkContext
import pyspark.sql.functions as f
from pyspark.sql.session import SparkSession
import csv
import time
import geopandas as gpd
import shapely.geometry as geom
from pyspark.sql import SQLContext
import sys


def createIndex(shapefile):                                                                         
    import rtree
    import fiona.crs
    import geopandas as gpd
    zones = gpd.read_file(shapefile).to_crs(fiona.crs.from_epsg(2263))
    index = rtree.Rtree()
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def findZone(p, index, zones):
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:                                      
        try:
            if zones.geometry[idx].contains(p):
                return str(zones.plctract10[idx])
        except:
               continue
    return None

def toCSV(_, records):
    for (tractID),(pop, norm) in records:
        yield ','.join((str(tractID),str(pop),str(norm)))

def processTweets(pid, records):
    import pyproj
    import fiona.crs

    with open('drug_sched2.txt') as file:
        sched2 = file.read().splitlines()

    with open('drug_illegal.txt') as file:
        term2 = file.read().splitlines()

    full_list = sched2 + term2

    reader = csv.reader(records, delimiter='|')
    for row in reader:
        if len(row) == 7:
            try:
                # if it has drug term, geosearch and yield tract ID
                if row[1] and row[2] and any(ele in row[5] for ele in full_list):
                
                    p = geom.Point(proj(float(row[2]),float(row[1])))
                    tract = findZone(p, index, zones)
                    yield(tract,1)
                    break
            except:
                continue

if __name__=='__main__':
    sc = SparkContext()
    spark = SparkSession(sc)
    sqlContext = SQLContext(sc)
    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
    print("***START***")
    print(current_time)
    print("compiling tweets")
    tweets = sc.textFile(sys.argv[1])
    result = tweets.mapPartitionsWithIndex(processTweets)\
            .reduceByKey(lambda x,y: x+y)
    print(result.take(10))
    #start base structure of full tractid and population
    df = sqlContext.read.load('hdfs:///tmp/bdm/500cities_tracts.geojson', format="json")
    #only keep the tract and population columns from geojson
    base_df = df.select(f.explode(df.features.properties).alias('properties')).select('properties.*')
#
    #join with the result by tract ID, normalize  the value
    result_new = base_df.join(result, base_df.plctract10 == result.tweets, "left").drop('tractID')\
          .fillna({'tweets':'0'}).withColumn('norm', f.col('tweets')/f.col('pop')).drop('tweets')
#   #sort by key and export as csv
    test = result_new.rdd.map(lambda x: (x[0], (x[1],x[2])))\
        .sortByKey()\
        .mapPartitionsWithIndex(toCSV)\
        .saveAsTextFile(sys.argv[2])
