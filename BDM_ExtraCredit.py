from pyspark import SparkContext
import pyspark.sql.functions as f
from pyspark.sql.session import SparkSession
import csv
import time
import geopandas as gpd
import shapely.geometry as geom
from pyspark.sql import SQLContext

def createIndex(shapefile): 
    import rtree
    import fiona.crs
    import geopandas as gpd
    zones = gpd.read_file(shapefile).to_crs(fiona.crs.from_epsg(2263))
    index = rtree.Rtree()
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def getTracts(shapefile):
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)                                                                         
    zones = gpd.read_file(shapefile).to_crs(fiona.crs.from_epsg(2263))
    tracts = list(zones.plctract10)
    pop = list(zones.plctrpop10)
    return tracts, pop

def findZone(p, index, zones):
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        try:
            if zones.geometry[idx].contains(p):
                return str(idx)
        except:    
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

    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
    index, zones = createIndex('500cities_tracts.geojson')
    shapefile =  gpd.read_file('500cities_tracts.geojson').to_crs(fiona.crs.from_epsg(2263))

    reader = csv.reader(records, delimiter='|')
    for row in reader:
        if len(row) == 7:
            try:
                if any(ele in row[5] for ele in full_list):
                    p = geom.Point(proj(float(row[2]), float(row[1])))
                    tweet_zone = findZone(p, index, zones)
                    if tweet_zone:
                        yield(shapefile['plctract10'][int(tweet_zone)], 1)
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
    #tweets = sc.textFile('hdfs:///tmp/bdm/tweets-100m.csv')
    #result = tweets.mapPartitionsWithIndex(processTweets)\
    #        .reduceByKey(lambda x,y: x+y)
    #result = spark.createDataFrame(result, ('tractID','tweets'))
    #print("start base structure")
    df = sqlContext.read.load('500cities_tracts.geojson', format="json")
    df.printSchema()
    #base_df = spark.createDataFrame(zip(tract, pop), schema=['tract', 'pop'])
    #print("test")
    #result_new = base_df.join(result, base_df.tract == result.tractID, "left").drop('tractID')\
    #      .fillna({'tweets':'0'}).withColumn('norm', f.col('tweets')/f.col('pop')).drop('tweets')

    #test = result_new.rdd.map(lambda x: (x[0], (x[1],x[2])))\
    #    .sortByKey()\
    #    .mapPartitionsWithIndex(toCSV)\
    #    .saveAsTextFile(sys.argv[1])
