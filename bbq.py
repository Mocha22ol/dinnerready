import csv
import sys
from pyspark.sql.session import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import broadcast
import time

def cleanTuple(input_tuple):
    if input_tuple[0] == "":
        return None
    else:
        try:
            result = tuple(map(int, input_tuple))
            if(len(result) == 1):
                return(0,result[0])
            else:
                return result
        except:
            return None

def mean(values):
    return sum(values) / float(len(values))

def variance(values, mean):
    return sum([(x-mean)**2 for x in values])

def covariance(x, mean_x, y, mean_y):
    covar = 0.0
    for i in range(len(x)):
        covar += (x[i] - mean_x) * (y[i] - mean_y)
    return covar

def getCoef(y):
    x = list(range(2015,2020))
    x_mean, y_mean = mean(x), mean(y)
    b1 = covariance(x, x_mean, y, y_mean) / variance(x, x_mean)
    return b1

def toCSV(_, records):
    for (physicalID),(count_2015,count_2016,count_2017,count_2018,count_2019,coef) in records:
        yield ','.join((str(physicalID),str(count_2015),str(count_2016),str(count_2017),str(count_2018),str(count_2019),str(coef)))

    
def processCenterline(partId, records):
    if partId==0:
        next(records)
    reader = csv.reader(records)
    for row in reader: 
        if row[0] and row[13] and (row[28] or row[10]):
            left_low = cleanTuple(tuple(row[2].split('-')))
            left_high = cleanTuple(tuple(row[3].split('-')))
            right_low = cleanTuple(tuple(row[4].split('-')))
            right_high = cleanTuple(tuple(row[5].split('-')))
            if left_low and left_high:
                yield(int(row[0]),left_low[0], left_low[1], left_high[0], left_high[1],int(row[13]),row[10].lower(), row[28].lower(),False)
            if right_low and right_high:
                yield(int(row[0]),right_low[0], right_low[1], right_high[0], right_high[1],int(row[13]),row[10].lower(), row[28].lower(),True)

def getPhysicalID(partId,records):
    if partId==0:
        next(records)
    reader = csv.reader(records)
    for row in reader: 
        try:
            yield (int(row[0]),1)
        except:
            continue

    
def processTickets(partId, records):
    #create a dictionary to recode the borough
    recodeBoro = {'MAN':1,'MH':1,'MN':1,'NEWY':1,'NEW Y':1,'NY':1,\
    'BRONX':2,'BX':2,\
    'BK':3,'K':3,'KING':3,'KINGS':3,\
    'Q':4,'QN':4,'QNS':4,'QU':4,'QUEEN':4,\
    'R':5,'RICHMOND':5}  
    #load tickets records
    if partId==0:
        next(records)
    reader = csv.reader(records)

    for row in reader:
        if len(row)==43:
            borough_ticket = None
            house_number_ticket = None
            result = None
            street_name_ticket = row[24].lower()
            try:
                borough_ticket = recodeBoro[row[21]]
            except KeyError:
                continue
                
            try:
                house_number_ticket = cleanTuple(tuple((row[23].split('-'))))
                year = int(row[4][-4:])
            except:
                continue
            if house_number_ticket and borough_ticket and street_name_ticket and (year in range(2015,2020)):
                yield(year, house_number_ticket[0], house_number_ticket[1], borough_ticket, street_name_ticket, house_number_ticket[1]%2 == 0)

if __name__=='__main__':
    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
    print("***START***")
    print(current_time)
    sc = SparkContext()
    spark = SparkSession(sc)

    tickets = sc.textFile('hdfs:///tmp/bdm/nyc_parking_violation/')
    #loading parking tickets and creating dataframe
    parking_ticket_clean = tickets.mapPartitionsWithIndex(processTickets)
    parking_tickets_df = spark.createDataFrame(parking_ticket_clean, ('year','house_number_1','house_number_2' ,'boro','street_name','even_flag'))
    
    #loading centerline segments with name and label
    print("loading")
    centerlines = sc.textFile('hdfs:///tmp/bdm/nyc_cscl.csv')
    centerline_all = centerlines.mapPartitionsWithIndex(processCenterline)
    print("loading2")
    #get full list of centerline physicalID and create dataframe 
    centerline_full_id_only = centerlines.mapPartitionsWithIndex(getPhysicalID).distinct()
    centerline_base = spark.createDataFrame(centerline_full_id_only, ('ID','dummy'))
    print("loading3")
    #stacking centerline name + label but only keep the distinct values, save into a dataframe
    centerlines_df = spark.createDataFrame(centerline_all, ('physicalID','low_house_number_1','low_house_number_2','high_house_number_1','high_house_number_2','boro','street_name','street_label','even_flag'))
    centerlines_df_new = (centerlines_df.drop('street_name').withColumnRenamed("street_label", "street_name"))\
    .union(centerlines_df.drop('street_label')).distinct()
    #split tickets into odd and even
    print("loading4")
    tickets_odds = parking_tickets_df.filter(parking_tickets_df.even_flag == False)
    tickets_evens = parking_tickets_df.filter(parking_tickets_df.even_flag == True)
    #split segments into odd and even
    centerlines_odds = centerlines_df_new.filter(centerlines_df_new.even_flag == False)
    centerlines_evens = centerlines_df_new.filter(centerlines_df_new.even_flag == True)
    
    #odd join odd, get each year's count by physicalID
    print("loading5")
    odds_result = broadcast(centerlines_odds).join(tickets_odds,\
                            ((tickets_odds.house_number_1 <= centerlines_odds.high_house_number_1)&(tickets_odds.house_number_1 >= centerlines_odds.low_house_number_1)&(tickets_odds.house_number_2 <= centerlines_odds.high_house_number_2)&(tickets_odds.house_number_2 >= centerlines_odds.low_house_number_2))&\
                            ((tickets_odds.street_name == centerlines_odds.street_name))&\
                            (tickets_odds.boro == centerlines_odds.boro),\
                            'left').groupBy([centerlines_odds.physicalID, tickets_odds.year])\
                            .count()
    #even join even, get each year's count by physicalID
    evens_result = broadcast(centerlines_evens).join(tickets_evens,\
                            ((tickets_evens.house_number_1 <= centerlines_evens.high_house_number_1)&(tickets_evens.house_number_1 >= centerlines_evens.low_house_number_1)&(tickets_evens.house_number_2 <= centerlines_evens.high_house_number_2)&(tickets_evens.house_number_2 >= centerlines_evens.low_house_number_2))&\
                            ((tickets_evens.street_name == centerlines_evens.street_name))&\
                            (tickets_evens.boro == centerlines_evens.boro),\
                            'left').groupBy([centerlines_evens.physicalID, tickets_evens.year])\
                           .count()

    #stack all odd and even matched records
    full_result = odds_result.unionAll(evens_result)
    #group by id and year, and spread the rows into year columns
    new_result = full_result.groupBy(full_result.physicalID)\
    .pivot("year", ["2015", "2016", "2017", "2018", "2019"])\
    .sum("count")

    #use the full list as the base for the join, fill the nones with 0
    final_result = centerline_base.join(new_result, (centerline_base.ID == new_result.physicalID), 'left')\
    .fillna({'2015':'0','2016':'0', '2017':'0','2018':'0','2019':'0'}).drop('dummy','physicalID')

    #calculate coef, sort by id and output as csv
    final_result.rdd.map(lambda x: (x[0], (x[1],x[2],x[3],x[4],x[5],getCoef(x[1:6]))))\
        .sortByKey()\
        .mapPartitionsWithIndex(toCSV) \
        .saveAsTextFile(sys.argv[1])

    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
    print(current_time)
    print("***END***")



