#COMMAND
from pyspark import SparkConf, SparkContext
import numpy as np

#conf = SparkConf().setMaster('local').setAppName('RatingsHistogram')
#sc = SparkContext(conf=conf)

lines = sc.textFile('/home/raddy/Machine-Learning/sundog_spark/ml-100k/u.data')
ratings = lines.map(lambda x: x.split()[2])
values, counts = np.unique(ratings.collect(), return_counts=True)

for k, v in zip(values, counts):
  #  print(f'{k}: {v}')
    pass
 
#COMMAND
from pyspark import SparkConf, SparkContext

#conf = SparkConf().setMaster('local').setAppName('FriendsByAge')
#sc = SparkContext(conf=conf)

lines = sc.textFile('/home/raddy/Machine-Learning/sundog_spark/ml-100k/fakefriends.csv')
rdd = lines.map(lambda x: (int(x.split(',')[2]), int(x.split(',')[3])))
total_age = rdd.mapValues(lambda x: (x,1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
average_age = total_age.mapValues(lambda x: x[0]/x[1])
dict_average_age = dict()
for t in average_age.collect():
    dict_average_age[t[0]] = int(t[1])

for k in sorted(dict_average_age):
    #print(k,':', dict_average_age[k])
    pass
  
#COMMAND
from pyspark import SparkConf, SparkContext

#conf = SparkConf().setMaster('local').setAppName('MinTempByLocation')
#sc = SparkContext(conf=conf)

lines = sc.textFile('/home/raddy/Machine-Learning/sundog_spark/ml-100k/1800.csv')
parsed_lines = lines.map(lambda x: (x.split(',')[0], x.split(',')[2], int(x.split(',')[3])))
only_min = parsed_lines.filter(lambda x: x[1] == 'TMIN')
stationTemps = only_min.map(lambda x: (x[0],x[2]))
min_find = stationTemps.reduceByKey(lambda x,y: min(x,y))
results = min_find.collect()
print("{:.2f}".format(results[0][1] * 0.1 * (9.0/5.0) +32),"F", sep='')
print("{:.2f}".format(results[1][1] * 0.1 * (9.0/5.0) +32),"F", sep='')

#COMMAND
from pyspark import SparkConf, SparkContext

#conf = SparkConf().setMaster('local').setAppName('MaxTempByLocation')
#sc = SparkContext(conf=conf)

lines = sc.textFile('/home/raddy/Machine-Learning/sundog_spark/ml-100k/1800.csv')
parsed_lines = lines.map(lambda x: (x.split(',')[0], x.split(',')[2], int(x.split(',')[3])))
only_min = parsed_lines.filter(lambda x: x[1] == 'TMAX')
stationTemps = only_min.map(lambda x: (x[0],x[2]))
min_find = stationTemps.reduceByKey(lambda x,y: max(x,y))
results = min_find.collect()
print("{:.2f}".format(results[0][1] * 0.1 * (9.0/5.0) +32),"F", sep='')
print("{:.2f}".format(results[1][1] * 0.1 * (9.0/5.0) +32),"F", sep='')

#COMMAND
from pyspark import SparkConf, SparkContext

#conf = SparkConf().setMaster('local').setAppName('BookWordCount')
#sc = SparkContext(conf=conf)

lines = sc.textFile('/home/raddy/Machine-Learning/sundog_spark/ml-100k/book.txt')
flat_parsed_lines = lines.flatMap(lambda x:x.split())
word_counts = flat_parsed_lines.countByValue()

for word, count in word_counts.items():
    clean_word = word.encode('ascii', 'ignore')
    if clean_word:
        print(clean_word, count)
print(len(flat_parsed_lines.collect()))

  
  
