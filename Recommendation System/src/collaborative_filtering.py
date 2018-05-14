import numpy as np
from time import time
import sys, argparse
from pyspark import SparkContext
from operator import add
from pyspark.conf import SparkConf

def similarity_cal(values):
    """
        Input:  A list of the stars gave for item i and item j from all users. 
        Format: (rate_i-avg_i, rate_j-avg_j, rate_i-avg_i, rate_j-avg_j, ... , rate_i-avg_i, rate_j-avg_j)
        Function: Using these stars, we calculate the similarity between item i and item j.
        Formula: Sim(i,j) = sigma{(rate_i - avg_i)*(rate_j - avg_j)} / sqrt((sigma(rate_i - avg_i)^2) * (sigma(rate_j - avg_j)^2))
        Output: Sim(i,j)
    """
    sum_xx = 0
    sum_yy = 0
    sum_xy = 0
    for i in range(0,len(values), 2):
        sum_xx += values[i] ** 2
        sum_yy += values[i+1] ** 2
        sum_xy += values[i] * values[i+1]
    return sum_xy/np.sqrt(sum_xx * sum_yy)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'Parallel Item-based Collaborative Filtering.', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--reviews', help = 'path of input review file')
    parser.add_argument('--avgStar', help = 'path of average rate file')
    parser.add_argument('--N', type = int, help = 'Partition Number')
    parser.add_argument('--output', help = 'path of output file (saved similarity matrix)')
    verbosity_group = parser.add_mutually_exclusive_group(required = False)
    verbosity_group.add_argument('--verbose', dest='verbose', action='store_true')
    verbosity_group.add_argument('--silend', dest='verbose', action='store_false')
    parser.set_defaults(verbose=False)
    
    args = parser.parse_args()
    conf = SparkConf()
    sc = SparkContext(appName = 'Collaborative Filtering', conf = conf)
    if not args.verbose:
        sc.setLogLevel("ERROR")
    start = time()

    """
        Method: Load the average star value through the file
        Save them in memory for further use
    """
    avg_star = sc.textFile(args.avgStar)\
                 .map(lambda x : tuple(x.split(',')))\
                 .map(lambda (i,avg_r) : (int(i), float(avg_star))).cache()
 
    """
        Used "Join" to calculate the similarity matrix
        reviews                :  user_id => (item_id, rate_iu)
                (join reviews) :  user_id => ((item_i, rate_iu), (item_j, rate_ju))
                (map)          :  item_i  => (item_j, rate_iu, rate_ju)
                (join avg_star):  item_i  => ((item_j, rate_iu, rate_ju), avg_i)
                (map)          :  item_j  => (item_i, avg_i, rate_iu, rate_ju)
                (join avg_star):  item_j  => ((item_i, avg_i, rate_iu, rate_ju), avg_j)
                (map)          :  (item_i, item_j) => (rate_iu, rate_ju, avg_i, avg_j)
                (map)          :  (item_i, item_j) => (rate_iu - avg_i, rate_ju - avg_j)
                (reduceByKey)  :  (item_i, item_j) => (r_iu, r_ju, r_iu, r_ju, ..., r_iu, r_ju)
                (sim calculate):  calculate the similarity between item_i and item_j
                (filter)       :  filter the item pairs which just has one common reviews or no common reviews.
    """
    """
    reviews = sc.textFile(args.reviews)\
                .map(lambda x: tuple(x.split(',')))\
                .map(lambda (u, i, riu):(int(u), (int(i), float(riu)))).partitionBy(args.N).cache()

    rating_pairs = reviews.join(reviews).filter(lambda (x,y) : y[0][0] < y[1][0])\
                          .map(lambda (x,y) : (y[0][0], (y[1][0], y[0][1], y[1][1])))\
                          .join(avg_star).map(lambda (i,((j, ri, rj), avg_i)) : (j, (i, avg_i, ri, rj)))\
                          .join(avg_star).map(lambda (j, ((i, avg_i, ri, rj), avg_j)) : ((i,j),(ri, rj, avg_i, avg_j)))\
                          .mapValues(lambda (ri, rj, avg_i, avg_j) : (ri - avg_i, rj - avg_j))\
                          .reduceByKey(lambda x,y : x+y)\
                          .mapValues(list).mapValues(similarity_cal).filter(lambda (x,y) : y < 0.9999 and y > -0.9999)    
    rating_pairs.saveAsTextFile(args.output)
    """

    """
        Used "Broadcast" to optimized the similarity matrix calculation
        Broadcast the "avg_star" rdd as a dict to avoid shuffle and Data Skew. 
        reviews                : user_id => (item_id, rate_iu)
                (join reviews) : user_id => ((item_i, rate_iu), (item_j, rate_ju))
                (map)          : user_id => ((item_i, rate_iu, avg_i, item_j, rate_ju, avg_j)
                (mapValues)    : (item_i, item_j) => (rate_iu - avg_i, rate_ju - avg_j)
                (reduceByKey)  :  (item_i, item_j) => (r_iu, r_ju, r_iu, r_ju, ..., r_iu, r_ju)
                (sim calculate):  calculate the similarity between item_i and item_j
                (filter)       :  filter the item pairs which just has one common reviews or no common reviews.    
    """
    broadcast_avg_star = sc.broadcast(avg_star.collect())
    avg_star.unpersist()
    reviews = sc.textFile(args.reviews)\
                .map(lambda x: tuple(x.split(',')))\
                .map(lambda (u, i, riu):(int(u), (int(i), float(riu)))).partitionBy(args.N).cache()

    rating_pairs = reviews.join(reviews).filter(lambda (x,y) : y[0][0] < y[1][0])\
                          .map(lambda (x,y) : ((y[0][0],y[1][0]), (y[0][1], y[1][1])))\
                          .map(lambda (x,y) : (x,(y[0],y[1],broadcast_avg_star.value[x[0]][1],broadcast_avg_star.value[x[1]][1])))\
                          .mapValues(lambda (ri, rj, avg_i, avg_j) : (ri - avg_i, rj-avg_j))\
                          .partitionBy(args.N)\
                          .reduceByKey(lambda x,y : x+y)\
                          .mapValues(list).mapValues(similarity_cal)\
                          .filter(lambda (x,y) : y<0.9999 and y > -0.9999)\
                          .saveAsTextFile(args.output)
