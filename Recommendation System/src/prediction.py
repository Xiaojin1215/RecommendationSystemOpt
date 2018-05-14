import numpy as np
import sys, argparse
from pyspark import SparkContext
from operator import add

def writeList(file, output):
    with open(file, 'a') as f:
        for lines in output:
            f.write(','.join(str(line) for line in lines))
            f.write('\n')
def readData(file, sc):
    return sc.textFile(file)\
             .map(lambda x : tuple(x.split(',')))\
             .map(lambda (u,i,riu) : (int(u), (int(i), float(riu))))
def predict(simMatrix, testData, review, N):
    simMatrix = simMatrix.groupByKey(numPartitions = N).mapValues(dict)
    review = review.groupByKey(numPartitions = N).mapValues(dict)
    broadReview = sc.broadcast(review.collectAsMap())
    res = testData.map(lambda (x, y): (y[0], (y[1], broadReview.value.get(x, '-')))) \
                .join(simMatrix, numPartitions = N) \
                .map(lambda (i, x): (sum( [x[0][1][key] * x[1][key] for key in x[0][1] if key in x[1]] ) \
                                             , sum( [x[1][key] for key in x[0][1] if key in x[1]] ), x[0][0])) \
                .filter(lambda (x, y, z): y > 0) \
                .map(lambda (x, y, z): ((x * 1.0 / y - z)**2, 1)) \
                .reduce(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    return res[0] * 1.0 / res[1]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'Parallel recommendation RMSE calculation',formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('review',help = 'review')
    parser.add_argument('test',help = 'average rate')
    parser.add_argument('N', type = int, help = 'Partition number')
    verbosity_group = parser.add_mutually_exclusive_group(required=False)
    verbosity_group.add_argument('--verbose', dest='verbose', action='store_true')
    verbosity_group.add_argument('--silent', dest='verbose', action='store_false')
    parser.set_defaults(verbose=False)
    args = parser.parse_args()

    sc = SparkContext(appName='Parallel MF')

    if not args.verbose:
        sc.setLogLevel("ERROR")

    #review -> (u, (i, riu))
    review = sc.textFile(args.review).map(lambda x: tuple(x.split(','))).map(lambda (u, i, riu):(int(i), (int(u), float(riu)))).repartition(args.N).cache()
    testData = readData(args.test, sc)
    simMatrix = sc.textFile(args.sim).map(lambda x : tuple(x.split(','))).map(lambda (i,j,sim) : ((int(i), int(j)), float(sim)))
    RMSE = predict(simMatrix, testData, review, args.N)
    print ('RMSE = : ', RMSE)
