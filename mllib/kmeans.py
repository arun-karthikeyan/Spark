#KMeans Model from Spark MLLib

import numpy as np
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.clustering import KMeans
from pyspark.mllib.random import RandomRDDs #For random clusters

#generating random data for use
c1_v = RandomRDDs.normalVectorRDD(sc,20,2,numPartitions=2,seed=1L).map(lambda v:np.add([1,5], v))

c2_v = RandomRDDs.normalVectorRDD(sc,16,2,numPartitions=2,seed=2L).map(lambda v:np.add([5,1], v))

c3_v = RandomRDDs.normalVectorRDD(sc,12,2,numPartitions=2,seed=3L).map(lambda v:np.add([4,6], v))

#concatenate the randomly generated datasets using the union function

c12 = c1_v.union(c2_v)
mydata = c12.union(c3_v)

mykmmodel = KMeans.train(mydata, k=1, maxIterations=20, runs=1, initializationMode='k-means||', seed=10L)

#we can use help(KMeans.train) to see the available parameter options
#k is the number of desired clusters
#maxIterations is the number of iterations to run
#initializationMode specifies wither random initializations or initialization by running k-means on a small sample (k-means||)
#runs is the number of times to run the K-Means algorithm with different initial starting points for cluster centers

#dir(mykmmodel) can be used to see the available functions on the trained K-Means model

#The compute cost funcion might not be available on the cloudera vm, spark mllib, it computes the Sum Squared Error, mykmmodel.computeCost(mydata)

#defining custom computeCost alternative
def getsse(point):
	this_center = mykmmodel.centers[mykmmodel.predict(point)]
	return (sum([x**2 for x in (point-this_center)]))

computeCost = mydata.map(getsse).collect() #collect the sum of squared errors corresponding to all points
print np.array(computeCost).mean() #display the average error
