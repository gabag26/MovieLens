import sys
from pyspark import SparkConf, SparkContext
from math import sqrt

#A little cheat sheet to run on cluster
#aws s3 cp s3://sahilgaba.com/FinalProject1M.py ./
#aws s3 cp s3://sahilgaba.com/ml-1m/movies.dat ./
#spark-submit --executor-memory 1g FinalProject1M.py
#./bin/spark-submit --executor-memory 1g MovieSimilarities1M.py
#ssh -i /Users/sahilgaba/Desktop/SparkCourse/myNewSpark.pem hadoop@ec2-54-149-70-212.us-west-2.compute.amazonaws.com

#Function to load movie names from the file, Outputs a dictionary with (movieId, movieName)
def loadMovieNames():
    movieNames = {}
    with open("movies.dat") as f:
        for line in f:
            fields = line.split('::')
            movieNames[int(fields[0])] = fields[1].decode('ascii', 'ignore')
    return movieNames

#Function to load movie content from the file, Outputs a dictionary with (movieId, (contentAttribute1, contentAttribute2, ....., contentAttribute19))
def loadMovieContent():
    movieContent = {}
    with open("movies.dat") as f:
        for line in f:
            fields = line.split('::')
            movieContent.setdefault(int(fields[0]), [])
            genres = fields[2].split('|')
            for i in range(0, len(genres)):
            	movieContent[int(fields[0])].append(str(genres[i]).rstrip())
    return movieContent

#Function that takes ((user1, user2), ((movie1, rating1), (movie2, rating2))) and returns movie pairs as ((movie1, movie2), (rating1, rating2))
def makePairs((user, ratings)):
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return ((movie1, movie2), (rating1, rating2))

#Function that filters duplicates after self-join: filters out one of ((user1, user2), ((movie1, rating1), (movie2, rating2))) and ((user2, user1), ((movie2, rating2), (movie1, rating1)))
def filterDuplicates( (userID, ratings) ):
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return movie1 < movie2
    
#Function that calculates cosine similarity for ((movie1, movie2), ((rating11, rating21), (rating12, rating22), ....(rating1n, rating2n)))
def computeCosineSimilarity(ratingPairs):
    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in ratingPairs:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1
    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)
    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))
    return (score, numPairs)

#Invoking spark on cloud
conf = SparkConf()
sc = SparkContext(conf = conf)

#configuring Hadoop with your key to read files from S3 service
#ADD YOUR INFORMATION HERE
sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "---")
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "---")

#Loading all the movie names
nameDict = loadMovieNames()

#Loading content attributes for all movies
contentDict = loadMovieContent()

#Reading in the ratings file 
#CHANGE PATH TO YOUR BUCKET
data = sc.textFile("s3n://<BucketName>/ratings.dat")

#Mapping the file to the required format ((user), (movie, rating))
ratings = data.map(lambda l: l.split("::")).map(lambda l: (int(l[0]), (int(l[1]), float(l[2]))))

#Self-Join operation- creates all possible ((user1, user2), ((movie1, movie2), (rating1, rating2))) pairs for all users. Contains duplicates with user1 and user2 switched
#Partitioning into 100 parts as next step is very expensive
ratingsPartitioned = ratings.partitionBy(100)

#Self-Join operation- creates all possible ((user1, user2), ((movie1, movie2), (rating1, rating2))) pairs for all users. Contains duplicates with user1 and user2 switched
joinedRatings = ratingsPartitioned.join(ratingsPartitioned)

#Filtering out the duplicates
uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

#Making movie pairs from ((user1, user2), ((movie1, rating1), (movie2, rating2))) to ((movie1, movie2), (rating1, rating2))
moviePairs = uniqueJoinedRatings.map(makePairs).partitionBy(100)

#Grouping all the pairs to make vectors with movie rating, each dimension is a user
moviePairRatings = moviePairs.groupByKey()

#Using the created vectors to calculate similarity
moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()

#Setting threshold for similarity
scoreThreshold = 0.96

#Setting minimum number of users to have rated both the movies to see if we can trust similarity
coOccurenceThreshold = 1000

#MoviedID for Star wars
movieID = int(260)

#Filtering results with target similarity and coOccurence threshold
filteredResults = moviePairSimilarities.filter(lambda((pair,sim)): \
(pair[0] == movieID or pair[1] == movieID) \
and sim[0] > scoreThreshold and sim[1] > coOccurenceThreshold)

#Collecting top 10 results
results = filteredResults.map(lambda((pair,sim)): (sim, pair)).sortByKey(ascending = False).take(10)
print "Top 10 similar movies for " + nameDict[movieID]

contentSim = {}
#Content vector for Star Wars
contentMovieID = contentDict[movieID]
for result in results:
    (sim, pair) = result
    similarMovieID = pair[0]
    if (similarMovieID == movieID):
        similarMovieID = pair[1]
    contentSimilarMovieID = contentDict[similarMovieID]
    sim = 0
        #Calculating the content similarity
    for i in range(0, len(contentMovieID)):
    	for j in range(0, len(contentSimilarMovieID)):
    		if(contentSimilarMovieID[j] == contentMovieID[i]):
    			sim += 1
    contentSim[similarMovieID] = sim

#Ranking with the content similarity
for w in sorted(contentSim, key=contentSim.get, reverse=True):
  print nameDict[w], contentSim[w]


