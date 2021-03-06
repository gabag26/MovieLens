import sys
from pyspark import SparkConf, SparkContext
from math import sqrt

#Function to load movie names from the file, Outputs a dictionary with (movieId, movieName)
def loadMovieNames():
    movieNames = {}
    with open("u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1].decode('ascii', 'ignore')
    return movieNames

#Function to load movie content from the file, Outputs a dictionary with (movieId, (contentAttribute1, contentAttribute2, ....., contentAttribute19))
def loadMovieContent():
    movieContent = {}
    with open("u.item") as f:
        for line in f:
            fields = line.split('|')
            movieContent.setdefault(int(fields[0]), [])
            for i in range(5, 24):
            	movieContent[int(fields[0])].append(int(fields[i]))
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

#Invoking spark context on local machine
conf = SparkConf().setMaster("local[*]").setAppName("MovieSimilarities")
sc = SparkContext(conf = conf)

#Loading all the movie names
nameDict = loadMovieNames()

#Loading content attributes for all movies
contentDict = loadMovieContent()

#Reading in the ratings file 
data = sc.textFile("u.data")

#Mapping the file to the required format ((user), (movie, rating))
ratings = data.map(lambda l: l.split()).map(lambda l: (int(l[0]), (int(l[1]), float(l[2]))))

#Self-Join operation- creates all possible ((user1, user2), ((movie1, movie2), (rating1, rating2))) pairs for all users. Contains duplicates with user1 and user2 switched
joinedRatings = ratings.join(ratings)

#Filtering out the duplicates
uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

#Making movie pairs from ((user1, user2), ((movie1, rating1), (movie2, rating2))) to ((movie1, movie2), (rating1, rating2))
moviePairs = uniqueJoinedRatings.map(makePairs)

#Grouping all the pairs to make vectors with movie rating, each dimension is a user
moviePairRatings = moviePairs.groupByKey()

#Using the created vectors to calculate similarity
moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()

#Setting threshold for similarity
scoreThreshold = 0.97

#Setting minimum number of users to have rated both the movies to see if we can trust similarity
coOccurenceThreshold = 50

#MoviedID for Star wars
movieID = int(50)

#Filtering results with target similarity and coOccurence threshold
filteredResults = moviePairSimilarities.filter(lambda((pair,sim)): \
(pair[0] == movieID or pair[1] == movieID) \
and sim[0] > scoreThreshold and sim[1] > coOccurenceThreshold)

#Collecting top 10 results
results = filteredResults.map(lambda((pair,sim)): (sim, pair)).sortByKey(ascending = False).take(10)
print "Top 10 similar movies for " + nameDict[movieID]


contentSim = dict()

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
    for i in range(0, 18):
    	sim += contentSimilarMovieID[i] * contentMovieID[i]
    contentSim[similarMovieID] = sim

#Ranking with the content similarity
for w in sorted(contentSim, key=contentSim.get, reverse=True):
  print nameDict[w]



