// u.data - userid, itemid, rating, timestamp
// List all the movies and the number of ratings
val data = sc.textFile("file:///home/ec2-user/movielens/ml-latest-small/ratings.csv")
// remove header information
val headerlessdata = data.mapPartitionsWithIndex( (idx, iter) => if (idx == 0) iter.drop(1) else iter )
val splitted = headerlessdata.map(x => x.split(","))
val itemid_rating_pairs = splitted.map(x=>(x(1), x(2).toDouble))
val movies_combined = itemid_rating_pairs.combineByKey(List (_), (x: List [Double], y: Double) => y :: x, (x: List[Double], y: List[Double]) => x ::: y)
val result1 = movies_combined.map(x => (x._1, x._2.groupBy(x => x) map { case (k,v) => k-> v.length }))
result1.take(20)

// -----------------------------------------------------------------------
// List all the users and the number of ratings they have done for a movie
// -----------------------------------------------------------------------
var userid_rating = splitted.map(x=>(x(0), 1))
var result2 = userid_rating.reduceByKey((x, y) => x+y)
result2.take(20)

// -----------------------------------------------------------------------
// List all the Movie IDs which have been rated (Movie Id with atleast one user rating it)
// -----------------------------------------------------------------------
val result3 = result1.keys
result3.take(20)

// -----------------------------------------------------------------------
// List all the Users who have rated the movies (Users who have rated atleast one movie)
// -----------------------------------------------------------------------
val result4 = result2.keys
result4.take(20)

// -----------------------------------------------------------------------
// List all the Movies with the max,min,average ratings given by any user
// -----------------------------------------------------------------------
val result5 = movies_combined.map(x => (x._1, x._2.max, x._2.min, x._2.sum.toDouble/x._2.length, x._2.length))
result5.take(20)