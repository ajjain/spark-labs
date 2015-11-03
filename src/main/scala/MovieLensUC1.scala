// List all the movies and the number of ratings
val data = sc.textFile("file:///home/ec2-user/movielens/ml-100k/u.data")
val pairs = data.map(x => (x.split("\t")(1), (x.split("\t")(2))))
val combined = pairs.combineByKey(List (_), (x: List [String], y: String) => y :: x, (x: List[String], y: List[String]) => x ::: y)
val result = combined.map(x => (x._1, x._2.groupBy(x => x) map { case (k,v) => k-> v.length }))
result.saveAsTextFile("file:///home/ec2-user/movielens/output/u.data")