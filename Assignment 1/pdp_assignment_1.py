# @Author: Sjors Grooff
# Date: 09-09-2021
# Command: python pdp_assignment_1.py --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar  -r hadoop hdfs:///user/maria_dev/ml-100k/u.data

# Import necessary libraries
from mrjob.job import MRJob
from mrjob.step import MRStep

class CountMovieRatings(MRJob):
	
	# Define the steps that need to be executed.
	def steps(self):
		return [
			MRStep(mapper=self.mapper_get_ratings,
				reducer=self.reducer_count_ratings_by_movie),				
			MRStep(reducer=self.reducer_sort_movie_by_rating_count)
			]

	# Load the data by splitting the incoming lines. 
    # We only keep the movie id and add a 1 to each tuple to be able to sum later.
	def mapper_get_ratings(self, _, line):
		(user_id, movie_id, rating, timestamp) = line.split('\t')
		yield movie_id, 1
	
	# Sum all the "1" values we have put in the tuple grouped my movie
	def reducer_count_ratings_by_movie(self, movie_id, value):
		yield None, (sum(value), movie_id)
	
	# Sorts the movies by number of ratings and reverse so we have the most rated movie first.
	def reducer_sort_movie_by_rating_count(self, _, rating_counts):
		for count, key in sorted(rating_counts, reverse=True):
    			yield (key, int(count))

# Mandatory line to make the script run.
if __name__ == '__main__':
	CountMovieRatings.run()