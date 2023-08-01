# Import the necessary modules
from mrjob.job import MRJob
import re

# Define the Mapper class
class UniqueWordCountMapper(MRJob):
    def mapper(self, _, line):
        # Split the line into words using whitespace as the delimiter
        words = re.findall(r'\w+', line.lower())

        # Emit each word with a count of 1
        for word in words:
            yield word, 1

# Define the Reducer class
class UniqueWordCountReducer(MRJob):
    def reducer(self, word, counts):
        # Sum up the counts for each word to get the total count
        total_count = sum(counts)

        # Emit the word with its total count
        yield word, total_count

# Entry point for running the MapReduce job
if __name__ == '__main__':
    UniqueWordCountMapper.run()
