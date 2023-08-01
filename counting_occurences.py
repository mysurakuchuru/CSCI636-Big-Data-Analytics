from mrjob.job import MRJob
import re

class BigramCountMapper(MRJob):
    def mapper(self, _, line):
        # Split the line into words using whitespace as the delimiter
        words = re.findall(r'\w+', line.lower())

        # Emit each word bigram as a key-value pair
        for i in range(len(words) - 1):
            bigram = words[i] + ',' + words[i+1]
            yield bigram, 1

class BigramCountReducer(MRJob):
    def reducer(self, bigram, counts):
        # Sum up the counts for each word bigram to get the total count
        total_count = sum(counts)

        # Emit the word bigram with its total count
        yield bigram, total_count

if __name__ == '__main__':
    BigramCountMapper.run()
