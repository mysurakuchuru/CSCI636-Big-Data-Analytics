from mrjob.job import MRJob
import re

# List of stopwords
STOPWORDS = set(['the', 'and', 'of', 'a', 'to', 'in', 'is', 'it'])

class NonStopWordCountMapper(MRJob):
    def mapper(self, _, line):
        # Split the line into words using whitespace as the delimiter
        words = re.findall(r'\w+', line.lower())

        # Emit each non-stopword with a count of 1
        for word in words:
            if word not in STOPWORDS:
                yield word, 1

    # Optional: Combiner function to optimize network traffic
    def combiner(self, word, counts):
        # Sum up the counts for each word before sending them to the reducer
        total_count = sum(counts)
        yield word, total_count

class NonStopWordCountReducer(MRJob):
    def reducer(self, word, counts):
        # Sum up the counts for each non-stopword to get the total count
        total_count = sum(counts)

        # Emit the non-stopword with its total count
        yield word, total_count

if __name__ == '__main__':
    NonStopWordCountMapper.run()

