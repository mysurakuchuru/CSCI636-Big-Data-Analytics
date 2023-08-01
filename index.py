from mrjob.job import MRJob
import re

# Regular expression to match words
WORD_RE = re.compile(r"\b\w+\b")

class InvertedIndex(MRJob):

    def mapper(self, _, line):
        # Extract the document ID and content from the input line
        doc_id, content = line.strip().split(': ', 1)

        # Tokenize the content and emit each word with the document ID
        for word in WORD_RE.findall(content):
            yield word.lower(), doc_id

    def combiner(self, word, doc_ids):
        # Combine the document IDs for each word locally
        doc_ids_list = list(doc_ids)
        yield word, doc_ids_list

    def reducer(self, word, doc_ids):
        # Merge the document IDs and remove duplicates
        unique_doc_ids = list(set(doc_id for doc_ids_list in doc_ids for doc_id in doc_ids_list))

        # Sort the document IDs for consistent output
        unique_doc_ids.sort()

        # Yield the word and its corresponding list of document IDs
        yield word, unique_doc_ids

if __name__ == '__main__':
    InvertedIndex.run()
