import os
import re
from collections import defaultdict

def clean_text(text):
    # Replace special characters and numerals with space
    cleaned_text = re.sub(r'[^a-zA-Z\s]', ' ', text)
    # Convert all words to lowercase
    cleaned_text = cleaned_text.lower()
    return cleaned_text

def index_files(directory):
    inverted_index = defaultdict(lambda: defaultdict(int))

    for filename in os.listdir(directory):
        with open(os.path.join(directory, filename), 'r', encoding='utf-8') as file:
            doc_id, content = file.read().split('\t', 1)
            content = clean_text(content)
            words = content.split()

            for word in words:
                # Increment the count of the word in the current document
                inverted_index[word][doc_id] += 1

        print(f"Processed file: {filename}")

    return inverted_index

def write_index_to_file(index, filename):
    with open(filename, 'w') as file:
        for word, doc_counts in index.items():
            file.write(f"{word} ")
            for doc_id, count in doc_counts.items():
                file.write(f"{doc_id}:{count} ")
            file.write("\n")

def create_unigram_index():
    fulldata_index = index_files('fulldata')
    write_index_to_file(fulldata_index, 'unigram_index.txt')

# Creating unigram index
create_unigram_index()