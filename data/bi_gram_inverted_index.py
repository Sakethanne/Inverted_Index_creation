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
            # Create pairs of adjacent words
            word_pairs = zip(words, words[1:])
            
            for word1, word2 in word_pairs:
                bigram = f"{word1} {word2}"
                inverted_index[bigram][doc_id] += 1

        print(f"Processed file: {filename}")

    return inverted_index

def write_index_to_file(index, filename):
    with open(filename, 'w') as file:
        for word, doc_counts in index.items():
            file.write(f"{word} ")
            for doc_id, count in doc_counts.items():
                file.write(f"{doc_id}:{count} ")
            file.write("\n")

def create_bigram_index():
    selected_bigrams = ["computer science", "information retrieval", "power politics", "los angeles", "bruce willis"]
    devdata_index = index_files('devdata')
    selected_bigram_index = defaultdict(lambda: defaultdict(int))

    for bigram in selected_bigrams:
        word1, word2 = bigram.split()
        for doc_id, counts in devdata_index[bigram].items():
            selected_bigram_index[bigram][doc_id] = counts

        print(f"Processed bigram: {bigram}")

    write_index_to_file(selected_bigram_index, 'selected_bigram_index.txt')

# Creating bigram index
create_bigram_index()