import pandas as pd
import spacy
import time
import multiprocessing as mp
from multiprocessing import Value, Lock
import os
from tqdm import tqdm
import sys

verbs = {
        'add':0,
        'fix':0,
        'use':0,
        'update':0,
        'remove':0,
        'make':0,
        'change':0,
        'move':0,
        'allow':0,
        'improve':0,
        'implement':0,
        'create':0,
        'upgrade':0,
        'feat':0,#
        'doc':0,
        'style':0,
        'refactor':0,
        'perf':0,
        'test':0,
        'chore':0,
        'hotfix':0
    }
tokenizer = spacy.load("en_core_web_sm")

chunks_dir = '../DATA/chunks'
if not os.path.exists(chunks_dir):
    os.makedirs(chunks_dir)

# lock = Lock()
# num_chunks = Value('i',0)

def get_verb(doc_tokenized):
    doc_verb = None
    for token in doc_tokenized:
        if token.pos_ == 'VERB':
            doc_verb = token.lemma_
            break
    if doc_verb != None:
        try:
            verbs[doc_verb] += 1
        except:
            doc_verb = None
    return  doc_verb

def row_processing(row):
    commit_message = row['message']
    commit_message_tokenized = tokenizer(commit_message)
    # row['message_tokenized'] = commit_message_tokenized
    commit_message_verb = get_verb(commit_message_tokenized)
    row['verb'] = commit_message_verb
    row['mask'] = commit_message_verb != None
    return row

def process_chunk(chunk:pd.DataFrame):
    # global num_chunks
    # with lock:
    #     num_chunks.acquire()
    #     num_chunks.value += 1
    #     print(num_chunks.value)
    #     num_chunks.release()
    # tqdm.pandas( position=num_chunks )
    # num_chunks = num_chunks + 1
    commit_id_first = chunk['commit'].iloc[0][:7]
    commit_id_last = chunk['commit'].iloc[-1][:7]
    chunk_first_element_index = chunk.index[0]
    chunk = chunk.apply(row_processing,axis=1)
    chunk_filename = f'{chunks_dir}/chunk_{chunk_first_element_index:08d}_{commit_id_first}_{commit_id_last}.csv'
    chunk.to_csv(chunk_filename,index=True)
    print(f'Saving to {chunk_filename}')
    return chunk

if __name__ == '__main__':
    
    data_file_name = 'full.csv'
    data_directory = '../DATA'
    data_file_path = f'{data_directory}/{data_file_name}'

    chunk_size = 100000
    
    print(f'Starting to read "{data_file_path}" ...')
    chunks = pd.read_csv(data_file_path, chunksize=chunk_size,usecols=['repo','author','commit','message'])
    pool = mp.Pool(processes=4)
    results = pool.map(process_chunk, chunks)
    
    pool.close()
    pool.join()

    combined_df = pd.concat(results)
    combined_df.to_csv('concatenated.csv')