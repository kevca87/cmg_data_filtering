import pandas as pd
import spacy
import time
import multiprocessing as mp
from multiprocessing import Value, Lock
import os
from tqdm import tqdm

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
        'upgrade':0
    }
tokenizer = spacy.load("en_core_web_sm")

chunks_dir = '../DATA/chunks'
if not os.path.exists(chunks_dir):
    os.makedirs(chunks_dir)

# lock = Lock()
# num_chunks = Value('i',0)

def contain_verb(commit_message):
    doc = tokenizer(commit_message)
    message_verb = None
    for token in doc:
        if token.pos_ == 'VERB':
            message_verb = token.lemma_
            break
    if message_verb != None:
        try:
            verbs[message_verb] += 1
        except:
            message_verb = None
    return  message_verb

def process_chunk(chunk:pd.DataFrame):
    # global num_chunks
    # with lock:
    #     num_chunks.acquire()
    #     num_chunks.value += 1
    #     print(num_chunks.value)
    #     num_chunks.release()
    # tqdm.pandas( position=num_chunks )
    # num_chunks = num_chunks + 1
    commit_id_first = chunk['commit'].iloc[0]
    commit_id_last = chunk['commit'].iloc[-1]
    print(commit_id_first)
    print(type(commit_id_first))
    chunk['verb'] = chunk['message'].apply(contain_verb)
    chunk_filename = f'{chunks_dir}/chunk_{commit_id_first}_{commit_id_last}.csv'
    chunk.to_csv(chunk_filename)
    print(f'Saving to {chunk_filename}')
    return chunk

if __name__ == '__main__':

    data_file_name = 'full.csv'
    data_directory = '../DATA'
    data_file_path = f'{data_directory}/{data_file_name}'

    chunk_size = 1000
    
    print(f'Starting to read "{data_file_path}" ...')
    chunks = pd.read_csv(data_file_path, chunksize=chunk_size,usecols=['commit','message','repo'],nrows=10000)
    
    pool = mp.Pool(processes=4)
    results = pool.map(process_chunk, chunks)
    
    pool.close()
    pool.join()

    combined_df = pd.concat(results, ignore_index=True)