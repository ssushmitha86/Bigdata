import re
import requests
import mmap
import os
import psutil
from collections import Counter
import time
from multiprocessing import Pool
import matplotlib.pyplot as plt
import numpy as np
import threading

def download_stopwords():
    url = "https://gist.githubusercontent.com/sebleier/554280/raw/7e0e4a1ce04c2bb7bd41089c9821dbcf6d0c786c/NLTK's%2520list%2520of%2520english%2520stopwords"
    response = requests.get(url)
    return set(response.text.lower().split())

def process_chunk(args):
    chunk, stop_words = args
    chunk = re.sub(r'[^a-zA-Z\-]+', ' ', chunk)
    words = chunk.split()
    return Counter(word for word in words if word.lower() not in stop_words)

def top_k_frequent_words(content, k, stop_words, num_processes=10):
    chunk_size = len(content) // (num_processes*5)
    """chunk_size = len(content) // (num_processes*5) ----  We multiply the processes by 5 to reduce the chunk size for the 2.5GB File
    chunk_size = len(content) // (num_processes*30) ---- We multiply the processes by 30 to reduce the chunk size for the 16GB File"""
    chunks = [content[i:i+chunk_size] for i in range(0, len(content), chunk_size)]

    with Pool(processes=num_processes) as pool:
        counters = pool.map(process_chunk, [(chunk, stop_words) for chunk in chunks])

    total_counter = sum(counters, Counter())
    return total_counter.most_common(k)

def analyze_performance(file_path, k, stop_words, num_processes):
    with open(file_path, 'r', encoding='utf-8') as file:
        content = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
        content = content.read().decode('utf-8')

    process = psutil.Process(os.getpid())

    start_time = time.time()

    cpu_percentages = []

    def monitor_cpu():
        while getattr(cpu_monitor_thread, "do_run", True):
            cpu_percentages.append(psutil.cpu_percent(percpu=True))
            time.sleep(1)

    cpu_monitor_thread = threading.Thread(target=monitor_cpu)
    cpu_monitor_thread.start()

    top_k_words = top_k_frequent_words(content, k, stop_words, num_processes)
    end_time = time.time()

    cpu_monitor_thread.do_run = False
    cpu_monitor_thread.join()

    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.2f} seconds")
    print(f"Top {k} most frequent words: {top_k_words}")

    avg_cpu_percentages = np.mean(cpu_percentages, axis=0)
    overall_cpu_usage = np.mean(avg_cpu_percentages)

    memory_usage = process.memory_info().rss / 1024 ** 2
    print(f"CPU utilization: {overall_cpu_usage:.2f}%")
    print(f"Memory usage: {memory_usage:.2f} MB")

    plot_cpu_utilization(cpu_percentages)

def plot_cpu_utilization(cpu_percentages):
    avg_cpu_percentages = np.mean(cpu_percentages, axis=0)
    num_cores = len(avg_cpu_percentages)
    x = np.arange(num_cores)

    plt.bar(x, avg_cpu_percentages)
    plt.xticks(x, [f"Core {i+1}" for i in range(num_cores)])
    plt.xlabel("CPU Cores")
    plt.ylabel("CPU Utilization (%)")
    plt.title("CPU Utilization During Execution")
    plt.savefig("cpu_utilization.png")
    plt.show()

def main():
    stop_words = download_stopwords()
    k = 7
    cp = "/Users/mohammeddanishhussain/Downloads/dataset_updated/dataset_updated/"
    file_paths = [cp + "data_2.5GB.txt"]

    num_processes = os.cpu_count()
    print(f"Using {num_processes} processes...")

    for file_path in file_paths:
        print(f"Processing {file_path}...")
        analyze_performance(file_path, k, stop_words, num_processes)
        print("\n")

if __name__ == "__main__":
    main()

