import random
import os
import threading
import time
import timeit
import multiprocessing
from fileinput import filename
from os import cpu_count
from time import perf_counter


bitmap = multiprocessing.RawArray('i', (2 ** 27)+1)
# bitmap = [0] * ((2**32+31)//32)

def find_new_line(chunks: list[list[int, int]], file) -> list[list[int, int]]:
    # go through each pair of [start, end]
    # adjust end to the closest start of the line
    # shift current end and next start
    for i in range(len(chunks) - 1):
        shift = 0
        file.seek(chunks[i][1])
        while file.read(1) != b'\n':
            shift += 1
        chunks[i][1] += shift + 1
        chunks[i + 1][0] += shift + 1
    return chunks


def split_file_to_chunks(filename: str, process_count: int) -> list[list[int,int]]:
    # check filesize and create several ranges
    # to read file in chunks

    filesize = os.path.getsize(filename)
    chunks_len = filesize // process_count
    chunks = [[i, i + chunks_len] for i in range(0, filesize - process_count, chunks_len)]
    with open(filename, 'br') as file:
        chunks = find_new_line(chunks, file)
    return chunks


def fill_bitmap(filename: str, chunk: list[int, int]) -> None:
    global bitmap
    start_pos = chunk[0]
    end_pos = chunk[1]
    with open(filename, 'r') as file:
        # move pointer to starting position
        file.seek(start_pos)
        curr_len = 0
        while start_pos + curr_len <= end_pos:
            # read line by line in current chunk
            curr_line = file.readline()
            curr_len += len(curr_line)
            # convert ip to integer
            octets = curr_line.strip().split(".")
            ip_to_number = (int(octets[0]) << 24) | (int(octets[1]) << 16) | (int(octets[2]) << 8) | (int(octets[3]))
            # set corresponding bit in the bitmap
            bitmap[ip_to_number // 32] |= (1 << (ip_to_number % 32))


def count_unique_ips(start_pos: int, end_pos: int) -> int:
    global bitmap
    return sum(1 for x in range(start_pos, end_pos) if (bitmap[x // 32] & (1 << (x % 32))))


def main() -> int:

    start = time.perf_counter()
    # little config
    # filename = "ip_addresses"
    filename = "test_data.txt"
    process_count = multiprocessing.cpu_count()//2

    # split file into chunks [a,b] ]to read in separate processes
    # then modify the ranges, so that a is always at the beginning of a new line
    chunks_ranges = split_file_to_chunks(filename, process_count)
    names = [filename] * process_count

    # read file and fill bitmap to count unique ip
    with multiprocessing.Pool(processes=process_count) as pool:
        pool.starmap(fill_bitmap, zip(names, chunks_ranges))
    end = time.perf_counter()

    print("done reading file and filling bitmap, took: ", end - start)
    start = time.perf_counter()
    # create chunks of [a,b] to split entire bitmap
    chunks_len = 2 ** 32 // process_count
    chunks_ranges = [[i, i + chunks_len] for i in range(0, 2 ** 32 - process_count, chunks_len)]
    with multiprocessing.Pool(processes=process_count) as pool:
        chunk_sum = pool.starmap(count_unique_ips, chunks_ranges)

    res = sum(chunk_sum)
    end = time.perf_counter()
    print("done counting unique number, took: ", end - start)
    return res


if __name__ == "__main__":
    result = main()
    print(result)
