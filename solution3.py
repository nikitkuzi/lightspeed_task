import time
import os
import multiprocessing
import hashlib
import math


def hash(item):
    hash_value = hashlib.sha1(item.encode('utf8')).hexdigest()
    return int(hash_value, 16)


def rho(x):
    return (x ^ (x - 1)).bit_length()


def add(item):
    # Hash the item
    hash_value = hash(item)
    # Choose a register based on the first b bits of the hash
    register_index = hash_value & (m - 1)
    # Update the register with the rank (number of leading zeros) of the hash value
    registers[register_index] = max(registers[register_index], rho(hash_value >> b))


def count_sum(start, end):
    return sum(0.5 ** reg for reg in registers[start:end])


def count_zeros(start, end):
    return sum(1 for reg in registers[start:end] if reg == 0)


def estimate():
    # Compute the raw HyperLogLog estimate
    chunks_len = len(registers) // process_count
    chunks = [[i, i + chunks_len] for i in range(0, len(registers) - process_count, chunks_len)]
    chunks[-1][1] = len(registers)
    with multiprocessing.Pool(processes=process_count) as pool:
        partial_sum = pool.starmap(count_sum, chunks)
    Z = 1.0 / sum(partial_sum)
    E = alphaMM * Z

    # Apply the correction for very small or large cardinalities
    # If E <= 2.5 * m, use the raw estimate (adjusted)
    if E <= 2.5 * m:
        with multiprocessing.Pool(processes=process_count) as pool:
            partial_zeros = pool.starmap(count_zeros, chunks)
        V = sum(partial_zeros)
        if V > 0:
            E = m * math.log(m / V)

    # If the estimate is very large, apply a different correction
    if E > 1 / 30.0:
        E = -(2 ** 32) * math.log(1 - E / 2 ** 32)

    return E


def find_new_line(chunks: list[list[int]], file_with_ip: str) -> list[list[int]]:
    # go through each pair of [start, end]
    # adjust end to the closest start of the line
    # shift current end and next start

    with open(file_with_ip, 'br') as file:
        for i in range(len(chunks) - 1):
            shift = 0
            file.seek(chunks[i][1])
            while file.read(1) != b'\n':
                shift += 1
            chunks[i][1] += shift + 1
            chunks[i + 1][0] += shift + 1
    return chunks


def split_file_to_chunks(file_with_ip: str, process_count: int) -> list[list[int]]:
    # check filesize and create several ranges
    # to read file in chunks

    filesize = os.path.getsize(file_with_ip)
    chunks_len = filesize // process_count
    chunks = [[i, i + chunks_len] for i in range(0, filesize - process_count, chunks_len)]
    chunks[-1][1] = filesize
    chunks = find_new_line(chunks, file_with_ip)
    return chunks


def read_chunk(file_with_ip: str, chunk: list[int, int]):
    start_pos = chunk[0]
    end_pos = chunk[1]
    with open(file_with_ip, 'r') as file:
        # move pointer to starting position
        file.seek(start_pos)
        while start_pos < end_pos:
            # read line by line in current chunk
            curr_line = file.readline()
            start_pos += len(curr_line)
            # hash the IP and add it to HyperLogLog count
            ip_hash = hashlib.sha256(curr_line.encode('utf-8')).hexdigest()
            add(ip_hash)


b = 15  # Number of bits used for registers (log2 of the number of registers)
m = 2 ** b  # Number of registers (2^b)
alphaMM = (0.7213 / (1 + 1.079 / m)) * m * m  # Correction factor for small m
registers = multiprocessing.RawArray('i', m)  # Initialize the registers to 0
process_count = multiprocessing.cpu_count() // 2


def main():
    # file_with_ip = 'test_data3.txt' # 1000
    file_with_ip = "test_data.txt"  # 9988184
    # file_with_ip = "test_data2.txt"  # 98845647
    # file_with_ip = "ip_addresses" # 1000000000
    names = [file_with_ip] * process_count

    # split file into chunks [a,b] ]to read in separate processes
    # then modify the ranges, so that a is always at the beginning of a new line
    chunks_ranges = split_file_to_chunks(file_with_ip, process_count)

    start_time = time.perf_counter()

    # read file and apply Hyper Log Log
    with multiprocessing.Pool(processes=process_count) as pool:
        pool.starmap(read_chunk, zip(names, chunks_ranges))
    # count uniques
    unique_ips = estimate()
    end_time = time.perf_counter()
    print("done reading file and counting, took: ", end_time - start_time)
    return unique_ips


if __name__ == "__main__":
    res = main()
    print(res)
