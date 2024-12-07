import os
import time
import multiprocessing


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


def fill_bitmap(file_with_ip: str, bitmap_file: str, chunk: list[int, int]) -> None:
    start_pos = chunk[0]
    end_pos = chunk[1]
    with open(bitmap_file, 'rb+') as bitmap:
        with open(file_with_ip, 'r') as file:
            # move pointer to starting position
            file.seek(start_pos)
            while start_pos < end_pos:
                # read line by line in current chunk
                curr_line = file.readline()
                start_pos += len(curr_line)
                # convert ip to integer
                octets = curr_line.strip().split(".")
                ip_to_number = (int(octets[0]) << 24) | (int(octets[1]) << 16) | (int(octets[2]) << 8) | (
                    int(octets[3]))
                # set corresponding bit in the bitmap
                # and write it to the bitmap file
                bitmap.seek(ip_to_number // 8)
                byte = ord(bitmap.read(1))
                byte |= (1 << (7 - (ip_to_number % 8)))
                bitmap.seek(-1, 1)
                bitmap.write(bytes([byte]))


def count_unique_ips(bitmap_file: str, chunk: list[int, int]) -> int:
    # adjust ranges
    # and count set bits in the bytes of the bitmap file
    start_pos = chunk[0] // 8
    end_pos = chunk[1] // 8
    with open(bitmap_file, 'rb') as file:
        sm = 0
        file.seek(start_pos)
        while start_pos < end_pos:
            byte = file.read(1)
            sm += bin(ord(byte)).count('1')
            start_pos += 1
    return sm


def main() -> int:
    start_time = time.perf_counter()
    # little config
    # file_with_ip = "ip_addresses"
    # file_with_ip = 'test_data3.txt' # 1000
    # file_with_ip = "test_data.txt" # 9988184
    file_with_ip = "test_data2.txt"  # 98845647
    bitmap_file = "bitmap.dat"
    process_count = multiprocessing.cpu_count() // 2
    file_names = [file_with_ip] * process_count
    bitmap_names = [bitmap_file] * process_count
    with open(bitmap_file, 'wb') as file:
        file.write(bytearray(2 ** 29))

    # split file into chunks [a,b] to read in separate processes
    # then modify the ranges, so that a is always at the beginning of a new line
    chunks_ranges = split_file_to_chunks(file_with_ip, process_count)

    # read file and fill bitmap to count unique ip
    with multiprocessing.Pool(processes=process_count) as pool:
        pool.starmap(fill_bitmap, zip(file_names, bitmap_names, chunks_ranges))
    end_time = time.perf_counter()

    print("done reading file and filling bitmap, took: ", end_time - start_time)

    start_time = time.perf_counter()

    # create chunks of [a,b] to split entire bitmap
    chunks_len = 2 ** 32 // process_count
    chunks_ranges = [[i, i + chunks_len] for i in range(0, 2 ** 32 - process_count, chunks_len)]
    chunks_ranges[-1][1] = 2 ** 32

    with multiprocessing.Pool(processes=process_count) as pool:
        chunk_sum = pool.starmap(count_unique_ips, zip(bitmap_names, chunks_ranges))
    res = sum(chunk_sum)

    end_time = time.perf_counter()
    print("done counting unique number, took: ", end_time - start_time)
    # remove file if needed
    # os.remove(bitmap_file)
    return res


if __name__ == "__main__":
    result = main()
    print(result)
