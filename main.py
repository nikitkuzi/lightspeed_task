import random
import os
import timeit



def setup():
    with open("bitmap.bin", "wb") as file:
        file.write(b'0' * 2 **32)


def main():
    if not os.path.exists("bitmap.bin"):
        setup()
        print("done")

    distinct = 0
    with open("bitmap.bin", "rb+") as bitmap:
        with open("test_data.txt", 'r') as test_data:
            for line in test_data:
                bytes_in_ip = [int(num) for num in line[:-1].split(".")]
                ip_converted_to_num = (bytes_in_ip[0] << 24) | (bytes_in_ip[1] << 16) | (bytes_in_ip[2] << 8) | (
                bytes_in_ip[3])
                bitmap.seek(ip_converted_to_num)
                bit = bitmap.read(1)
                if bit[0] == 48:
                    distinct += 1
                    bitmap.seek(ip_converted_to_num)
                    bitmap.write(b'1')
    return distinct


if __name__ == "__main__":
    result = main()
    print(result)
