import os

CHUNK_SIZE = 16 * 1024 * 1024

MYDATA_CSV = 'mydata.csv'


def main():
    file_size = os.path.getsize(MYDATA_CSV)
    print("file size: " + str(file_size) + " bytes")
    print(CHUNK_SIZE)
    current_pos = 0
    lines_counter = 0

    while current_pos < file_size:
        if current_pos + CHUNK_SIZE > file_size:
            lines_counter += last_chunk(current_pos)
        else:
            lines_counter += first_chunk(current_pos)
        current_pos += CHUNK_SIZE
    print("The total number of lines in the file: " + str(lines_counter))


def first_chunk(current_pos):

    with open(MYDATA_CSV, "rb") as file:
        file.seek(current_pos)
        arr = list(file.read(CHUNK_SIZE).decode(encoding='utf-8'))
        print(arr)
        amount_of_line_breaks = arr.count('\n')

    if arr[-1] != '\n':
        amount_of_line_breaks += 1

    if arr[0] == '\n':
        amount_of_line_breaks -= 1

    print("amount of lines in chunk: " + str(amount_of_line_breaks))
    return amount_of_line_breaks


def last_chunk(current_pos):
    file = open(MYDATA_CSV, "rb")
    file.seek(current_pos, 0)
    arr1 = list(file.read().decode(encoding='utf-8'))
    print(arr1)
    amount_of_line_breaks = arr1.count('\n')

    if arr1[0] == '\n':
        amount_of_line_breaks -= 1

    print(amount_of_line_breaks)
    return amount_of_line_breaks


if __name__ == '__main__':
    main()

# 3.5.	Check the algorithm of step (4) with multiple chunks. Define a chunk size to be 16MB. Write a function that
# process “mydata.csv “
# in chunks and count number of lines for each chunk. For example, first chunk will be 0-16MB, second chunk
# 16MB-32BM, and so on, until the last chunk, which might be smaller.
