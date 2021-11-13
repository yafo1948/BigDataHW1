import os

PANDAS_FILE = 'mydatapandas.parquet'

DASK_FILE = 'mydatadask.parquet'

PYARROW_FILE = 'mydatapyarrow.parquet'

MYDATA_CSV = 'mydata.csv'

NUM_OF_RECORDS = 10


def main():
    file_size = os.path.getsize(MYDATA_CSV)
    print("file size: " + str(file_size) + " bytes")
    middle = int(file_size / 2)
    print(middle)
    amount_of_lines = first_chunk(middle)
    amount_of_lines += last_chunk(middle)
    print("The amount of lines in the file: " + str(amount_of_lines))


def first_chunk(middle):
    file = open(MYDATA_CSV, "rb")
    arr = list(file.read(middle).decode(encoding='utf-8'))
    print(arr)
    amount_of_line_breaks_in_chunk = arr.count('\n')

    if arr[-1] != '\n':
        amount_of_line_breaks_in_chunk += 1

    print("amount of lines in first chunk: " + str(amount_of_line_breaks_in_chunk))
    return amount_of_line_breaks_in_chunk


def last_chunk(middle):
    file = open(MYDATA_CSV, "rb")
    file.seek(middle+1, 0)
    arr1 = list(file.read().decode(encoding='utf-8'))
    print(arr1)
    amount_of_line_breaks_in_chunk = arr1.count('\n')

    if arr1[0] == '\n':
        amount_of_line_breaks_in_chunk -= 1

    print(amount_of_line_breaks_in_chunk)
    return amount_of_line_breaks_in_chunk


if __name__ == '__main__':
    main()

# 3.4.	Suggest an algorithm to resolve the issue from the step (3) and implement it.
# 1. Read 'middle' amount of bytes from the file
# 2. Count the number of line breaks ('\n') for the first chunk
# 3. If the last char on the chunk is Not a Line break ('\n) -> add 1 to the count
# 4. Seek to middle+1 bytes on the file
# 5. Count the number of line breaks ('\n') for the second chunk
# 6. If the first char on the chunk is a Line break ('\n') -> subtract 1 from the count
# 7. Return the sum of lines from both chunks
