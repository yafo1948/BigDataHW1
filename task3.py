import os

PANDAS_FILE = 'mydatapandas.parquet'

DASK_FILE = 'mydatadask.parquet'

PYARROW_FILE = 'mydatapyarrow.parquet'

MYDATA_CSV = 'mydata.csv'


def main():
    file_size = os.path.getsize(MYDATA_CSV)
    print("file size: " + str(file_size) + " bytes")
    middle = int(file_size / 2)
    print("The middle point of the file in bytes: " + str(middle))
    amount_of_lines = first_chunk(middle)
    amount_of_lines += last_chunk(middle)
    print("amount of lines in file: " + str(amount_of_lines))


def first_chunk(middle):
    file = open(MYDATA_CSV, "rb")
    arr = list(file.read(middle).decode(encoding='utf-8'))
    print(arr)
    corrected_amount_of_lines = arr.count('\n') + 1
    print("amount of lines in first chunk: " + str(corrected_amount_of_lines))
    return corrected_amount_of_lines


def last_chunk(middle):
    file = open(MYDATA_CSV, "rb")
    file.seek(middle+1, 0)
    arr1 = list(file.read().decode(encoding='utf-8'))
    print(arr1)
    amount_of_lines = arr1.count('\n')
    print(amount_of_lines)
    return amount_of_lines


if __name__ == '__main__':
    main()

# Task 3.3:	Explain why total number of lines from the first chunk and second chunk is larger than the number of lines
# calculated in the step (1) of Task 2.
# >>> Because the chunks were split mid line:
# >>> First chunk had more lines than line breaks - so we added 1 to the count
# >>> Second chunk had more lines than line breaks -
# >>> but it was balanced with the ending line break so no need to correct
