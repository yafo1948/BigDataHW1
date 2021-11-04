# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import random

import numpy as np
import pandas as pd

NUM_OF_RECORDS = 10


def main():
    fruit=['Orange', 'Grape', 'Apple', 'Banana', 'Pineapple', 'Avocado']
    color=['Red', 'Green', 'Yellow', 'Blue']
    rng = np.random.default_rng(123)
    df = pd.DataFrame()
    df["fruit"] = np.random.choice(fruit, NUM_OF_RECORDS)
    df["price"] = rng.integers(low=10, high=101, size=NUM_OF_RECORDS)
    df["color"] =np.random.choice(color, NUM_OF_RECORDS)
    df["id"] = df.index+1
    df.to_csv('mydata.csv', index=False)
    print("end")



if __name__ == '__main__':
    main()


