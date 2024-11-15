import pandas as pd

file_path = r"D:\SFU\CMPT 732 Big Data\Final\archive(4)\fraudTest.csv"

data = pd.read_csv(file_path)

print(data.head())
