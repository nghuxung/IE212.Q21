import pandas as pd

df = pd.read_parquet("output/people_count_result.parquet")

print(df.head())

print("\n========== STATISTICS ==========")

print("Total frames:", len(df))

print("Average people:", df["person_count"].mean())

print("Max people:", df["person_count"].max())