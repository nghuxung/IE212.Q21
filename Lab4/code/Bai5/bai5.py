from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
import os

spark = SparkSession.builder.appName("Bai5").getOrCreate()

def main():

    reviews_df = spark.read.csv(
        "./input/Order_Reviews.csv",
        header=True,
        inferSchema=True,
        sep=";"
    )

    clean_reviews = reviews_df \
        .filter(col("Review_Score").rlike("^[1-5]$")) \
        .withColumn("Review_Score", col("Review_Score").cast("int"))

    avg_score = clean_reviews.select(avg("Review_Score")).collect()[0][0]

    score_dist = clean_reviews.groupBy("Review_Score").count().orderBy("Review_Score")

    output = "\n=== THỐNG KÊ ĐIỂM ĐÁNH GIÁ TRUNG BÌNH, SỐ LƯỢNG ĐÁNH GIÁ THEO TỪNG MỨC ===\n\n"
    output += f"Điểm trung bình: {round(avg_score, 2)}\n\n"

    output += "Số lượng đánh giá theo từng mức\n"
    for row in score_dist.collect():
        output += f"{row['Review_Score']}: {row['count']}\n"

    print(output)

    os.makedirs("output", exist_ok=True)
    with open("output/bai5_ketqua.txt", "w", encoding="utf-8") as f:
        f.write(output)

    spark.stop()

if __name__ == "__main__":
    main()