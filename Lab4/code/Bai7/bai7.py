from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

def main():
    spark = SparkSession.builder.appName("Bai7").getOrCreate()

    path_items = "file://" + os.path.abspath("./input/Order_Items.csv")
    path_reviews = "file://" + os.path.abspath("./input/Order_Reviews.csv")

    items_df = spark.read.option("header", "true").option("sep", ";").option("multiLine", "true").csv(path_items)
    reviews_df = spark.read.option("header", "true").option("sep", ";").option("multiLine", "true").csv(path_reviews)

    reviews_df = reviews_df.withColumn("Review_Score", F.col("Review_Score").cast("double"))

    stats_df = items_df.join(reviews_df, "Order_ID") \
        .groupBy("Product_ID") \
        .agg(
            F.count("Order_Item_ID").alias("Total_Sales"),
            F.avg("Review_Score").alias("Avg_Rating")
        ) \
        .orderBy(F.col("Total_Sales").desc())

    all_products = stats_df.collect()

    best_seller = all_products[0]

    output = "\n=== SẢN PHẨM CÓ SỐ LƯỢNG BÁN RA CAO NHẤT VÀ TÍNH ĐIỂM ĐÁNH GIÁ TRUNG BÌNH CHO TỪNG SẢN PHẨM ===\n\n"
    
    output += "Sản phẩm bán chạy nhất:\n"
    output += f"ID: {best_seller['Product_ID']}\n"
    output += f"Số lượng bán ra: {best_seller['Total_Sales']}\n"
    output += f"Điểm đánh giá trung bình: {round(best_seller['Avg_Rating'], 2) if best_seller['Avg_Rating'] else 0.0}\n\n"

    output += "Danh sách thống kê tất cả sản phẩm:\n"
    
    for row in all_products:
        avg_score = round(row['Avg_Rating'], 2) if row['Avg_Rating'] is not None else 0.0
        output += f"ID: {row['Product_ID']} | Bán ra: {row['Total_Sales']} | Đánh giá TB: {avg_score}\n"
    
    print(output)

    os.makedirs("output", exist_ok=True)
    with open("output/bai7_ketqua.txt", "w", encoding="utf-8") as f:            
        f.write(output)

    spark.stop()

if __name__ == "__main__":
    main()