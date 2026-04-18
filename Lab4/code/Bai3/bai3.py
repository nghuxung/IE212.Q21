from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName("Bai3").getOrCreate()

def main():

    customer_df = spark.read.csv("./input/Customer_List.csv", header=True, inferSchema=True, sep=";")
    orders_df = spark.read.csv("./input/Orders.csv", header=True, inferSchema=True, sep=";")

    orders_with_country = orders_df.join(customer_df, "Customer_Trx_ID", "inner")

    orders_by_country = orders_with_country.groupBy("Customer_Country").count()
    
    result = orders_by_country.orderBy("count", ascending=False)

    output = "=== ĐƠN HÀNG THEO QUỐC GIA ===\n"

    for row in result.collect():
        output += f"{row['Customer_Country']}: {row['count']}\n"

    print(output)

    os.makedirs("output", exist_ok=True)
    with open("output/bai3_ketqua.txt", "w", encoding="utf-8") as f:
        f.write(output)

    spark.stop()

if __name__ == "__main__":
    main()