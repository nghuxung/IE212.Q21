from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month
import os

spark = SparkSession.builder.appName("Bai4").getOrCreate()

def main():

    orders_df = spark.read.csv("./input/Orders.csv", header=True, inferSchema=True, sep=";")

    orders_time = orders_df \
        .withColumn("year", year("Order_Purchase_Timestamp")) \
        .withColumn("month", month("Order_Purchase_Timestamp"))

    orders_by_time = orders_time.groupBy("year", "month").count()

    result = orders_by_time.orderBy("year", "month", ascending=[True, False])

    output = "=== SỐ LƯỢNG ĐƠN HÀNG NHÓM THEO NĂM, THÁNG ĐẶT HÀNG ===\n"

    output += f"\n{'Năm':<10}{'Tháng':<10}{'Số đơn hàng':<15}\n"
    output += "-" * 35 + "\n"

    for row in result.collect():
        output += f"{str(row['year']):<10}{str(row['month']):<10}{str(row['count']):<15}\n"

    print(output)

    os.makedirs("output", exist_ok=True)
    with open("output/bai4_ketqua.txt", "w", encoding="utf-8") as f:
        f.write(output)

    spark.stop()

if __name__ == "__main__":
    main()