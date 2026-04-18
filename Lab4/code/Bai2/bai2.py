from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Bai2") \
    .master("local[*]") \
    .getOrCreate()

def main():

    customer_df = spark.read.csv("./input/Customer_List.csv", header=True, inferSchema=True, sep=";")
    orders_df = spark.read.csv("./input/Orders.csv", header=True, inferSchema=True, sep=";")
    order_items_df = spark.read.csv("./input/Order_Items.csv", header=True, inferSchema=True, sep=";")

    total_orders = orders_df.select("Order_ID").distinct().count()
    total_customers = customer_df.select("Customer_Trx_ID").distinct().count()
    total_sellers = order_items_df.select("Seller_ID").distinct().count()

    result = f"""
Tổng số đơn hàng:: {total_orders}
Tổng số khách hàng: {total_customers}
Tổng số người bán:: {total_sellers}
"""
    print(result)
    import os
    os.makedirs("output", exist_ok=True)

    with open("output/bai2_ketqua.txt", "w", encoding="utf-8") as f:
        f.write(result)

    spark.stop()

if __name__ == "__main__":
    main()