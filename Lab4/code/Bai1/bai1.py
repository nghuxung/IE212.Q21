from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Bai1") \
    .master("local[*]") \
    .getOrCreate()

def main():

    customer_df = spark.read.csv("./input/Customer_List.csv", header=True, inferSchema=True, sep=";")
    order_items_df = spark.read.csv("./input/Order_Items.csv", header=True, inferSchema=True, sep=";")
    order_reviews_df = spark.read.csv("./input/Order_Reviews.csv", header=True, inferSchema=True, sep=";")
    orders_df = spark.read.csv("./input/Orders.csv", header=True, inferSchema=True, sep=";")
    products_df = spark.read.csv("./input/Products.csv", header=True, inferSchema=True, sep=";")

    schema_info = ""

    datasets = {
        "Customer_List": customer_df,
        "Order_Items": order_items_df,
        "Order_Reviews": order_reviews_df,
        "Orders": orders_df,
        "Products": products_df
    }

    for name, df in datasets.items():
        schema_info += f"--- {name} Schema ---\n"
        for field in df.schema:
            schema_info += f"{field.name}: {field.dataType.simpleString()}\n"
        schema_info += "\n\n"
        
    with open("output/bai1_ketqua.txt", "w", encoding="utf-8") as f:
        f.write(schema_info)
        
    print(schema_info) 

    spark.stop()

if __name__ == "__main__":
    main()
    