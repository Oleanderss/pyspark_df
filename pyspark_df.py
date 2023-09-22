from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")


category = spark.read.csv('csv_files/category.csv', sep=',', header=True)
products = spark.read.csv('csv_files/products.csv', sep=',', header=True)
relation = spark.read.csv('csv_files/relation.csv', sep=',', header=True)


df = products.join(relation, products['id'] == relation.product_id, "left") \
    .join(category, category['id'] == relation.category_id, "left") \
    .select(['p_name', 'c_name'])


df.show()
