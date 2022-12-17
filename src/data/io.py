import pandas as pd
import pyspark.sql.functions as F
import yaml
import os
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


class SparkDataLoader:
    def __init__(self):
        # Create a SparkSession
        self.spark = SparkSession.builder.getOrCreate()

    def load_csv(self, filePath):
        """Loads data from a CSV file into a Spark DataFrame"""
        sdf = self.spark.read.csv(filePath, header=True, inferSchema=True)
        return sdf

class PandasDataLoader:
    def __init__(self) -> None:
        pass

    def load_csv(self, filePath):
        """Loads data from a CSV file into a Pandas DataFrame"""
        pdf = pd.read_csv(filePath)

class SparkDataTransformer:
    def __init__(self) -> None:
        pass

    def transform_raw(self, sparkDataFrame):
        # Read the YAML file and parse data into a dictionary
        with open("/Users/atlanticroc/atlantida/loyalty101/src/mapping.yaml") as f:
            mapping = yaml.safe_load(f)

        # Define a dictionary mapping the old column names to the new column names
        column_map = mapping["column_map_raw"]

        # Define a dictionary mapping the old data types to the new data types
        type_map = mapping["type_map_raw"]

        # Create a copy of the DataFrame with the modified schema
        sdf_modified = sparkDataFrame

        # Iterate over the dictionary and modify the schema
        for old_name, new_name in column_map.items():
            # Rename the columns
            sdf_modified = sdf_modified.withColumnRenamed(old_name, new_name)

            # Cast the column to the new data type
            sdf_modified = sdf_modified.withColumn(new_name, F.col(new_name).cast(type_map[old_name]))

        # Add total sales and adjust customer id data type
        sdf_modified = (
            sdf_modified
            .withColumn("salesAmount", (F.col("skuQty") * F.col("skuPrice")).cast("double"))
            .withColumn("customerId", F.col("customerId").cast("integer").cast("string"))
        )

        return sdf_modified

class SparkDataWriter:
    def __init__(self, directoryPath: str) -> None:
        self.directoryPath = directoryPath

    def write(self, sparkDataFrame: DataFrame, fileName):
        filePath = os.path.join(self.directoryPath, fileName)
        sparkDataFrame.write.format("csv").option("header",True).mode("overwrite").save(filePath)

