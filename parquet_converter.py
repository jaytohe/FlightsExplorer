#!/usr/bin/env python3
import sys
import os
import argparse
import pyspark.sql.functions as F # type: ignore
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser(description="Convert all CSV files in a directory to Parquet")
    parser.add_argument("input_csvs_directory", help="Path to CSV files")
    parser.add_argument("output_parquet_directory", help="Output path for parquet files")
    args = parser.parse_args()

    if len(sys.argv) != 3:
        parser.print_help()
        sys.exit(1)

    if not os.path.exists(args.input_csvs_directory):
        print("Input directory does not exist")
        sys.exit(1)

    if not os.path.exists(args.output_parquet_directory):
        print("Creating output directory: " + args.output_parquet_directory)
        os.makedirs(args.output_parquet_directory)


    spark = SparkSession.builder.appName("CSVtoParquet").getOrCreate()
    # Find all CSV files in input directory and subdirectories by walking the file tree
    for dirpath, dirnames, filenames in os.walk(top=args.input_csvs_directory, topdown=True):
        for csv_filename in [f for f in filenames if f.endswith(".csv")]:
            csv_path = os.path.join(dirpath, csv_filename)
            print(csv_path)

            # Read CSV file into DataFrame
            print("Reading CSV file: " + csv_path)
            df = spark.read.csv(csv_path, header=True, inferSchema=True)

            # Write DataFrame to Parquet file
            parquet_path = os.path.join(args.output_parquet_directory, os.path.splitext(csv_filename)[0] + ".parquet")
            print("Writing Parquet file: " + parquet_path)
            df.write.parquet(parquet_path)

    spark.stop()
    sys.exit(0)

if __name__ == "__main__":
    main()

