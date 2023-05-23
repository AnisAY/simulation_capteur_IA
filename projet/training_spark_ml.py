import io
import logging
import sys
import traceback
from datetime import datetime

import urllib3

from minio import Minio
from pyspark import SparkContext
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import unix_timestamp
import minio


def create_model(iter, features_cols, labelCol):
    # create LinearRegression estimator and set parameters
    lr = LinearRegression(maxIter=iter,
                          featuresCol=features_cols,
                          labelCol=labelCol)
    return lr


def set_train_and_validation_ds(data, seed):
    return data.randomSplit([0.7, 0.3], seed=seed)


def test_model(spark, model):
    # Methode pour tester le modèle avec un jeu de données test
    testData = spark.read.format("libsvm").load("path/to/test/file")
    predictions = model.transform(testData)
    predictions.show()


def save_model(model):
    # Methode pour sauvegarder le modèle vers un lieu spécifié
    model.save("saved_model/model_test")


def main():
    seed = 49
    # create SparkSession

    try:
        spark = SparkSession.builder.appName("LinearRegressionExample").getOrCreate()
        logging.info('Spark session successfully created')
    except Exception as e:
        traceback.print_exc(file=sys.stderr)  # To see traceback of the error.
        logging.error(f"Couldn't create the spark session due to exception: {e}")
        exit(0)
    # read input data
    minio_client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    found = minio_client.bucket_exists("warehouse")
    if not found:
        print("Bucket 'donnes-capteurs' n'existe déjà")
    else:
        print("Bucket 'donnes-capteurs' existe déjà")

    obj: urllib3.response.HTTPResponse = minio_client.get_object(
        'warehouse',
        'conso-elec-gaz-annuelle-par-naf-agregee-region.csv',
    )

    content = obj.data.decode('utf-8')
    lines = content.splitlines(keepends=True)

    rdd = spark.sparkContext.parallelize(lines)
    df_spark = spark.read.option("header", True).option("inferSchema", True).csv(rdd)
    df_spark = df_spark.dropna()

    df_spark.show()
    for col in ["order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
                "order_estimated_delivery_date", "order_delivered_customer_date"]:
        df_spark = df_spark.withColumn(col + "_int", unix_timestamp(col).cast("int"))

    # Diviser le dataset entre entrainement et validation
    train_data, validation_data = set_train_and_validation_ds(df_spark, seed)

    train_data.show()
    validation_data.show()

    features_cols = ["order_purchase_timestamp_int", "order_approved_at_int",
                     "order_delivered_carrier_date_int", "order_estimated_delivery_date_int"]
    target_col = "order_delivered_customer_date_int"

    assembler = VectorAssembler(inputCols=features_cols, outputCol="features")

    # transform the data using the VectorAssembler
    data_with_features = assembler.transform(df_spark)

    lr = create_model(iter=500, features_cols="features", labelCol=target_col)

    # fit the model to the data
    model = lr.fit(data_with_features)

    # print the model coefficients and intercept
    print("Coefficients: " + str(model.coefficients))
    print("Intercept: " + str(model.intercept))

    # save the model
    save_model(model)

    # evaluate the model on test data


if __name__ == '__main__':
    main()
