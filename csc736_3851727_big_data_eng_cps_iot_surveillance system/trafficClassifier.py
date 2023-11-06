#Author: Keanu Johnston
#Institution: University of the Western Cape
#Module: Big data engineering
#Date: 03 Nov 2023 

#I. hadoop/spark imports
import os
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, abs

#II. flask imports
from flask import Flask, render_template, jsonify, Response

#III. imports for classifying mqtt (iot) traffic
import csv
from tabulate import tabulate

#IV. classifier imports - RandomForestClassifier
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics

#A. Function to classify traffic based on traffic features - Decision tree
def classify_traffic(time_diff, ap, ip, bf):
    if time_diff < 0.01:
        if ap == "MQTT":
            return "Loic"
        elif abs(bf) > 0.0001:
            return "Stacheldracht"
        else:
            return "Normal"
    else:
        return "Unknown"

#B. Function to read data from the dataset and store it in a dictionary
def read_dataset(file_path):
    data = []  # Initialize a list to store the data and results
    with open(file_path, 'r') as file:
        csv_reader = csv.DictReader(file)
        print("\n========= Object of Traffic Identifier for loic, hping or normal traffic =========\n")
        row_count = 0  # Initialize a counter for the rows
        max_rows = 20
        for row in csv_reader:
            if row_count >= max_rows:
                break  # Exit the loop when the maximum number of rows is reached
            time_diff = float(row['Time'])
            ap = row['Protocol']
            ip = row['Destination']
            bf_str = row['Bytes in Flight']
            try:
                bf = int(bf_str)
            except ValueError:
                bf = 0  # Set a default value or handle it in a way that makes sense for your use case
            result = classify_traffic(time_diff, ap, ip, bf)
            print(f"Data: {row}, Traffic Type: {result}")
            data.append({"Data": row, "Traffic Type": result})

            row_count += 1  # Increment the row counter
    return data

#C. Define a UDF (User-Defined Function) for the traffic classification logic - Decision tree
def classify_traffic_udf(time_diff, ap, bf):
    if time_diff < 0.01:
        if ap == "MQTT":
            return "Loic"
        elif abs(bf) > 0.0001:
            return "Stacheldracht"
        else:
            return "Normal"
    else:
        return "Unknown"

# #C. for printing new dataset in tabular format
# dataset_path = 'normal_and_attack_traffic.csv'
# data = read_dataset(dataset_path)

#D. HDFSSession configuration - Set HADOOP_CONF_DIR environment variable
os.environ['HADOOP_CONF_DIR'] = '/root/hadoop-3.2.4/etc/hadoop'
# Replace <CONTAINER_IP> with the actual IP address of the Hadoop container
hadoop_container_ip = "172.25.0.2"

#E. SparkSession configuration
spark = SparkSession.builder \
    .appName("Restaurant analysis") \
    .config("spark.hadoop.fs.defaultFS", f"hdfs://{hadoop_container_ip}:9000") \
    .getOrCreate()

#F. Begin Spark filtering
try:
    # G. Load the data
    df = spark.read.csv(f"hdfs://{hadoop_container_ip}:9000/MQTT_Attack_stacheldraht.csv", header=True, inferSchema=True)
    # Load df data as row objects
    data = df.collect()
    # Print the schema to check column names
    # df.printSchema()
    # Verify that the DataFrame contains all required fields
    print("\n====== Loading data... ======\n")
    print("\n====== A. Full list of incoming IoT traffic ======\n")
    df.show()
    print("\n====== B. First 5 rows of incoming IoT traffic ======\n")
    df.show(5, truncate=True)

    # H. Labeling row in traffic as either loic, stacheldracht or normal
    dataset_path = 'MQTT_attack_loic.csv'
    data = read_dataset(dataset_path)
    # Register the UDF with Spark for code that is not easily expressable with spark functions
    spark.udf.register("classify_traffic_udf", classify_traffic_udf)
    # Read the dataset as a DataFrame
    #file_path = "MQTT_attack_loic.csv"
    df2 = spark.read.csv(f"hdfs://{hadoop_container_ip}:9000/AMPQ_Attack_loic.csv", header=True, inferSchema=True)
    # Rename the "No." column to "No" (without the period)
    df2 = df2.withColumnRenamed("No.", "No")
    # Apply the UDF to classify the traffic and create a new column
    df2 = df2.withColumn("Traffic Type", when((col("Time").cast("double") < 0.001) & (col("Protocol") == "MQTT"), "Loic")
                                        .when((col("Time").cast("double") < 0.001) & (abs(col("Bytes in Flight").cast("double")) > 0.0001), "Stacheldracht")
                                        .otherwise("Normal"))
    # Print header
    print("\n==== Identifying traffic as Normal, Loic or Stacheldracht type ====\n")
    # Show the results
    df2.show()

    df2.printSchema()
    
    # Optionally, you can save the results to a new CSV file, Granted the hdfs permissions are granted
    # Specify the HDFS path where you want to write the CSV file
    # hdfs_output_path = f"hdfs://{hadoop_container_ip}:9000/classified_traffic.csv"

    # # Save the DataFrame 'df' to HDFS as a CSV file
    # df.write.csv(hdfs_output_path, header=True, mode="overwrite")

    # I(skip). upload csv file to hadoop
    # Specify the local file path and HDFS destination path for data upload
    # local_file_path = "restaurant_location_info.json"
    # hdfs_dest_path = f"/"

    # # Call the upload function
    # upload_to_hdfs(local_file_path, hdfs_dest_path, hadoop_container_ip)


    # I. Filtering Traffic by traffic type
    print("\n====== C. Dataframe filter to show incoming IoT attack traffic only ======\n")
    filtered_df = df.filter((col("Protocol") != "TCP"))
    # Select relevant columns: IP Address, Traffic Type
    selected_df = filtered_df.select("`No.`","Time","Source","Protocol")
    # Output results - incoming traffic as normal or attacked
    selected_df.show()


    # J. Classifing dataset - RandomForestClassifier
    print("\n====== Random Forest Classifier for Network Traffic Data ======\n")

    # Data Preprocessing
    df2 = df2.dropna()  # Remove rows with NULL values

    # Rename the "No." column to "No" (without the period)
    df2 = df2.withColumnRenamed("No.", "No")

    # Handling non-numeric values in 'Bytes in Flight' column
    df2 = df2.withColumn("Bytes in Flight", when(col("Bytes in Flight") == "NULL", 0).otherwise(col("Bytes in Flight").cast("integer")))

    # Handling non-numeric values in 'Calculated Window Size' column
    df2 = df2.withColumn("Calculated Window Size", when(col("Calculated Window Size") == "NULL", 0).otherwise(col("Calculated Window Size").cast("integer")))

    # Define your features (X)
    features = ['Time', 'Bytes in Flight', 'Length', 'Calculated Window Size']
    assembler = VectorAssembler(inputCols=features, outputCol="features")
    X = assembler.transform(df2)

    # Convert 'Traffic Type' column to numerical labels
    indexer = StringIndexer(inputCol="Traffic Type", outputCol="label")
    X = indexer.fit(X).transform(X)

    # Split the dataset into training and testing sets
    (trainingData, testData) = X.randomSplit([0.8, 0.2], seed=42)

    # Initialize the RandomForestClassifier
    rf_classifier = RandomForestClassifier(numTrees=100, featuresCol="features", labelCol="label", seed=42)

    # Create a pipeline
    pipeline = Pipeline(stages=[rf_classifier])

    # Fit the model
    model = pipeline.fit(trainingData)

    # Make predictions on the test set
    predictions = model.transform(testData)

    # Evaluate the model
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)

    print("Accuracy:", accuracy)

    # Confusion Matrix
    predictionAndLabels = predictions.select("prediction", "label")
    metrics = MulticlassMetrics(predictionAndLabels.rdd)
    confusion_matrix = metrics.confusionMatrix()
    print("Confusion Matrix:\n", confusion_matrix)

    # Classification Report
    labels = predictions.select("label").distinct().rdd.map(lambda x: x[0]).collect()
    class_report = []

    for label in labels:
        true_positive = predictions.filter((col("prediction") == label) & (col("label") == label)).count()
        false_positive = predictions.filter((col("prediction") == label) & (col("label") != label)).count()
        false_negative = predictions.filter((col("prediction") != label) & (col("label") == label)).count()
        true_negative = predictions.filter((col("prediction") != label) & (col("label") != label)).count()

        if true_positive + false_positive == 0:
            precision = 0  # Assigning precision as 0 if there are no true positives
        else:
            precision = true_positive / (true_positive + false_positive)

        if true_positive + false_negative == 0:
            recall = 0  # Assigning recall as 0 if there are no true positives
        else:
            recall = true_positive / (true_positive + false_negative)

        if precision + recall == 0:
            f1_score = 0  # Assigning F1 score as 0 if precision and recall are both 0
        else:
            f1_score = 2 * (precision * recall) / (precision + recall)

        class_report.append((label, precision, recall, f1_score, true_positive, false_positive, false_negative, true_negative))


    print("Classification Report:")
    print("Label   Precision   Recall   F1 Score   TP   FP   FN   TN")
    for item in class_report:
        print(f"{int(item[0]):2d}      {item[1]:.2f}      {item[2]:.2f}      {item[3]:.2f}      {item[4]:2d}   {item[5]:2d}   {item[6]:2d}   {item[7]:2d}")


#K. Error handling
except Exception as e:
    print(f"An error occurred in spark session: {str(e)}")

finally:
    # Stop the SparkSession when you're done
    spark.stop()

#L. FlaskSession configuration - methods to get data from python program to dashbaord
app = Flask(__name__)

#M. Home page
@app.route('/')
def index():
    return render_template('classifierdashboard.html')

#N. Send incoming traffic to html dashboard as response object
@app.route('/get_traffic_data_csv', methods=['GET'])
def get_traffic_data_csv():
    dataset_path = 'MQTT_attack_loic.csv'  # Replace with the actual path to your dataset
    data = read_dataset(dataset_path)

    print("=====data====\n",data)
    # Convert the results into a CSV string
    output = []
    for item in data:
        data_values = item["Data"].values()
        data_values_list = [str(value) for value in data_values]  # Convert values to strings
        output.append(','.join(data_values_list) + ',' + item["Traffic Type"])

    csv_data = '\n'.join(output)

    # Set response headers to specify a CSV response
    response = Response(csv_data, content_type='text/csv')
    response.headers["Content-Disposition"] = "attachment; filename=traffic_results.csv"

    print("=====Response====",response)
    return response

#O send incoming data as an HTML object
@app.route('/get_traffic_data_html', methods=['GET'])
def get_traffic_data_html():
    dataset_path = 'MQTT_Attack_stacheldraht.csv'  # Replace with the actual path to your dataset
    data = read_dataset(dataset_path)

    # Convert the data to an HTML table format
    html_data = "<table border='1'><thead><tr><th>No.</th><th>Time</th><th>Bytes in Flight</th><th>Source</th><th>Destination</th><th>Protocol</th><th>Length</th><th>Info</th><th>Calculated Window Size</th><th>Traffic Type</th></tr></thead><tbody>"

    for item in data:
        html_data += f"<tr><td>{item['Data']['No.']}</td><td>{item['Data']['Time']}</td><td>{item['Data']['Bytes in Flight']}</td><td>{item['Data']['Source']}</td><td>{item['Data']['Destination']}</td><td>{item['Data']['Protocol']}</td><td>{item['Data']['Length']}</td><td>{item['Data']['Info']}</td><td>{item['Data']['Calculated Window Size']}</td><td>{item['Traffic Type']}</td></tr>"

    html_data += "</tbody></table>"

    # Send the data as HTML
    return html_data

if __name__ == '__main__':
    app.run(debug=True)
