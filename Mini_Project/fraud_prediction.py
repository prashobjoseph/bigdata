from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator


# ---------------------------
# 1. Initialize Spark Session
# ---------------------------

"""Initialize the Spark session with Hive support."""
spark = SparkSession.builder.appName("FraudDetection").enableHiveSupport().getOrCreate()

# ---------------------------
# 2. Load Data from Hive
# ---------------------------

"""Load data from Hive table into a Spark DataFrame."""
hive_df= spark.sql("SELECT * FROM bigdata_nov_2024.sop_credit_trans")
hive_df.show(5)

# Filter rows where is_fraud == 0 and limit to 10,000 rows
non_fraud_df = hive_df.filter(hive_df.is_fraud == 0).limit(10000)

# Filter rows where is_fraud == 1 (all rows)
fraud_df = hive_df.filter(hive_df.is_fraud == 1)

# Combine both DataFrames
hive_df = non_fraud_df.union(fraud_df)

# Show final data
hive_df.show()

#checking balance of dataset
data=hive_df.groupBy('is_fraud').count()
data.show()


# Selecting Relevant Features
feature_cols = ['amt', 'zip', 'population','Age']  # Adjust these based on your dataset

# Assemble features into a single vector
assembler = VectorAssembler(inputCols=feature_cols, outputCol='features')

# Index the target column ('is_fraud') for classification
indexer = StringIndexer(inputCol='is_fraud', outputCol='label')

# Apply transformations
prepared_df = assembler.transform(hive_df)
prepared_df = indexer.fit(prepared_df).transform(prepared_df)


# ---------------------------
# 4. Build and Train Model
# ---------------------------
print(" Training the model...")

# Split data into training and testing sets
print(" Splitting data into training and testing sets...")
train_df, test_df = prepared_df.randomSplit([0.8, 0.2], seed=42)
#print(f" Training Set: {train_df.count()} rows, Testing Set: {test_df.count()} rows")
print("Training Set: {} rows, Testing Set: {} rows".format(train_df.count(), test_df.count()))


# Define Logistic Regression Model
lr = LogisticRegression(featuresCol='features', labelCol='label', maxIter=10)

model = lr.fit(train_df)

# Evaluate Model
predictions = model.transform(test_df)
evaluator = BinaryClassificationEvaluator(labelCol='label')
auc = evaluator.evaluate(predictions)
#print(f" Model AUC: {auc}")
print("Model AUC: {}".format(auc))


# Display Predictions
predictions.select('features', 'label', 'prediction').show(10)

from pyspark.sql.functions import col

# Calculate TP, FP, FN, TN
tp = float(predictions.filter((col("label") == 1) & (col("prediction") == 1)).count())
fp = predictions.filter((col("label") == 0) & (col("prediction") == 1)).count()
fn = predictions.filter((col("label") == 1) & (col("prediction") == 0)).count()
tn = predictions.filter((col("label") == 0) & (col("prediction") == 0)).count()
print("tp:{}".format(tp))
print("fp:{}".format(fp))
print("fn:{}".format(fn))
print("tn:{}".format(tn))

x=float(tp+fp)
y=float(tp+tn)

# Calculate Precision, Recall, F1 Score
precision = tp/x
recall = tp/y 

f1_score = 2 * (precision * recall) / (precision + recall)

print("Precision: {}".format(precision))
print("Recall: {}".format(recall))
print("F1 Score: {}".format(f1_score))

# Select the necessary columns (true labels and predictions)
predictions1 = predictions.select("label", "prediction")

# Initialize the evaluator for precision, recall, and F1 score
evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="prediction")

# ---------------------------
# 5. Predict Single Record
# ---------------------------
print("Predicting a single record...")

# Example Record for Prediction
sample_data = [(100.0, 84002,50000, 25)]  # Example: 'amt', 'zip', 'population','Age'
sample_df = spark.createDataFrame(sample_data, ['amt', 'zip', 'population','Age'])

# Assemble Sample Record
sample_df = assembler.transform(sample_df)

# Make Prediction
sample_prediction = model.transform(sample_df)

result = sample_prediction.select("prediction")
result



if result == 1.0:
    print(" This transaction is predicted to be FRAUDULENT!")
else:
    print(" This transaction is predicted to be LEGITIMATE.")

#creating data frame with predicted value for the entire data

# Predict fraud
predictions = model.transform(prepared_df)

# Add prediction column while preserving all original columns
result_df = predictions.drop('features', 'rawPrediction', 'probability') \
                       .withColumnRenamed('prediction', 'is_fraud_predicted')

# Show sample results
result_df.show(10)

# Count matching rows
matching_count = result_df.filter(result_df['is_fraud'] == result_df['is_fraud_predicted']).count()

# Count non-matching rows
non_matching_count = result_df.filter(result_df['is_fraud'] != result_df['is_fraud_predicted']).count()

# Display results
print("Matching Count: {}".format(matching_count))
print("Non-Matching Count: {}".format(non_matching_count))


# Write the result DataFrame to a new Hive table
result_df.write.mode("overwrite").saveAsTable("bigdata_nov_2024.sop_credit_trans_predicted")

print(" Successfully written to Hive table: bigdata_nov_2024.sop_credit_trans_predicted")

# ---------------------------
# 6. Stop Spark Session
# ---------------------------
spark.stop()

