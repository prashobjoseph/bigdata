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

# Selecting Relevant Features
feature_cols = ['amt', 'zip', 'population','Age']  # Adjust these based on your dataset

# Assemble features into a single vector
assembler = VectorAssembler(inputCols=feature_cols, outputCol='features')

# Index the target column ('is_fraud') for classification
indexer = StringIndexer(inputCol='is_fraud', outputCol='label')

# Apply transformations
prepared_df = assembler.transform(hive_df)
prepared_df = indexer.fit(prepared_df).transform(prepared_df)

# 4. Build and Train Model
# ---------------------------
print("🤖 Training the model...")

# Split data into training and testing sets
print("📊 Splitting data into training and testing sets...")
train_df, test_df = prepared_df.randomSplit([0.8, 0.2], seed=42)
print(f"✅ Training Set: {train_df.count()} rows, Testing Set: {test_df.count()} rows")

# Define Logistic Regression Model
lr = LogisticRegression(featuresCol='features', labelCol='label', maxIter=10)

model = lr.fit(train_df)

# Evaluate Model
predictions = model.transform(test_df)
evaluator = BinaryClassificationEvaluator(labelCol='label')
auc = evaluator.evaluate(predictions)
print(f"✅ Model AUC: {auc}")

# Display Predictions
predictions.select('features', 'label', 'prediction').show(10)

from pyspark.sql.functions import col

# Calculate TP, FP, FN, TN
tp = predictions.filter((col("label") == 1) & (col("prediction") == 1)).count()
fp = predictions.filter((col("label") == 0) & (col("prediction") == 1)).count()
fn = predictions.filter((col("label") == 1) & (col("prediction") == 0)).count()
tn = predictions.filter((col("label") == 0) & (col("prediction") == 0)).count()

# Calculate Precision, Recall, F1 Score
precision = tp / (tp + fp) 
recall = tp / (tp + fn) 
f1_score = 2 * (precision * recall) / (precision + recall)

print(f"✅ Precision: {precision}")
print(f"✅ Recall: {recall}")
print(f"✅ F1 Score: {f1_score}")

# ---------------------------
# 5. Predict Single Record
# ---------------------------
print("🔍 Predicting a single record...")

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
    print("🚨 This transaction is predicted to be FRAUDULENT!")
else:
    print("✅ This transaction is predicted to be LEGITIMATE.")

import seaborn as sns
import matplotlib.pyplot as plt
# Convert to Pandas DataFrame
hive_pandas_df = hive_df.select('is_fraud','amt','zip','Age','population').toPandas()

# Bar plot for fraud distribution
#plt.figure(figsize=(8, 5))
sns.countplot(data=hive_pandas_df, x='is_fraud', palette='coolwarm')
plt.title('Fraud vs Non-Fraud Transactions')
plt.xlabel('Is Fraud')
plt.ylabel('Count')
plt.show()

# KDE Plot for transaction amounts
plt.figure(figsize=(10, 6))
sns.kdeplot(data=hive_pandas_df, x='amt', hue='is_fraud', fill=True, palette='coolwarm')
plt.title('Transaction Amount Distribution by Fraud Status')
plt.xlabel('Transaction Amount')
plt.ylabel('Density')
plt.show()

# Boxplot for Age
plt.figure(figsize=(10, 6))
sns.boxplot(data=hive_pandas_df, x='is_fraud', y='Age', palette='coolwarm')
plt.title('Age Distribution by Fraud Status')
plt.xlabel('Is Fraud')
plt.ylabel('Age')
plt.show()

# Scatter plot for Population and Fraud
plt.figure(figsize=(12, 8))
sns.scatterplot(data=hive_pandas_df, x='population', y='amt', hue='is_fraud', palette='coolwarm', alpha=0.7)
plt.title('Population vs Transaction Amount by Fraud Status')
plt.xlabel('Population')
plt.ylabel('Transaction Amount')
plt.show()

# Correlation heatmap
plt.figure(figsize=(10, 8))
sns.heatmap(hive_pandas_df.corr(), annot=True, cmap='coolwarm', fmt=".2f")
plt.title('Feature Correlation Heatmap')
plt.show()

# ---------------------------
# 6. Stop Spark Session
# ---------------------------
spark.stop()
