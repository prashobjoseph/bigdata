from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# ---------------------------
# 1. Initialize Spark Session
# ---------------------------
def initialize_spark():
    """Initialize the Spark session with Hive support."""
    return SparkSession.builder \
        .appName("FraudDetection") \
        .enableHiveSupport() \
        .getOrCreate()

# ---------------------------
# 2. Load Data from Hive
# ---------------------------
def load_data_from_hive(spark):
    """Load data from Hive table into a Spark DataFrame."""
    return spark.sql("SELECT * FROM bigdata_nov_2024.sop_credit_trans")

# ---------------------------
# 3. Preprocess Data
# ---------------------------
def preprocess_data(df):
    """Prepare and clean data for predictive modeling."""
    # Example: Handle missing values
    df = df.fillna({"category": "unknown", "amt": 0.0})
    
    # Example: Create a binary 'is_fraud' column if not already available
    if 'is_fraud' not in df.columns:
        df = df.withColumn('is_fraud', when(col('category') == 'fraud', 1).otherwise(0))
    
    # Selecting Relevant Features
    feature_cols = ['amt', 'Age', 'population']  # Adjust based on your dataset
    
    assembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
    
    # StringIndexer for Label Encoding (if required)
    indexer = StringIndexer(inputCol='is_fraud', outputCol='label')
    
    return df, assembler, indexer

# ---------------------------
# 4. Build and Train Model
# ---------------------------
def train_fraud_model(df, assembler, indexer):
    """Train a Logistic Regression model to predict fraud."""
    # Split data into training and testing sets
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    
    # Define Logistic Regression Model
    lr = LogisticRegression(featuresCol='features', labelCol='label', maxIter=10)
    
    # Build Pipeline
    pipeline = Pipeline(stages=[assembler, indexer, lr])
    
    # Train Model
    model = pipeline.fit(train_df)
    
    # Evaluate Model
    predictions = model.transform(test_df)
    evaluator = BinaryClassificationEvaluator(labelCol='label')
    auc = evaluator.evaluate(predictions)
    print(f"Model AUC: {auc}")
    
    return model, predictions

# ---------------------------
# 5. Detect Fraud in a Specific Record
# ---------------------------
def predict_single_record(model, spark):
    """Predict if a single transaction record is fraudulent."""
    sample_data = [(100.0, 30, 50000)]  # Example record: amt, Age, population
    sample_df = spark.createDataFrame(sample_data, ['amt', 'Age', 'population'])
    
    assembler = VectorAssembler(inputCols=['amt', 'Age', 'population'], outputCol='features')
    sample_df = assembler.transform(sample_df)
    
    prediction = model.transform(sample_df)
    result = prediction.select('prediction').collect()[0]['prediction']
    
    if result == 1.0:
        print("🚨 This transaction is predicted to be FRAUDULENT!")
    else:
        print("✅ This transaction is predicted to be LEGITIMATE.")

# ---------------------------
# 6. Main Execution
# ---------------------------
if __name__ == "__main__":
    # Initialize Spark
    spark = initialize_spark()
    
    # Load Hive Data
    hive_df = load_data_from_hive(spark)
    hive_df.show(5)
    
    # Preprocess Data
    prepared_df, assembler, indexer = preprocess_data(hive_df)
    
    # Train Model
    model, predictions = train_fraud_model(prepared_df, assembler, indexer)
    predictions.select('features', 'label', 'prediction').show(10)
    
    # Predict on a Single Record
    predict_single_record(model, spark)
