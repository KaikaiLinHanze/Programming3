"""
File: InterPro_annotation prediction
Name: Kai-Kai Lin
Date: 03.Oct.2022
----------------------
This is the final for programming 3. In this assignment, I will develop scikit-learn machine learning models to predict the function of the proteins in the specific dataset.
This model will use small InterPro_annotations_accession to predict large InterPro_annotations_accession.
The definition of small InterPro_annotations_accession and large InterPro_annotations_accession is defined as below:
If InterPro_annotations_accession's feature length(Stop_location-Start_location) / Sequence_length > 0.9, it is large InterPro_annotations_accession.
Otherwise, it is a small InterPro_annotations_accession.
We can briefly rewrite as:
            |(Stop - Start)|/Sequence >  0.9 --> Large
            |(Stop - Start)|/Sequence <= 0.9 --> small
I will also check the "bias" and "noise" that does not make sense from the dataset.
ie. lines(-) from the TSV file which don't contain InterPRO numbers
ie. proteins which don't have a large feature (according to the criteria above)

Goal:
The goal of this assignment is to predict large InterPro_annotations_accession by small InterPro_annotations_accession.
I will use the dataset from /data/dataprocessing/interproscan/all_bacilli.tsv file on assemblix2012 and assemblix2019. 
However, this file contains ~4,200,000 protein annotations, so I will put a subset of all_bacilli.tsv on GitHub and on local for code testing.
"""

from pyspark.sql.types import StructType, StructField, IntegerType, StringType,FloatType
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import numpy as np
import warnings
import time
warnings.filterwarnings('ignore')
import pickle
from pyspark.ml.feature import StringIndexer,VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import DecisionTreeClassifier, NaiveBayes, RandomForestClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder 

def create_df(path):
    """
    it will help you to make a df from spark
    path: str, for the correct file path
    return: spark df.
    """
    schema = StructType([
    StructField("Protein_accession", StringType(), True),
    StructField("Sequence_MD5_digest", StringType(), True),
    StructField("Sequence_length", IntegerType(), True),
    StructField("Analysis", StringType(), True),
    StructField("Signature_accession", StringType(), True),
    StructField("Signature_description", StringType(), True),
    StructField("Start_location", IntegerType(), True),
    StructField("Stop_location", IntegerType(), True),
    StructField("Score", FloatType(), True),
    StructField("Status", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("InterPro_annotations_accession", StringType(), True),
    StructField("InterPro_annotations_description", StringType(), True),
    StructField("GO_annotations", StringType(), True),
    StructField("Pathways_annotations", StringType(), True)])
    spark = SparkSession.builder.master("local[16]")\
                            .config('spark.driver.memory', '128g')\
                            .config('spark.executor.memory', '128g')\
                            .config("spark.sql.debug.maxToStringFields","100")\
                            .appName("InterPro").getOrCreate()
    return spark.read.option("sep","\t").option("header","False").csv(path,schema=schema)

def data_preprocessing(df):
    """
    It will help you to finish preprocessing data.
    df: spark df
    return small_df,large_df
    """
    # remove InterPro_annotations_accession == "-"
    # get the length of protein
    # get the ratio to distinguish them to large and small InterPro_annotations_accession
    # 1 for large, 0 for small InterPro_annotations_accession
    df = df.filter(df.InterPro_annotations_accession != "-")\
        .withColumn("Ratio", (abs(df["Stop_location"] - df["Start_location"])/df["Sequence_length"]))\
        .withColumn("Size", when((abs(df["Stop_location"] - df["Start_location"])/df["Sequence_length"])>0.9,1).otherwise(0))

    # get the intersection to make sure there is a match of large and small InterPro_annotations_accession(at least one large and one small InterPro_annotations_accession)
    intersection = df.filter(df.Size == 0).select("Protein_accession").intersect(df.filter(df.Size == 1).select("Protein_accession"))
    intersection_df = intersection.join(df,["Protein_accession"])

    # get the number of small InterPro_annotations_accession in each Protein_accession
    small_df = intersection_df.filter(df.Size == 0).groupBy(["Protein_accession"]).pivot("InterPro_annotations_accession").count()

    # There are several InterPro_annotations_accession with the same Protein_accession. I only choose the largest one.
    large_df = intersection_df.filter(df.Size == 1).groupby(["Protein_accession"]).agg(max("Ratio").alias("Ratio"))
    large_df = large_df.join(intersection_df,["Protein_accession","Ratio"],"inner").dropDuplicates(["Protein_accession"])

    # Drop the useless columns
    columns = ("Sequence_MD5_digest","Analysis","Signature_accession","Signature_description",
        "Score","Status","Date","InterPro_annotations_description","GO_annotations",
        "Pathways_annotations","Ratio","Size","Stop_location","Start_location","Sequence_length")
    large_df = large_df.drop(*columns)
    return small_df, large_df

def ML_df_create(small_df,large_df):
    """
    It will help you to create a correct ML dataframe.
    small_df: spark df, preprocessing to fit the criteria ratio<=0.9
    large_df: spark df, preprocessing to fit the criteria ratio>0.9
    return ML_df
    """
    # Create the df for ML, we do not need Protein_accession anymore.
    ML_df = large_df.join(small_df,["Protein_accession"],"outer").fillna(0).drop("Protein_accession")

    # catalogize y variable
    Label = StringIndexer(inputCol="InterPro_annotations_accession", outputCol="InterPro_index")

    # catalogize X variable
    input_columns = ML_df.columns[1:]
    assembler = VectorAssembler(inputCols=input_columns,outputCol="InterPro_features")

    pipeline = Pipeline(stages=[Label,assembler])
    ML_final = pipeline.fit(ML_df).transform(ML_df)
    return ML_final

def split_data(ML_final,percentage=0.7):
    """
    it can help you split the data to trainning data and test data.
    ML_final: df
    percentage:int, you can set another value.
    return: trainData, df; testData,df
    """
    (trainData, testData) = ML_final.randomSplit([percentage, 1-percentage],seed=42)
    return trainData, testData

def ML_model(trainData,testData,model_type="rf"):
    """
    it will help you to make a model you want.
    trainData: spark df
    testData: spark df
    model_type: str, rf for random forest; dtc for decision tree, nb for naive bayes
    return model
    """
    # create a model
    if model_type == "rf":
        model = RandomForestClassifier(labelCol="InterPro_index",
                                        featuresCol="InterPro_features",
                                        predictionCol="prediction")

    if model_type == "dtc":
        model = DecisionTreeClassifier(labelCol="InterPro_index",
                                        featuresCol="InterPro_features",
                                        predictionCol="prediction")

    if model_type == "nb":
        model = NaiveBayes(modelType="multinomial",labelCol="InterPro_index",
                            featuresCol="InterPro_features",
                            predictionCol="prediction")

    model = model.fit(trainData)
    predict = model.transform(testData)

    # evaluate the result
    evaluator = MulticlassClassificationEvaluator(labelCol='InterPro_index',
                                                predictionCol = 'prediction',
                                                metricName='accuracy')

    accuracy = evaluator.evaluate(predict)
    print(f"Accuracy is {accuracy}")
    return model

def tuning_model(trainData,testData, model_type="rf"):
    """
    If you want to make your model better you must run this.
    trainData: spark df
    testData: spark df
    model_type: str, rf for random forest; dtc for decision tree, nb for naive bayes
    return tuning model
    """
    if model_type == "rf":
        # create a model
        model = RandomForestClassifier(labelCol="InterPro_index",
                                        featuresCol="InterPro_features",
                                        predictionCol="prediction", 
                                        seed=42,
                                        maxMemoryInMB = 256)
        # Tuning
        paramGrid = (ParamGridBuilder()
                    .addGrid(model.maxDepth, [5,10,20])
                    .addGrid(model.numTrees, [20,100])
                    .build())

    if model_type == "dtc":
        # create a model
        dtc = DecisionTreeClassifier(labelCol="InterPro_index",
                                    featuresCol="InterPro_features",
                                    predictionCol="prediction")  
        # Tuning
        paramGrid = (ParamGridBuilder()
                    .addGrid(dtc.maxDepth, [2,4,6,8,10,12])
                    .build())

    if model_type == "nb":
        # create a model
        nb = NaiveBayes(modelType="multinomial",labelCol="InterPro_index",
                            featuresCol="InterPro_features",
                            predictionCol="prediction",)    
        # Tuning
        paramGrid = (ParamGridBuilder()
                    .addGrid(nb.smoothing, [0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.5, 2.0])
                    .build())

    # evaluate the result
    evaluator = MulticlassClassificationEvaluator(labelCol='InterPro_index',
                                            predictionCol = 'prediction',
                                            metricName='accuracy')

    # KFold
    cv = CrossValidator(estimator=model,
                    evaluator=evaluator,
                    estimatorParamMaps=paramGrid,
                    numFolds=5,
                    parallelism=10,
                    seed=42)

    # Run Cross-validation
    cvModel = cv.fit(trainData)

    # Make predictions on testData. cvModel uses the bestModel.
    cvPredictions = cvModel.transform(testData)

    # Evaluate bestModel found from Cross Validation
    print(evaluator.evaluate(cvPredictions))
    return cvModel

def save_file(df,path):
    """
    it will help you to safe the df.
    df: spark df, for train or test df.
    paht: str, the location you want to save you file.
    return str
    """
    df.toPandas().set_index('InterPro_annotations_accession').to_pickle(path)
    return print('file safe in{path}')

def save_model(model,path):
    """
    it will help you to safe the model.
    model: spark model
    paht: str, the location you want to save you model.
    return str
    """
    model.bestModel.write().overwrite().save(path)
    return print('model safe in{path}')

def main():
    start = time.time()
    df = create_df("/data/dataprocessing/interproscan/all_bacilli.tsv")
    small_df,large_df = data_preprocessing(df)
    ML_final = ML_df_create(small_df,large_df)    
    trainData,testData = split_data(ML_final,0.7)
    model = ML_model(trainData,testData,"nb")
    cvModel = tuning_model(trainData,testData, "nb")
    end = time.time()
    print(end-start)
    save_file(trainData,"/students/2021-2022/master/Kai_DSLS/trainData.pkl")
    save_file(testData,"/students/2021-2022/master/Kai_DSLS/testData.pkl")
    save_model(model,"/students/2021-2022/master/Kai_DSLS/NaiveBayesModel")
    save_model(cvModel,"/students/2021-2022/master/Kai_DSLS/NaiveBayesBestModel")

if __name__ == '__main__':
    main()




