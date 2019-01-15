"""
DCC loader module
    extract_observations:
    extract_ontological_observations:
    extract_unidimensional_observations:
    extract_time_series_observations:
    extract_categorical_observations:
    extract_samples:
"""
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import explode, lit

import xml.etree.ElementTree as ET
import pandas as pd


def extract_observations(spark_session: SparkSession,
                         file_path: str) -> Tuple[DataFrame, DataFrame]:
    """

    :param spark_session:
    :param file_path:
    :return:
    """
    experiments_df = extract_experiment_files(spark_session, file_path)
    unidimensional_observations = extract_unidimensional_observations(experiments_df)
    ontological_observations = extract_ontological_observations(experiments_df)
    return unidimensional_observations, ontological_observations


def extract_ontological_observations(experiments_df: DataFrame) -> DataFrame:
    """

    :param experiments_df:
    :return:
    """
    ontological_observations = experiments_df \
        .withColumn('ontologyParameter', explode('procedure.ontologyParameter'))
    ontological_observations = ontological_observations \
        .withColumn('procedureId', experiments_df['procedure']['_procedureID']) \
        .withColumn('parameterId', ontological_observations['ontologyParameter']['_parameterID']) \
        .withColumn('term', ontological_observations['ontologyParameter']['term']) \
        .drop('procedure') \
        .drop('ontologyParameter')
    return ontological_observations


def extract_unidimensional_observations(experiments_df: DataFrame) -> DataFrame:
    """

    :param experiments_df:
    :return:
    """
    unidimensional_observations = experiments_df \
        .withColumn('simpleParameter', explode('procedure.simpleParameter'))
    unidimensional_observations = unidimensional_observations \
        .withColumn('procedureId', experiments_df['procedure']['_procedureID']) \
        .withColumn('parameterId', unidimensional_observations['simpleParameter']['_parameterID']) \
        .withColumn('value', unidimensional_observations['simpleParameter']['value']) \
        .drop('procedure') \
        .drop('simpleParameter')
    return unidimensional_observations


def extract_time_series_observations(experiments_df: DataFrame) -> DataFrame:
    """

    :param experiments_df:
    :return:
    """
    time_series_observations = experiments_df \
        .withColumn('seriesParameter', explode('procedure.seriesParameter'))
    time_series_observations = time_series_observations \
        .withColumn('procedureId', experiments_df['procedure']['_procedureID']) \
        .withColumn('parameterId', time_series_observations['seriesParameter']['_parameterID']) \
        .withColumn('value', time_series_observations['seriesParameter']['value']) \
        .drop('procedure') \
        .drop('seriesParameter')
    return time_series_observations


def extract_metadata_observations(experiments_df: DataFrame) -> DataFrame:
    """

    :param experiments_df:
    :return:
    """
    time_series_observations = experiments_df \
        .withColumn('seriesParameter', explode('procedure.seriesParameter'))
    time_series_observations = time_series_observations \
        .withColumn('procedureId', experiments_df['procedure']['_procedureID']) \
        .withColumn('parameterId', time_series_observations['seriesParameter']['_parameterID']) \
        .withColumn('value', time_series_observations['seriesParameter']['value']) \
        .drop('procedure') \
        .drop('seriesParameter')
    return time_series_observations


def extract_categorical_observations(experiments_df: DataFrame) -> DataFrame:
    """

    :param experiments_df:
    :return:
    """
    return experiments_df


def extract_experiment_files(spark_session: SparkSession,
                             experiment_dir_path: str) -> DataFrame:
    """

    :param spark_session:
    :param experiment_dir_path:
    :return:
    """
    experiments_df = spark_session.read.format("com.databricks.spark.xml") \
        .options(rowTag="experiment", samplingRatio="1").load(experiment_dir_path)
    return experiments_df

def xml_2_df(spark_session: SparkSession, xml_dir_path: str):
    tree = ET.parse(xml_dir_path)
    root = tree.getRoot()
    for child in root:
        print(child.tag, child.attrib, child.text)


def extract_samples(spark_session: SparkSession, specimen_dir_path: str) -> DataFrame:
    """

    :param spark_session:
    :param specimen_dir_path:
    :return:

    Need:
        datasourceShortName
        CentreSpecimen df
        Centre df
        Specimen df
        Mouse df
        Embryo df
        Pipeline df
        Project df
        StatusCode df
        ColonyID df
        Genotype df
        ParentalStrain df (NOTHING TO DO, AS MYSQL TABLE IS EMPTY)
        ChromosomalAlteration df (NOTHING TO DO, AS MYSQL TABLE IS EMPTY)
        RelatedSpecimen df







    """
    tree = ET.parse("/Users/mrelac/workspace/py/impc-etl/tests/data/J.2018-10-05.9.specimenMOD2.xml")
    centre_specimen_set = tree.getroot()
    print(centre_specimen_set.tag, centre_specimen_set.attrib, centre_specimen_set.text)

    d = []
    col_names = ['datasourceShortName', 'project', 'phenotypingCentre', 'productionCentre', 'specimenID', 'colonyID', 'gender', 'isBaseline', 'litterId', 'strainID', 'zygosity', DATE_OF_STATUSCODE, STATUSCODE_VALUE, 'DOB', 'stage', 'stageUnit', MGI_GENE_ID, GENE_SYMBOL, MGI_ALLELE_ID, FATHER_ZYGOSITY, MOTHER_ZYGOSITY, RELATED_SPECIMEN_ID, RELATED_SPECIMEN_RELATIONSHIP ]
    specimen_df = pd.DataFrame(columns = col_names)

    for centre in centre_specimen_set:
        print(centre.tag, centre.attrib, centre.text)

        inner = {}

        for child in centre:
            print (child.tag, child.attrib, child.text)

            if (child.tag == 'embryo'):
                inner[child.tag]

    # mice_df = spark_session.read.format("com.databricks.spark.xml") \
    #     .options(rowTag="mouse", mode="DROPMALFORMED").load(specimen_dir_path)
    # embryos_df = spark_session.read.format("com.databricks.spark.xml") \
    #     .options(rowTag="embryo", mode="DROPMALFORMED").load(specimen_dir_path)
    # mice_df = mice_df.withColumn('type', lit('Mouse')).withColumn('_stage', lit(None)).withColumn('_stageUnit', lit(None))
    # embryos_df = embryos_df.withColumn('type', lit('Embryo')).withColumn('_DOB', lit(None)).select(mice_df.schema.names)
    # return mice_df.unionAll(embryos_df)
