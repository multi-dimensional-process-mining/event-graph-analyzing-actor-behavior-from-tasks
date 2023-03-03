import os
import pandas as pd
from os import path
from neo4j import GraphDatabase

import PatternEncoderFactory
from Clusterer import Clusterer
from GraphConfigurator import GraphConfigurator


class TaskClusterModule:
    def __init__(self, graph, password, analysis_directory, pattern_filter_description, pattern_filter_cypher, encoding,
                 num_clusters):
        print("Initializing task cluster module...")
        self.graph = graph
        self.driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", password))
        self.analysis_directory = analysis_directory
        self.pattern_filter_description = pattern_filter_description
        self.encoding = encoding
        self.num_clusters = num_clusters

        self.meta_directory = f"meta-output\\patterns\\"
        self.gc = GraphConfigurator(graph)
        if path.exists(f"{self.meta_directory}patterns_{graph}_{pattern_filter_description}.pkl"):
            self.df_patterns = pd.read_pickle(f"{self.meta_directory}patterns_{graph}_{pattern_filter_description}.pkl")
        else:
            with self.driver.session() as session:
                self.df_patterns = session.read_transaction(query_pattern_variants_and_frequencies, pattern_filter_cypher)

        self.df_patterns_clustered = pd.DataFrame([])
        self.cluster(encoding, num_clusters)

    def get_patterns_clustered(self):
        return self.df_patterns_clustered

    def cluster_evaluation(self, encoding, list_num_clusters):
        e = PatternEncoderFactory.get_pattern_encoder(self.gc.get_name_data_set(), encoding[0], encoding[1],
                                                      encoding[2])
        pattern_subset_to_cluster = self.df_patterns[self.df_patterns['path_length'] > 1]
        pattern_subset_encoded = e.encode(pattern_subset_to_cluster)
        for num_clusters in list_num_clusters:
            silhouette_score = Clusterer(show_dendrogram=False) \
                .get_silhouette_score(pattern_subset_encoded, num_clusters)
            print(f"Number of clusters: {num_clusters} \t\t Silhouette score: {silhouette_score}")

    def cluster(self, encoding, num_clusters):
        if path.exists(
                f"{self.meta_directory}patterns_{self.graph}_{self.pattern_filter_description}_{num_clusters}.pkl"):
            print("\tRetrieve clustering from pickle...")
            self.df_patterns_clustered = pd.read_pickle(
                f"{self.meta_directory}patterns_{self.graph}_{self.pattern_filter_description}_{num_clusters}.pkl")
        else:
            print("\tPerform new clustering...")
            e = PatternEncoderFactory.get_pattern_encoder(self.gc.get_name_data_set(), encoding[0], encoding[1],
                                                          encoding[2])
            pattern_subset_to_cluster = self.df_patterns[self.df_patterns['path_length'] > 1]
            pattern_subset_encoded = e.encode(pattern_subset_to_cluster)
            df_clusters, num_clusters = Clusterer(show_dendrogram=False) \
                .cluster_task_variants_agglomerative(pattern_subset_encoded, num_clusters)
            self.df_patterns_clustered = pd.concat([self.df_patterns, df_clusters], axis=1)
            self.cluster_encoding = encoding
            os.makedirs(self.meta_directory, exist_ok=True)
            print("\tSave clustering to pickle...")
            self.df_patterns_clustered.to_pickle(
                f"{self.meta_directory}patterns_{self.graph}_{self.pattern_filter_description}_{num_clusters}.pkl")


def query_pattern_variants_and_frequencies(tx, filter):
    q = f'''
        MATCH (ti:TaskInstance)
        WITH ti.path AS path, ti.ID AS ID, size(ti.path) AS path_length
        WITH DISTINCT path, path_length, ID, COUNT (*) AS frequency {filter}
        RETURN path, path_length, ID, frequency
        '''
    # print(q)
    result = tx.run(q)
    df_pattern_variants = pd.DataFrame([dict(record) for record in result])
    return df_pattern_variants
