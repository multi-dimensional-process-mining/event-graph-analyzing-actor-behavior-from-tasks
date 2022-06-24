import pandas as pd
import matplotlib.pyplot as plt
from sklearn import metrics
from scipy.cluster.hierarchy import dendrogram, linkage
from sklearn.cluster import AgglomerativeClustering


class Clusterer:

    def __init__(self, show_dendrogram=False, affinity="euclidean", linkage="ward"):
        self.show_dendrogram = show_dendrogram
        self.affinity = affinity
        self.linkage = linkage

    # def cluster_task_variants_agglomerative(self, df_task_variants, df_task_variants_encoded):
    #     if self.show_dendrogram:
    #         fig = plt.figure(figsize=(25, 10))
    #         dn = dendrogram(linkage(df_task_variants_encoded.values, 'ward'))
    #         plt.show()
    #     num_clusters = int(input("Specify number of clusters: "))
    #
    #     clusters = AgglomerativeClustering(n_clusters=num_clusters, affinity=self.affinity, linkage=self.linkage) \
    #         .fit_predict(df_task_variants_encoded.values)
    #     # df_task_variants_clustered = df_task_variants.copy()
    #     index = list(df_task_variants_encoded.index.values)
    #     df_clusters = pd.DataFrame(index=index, data={'cluster': clusters})
    #     # df_task_variants_clustered["cluster"] = clusters
    #     # df_task_variants_clustered = df_task_variants_clustered[['cluster', 'path']]
    #     # df_task_variants_clustered = df_task_variants_clustered.sort_values(by=['cluster', 'rank'],
    #     #                                                                     ascending=True)
    #     df_task_variants_clustered = pd.concat([df_task_variants, df_clusters], axis=1)
    #     return df_task_variants_clustered, num_clusters

    def cluster_task_variants_agglomerative(self, df_task_variants_encoded, num_clusters=""):
        if self.show_dendrogram:
            fig = plt.figure(figsize=(25, 10))
            dn = dendrogram(linkage(df_task_variants_encoded.values, 'ward'))
            plt.show()
            num_clusters = int(input("Specify number of clusters: "))
        else:
            num_clusters = num_clusters

        clusters = AgglomerativeClustering(n_clusters=num_clusters, affinity=self.affinity, linkage=self.linkage) \
            .fit_predict(df_task_variants_encoded.values)
        # df_task_variants_clustered = df_task_variants.copy()
        index = list(df_task_variants_encoded.index.values)
        df_clusters = pd.DataFrame(index=index, data={'cluster': clusters})
        # df_task_variants_clustered["cluster"] = clusters
        # df_task_variants_clustered = df_task_variants_clustered[['cluster', 'path']]
        # df_task_variants_clustered = df_task_variants_clustered.sort_values(by=['cluster', 'rank'],
        #                                                                     ascending=True)

        return df_clusters, num_clusters

    def get_silhouette_score(self, df_task_variants_encoded, num_clusters):
        clusters = AgglomerativeClustering(n_clusters=num_clusters, affinity=self.affinity, linkage=self.linkage) \
            .fit_predict(df_task_variants_encoded.values)
        s_score = metrics.silhouette_score(df_task_variants_encoded.values, clusters)
        return s_score
