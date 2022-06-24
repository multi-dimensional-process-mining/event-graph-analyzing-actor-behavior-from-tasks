import analysis_confs


class AnalysisConfigurator:
    def __init__(self, pattern_subset_description):
        self.pattern_subset_description = pattern_subset_description
        self.analysis_directory = analysis_confs.analysis_directory[self.pattern_subset_description]
        self.pattern_filter_description = analysis_confs.pattern_filter_description[self.pattern_subset_description]
        self.pattern_filter_cypher = analysis_confs.pattern_filter_cypher[self.pattern_subset_description]
        self.encoding = analysis_confs.encoding[self.pattern_subset_description]
        self.num_clusters = analysis_confs.num_clusters[self.pattern_subset_description]
        self.exclude_clusters = analysis_confs.exclude_clusters[self.pattern_subset_description]
        self.cluster_descriptions = analysis_confs.cluster_descriptions[self.pattern_subset_description]

    def get_pattern_filter_description(self):
        return self.pattern_filter_description

    def get_analysis_directory(self):
        return self.analysis_directory

    def get_pattern_filter_cypher(self):
        return self.pattern_filter_cypher

    def get_encoding(self):
        return self.encoding

    def get_num_clusters(self):
        return self.num_clusters

    def get_exclude_clusters(self):
        return self.exclude_clusters

    def get_cluster_descriptions(self):
        return self.cluster_descriptions
