import os
import pandas as pd
import pydot
from graphviz import Digraph
from neo4j import GraphDatabase
from _collections import OrderedDict
from vis_reference import data_set_dictionaries

from GraphConfigurator import GraphConfigurator


class VariantVisualizer:

    def __init__(self, graph, analysis_directory):
        self.graph = graph
        self.gc = GraphConfigurator(graph)
        self.driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", self.gc.get_password()))

        with self.driver.session() as session:
            self.df_task_variants = \
                session.read_transaction(query_variants)

        self.node_properties = data_set_dictionaries.node_properties[self.gc.get_name_data_set()]
        self.abbr_dict = data_set_dictionaries.abbr_dict[self.gc.get_name_data_set()]

        self.analysis_directory = analysis_directory
        self.output_directory_colored = os.path.join(analysis_directory, "variant_visualizations_colored")
        self.output_directory_abbrev = os.path.join(analysis_directory, "variant_visualizations_abbrev")

    def visualize_variants_colored(self, print_variants_not_in_cluster=False):
        os.makedirs(self.output_directory_colored, exist_ok=True)

        df_variants_clustered = self.df_task_variants[~self.df_task_variants['cluster'].isna()].copy()

        cluster_list = list(df_variants_clustered['cluster'].unique())
        for cluster in cluster_list:
            df_variants_in_cluster = df_variants_clustered[df_variants_clustered['cluster'] == cluster].copy()
            df_variants_in_cluster.sort_values(by=['ID', 'frequency'], ascending=[True, False], inplace=True)
            dot_grouped_variants = get_dot_grouped_variants_colored(df_variants_in_cluster,
                                                                    node_properties=self.node_properties)
            (graph,) = pydot.graph_from_dot_data(dot_grouped_variants.source)
            graph.write_png(f"{self.output_directory_colored}\\variants_in_cluster_{cluster}.png")

        if print_variants_not_in_cluster:
            output_directory_not_clustered = os.path.join(self.output_directory_colored, "variants_not_in_cluster")
            os.makedirs(output_directory_not_clustered, exist_ok=True)

            df_variants_not_in_cluster = self.df_task_variants[self.df_task_variants['cluster'].isna()].copy()
            df_variants_not_in_cluster.reset_index(level=0, inplace=True)

            for index, row in df_variants_not_in_cluster.iterrows():
                path = df_variants_not_in_cluster.loc[index, 'path']
                variant_id = df_variants_not_in_cluster.loc[index, 'ID']
                dot_single_variant = get_dot_single_variant_colored(variant_id=variant_id, path=path,
                                                                    node_properties=self.node_properties)
                (graph,) = pydot.graph_from_dot_data(dot_single_variant.source)
                graph.write_png(f"{output_directory_not_clustered}\\variant_{variant_id}.png")

        print_legend(self.node_properties, self.output_directory_colored)


def get_dot_grouped_variants_colored(df_grouped_variants, node_properties):
    dot = Digraph(comment='Query Result')
    dot.attr("graph", rankdir="LR", margin="0", nodesep="0.25", ranksep="0.05")
    dot.attr("node", fixedsize="true", fontname="Helvetica", fontsize="10", margin="0")
    for index, row in df_grouped_variants.iloc[::-1].iterrows():
        path = row.path
        node_id = 1
        path_label = str(int(row['ID']))
        path_frequency = str(int(row['frequency']))

        with dot.subgraph() as s:
            s.attr(newrank="True")
            s.node(str(index), f'{path_label}\t{path_frequency}\l', shape="rect", width="1.7", color="white",
                   penwidth=str(0.5))
            s.edge(str(index), f'{index}_{node_id}', style="invis")

            for pos, event in enumerate(path[:-1]):
                s.node(f'{index}_{node_id}', "", style="filled",
                       shape=node_properties[event][0],
                       width=node_properties[event][1], height=node_properties[event][2],
                       fillcolor=node_properties[event][3], fontcolor="black", penwidth=str(0.5))
                s.edge(f'{index}_{node_id}', f'{index}_{node_id + 1}', style="invis")
                node_id += 1

            s.node(f'{index}_{node_id}', "", style="filled",
                   shape=node_properties[path[-1]][0],
                   width=node_properties[path[-1]][1], height=node_properties[path[-1]][2],
                   fillcolor=node_properties[path[-1]][3], fontcolor="black", penwidth=str(0.5))
    return dot


def get_dot_single_variant_colored(variant_id, path, node_properties, print_rank=True):
    dot = Digraph(comment='Query Result')
    dot.attr("graph", rankdir="LR", margin="0", nodesep="0.25", ranksep="0.05")
    dot.attr("node", fixedsize="true", fontname="Helvetica", fontsize="11", margin="0")

    node_id = 1
    if print_rank:
        dot.node(str(variant_id), f'{variant_id}\l', shape="rect", width="0.3", color="white", penwidth=str(0.5))
        dot.edge(str(variant_id), f'{variant_id}_{node_id}', style="invis")

    for event in path[:-1]:
        dot.node(f'{variant_id}_{node_id}', "", style="filled", shape=node_properties[event][0],
                 width=node_properties[event][1], height=node_properties[event][2],
                 fillcolor=node_properties[event][3], fontcolor="black", penwidth=str(0.5))
        dot.edge(f'{variant_id}_{node_id}', f'{variant_id}_{node_id + 1}', style="invis")
        node_id += 1
    dot.node(f'{variant_id}_{node_id}', "", style="filled", shape=node_properties[path[-1]][0],
             width=node_properties[path[-1]][1], height=node_properties[path[-1]][2],
             fillcolor=node_properties[path[-1]][3], fontcolor="black", penwidth=str(0.5))
    return dot


def print_legend(node_properties, output_directory):
    dot = Digraph(comment='Query Result')
    dot.attr("graph", rankdir="LR", margin="0", nodesep="0.05", ranksep="0.05")
    dot.attr("node", fixedsize="false", fontname="Helvetica", fontsize="12", margin="0")

    node_id = 1

    for key, value in OrderedDict(reversed(list(node_properties.items()))).items():
        with dot.subgraph() as s:
            s.attr(newrank="True")
            s.node(str(node_id), "", style="filled", shape=node_properties[key][0],
                   width=node_properties[key][1], height=node_properties[key][2],
                   fillcolor=node_properties[key][3], fontcolor="black", penwidth=str(0.5))
            s.edge(str(node_id), str(node_id + 1), style="invis")
            s.node(str(node_id + 1), f'{key}\l', shape="rect", width="2", color="white",
                   height=node_properties[key][2], fontcolor="black", penwidth=str(0.5))
            node_id += 2

    (graph,) = pydot.graph_from_dot_data(dot.source)
    graph.write_png(f"{output_directory}\\legend.png")


def query_variants(tx):
    q = f'''
        MATCH (ti:TaskInstance)
        WITH ti.path AS path, ti.ID AS ID, ti.cluster AS cluster
        WITH DISTINCT path, ID, cluster, COUNT (*) AS frequency
        RETURN cluster, ID, path, frequency
        '''
    result = tx.run(q)
    df_variants = pd.DataFrame([dict(record) for record in result])
    return df_variants
