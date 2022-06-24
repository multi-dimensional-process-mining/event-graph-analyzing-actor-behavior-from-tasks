from neo4j import GraphDatabase
from graphviz import Digraph
import os
import numpy as np
import pandas as pd
import matplotlib as mpl
from matplotlib import cm
from matplotlib.colors import ListedColormap
from matplotlib import pyplot as plt
from palettable.colorbrewer.diverging import PuOr_9_r
from palettable.colorbrewer.sequential import Purples_4_r, Oranges_4, Purples_5_r, Oranges_5
from matplotlib.ticker import FuncFormatter
from colormap import rgb2hex, rgb2hls, hls2rgb
from vis_reference import data_set_dictionaries
from PerformanceRecorder import PerformanceRecorder

# COLORS
white = "#ffffff"
black = "#000000"
light_grey = '#dedede'
dark_grey = '#787878'

color_dict = {'resource': {'medium': '#d73027',  # red
                           'dark': '#570000',
                           'light_grey': '#dbd1d0',
                           'medium_grey': '#bfb5b4'},
              'case': {'medium': '#4575b4',  # blue
                       'dark': '#002759',
                       'light_grey': '#dedfe3',
                       'medium_grey': '#bcbdc2'},
              'purple': {'medium': '#7904ba',  # purple
                         'dark': '#48036e',
                         'light_grey': '#cbbad4',
                         'medium_grey': '#9e8ca8'},
              'grey': {'medium': '#3d3d3d',  # grey
                         'dark': '#000000',
                         'light_grey': '#bcbcbc',
                         'medium_grey': '#a9a9a9'},
              1: {'medium': '#1b9e77',  # forest green
                  'dark': '#0c5741',
                  'light_grey': '#bed4ce',
                  'medium_grey': '#92a8a2',
                  'color_map_list': ['#bed4ce', '#abc6be', '#8aaca0', '#659181', '#4b7d6b', '#3e7461']},
              2: {'medium': '#d95f02',  # orange
                  'dark': '#803801',
                  'light_grey': '#d1bdae',
                  'medium_grey': '#c9aa93',
                  'color_map_list': ['#edd4c9', '#e5c5b6', '#d4a892', '#bd8666', '#ac6e48', '#a3623a']},
              3: {'medium': '#7570b3',  # purple
                  'dark': '#5b578a',
                  'light_grey': '#aaa8bd',
                  'medium_grey': '#9391a3',
                  'color_map_list': ['#edd4c9', '#e5c5b6', '#d4a892', '#bd8666', '#ac6e48', '#a3623a']},
              4: {'medium': '#e6ab02',  # yellow
                  'dark': '#a87d02',
                  'light_grey': '#d4c494',
                  'medium_grey': '#ad9c6a',
                  'color_map_list': ['#edd4c9', '#e5c5b6', '#d4a892', '#bd8666', '#ac6e48', '#a3623a']}
              }


class DFGVisualizer:

    def __init__(self, graph, password, name_data_set, entity_labels, action_lifecycle_labels, analysis_directory,
                 exclude_clusters=None):
        self.driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", password))
        self.graph = graph
        self.name_data_set = name_data_set
        self.entity_labels = entity_labels
        self.action_lifecycle_labels = action_lifecycle_labels
        self.analysis_directory = analysis_directory
        self.cluster_descriptions = data_set_dictionaries.cluster_descriptions[self.name_data_set]
        self.abbr_dict_lpm = data_set_dictionaries.abbr_dict_lpm_2[self.name_data_set]
        self.exclude_clusters = exclude_clusters

    def visualize_intra_cluster_graph(self, cluster):
        dot = Digraph(comment='Query Result')
        dot.attr("graph", rankdir="LR", margin="0", ranksep="2.0", nodesep="1.0")
        with self.driver.session() as session:
            session.read_transaction(self.get_intra_cluster_DFG, dot, cluster)
        output_directory = os.path.join(self.analysis_directory, "DFGs", "DFG_inter_cluster")
        os.makedirs(output_directory, exist_ok=True)
        dot.render(f'{output_directory}\\DFG_inter_cluster_{cluster}', view=True)

    def visualize_cluster_DFG(self, entity_type, df_show_threshold, start_end_date=None, resources=None,
                              print_description=False):
        dot = Digraph(comment='Query Result')
        dot.attr("graph", rankdir="LR", margin="0", ranksep="1.5", nodesep="0.5")
        with self.driver.session() as session:
            dfg_id = 0
            task_instance_ids = session.read_transaction(get_task_instance_ids, start_end_date, resources)
            session.read_transaction(self.get_DFG, dot, dfg_id, task_instance_ids, entity_type, df_show_threshold,
                                     print_description=print_description)
        output_directory = os.path.join(self.analysis_directory, "DFGs", "DFG_clusters")
        os.makedirs(output_directory, exist_ok=True)
        dot.render(f'{output_directory}\\DFG_{entity_type}_{df_show_threshold}'
                   f'{get_file_name_resources(resources)}{get_file_name_time_frame(start_end_date)}', view=True)

    def visualize_cluster_DFG_concept_drift_comparison(self, entity_type, df_show_threshold, start_end_dates):
        dot = Digraph(comment='Query Result')
        dot.attr("graph", rankdir="LR", margin="0", ranksep="1.0", nodesep="0.5")
        with self.driver.session() as session:
            dfg_id = 0
            task_instance_ids_before = session.read_transaction(get_task_instance_ids,
                                                                start_end_date=start_end_dates[0])
            task_instance_ids_after = session.read_transaction(get_task_instance_ids, start_end_date=start_end_dates[1])
            session.read_transaction(self.get_DFG_concept_drift_comparison, dot, dfg_id, task_instance_ids_before,
                                     task_instance_ids_after,
                                     entity_type, df_show_threshold)
        output_directory = os.path.join(self.analysis_directory, "DFGs", "DFG_clusters")
        os.makedirs(output_directory, exist_ok=True)
        dot.render(f'{output_directory}\\DFG_concept_drift_comparison_{df_show_threshold}', view=True)

    def visualize_cluster_DFG_resources(self, df_show_threshold_case, df_show_threshold_resource, resources_lists_over,
                                        start_end_date=None, resources=None):
        dot = Digraph(comment='Query Result')
        dot.attr("graph", rankdir="LR", margin="0", ranksep="3.0", nodesep="0.5")
        with self.driver.session() as session:
            dfg_id = 0
            dfg_ids = [dfg_id]
            task_instance_ids = session.read_transaction(get_task_instance_ids, start_end_date, resources)
            list_task_instance_ids = [task_instance_ids]
            for resource_list in resources_lists_over:
                dfg_id += 1
                dfg_ids.append(dfg_id)
                task_instance_ids = session.read_transaction(get_task_instance_ids, start_end_date, resource_list)
                list_task_instance_ids.append(task_instance_ids)
            session.read_transaction(self.get_DFG_resources_overlaid, dot, dfg_ids, list_task_instance_ids,
                                     df_show_threshold_case, df_show_threshold_resource, resources_lists_over)
        output_directory = os.path.join(self.analysis_directory, "DFGs", "DFG_clusters")
        os.makedirs(output_directory, exist_ok=True)
        dot.render(
            f'{output_directory}\\DFG_resources_overlaid_{df_show_threshold_case}_{get_file_name_time_frame(start_end_date)}{get_file_name_resources(resources)}_{df_show_threshold_resource}',
            view=True)

    def get_DFG(self, tx, dot, dfg_id, task_instance_ids, entity_type, df_show_threshold, print_description=False):
        pr = PerformanceRecorder(self.graph, f'constructing_DFG')
        font_size_large = 38
        font_size_small = 34
        # set default graph properties
        dot.attr("node", shape="rectangle", fixedsize="false", fontname="Helvetica",
                 fontsize=str(font_size_large), margin="0.2", color=black, style="rounded,filled", fillcolor=white,
                 fontcolor=black, penwidth="3")
        dot.attr("edge", fontname="Helvetica", fontsize=str(font_size_small))

        node_results, log_min_node_freq, log_max_node_freq = self.query_DFG_nodes_min_max_absolute(tx,
                                                                                                   task_instance_ids)
        pr.record_performance("query_nodes")
        edge_results = self.query_DFG_edges_absolute(tx, task_instance_ids, entity_type)
        pr.record_performance("query_edges")
        start_edge_results, end_edge_results = self.query_DFG_start_and_end_absolute(tx, task_instance_ids, entity_type,
                                                                                     dfg_id)
        pr.record_performance("query_start_end_edges")
        edge_results.extend(start_edge_results)
        edge_results.extend(end_edge_results)

        edge_frequencies_absolute = list_edge_frequencies(edge_results, 'abs_freq')

        # edge_show_threshold = get_edge_cutoff(edge_frequencies_absolute, df_show_threshold)
        edge_show_threshold = 0
        edge_weight_threshold = 10  # absolute weight threshold
        log_min_edge_freq, log_max_edge_freq = get_edge_log_min_max(edge_weight_threshold,
                                                                    max(edge_frequencies_absolute))
        pr.record_performance("calculate_parameters")

        node_ids = []
        start_count = 0
        end_count = 0
        for record in edge_results:
            if record['abs_freq'] > edge_show_threshold:
            # if df_show_threshold == 1 or record['abs_freq'] > edge_show_threshold:
                n1_id = f"{record['n1']}"
                n2_id = f"{record['n2']}"
                if n1_id[:5] == 'start':
                    start_count += int(record['abs_freq'])
                if n2_id[:3] == 'end':
                    end_count += int(record['abs_freq'])
                node_ids.extend([n1_id, n2_id])
                edge_weight = get_edge_weight(record['abs_freq'], edge_weight_threshold, log_min_edge_freq,
                                              log_max_edge_freq)
                edge_color, edge_font_color = get_medium_and_dark_color(True, 'case', record['abs_freq'],
                                                                        edge_weight_threshold,
                                                                        dfg_id=0)
                if record['abs_freq'] > 150:
                    edge_label = str(record['abs_freq'])
                else:
                    edge_label = ""
                    edge_color = 'lightgrey'
                dot.edge(n1_id, n2_id, xlabel=edge_label, penwidth=f"{edge_weight}", color=edge_color, fontcolor=edge_font_color)
        node_ids = set(node_ids)
        pr.record_performance("draw_edges")

        for record in node_results:
            if f"{record['cluster']}" in node_ids:
                node_id = f"{record['cluster']}"
                if print_description:
                    node_description = f"{self.cluster_descriptions[int(record['cluster'])]}<br/>{record['abs_freq']}>"
                else:
                    node_description = f"<<b>C{int(record['cluster'])}</b><br/><FONT POINT-SIZE=\"{str(font_size_small)}\">{record['abs_freq']}</FONT>>"
                node_fill_color, node_font_color = get_node_colors(record['abs_freq'], log_min_node_freq,
                                                                   log_max_node_freq)
                dot.node(node_id, node_description, fillcolor=node_fill_color, fontcolor=node_font_color)
        dot.node(f"start_{dfg_id}", f"<<b>start</b><br/><FONT POINT-SIZE=\"{str(font_size_small)}\">{start_count}</FONT>>", node='source')
        dot.node(f"end_{dfg_id}", f"<<b>end</b><br/><FONT POINT-SIZE=\"{str(font_size_small)}\">{end_count}</FONT>>", node='sink')
        pr.record_performance("draw_nodes")
        return dot

    def get_DFG_concept_drift_comparison(self, tx, dot, dfg_id, task_instance_ids_before, task_instance_ids_after,
                                         entity_type, df_show_threshold):
        pr = PerformanceRecorder(self.graph, f'constructing_DFG')
        # set default graph properties
        font_size_large = 38
        font_size_small = 34
        dot.attr("node", shape="rectangle", fixedsize="false", fontname="Helvetica",
                 fontsize=str(font_size_large), margin="0.2", color=black, style="rounded,filled", fillcolor=white,
                 fontcolor=black, penwidth="3")
        dot.attr("edge", fontname="Helvetica", fontsize=str(font_size_small))

        df_edge_results_joint, df_node_results_joint = self.get_edge_and_node_results_joint_cd_comparison(tx,
                                                                                                          entity_type,
                                                                                                          dfg_id,
                                                                                                          task_instance_ids_before,
                                                                                                          task_instance_ids_after)
        pr.record_performance("query_nodes_and_edges")

        edge_abs_max = max(abs(min(df_edge_results_joint['percentage_difference'].tolist())),
                           abs(max(df_edge_results_joint['percentage_difference'].tolist())))
        node_abs_max = max(abs(min(df_node_results_joint['percentage_difference'].tolist())),
                           abs(max(df_node_results_joint['percentage_difference'].tolist())))
        abs_max = max(edge_abs_max, node_abs_max)
        # edge_abs_max_rel = max(abs(min(df_edge_results_joint['relative_percentage_difference'].tolist())),
        #                    abs(max(df_edge_results_joint['relative_percentage_difference'].tolist())))
        # node_abs_max_rel = max(abs(min(df_node_results_joint['relative_percentage_difference'].tolist())),
        #                    abs(max(df_node_results_joint['relative_percentage_difference'].tolist())))
        edge_show_threshold_abs = 10  # minimum absolute frequency of before and after
        edge_color_threshold_abs = 100  # minimum sum of absolute frequency before and after
        min_perc_case_diff = 0.02
        min_rel_perc_case_diff = 1.0
        edge_max_freq_abs = max(df_edge_results_joint['sum_freq'].tolist())
        log_min_edge_freq, log_max_edge_freq = get_edge_log_min_max(edge_show_threshold_abs, edge_max_freq_abs)
        pr.record_performance("calculate_parameters")

        node_ids = []
        for index, row in df_edge_results_joint.iterrows():
            if row['before_freq'] >= edge_show_threshold_abs and row['after_freq'] >= edge_show_threshold_abs:
                n1_id = str(row['n1'])
                n2_id = str(row['n2'])
                node_ids.extend([n1_id, n2_id])
                edge_weight = get_edge_weight(row['sum_freq'], edge_show_threshold_abs, log_min_edge_freq,
                                              log_max_edge_freq)
                if row['sum_freq'] >= edge_color_threshold_abs and not min_perc_case_diff * -1.0 < row[
                    'percentage_difference'] < min_perc_case_diff:
                    edge_color, edge_font_color = get_colors_comparison(row['percentage_difference'], abs_max)
                    count_before = int(row['before_freq'])
                    count_after = int(row['after_freq'])
                    count_difference = count_after - count_before
                    if count_difference < 0:
                        count_difference_string = f"-{abs(count_difference)}"
                    else:
                        count_difference_string = f"+{abs(count_difference)}"
                    dot.edge(n1_id, n2_id, xlabel=f'{int(row["before_freq"])}\n{count_difference_string}',
                             penwidth=f"{edge_weight}", color=edge_color, fontcolor=edge_font_color)
                else:
                    dot.edge(n1_id, n2_id, penwidth=f"{edge_weight}", color='lightgrey')
        node_ids = set(node_ids)

        for index, row in df_node_results_joint.iterrows():
            node_id = str(row["cluster"])
            if node_id in node_ids:
                node_fill_color, node_font_color = white, black
                count_before = int(row['before_freq'])
                count_after = int(row['after_freq'])
                count_difference = count_after - count_before
                if count_difference < 0:
                    count_difference_string = f"-{abs(count_difference)}"
                else:
                    count_difference_string = f"+{abs(count_difference)}"
                if node_id in [f'start_{dfg_id}', f'end_{dfg_id}']:
                    node_description = f"<<b>{node_id[:node_id.index('_')]}</b><br/><FONT COLOR=\"{dark_grey}\" POINT-SIZE=\"{str(font_size_small)}\">{int(row['before_freq'])}<br/>{count_difference_string}</FONT>>"
                else:
                    freq_font_color = dark_grey
                    if not min_perc_case_diff * -1.0 < row['percentage_difference'] < min_perc_case_diff:
                        node_fill_color, _ = get_colors_comparison(row['percentage_difference'], abs_max)
                        freq_font_color = black
                        if abs(row['percentage_difference']) > 0.7 * abs_max:
                            node_font_color = white
                            freq_font_color = white
                    node_description = f"<<b>C{int(row['cluster'])}</b><br/><FONT COLOR=\"{freq_font_color}\" POINT-SIZE=\"{str(font_size_small)}\">{int(row['before_freq'])}<br/>{count_difference_string}</FONT>>"
                dot.node(node_id, node_description, fillcolor=node_fill_color, fontcolor=node_font_color)
        pr.record_performance('draw_nodes')
        print_cmap(abs_max, min_perc_case_diff)
        return dot

    def get_DFG_resources_overlaid(self, tx, dot, dfg_ids, list_task_instance_ids, df_show_threshold_case,
                                   df_show_threshold_resource, resources_over):
        pr = PerformanceRecorder(self.graph, f'constructing_DFG')
        # set default graph properties
        font_size_large = 38
        font_size_small = 34
        dot.attr("node", shape="rectangle", fixedsize="false", fontname="Helvetica",
                 fontsize=str(font_size_large), margin="0.2", color=black, style="rounded,filled", fillcolor=white,
                 fontcolor=black, penwidth="3")
        dot.attr("edge", fontname="Helvetica", fontsize=str(font_size_small))

        df_edge_results_over, df_node_results_over = self.get_edge_and_node_results_joint_resource_specific(tx,
                                                                                                            'resource',
                                                                                                            dfg_ids[1:],
                                                                                                            list_task_instance_ids[
                                                                                                            1:])
        df_edge_results_under, _ = self.get_edge_and_node_results_joint_resource_specific(tx, 'case', dfg_ids[:1],
                                                                                          list_task_instance_ids[:1],
                                                                                          include_start_end_nodes=False)
        pr.record_performance("query_nodes_and_edges")

        df_edge_results_all = df_edge_results_over.append(df_edge_results_under)

        # max_edge_frequency = max(max(df_edge_results_over['abs_freq'].tolist()), max(df_edge_results_under['abs_freq'].tolist()))
        max_edge_frequency = max(df_edge_results_all['abs_freq'].tolist())
        df_show_threshold = 1.1
        # df_show_threshold = min(df_show_threshold_resource, df_show_threshold_case)
        log_min_edge_freq, log_max_edge_freq = get_edge_log_min_max(df_show_threshold, max_edge_frequency)
        pr.record_performance("calculate_parameters")

        node_ids = []
        for index, row in df_edge_results_all.iterrows():
            if (row['dfg_id'] == 0 and row['abs_freq'] >= df_show_threshold_case) or (
                    row['dfg_id'] > 0 and row['abs_freq'] >= df_show_threshold_resource):
                n1_id = str(row['n1'])
                n2_id = str(row['n2'])
                node_ids.extend([n1_id, n2_id])
                edge_weight = get_edge_weight(row['abs_freq'], df_show_threshold, log_min_edge_freq, log_max_edge_freq)
                if row['dfg_id'] > 0:
                    edge_color, edge_font_color = get_medium_and_dark_color(True, 'resource', row['abs_freq'],
                                                                            df_show_threshold-1, dfg_id=int(row['dfg_id']))
                    dot.edge(n1_id, n2_id, xlabel=f'{int(row["abs_freq"])}', penwidth=f"{edge_weight}",
                             color=edge_color, fontcolor=edge_font_color)
                else:
                    edge_color, edge_font_color = get_medium_and_dark_color(False, 'case', row['abs_freq'],
                                                                            df_show_threshold, dfg_id=int(row['dfg_id']))
                    dot.edge(n1_id, n2_id, xlabel=f'{int(row["abs_freq"])}', penwidth=f"{edge_weight}",
                             color=edge_color, fontcolor=edge_font_color)
        node_ids = set(node_ids)

        for index, row in df_node_results_over.iterrows():
            node_id = str(row["n"])
            if node_id in node_ids:
                _, node_fill_color = get_medium_and_dark_color(False, 'resource', row['abs_freq'], df_show_threshold, dfg_id=int(row['dfg_id']))
                _, node_font_color = get_medium_and_dark_color(True, 'resource', row['abs_freq'], 0, dfg_id=int(row['dfg_id']))
                # _, node_font_color = get_medium_and_dark_color(True, 'resource', row['abs_freq'], df_show_threshold, dfg_id=int(row['dfg_id']))
                if node_id[:node_id.index('_')] == 'start':
                    node_description = f"<<b><FONT COLOR=\"black\">{resources_over[row['dfg_id']-1][0]}</FONT><br/>{node_id[:node_id.index('_')]}</b><br/><FONT COLOR=\"{node_font_color}\" POINT-SIZE=\"{str(font_size_small)}\">{int(row['abs_freq'])}</FONT>>"
                elif node_id[:node_id.index('_')] == 'end':
                    node_description = f"<<b>{node_id[:node_id.index('_')]}</b><br/><FONT COLOR=\"{node_font_color}\" POINT-SIZE=\"{str(font_size_small)}\">{int(row['abs_freq'])}</FONT>>"
                else:
                    node_description = f"<<b>C{int(float(row['n'][:row['n'].index('_')]))}</b><br/><FONT COLOR=\"{node_font_color}\" POINT-SIZE=\"{str(font_size_small)}\">{int(row['abs_freq'])}</FONT>>"
                dot.node(node_id, node_description, fillcolor=node_fill_color, fontcolor=node_font_color)
        pr.record_performance('draw_nodes')
        return dot

    def get_intra_cluster_DFG(self, tx, dot, cluster):
        # set default graph properties
        font_size_large = 38
        font_size_small = 34
        dot.attr("node", shape="rectangle", fixedsize="false", fontname="Helvetica",
                 fontsize=str(font_size_large), margin="0.2", color=black, style="rounded,filled", fillcolor=white,
                 fontcolor=black, penwidth="3")
        dot.attr("edge", color=color_dict['grey']['medium'], penwidth="2", fontname="Helvetica", fontsize=str(font_size_small),
                 fontcolor=color_dict['grey']['dark'])
        grey_threshold = 300

        node_results, log_min_node_freq, log_max_node_freq = query_inter_cluster_DFG_nodes_min_max(tx, cluster)
        edge_results, log_min_edge_freq, log_max_edge_freq = query_inter_cluster_DFG_edges_min_max(tx, cluster,
                                                                                                   grey_threshold)
        start_node_results, end_node_results = query_inter_cluster_DFG_start_and_end(tx, cluster)

        start_count = 0
        for record in start_node_results:
            edge_weight = get_edge_weight(int(record["freq"]), grey_threshold, log_min_edge_freq, log_max_edge_freq)
            if int(record["freq"]) <= grey_threshold:
                dot.edge("start", record['start'], xlabel=str(record["freq"]), color=color_dict['grey']['light_grey'],
                         penwidth=f"{edge_weight}", fontcolor=color_dict['grey']['medium_grey'])
                start_count += record["freq"]
            else:
                dot.edge("start", record['start'], xlabel=str(record["freq"]), penwidth=f"{edge_weight}")
                start_count += record["freq"]
                start_description = f"<<b>[start]</b><br/><FONT POINT-SIZE=\"{str(font_size_small)}\">{str(start_count)}</FONT>>"
        dot.node("start", start_description, node='source')

        for record in node_results:
            node_id = record['action']
            action_abbr = self.abbr_dict_lpm[record['action']]
            node_description = f"<<b>{action_abbr}</b><br/><FONT POINT-SIZE=\"{str(font_size_small)}\">{record['freq']}</FONT>>"
            node_fill_color, node_font_color = get_node_colors(int(record['freq']), log_min_node_freq,
                                                               log_max_node_freq)
            dot.node(node_id, node_description, fillcolor=node_fill_color, fontcolor=node_font_color)

        for record in edge_results:
            n1_id = record['e1_action']
            n2_id = record['e2_action']
            edge_weight = get_edge_weight(int(record["freq"]), grey_threshold, log_min_edge_freq, log_max_edge_freq)
            if int(record["freq"]) <= grey_threshold:
                dot.edge(n1_id, n2_id, xlabel=str(record["freq"]), color=color_dict['grey']['light_grey'],
                         penwidth=f"{edge_weight}", fontcolor=color_dict['grey']['medium_grey'])
            else:
                dot.edge(n1_id, n2_id, xlabel=str(record["freq"]), penwidth=f"{edge_weight}")

        end_count = 0
        for record in end_node_results:
            edge_weight = get_edge_weight(int(record["freq"]), grey_threshold, log_min_edge_freq, log_max_edge_freq)
            if int(record["freq"]) <= grey_threshold:
                dot.edge(record['end'], "end", xlabel=str(record["freq"]), color=color_dict['grey']['light_grey'],
                         penwidth=f"{edge_weight}", fontcolor=color_dict['grey']['medium_grey'])
                end_count += record["freq"]
            else:
                dot.edge(record['end'], "end", xlabel=str(record["freq"]), penwidth=f"{edge_weight}")
                end_count += record["freq"]
        end_description = f"<<b>[end]</b><br/><FONT POINT-SIZE=\"{str(font_size_small)}\">{str(end_count)}</FONT>>"
        dot.node("end", end_description, node='sink')
        return dot

    def get_edge_and_node_results_joint_cd_comparison(self, tx, entity_type, dfg_id, task_instance_ids_1,
                                                      task_instance_ids_2):
        edge_results_1 = self.query_DFG_edges_absolute(tx, task_instance_ids_1, entity_type)
        edge_results_2 = self.query_DFG_edges_absolute(tx, task_instance_ids_2, entity_type)
        start_edge_results_1, end_edge_results_1 = self \
            .query_DFG_start_and_end_absolute(tx, task_instance_ids_1, entity_type, dfg_id)
        start_edge_results_2, end_edge_results_2 = self \
            .query_DFG_start_and_end_absolute(tx, task_instance_ids_2, entity_type, dfg_id)
        edge_results_1.extend(start_edge_results_1)
        edge_results_1.extend(end_edge_results_1)
        edge_results_2.extend(start_edge_results_2)
        edge_results_2.extend(end_edge_results_2)
        df_edge_results_1 = pd.DataFrame([dict(record) for record in edge_results_1])
        df_edge_results_1.rename(columns={'abs_freq': 'before_freq'}, inplace=True)
        df_edge_results_2 = pd.DataFrame([dict(record) for record in edge_results_2])
        df_edge_results_2.rename(columns={'abs_freq': 'after_freq'}, inplace=True)
        df_edge_results_joint = pd.merge(df_edge_results_1, df_edge_results_2, on=['n1', 'n2'])
        df_edge_results_joint['sum_freq'] = df_edge_results_joint['before_freq'] + df_edge_results_joint['after_freq']

        nr_cases_before = 12208
        nr_cases_after = 14164
        df_edge_results_joint['before_percentage'] = df_edge_results_joint['before_freq'] / nr_cases_before
        df_edge_results_joint['after_percentage'] = df_edge_results_joint['after_freq'] / nr_cases_after
        df_edge_results_joint['sum_percentage'] = df_edge_results_joint['before_percentage'] + df_edge_results_joint[
            'after_percentage']
        df_edge_results_joint['percentage_difference'] = df_edge_results_joint['after_percentage'] - \
                                                         df_edge_results_joint['before_percentage']
        # df_edge_results_joint['relative_percentage_difference'] = (df_edge_results_joint['after_percentage'] -
        #                                                            df_edge_results_joint['before_percentage']) / \
        #                                                           df_edge_results_joint['before_percentage']
        df_edge_results_joint['relative_percentage_difference'] = df_edge_results_joint['after_percentage'] / \
                                                                  df_edge_results_joint['before_percentage']
        df_edge_results_joint['abs_percentage_difference'] = abs(df_edge_results_joint['percentage_difference'])
        df_edge_results_joint.sort_values(by=['sum_freq'], ascending=False, inplace=True)

        node_results_1 = self.query_DFG_nodes_absolute(tx, task_instance_ids_1)
        node_results_2 = self.query_DFG_nodes_absolute(tx, task_instance_ids_2)
        df_node_results_1 = pd.DataFrame([dict(record) for record in node_results_1])
        df_node_results_1.rename(columns={'abs_freq': 'before_freq'}, inplace=True)
        df_node_results_2 = pd.DataFrame([dict(record) for record in node_results_2])
        df_node_results_2.rename(columns={'abs_freq': 'after_freq'}, inplace=True)
        df_node_results_joint = pd.merge(df_node_results_1, df_node_results_2, on=['cluster'])
        sum_start_before = sum(
            df_edge_results_joint[df_edge_results_joint['n1'] == f'start_{dfg_id}']['before_freq'].tolist())
        sum_start_after = sum(
            df_edge_results_joint[df_edge_results_joint['n1'] == f'start_{dfg_id}']['after_freq'].tolist())
        sum_end_before = sum(
            df_edge_results_joint[df_edge_results_joint['n2'] == f'end_{dfg_id}']['before_freq'].tolist())
        sum_end_after = sum(
            df_edge_results_joint[df_edge_results_joint['n2'] == f'end_{dfg_id}']['after_freq'].tolist())
        df_node_results_joint = df_node_results_joint.append(
            {'cluster': f'start_{dfg_id}', 'before_freq': sum_start_before, 'after_freq': sum_start_after},
            ignore_index=True)
        df_node_results_joint = df_node_results_joint.append(
            {'cluster': f'end_{dfg_id}', 'before_freq': sum_end_before, 'after_freq': sum_end_after}, ignore_index=True)

        df_node_results_joint['before_percentage'] = df_node_results_joint['before_freq'] / nr_cases_before
        df_node_results_joint['after_percentage'] = df_node_results_joint['after_freq'] / nr_cases_after
        df_node_results_joint['sum_percentage'] = df_node_results_joint['before_percentage'] + df_node_results_joint[
            'after_percentage']
        df_node_results_joint['percentage_difference'] = df_node_results_joint['after_percentage'] - \
                                                         df_node_results_joint['before_percentage']
        # df_node_results_joint['relative_percentage_difference'] = (df_node_results_joint['after_percentage'] -
        #                                                            df_node_results_joint['before_percentage']) / \
        #                                                           df_node_results_joint['before_percentage']
        df_node_results_joint['relative_percentage_difference'] = df_node_results_joint['after_percentage'] / \
                                                                  df_node_results_joint['before_percentage']

        return df_edge_results_joint, df_node_results_joint

    def get_edge_and_node_results_joint_resource_specific(self, tx, entity_type, dfg_ids, list_task_instance_ids,
                                                          include_start_end_nodes=True):
        for dfg_id in dfg_ids:
            edge_results = self.query_DFG_edges_absolute_resource_specific(tx, list_task_instance_ids[
                dfg_ids.index(dfg_id)], entity_type)
            df_edge_results = pd.DataFrame([dict(record) for record in edge_results])
            node_results = self.query_DFG_nodes_absolute_resource_specific(tx, list_task_instance_ids[
                dfg_ids.index(dfg_id)])
            df_node_results = pd.DataFrame([dict(record) for record in node_results])
            if include_start_end_nodes:
                start_edge_results, end_edge_results = self.query_DFG_start_and_end_absolute_resource_specific(tx,
                                                                                                               list_task_instance_ids[
                                                                                                                   dfg_ids.index(
                                                                                                                       dfg_id)],
                                                                                                               entity_type,
                                                                                                               dfg_id)
                df_edge_results = df_edge_results.append(pd.DataFrame([dict(record) for record in start_edge_results]))
                df_edge_results = df_edge_results.append(pd.DataFrame([dict(record) for record in end_edge_results]))
                sum_start = sum(df_edge_results[df_edge_results['n1'] == f'start_{dfg_id}']['abs_freq'].tolist())
                sum_end = sum(df_edge_results[df_edge_results['n2'] == f'end_{dfg_id}']['abs_freq'].tolist())
                df_node_results = df_node_results.append({'n': f'start_{dfg_id}', 'abs_freq': sum_start},
                                                         ignore_index=True)
                df_node_results = df_node_results.append({'n': f'end_{dfg_id}', 'abs_freq': sum_end},
                                                         ignore_index=True)
            df_edge_results['dfg_id'] = dfg_id
            df_node_results['dfg_id'] = dfg_id
            if dfg_ids.index(dfg_id) == 0:
                df_edge_results_all = df_edge_results.copy()
                df_node_results_all = df_node_results.copy()
            else:
                df_edge_results_all = df_edge_results_all.append(df_edge_results)
                df_node_results_all = df_node_results_all.append(df_node_results)
        return df_edge_results_all, df_node_results_all

    def query_DFG_nodes_min_max_absolute(self, tx, task_instance_ids):
        q = f'''
                MATCH (tc:TaskCluster)<-[:OBSERVED]-(ti:TaskInstance) WHERE NOT tc.Name IN {self.exclude_clusters}
                    AND ID(ti) IN {task_instance_ids}
                WITH DISTINCT ti.cluster AS cluster, count(ti) AS abs_freq
                WITH MAX(abs_freq) AS max, MIN(abs_freq) AS min
                RETURN min, max
                '''
        max_node_freq = tx.run(q).single()["max"]
        log_max_node_freq = np.log(int(max_node_freq))
        min_node_freq = tx.run(q).single()["min"]
        log_min_node_freq = np.log(int(min_node_freq))

        q = f'''
                MATCH (tc:TaskCluster)<-[:OBSERVED]-(ti:TaskInstance) WHERE NOT tc.Name IN {self.exclude_clusters}
                    AND ID(ti) IN {task_instance_ids}
                WITH DISTINCT ti.cluster AS cluster, count(ti) AS abs_freq
                RETURN cluster, abs_freq
                '''
        node_results = list(tx.run(q))
        return node_results, log_min_node_freq, log_max_node_freq

    def query_DFG_nodes_absolute(self, tx, task_instance_ids):
        q = f'''
                MATCH (tc:TaskCluster)<-[:OBSERVED]-(ti:TaskInstance) WHERE NOT tc.Name IN {self.exclude_clusters}
                    AND ID(ti) IN {task_instance_ids}
                WITH DISTINCT ti.cluster AS cluster, count(ti) AS abs_freq
                RETURN cluster, abs_freq
                '''
        node_results = list(tx.run(q))
        return node_results

    def query_DFG_edges_absolute(self, tx, task_instance_ids, entity_type):
        q = f'''
                MATCH (tc1:TaskCluster)<-[:OBSERVED]-(ti1:TaskInstance) 
                    WHERE NOT tc1.Name IN {self.exclude_clusters} AND ID(ti1) IN {task_instance_ids}
                WITH ti1
                MATCH (tc2:TaskCluster)<-[:OBSERVED]-(ti2:TaskInstance) 
                    WHERE NOT tc2.Name IN {self.exclude_clusters} AND ID(ti2) IN {task_instance_ids}
                WITH ti1, ti2
                MATCH (ti1)-[df:DF_TI {{EntityType: '{entity_type}'}}]->(ti2)
                WITH DISTINCT ti1.cluster AS n1, ti2.cluster AS n2, count(df) AS abs_freq
                RETURN n1, n2, abs_freq ORDER BY abs_freq DESC
                '''
        edge_results = list(tx.run(q))
        return edge_results

    def query_DFG_start_and_end_absolute(self, tx, task_instance_ids, entity_type, dfg_id):
        if entity_type == 'resource':
            q = f'''
                    CALL {{
                        MATCH (tc:TaskCluster) WHERE NOT tc.Name IN {self.exclude_clusters}
                        MATCH (tc)<-[:OBSERVED]-(ti:TaskInstance) WHERE ID(ti) IN {task_instance_ids}
                        WITH ti
                        MATCH (ti) WHERE NOT ()-[:DF_TI {{EntityType:'resource'}}]->(ti)
                        RETURN ti
                    UNION
                        MATCH (tc:TaskCluster) WHERE NOT tc.Name IN {self.exclude_clusters}
                        MATCH (tc)<-[:OBSERVED]-(ti:TaskInstance) WHERE ID(ti) IN {task_instance_ids}
                        WITH ti
                        MATCH (ti0:TaskInstance)-[df:DF_TI {{EntityType:'resource'}}]->(ti) 
                            WHERE NOT date(ti0.end_time) = date(ti.start_time)
                        RETURN ti
                    }}
                    WITH DISTINCT ti.cluster AS n2, count(ti) AS abs_freq, "start_{int(dfg_id)}" AS n1
                    RETURN n1, n2, abs_freq
                    '''
            start_node_results = list(tx.run(q))
            q = f'''
                    CALL {{
                        MATCH (tc:TaskCluster) WHERE NOT tc.Name IN {self.exclude_clusters}
                        MATCH (tc)<-[:OBSERVED]-(ti:TaskInstance) WHERE ID(ti) IN {task_instance_ids}
                        WITH ti
                        MATCH (ti) WHERE NOT (ti)-[:DF_TI {{EntityType:'resource'}}]->()
                        RETURN ti
                    UNION
                        MATCH (tc:TaskCluster) WHERE NOT tc.Name IN {self.exclude_clusters}
                        MATCH (tc)<-[:OBSERVED]-(ti:TaskInstance) WHERE ID(ti) IN {task_instance_ids}
                        WITH ti
                        MATCH (ti)-[df:DF_TI {{EntityType:'resource'}}]->(ti0:TaskInstance) 
                            WHERE NOT date(ti.end_time) = date(ti0.start_time)
                        RETURN ti
                    }}
                    WITH DISTINCT ti.cluster AS n1, count(ti) AS abs_freq, "end_{dfg_id}"AS n2
                    RETURN n1, n2, abs_freq
                    '''
            end_node_results = list(tx.run(q))
        else:
            q = f'''
                    MATCH (tc:TaskCluster) WHERE NOT tc.Name IN {self.exclude_clusters}
                    MATCH (tc)<-[:OBSERVED]-(ti:TaskInstance) WHERE ID(ti) IN {task_instance_ids}
                    WITH ti
                    MATCH (ti) WHERE NOT ()-[:DF_TI {{EntityType:'case'}}]->(ti)
                    WITH DISTINCT ti.cluster AS n2, count(ti) AS abs_freq, "start_{dfg_id}" AS n1
                    RETURN n1, n2, abs_freq
                    '''
            start_node_results = list(tx.run(q))
            q = f'''
                    MATCH (tc:TaskCluster) WHERE NOT tc.Name IN {self.exclude_clusters}
                    MATCH (tc)<-[:OBSERVED]-(ti:TaskInstance) WHERE ID(ti) IN {task_instance_ids}
                    WITH ti
                    MATCH (ti) WHERE NOT (ti)-[:DF_TI {{EntityType:'case'}}]->()
                    WITH DISTINCT ti.cluster AS n1, count(ti) AS abs_freq, "end_{dfg_id}"AS n2
                    RETURN n1, n2, abs_freq
                    '''
            end_node_results = list(tx.run(q))
        return start_node_results, end_node_results

    def query_DFG_nodes_absolute_resource_specific(self, tx, task_instance_ids):
        q = f'''
                MATCH (tc:TaskCluster)<-[:OBSERVED]-(ti:TaskInstance) WHERE NOT tc.Name IN {self.exclude_clusters}
                    AND ID(ti) IN {task_instance_ids}
                WITH DISTINCT ti.cluster + "_" + ti.rID AS n, count(ti) AS abs_freq
                RETURN n, abs_freq
                '''
        node_results = list(tx.run(q))
        return node_results

    def query_DFG_edges_absolute_resource_specific(self, tx, task_instance_ids, entity_type):
        q = f'''
                MATCH (tc1:TaskCluster)<-[:OBSERVED]-(ti1:TaskInstance) 
                    WHERE NOT tc1.Name IN {self.exclude_clusters} AND ID(ti1) IN {task_instance_ids}
                WITH ti1
                MATCH (tc2:TaskCluster)<-[:OBSERVED]-(ti2:TaskInstance) 
                    WHERE NOT tc2.Name IN {self.exclude_clusters} AND ID(ti2) IN {task_instance_ids}
                WITH ti1, ti2
                MATCH (ti1)-[df:DF_TI {{EntityType: '{entity_type}'}}]->(ti2)
                WITH DISTINCT ti1.cluster + "_" + ti1.rID AS n1, ti2.cluster + "_" + ti2.rID AS n2, count(df) AS abs_freq
                RETURN n1, n2, abs_freq ORDER BY abs_freq DESC
                '''
        edge_results = list(tx.run(q))
        return edge_results

    def query_DFG_start_and_end_absolute_resource_specific(self, tx, task_instance_ids, entity_type, dfg_id):
        if entity_type == 'resource':
            q = f'''
                    CALL {{
                        MATCH (tc:TaskCluster) WHERE NOT tc.Name IN {self.exclude_clusters}
                        MATCH (tc)<-[:OBSERVED]-(ti:TaskInstance) WHERE ID(ti) IN {task_instance_ids}
                        WITH ti
                        MATCH (ti) WHERE NOT ()-[:DF_TI {{EntityType:'resource'}}]->(ti)
                        RETURN ti
                    UNION
                        MATCH (tc:TaskCluster) WHERE NOT tc.Name IN {self.exclude_clusters}
                        MATCH (tc)<-[:OBSERVED]-(ti:TaskInstance) WHERE ID(ti) IN {task_instance_ids}
                        WITH ti
                        MATCH (ti0:TaskInstance)-[df:DF_TI {{EntityType:'resource'}}]->(ti) 
                            WHERE NOT date(ti0.end_time) = date(ti.start_time)
                        RETURN ti
                    }}
                    WITH DISTINCT ti.cluster + "_" + ti.rID AS n2, count(ti) AS abs_freq, "start_{int(dfg_id)}" AS n1
                    RETURN n1, n2, abs_freq
                    '''
            start_node_results = list(tx.run(q))
            q = f'''
                    CALL {{
                        MATCH (tc:TaskCluster) WHERE NOT tc.Name IN {self.exclude_clusters}
                        MATCH (tc)<-[:OBSERVED]-(ti:TaskInstance) WHERE ID(ti) IN {task_instance_ids}
                        WITH ti
                        MATCH (ti) WHERE NOT (ti)-[:DF_TI {{EntityType:'resource'}}]->()
                        RETURN ti
                    UNION
                        MATCH (tc:TaskCluster) WHERE NOT tc.Name IN {self.exclude_clusters}
                        MATCH (tc)<-[:OBSERVED]-(ti:TaskInstance) WHERE ID(ti) IN {task_instance_ids}
                        WITH ti
                        MATCH (ti)-[df:DF_TI {{EntityType:'resource'}}]->(ti0:TaskInstance) 
                            WHERE NOT date(ti.end_time) = date(ti0.start_time)
                        RETURN ti
                    }}
                    WITH DISTINCT ti.cluster + "_" + ti.rID AS n1, count(ti) AS abs_freq, "end_{dfg_id}"AS n2
                    RETURN n1, n2, abs_freq
                    '''
            end_node_results = list(tx.run(q))
        else:
            q = f'''
                    MATCH (tc:TaskCluster) WHERE NOT tc.Name IN {self.exclude_clusters}
                    MATCH (tc)<-[:OBSERVED]-(ti:TaskInstance) WHERE ID(ti) IN {task_instance_ids}
                    WITH ti
                    MATCH (ti) WHERE NOT ()-[:DF_TI {{EntityType:'case'}}]->(ti)
                    WITH DISTINCT ti.cluster + "_" + ti.rID AS n2, count(ti) AS abs_freq, "start_{dfg_id}" AS n1
                    RETURN n1, n2, abs_freq
                    '''
            start_node_results = list(tx.run(q))
            q = f'''
                    MATCH (tc:TaskCluster) WHERE NOT tc.Name IN {self.exclude_clusters}
                    MATCH (tc)<-[:OBSERVED]-(ti:TaskInstance) WHERE ID(ti) IN {task_instance_ids}
                    WITH ti
                    MATCH (ti) WHERE NOT (ti)-[:DF_TI {{EntityType:'case'}}]->()
                    WITH DISTINCT ti.cluster + "_" + ti.rID AS n1, count(ti) AS abs_freq, "end_{dfg_id}"AS n2
                    RETURN n1, n2, abs_freq
                    '''
            end_node_results = list(tx.run(q))
        return start_node_results, end_node_results


def query_inter_cluster_DFG_nodes_min_max(tx, cluster):
    q = f'''
            MATCH (tc:TaskCluster {{Name: {cluster}}})<-[:OBSERVED]-(ti:TaskInstance)
            MATCH (ti)-[:CONTAINS]->(e:Event)
            WITH DISTINCT e.activity_lifecycle AS e1_action, count(e) AS freq
            WITH MAX(freq) AS max, MIN(freq) AS min
            RETURN min, max
            '''
    max_node_freq = tx.run(q).single()["max"]
    log_max_node_freq = np.log(int(max_node_freq))
    min_node_freq = tx.run(q).single()["min"]
    log_min_node_freq = np.log(int(min_node_freq))

    q = f'''
        MATCH (tc:TaskCluster {{Name: {cluster}}})<-[:OBSERVED]-(ti:TaskInstance)
        MATCH (ti)-[:CONTAINS]->(e:Event)
        WITH DISTINCT e.activity_lifecycle AS action, count(e) AS freq
        RETURN action, freq
        '''
    node_results = list(tx.run(q))
    return node_results, log_min_node_freq, log_max_node_freq


def query_inter_cluster_DFG_edges_min_max(tx, cluster, grey_threshold):
    q = f'''
            MATCH (tc:TaskCluster {{Name: {cluster}}})<-[:OBSERVED]-(ti:TaskInstance)
            MATCH (ti)-[:CONTAINS]->(e1:Event)-[df:DF {{EntityType: "joint"}}]->(e2:Event)
            WITH DISTINCT e1.activity_lifecycle AS e1_action, e2.activity_lifecycle AS e2_action, count(df) AS freq
            RETURN MAX(freq)
            '''
    max_edge_freq = tx.run(q).single()[0]
    log_max_edge_freq = np.log(int(max_edge_freq))
    min_edge_freq = grey_threshold
    log_min_edge_freq = np.log(int(min_edge_freq))

    q = f'''
        MATCH (tc:TaskCluster {{Name: {cluster}}})<-[:OBSERVED]-(ti:TaskInstance)
        MATCH (ti)-[:CONTAINS]->(e1:Event)-[df:DF {{EntityType: "joint"}}]->(e2:Event)
        WITH DISTINCT e1.activity_lifecycle AS e1_action, e2.activity_lifecycle AS e2_action, count(df) AS freq
        RETURN e1_action, e2_action, freq ORDER BY freq ASC
        '''
    edge_results = list(tx.run(q))
    return edge_results, log_min_edge_freq, log_max_edge_freq


def query_inter_cluster_DFG_start_and_end(tx, cluster):
    q = f'''
            MATCH (tc:TaskCluster {{Name: {cluster}}})<-[:OBSERVED]-(ti:TaskInstance)
            MATCH (ti)-[:CONTAINS]->(e:Event) WHERE NOT (:Event)-[:DF {{EntityType: "joint"}}]->(e)
            WITH DISTINCT e.activity_lifecycle AS start, count(e) AS freq
            RETURN start, freq
            '''
    start_node_results = list(tx.run(q))
    q = f'''
            MATCH (tc:TaskCluster {{Name: {cluster}}})<-[:OBSERVED]-(ti:TaskInstance)
            MATCH (ti)-[:CONTAINS]->(e:Event) WHERE NOT (e)-[:DF {{EntityType: "joint"}}]->(:Event)
            WITH DISTINCT e.activity_lifecycle AS end, count(e) AS freq
            RETURN end, freq
            '''
    end_node_results = list(tx.run(q))
    return start_node_results, end_node_results


def get_edge_log_min_max(edge_min, edge_max):
    log_min_edge_freq = np.log(int(edge_min))
    log_max_edge_freq = np.log(int(edge_max))
    return log_min_edge_freq, log_max_edge_freq


def get_task_instance_ids(tx, start_end_date=None, resources=None):
    task_instance_ids = []
    q = f'''
        MATCH (ti:TaskInstance)
        WITH ID(ti) AS ti_id
        RETURN ti_id
        '''
    # print(q)
    result = tx.run(q)
    for record in result:
        task_instance_ids.append(int(record['ti_id']))
    if start_end_date is not None:
        task_instance_ids = filter_task_instances_time_frame(tx=tx, start_date=start_end_date[0],
                                                             end_date=start_end_date[1], ti_ids=task_instance_ids)
    if resources is not None:
        task_instance_ids = filter_task_instances_resource(tx=tx, resources=resources, ti_ids=task_instance_ids)
    return task_instance_ids


def filter_task_instances_resource(tx, resources, ti_ids=None):
    q = f'''
    MATCH (ti:TaskInstance)-[:CORR]->(n:Entity {{EntityType: 'resource'}}) WHERE n.ID IN {resources} {get_ti_id_query(ti_ids)}
    WITH ID(ti) AS ti_id
    RETURN ti_id
    '''
    # print(q)
    result = tx.run(q)
    ti_ids = []
    for record in result:
        ti_ids.append(int(record['ti_id']))
    return ti_ids


def filter_task_instances_time_frame(tx, start_date, end_date, ti_ids=None):
    if ti_ids is None:
        ti_ids = []
    q = f'''
    MATCH (n:Entity {{EntityType: 'case'}})<-[:CORR]-(e1:Event) 
        WHERE NOT (:Event)-[:DF {{EntityType:'case'}}]->(e1) 
        AND date("{start_date}") <= date(e1.timestamp) <= date("{end_date}")
    WITH COLLECT(n.ID) AS case_list
    MATCH (n:Entity {{EntityType: 'case'}})<-[:CORR]-(e2:Event) 
        WHERE NOT (e2)-[:DF {{EntityType:'case'}}]->(:Event) 
        AND date("{start_date}") <= date(e2.timestamp) <= date("{end_date}") 
        AND n.ID in case_list
    WITH n.ID AS case_id
    RETURN case_id
    '''
    # print(q)
    result = tx.run(q)
    case_ids = []
    for record in result:
        case_ids.append(record['case_id'])

    q = f'''
        MATCH (ti:TaskInstance)-[:CORR]->(n:Entity {{EntityType: 'case'}}) WHERE n.ID IN {case_ids} {get_ti_id_query(ti_ids)}
        WITH ID(ti) AS ti_id
        RETURN ti_id
        '''
    result = tx.run(q)
    ti_ids = []
    for record in result:
        ti_ids.append(int(record['ti_id']))

    return ti_ids


def list_edge_frequencies(q_edge_results, freq_column):
    edge_frequencies = []
    for record in q_edge_results:
        edge_frequencies.append(int(record[freq_column]))
    return edge_frequencies


def get_medium_and_dark_color(overlaid, entity_type, edge_frequency, edge_weight_threshold, dfg_id=None):
    if overlaid and edge_frequency > edge_weight_threshold:
        if dfg_id is None or dfg_id == 0:
            medium_color = color_dict[entity_type]['medium']
            dark_color = color_dict[entity_type]['dark']
        else:
            medium_color = color_dict[dfg_id]['medium']
            dark_color = color_dict[dfg_id]['dark']
    else:
        if dfg_id is None or dfg_id == 0:
            medium_color = color_dict[entity_type]['light_grey']
            dark_color = color_dict[entity_type]['medium_grey']
            medium_color = '#bcbcbc'
            dark_color = '#a9a9a9'
        else:
            medium_color = color_dict[dfg_id]['light_grey']
            dark_color = color_dict[dfg_id]['medium_grey']
    return medium_color, dark_color


def get_edge_cutoff(edge_frequencies, percentage):
    edge_frequencies = sorted(edge_frequencies, reverse=True)
    cum_sum_cut_off = percentage * sum(edge_frequencies)
    index_cut_off = np.argwhere(np.cumsum(edge_frequencies) >= cum_sum_cut_off)[0][0]
    cut_off = edge_frequencies[index_cut_off]
    return cut_off


def add_leading_zeros_and_reorder_list(resource_list):
    for index, resource in enumerate(resource_list):
        resource_list[index] = resource[:5] + resource[5:].zfill(3)
    resource_list = sorted(resource_list)
    return resource_list


def get_node_colors(node_freq, log_min_node_freq, log_max_node_freq):
    font_color = black
    c_map = cm.get_cmap('Greys')
    log_node_freq = np.log(node_freq)
    log_node_freq_norm = (log_node_freq - log_min_node_freq) / (1.3 * (log_max_node_freq - log_min_node_freq))
    rgba = c_map(log_node_freq_norm)
    fill_color = mpl.colors.rgb2hex(rgba)
    if log_node_freq_norm > 0.5:
        font_color = white
    return fill_color, font_color


def get_colors_comparison(percentage_difference, abs_max):
    c_map_purple = cm.get_cmap(Purples_5_r.mpl_colormap)
    c_map_orange = cm.get_cmap(Oranges_5.mpl_colormap)
    newcolors_purple = c_map_purple(np.linspace(0, 1, 150))
    newcolors_orange = c_map_orange(np.linspace(0, 1, 150))
    newcolors = np.concatenate((newcolors_purple[:128], newcolors_orange[22:]))
    c_map = ListedColormap(newcolors)
    norm = mpl.colors.Normalize(vmin=abs_max * -1.0, vmax=abs_max)
    rgba = c_map(norm(percentage_difference))
    normal_color = mpl.colors.rgb2hex(rgba)
    r, g, b = hex_to_rgb(normal_color)  # hex to rgb format
    dark_color = darken_color(r, g, b)
    return normal_color, dark_color


def print_cmap(abs_max, min_perc_case_diff):
    c_map_purple = cm.get_cmap(Purples_5_r.mpl_colormap)
    c_map_orange = cm.get_cmap(Oranges_5.mpl_colormap)
    newcolors_purple = c_map_purple(np.linspace(0, 1, 150))
    newcolors_orange = c_map_orange(np.linspace(0, 1, 150))
    newcolors = np.concatenate((newcolors_purple[:128], newcolors_orange[22:]))
    grey = np.array([211 / 256, 211 / 256, 211 / 256, 1])
    grey_end = round(128 / abs_max * (min_perc_case_diff)) + 128
    grey_start = round(128 / abs_max * (min_perc_case_diff) * -1.0) + 128
    newcolors[grey_start:grey_end, :] = grey
    c_map = ListedColormap(newcolors)
    norm = mpl.colors.Normalize(vmin=abs_max * -1.0, vmax=abs_max)
    im = cm.ScalarMappable(norm=norm, cmap=c_map)
    fig, ax = plt.subplots(1, 1)
    ax.figure.colorbar(im, ax=ax, orientation='horizontal', format=FuncFormatter(lambda y, _: '{:.0%}'.format(y)))
    ax.axis('off')
    # plt.savefig('colorbar.svg', format='svg')
    plt.show()


def get_edge_weight(frequency, threshold, log_min_edge_freq, log_max_edge_freq):
    edge_weight = np.log(max((threshold + 1), frequency))
    edge_weight = (edge_weight - log_min_edge_freq) / (log_max_edge_freq - log_min_edge_freq) * 10
    return edge_weight


def get_ti_id_query(ti_ids):
    ti_query = ""
    if isinstance(ti_ids, list):
        ti_query = f"AND ID(ti) IN {ti_ids}"
    return ti_query


def get_file_name_resources(resources):
    if resources is None:
        return ""
    file_name_resources = "_R"
    for resource in resources:
        file_name_resources += f"_{resource[5:]}"
    return file_name_resources


def get_file_name_time_frame(start_end_date):
    if start_end_date is None:
        return ""
    start_date = start_end_date[0].replace("-", "_")
    end_date = start_end_date[1].replace("-", "_")
    file_name_time_frame = f"_{start_date}_{end_date}"
    return file_name_time_frame


def hex_to_rgb(hex):
    hex = hex.lstrip('#')
    hlen = len(hex)
    return tuple(int(hex[i:i + hlen // 3], 16) for i in range(0, hlen, hlen // 3))


def adjust_color_lightness(r, g, b, factor):
    h, l, s = rgb2hls(r / 255.0, g / 255.0, b / 255.0)
    l = max(min(l * factor, 1.0), 0.0)
    r, g, b = hls2rgb(h, l, s)
    return rgb2hex(int(r * 255), int(g * 255), int(b * 255))


def darken_color(r, g, b, factor=0.2):
    return adjust_color_lightness(r, g, b, 1 - factor)
