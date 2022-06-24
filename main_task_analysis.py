from GraphConfigurator import GraphConfigurator
from AnalysisConfigurator import AnalysisConfigurator
import PreprocessSelector
from constructors.EventGraphConstructor import EventGraphConstructor
from constructors.HighLevelEventConstructor import HighLevelEventConstructor
from constructors.ClassConstructor import ClassConstructor
from constructors.ClusterConstructor import ClusterConstructor
from TaskAnalyzer import TaskAnalyzer
from DFGVisualizer import DFGVisualizer

# -------------- BEGIN CONFIG ----------------- #

# TO START:
# specify the name of the graph:
graph = "bpic2017_case_attr"
# and configure all the settings related to this graph name in "graph_confs.py"
gc = GraphConfigurator(graph)

# specify the analysis description (patterns to be analyzed and number of clusters)
pattern_subset_description = "P_freq_geq_10__C_20"
# and configure all analysis parameters in "analysis_confs.py"
ac = AnalysisConfigurator(pattern_subset_description)

# -------------- CONSTRUCTION ----------------- #
# IF STARTING FROM SCRATCH (without event graph constructed in neo4j)
# (1) set "step_preprocess" and "step_create_event_graph" to true:
step_preprocess = False
step_create_event_graph = False
# (2) create graph in Neo4j (with same password as specified in "graph_confs.py")
#     and allocate enough memory: set dbms.memory.heap.max_size=20G
# (3) specify path to import directory of neo4j database:
path_to_neo4j_import_directory = 'C:\\Users\\s111402\\.Neo4jDesktop\\relate-data\dbmss\\' \
                                 'dbms-7596d843-4d32-444d-87ff-5acea694caa3\\import\\'

# IF STARTING FROM SCRATCH OR FROM AN EVENT GRAPH PRECONSTRUCTED:
# set "step_construct_high_level_events" to true to construct high level events:
step_construct_high_level_events = False

# IF ALSO CREATING EVENT CLASS AND TASK INSTANCE CLASS CONSTRUCTS (necessary for analyzing atomic-composite granularity):
step_construct_classes = False

step_construct_clusters = False

step_construct_artificial = False

# -------------- TASK ANALYSIS ---------------- #
# IF EVENT GRAPH, HIGH LEVEL EVENTS, CLASS- AND CLUSTER CONSTRUCTS ARE ALREADY IN PLACE:
step_create_intra_task_DFG = True

step_create_DFG_clusters = False
entity_type = 'case'
df_show_threshold = 1.0
print_description = False
start_end_date = None
# start_end_date = ['2016-01-01', '2016-06-30']
# start_end_date = ['2016-08-01', '2017-02-01']

step_create_DFG_concept_drift_comparison = False
start_end_dates = [['2016-01-01', '2016-06-30'], ['2016-08-01', '2017-02-01']]

resources = None
step_create_DFG_resource_overlaid = False
# resources = ["User_29", "User_113"]
resources_lists_over = [["User_29"], ["User_113"]]
df_show_threshold_over = 5
df_show_threshold_under = 5

# --------------- END CONFIG ------------------ #

if step_preprocess:
    PreprocessSelector.get_preprocessor(graph, gc.get_filename(), gc.get_column_names(), gc.get_separator(),
                                        gc.get_timestamp_format(), path_to_neo4j_import_directory).preprocess()

if step_create_event_graph:
    EventGraphConstructor(gc.get_password(), path_to_neo4j_import_directory, graph) \
        .construct_single()

if step_construct_high_level_events:
    HighLevelEventConstructor(gc.get_password(), graph, gc.get_entity_labels(), gc.get_action_lifecycle_labels()) \
        .construct_single()

if step_construct_classes:
    class_constr = ClassConstructor(gc.get_password(), graph, gc.get_entity_labels(), gc.get_action_lifecycle_labels())
    class_constr.construct_action_classes()

ta = TaskAnalyzer(graph, gc.get_password(), ac.get_analysis_directory(), ac.get_pattern_filter_description(),
                  ac.get_pattern_filter_cypher(), ac.get_encoding(), ac.get_num_clusters())

if step_construct_clusters:
    ClusterConstructor(gc.get_password(), graph, gc.get_entity_labels(), gc.get_action_lifecycle_labels()) \
        .construct_clusters(ta.get_patterns_clustered(), ac.get_num_clusters())

# DFG
dfg_vis = DFGVisualizer(graph, gc.get_password(), gc.get_name_data_set(), gc.get_entity_labels(),
                        gc.get_action_lifecycle_labels(), ac.get_analysis_directory(), ac.get_exclude_clusters())
if step_create_DFG_clusters:
    dfg_vis.visualize_cluster_DFG(entity_type, df_show_threshold, start_end_date=start_end_date, resources=resources,
                                  print_description=print_description)
if step_create_intra_task_DFG:
    dfg_vis.visualize_intra_cluster_graph(14)
if step_create_DFG_concept_drift_comparison:
    dfg_vis.visualize_cluster_DFG_concept_drift_comparison(entity_type, df_show_threshold, start_end_dates)
if step_create_DFG_resource_overlaid:
    dfg_vis.visualize_cluster_DFG_resources(df_show_threshold_under, df_show_threshold_over, resources_lists_over,
                                            start_end_date=start_end_date, resources=resources)
