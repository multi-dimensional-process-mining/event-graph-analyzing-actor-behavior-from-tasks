from GraphConfigurator import GraphConfigurator
from AnalysisConfigurator import AnalysisConfigurator
import PreprocessSelector
from constructors.EventGraphConstructor import EventGraphConstructor
from constructors.HighLevelEventConstructor import HighLevelEventConstructor
from constructors.ClusterConstructor import ClusterConstructor
from TaskClusterModule import TaskClusterModule
from DFGVisualizer import DFGVisualizer

# --------------------------- BEGIN CONFIG ----------------------------- #
# TO START:
# specify the name of the graph:
graph = "bpic2017_case_attr"
# and configure all the settings related to this graph name in "graph_confs.py"
gc = GraphConfigurator(graph)

# specify the analysis description (patterns to be analyzed and number of clusters)
pattern_subset_description = "P_freq_geq_10__C_20"
# and configure all analysis parameters in "analysis_confs.py"
ac = AnalysisConfigurator(pattern_subset_description)

# --------------------------- CONSTRUCTION ----------------------------- #
# IF STARTING FROM SCRATCH (without event graph constructed in neo4j)
# (1) create graph in Neo4j (with same password as specified in "graph_confs.py")
#     and allocate enough memory: set dbms.memory.heap.max_size=20G
# (2) specify path to import directory of neo4j database:
path_to_neo4j_import_directory = 'C:\\Users\\s111402\\.Neo4jDesktop\\relate-data\dbmss\\' \
                                 'dbms-95e392fb-324f-40c5-a2ec-c7cdfd0eb78e\\import\\'
# (3) set "step_preprocess" and "step_create_event_graph" to true:
step_preprocess = True
step_construct_event_graph = True

# IF EVENT GRAPH IS ALREADY CONSTRUCTED:
# set "step_construct_high_level_events" to true to construct high level events:
step_construct_high_level_events = True
# and set "step_construct_clusters" to true to perform clustering and construct clusters:
step_construct_clusters = True

# --------------- PROCESS VISUALIZATION using TASK DFGs ---------------- #
# IF EVENT GRAPH, HIGH LEVEL EVENTS AND CLUSTER CONSTRUCTS ARE IN PLACE:
step_create_intra_task_DFG = False

step_create_inter_task_DFG = False
entity_type = 'case'
df_show_threshold = 1.0
print_description = False
# start_end_date = None
start_end_date = ['2016-01-01', '2016-06-30']
# start_end_date = ['2016-08-01', '2017-02-01']

step_create_DFG_concept_drift_comparison = False
start_end_dates = [['2016-01-01', '2016-06-30'], ['2016-08-01', '2017-02-01']]

step_create_inter_task_DFG_resource_specific = False
# resources = None
resources = ["User_29", "User_113"]
resources_lists_over = [["User_29"], ["User_113"]]
df_show_threshold_over = 5
df_show_threshold_under = 5

# ------------------------------ END CONFIG ---------------------------- #

# [1.a] CONSTRUCTION
if step_preprocess:
    PreprocessSelector.get_preprocessor(graph, gc.get_filename(), gc.get_column_names(), gc.get_separator(),
                                        gc.get_timestamp_format(), path_to_neo4j_import_directory).preprocess()

if step_construct_event_graph:
    EventGraphConstructor(gc.get_password(), path_to_neo4j_import_directory, graph) \
        .construct()

if step_construct_high_level_events:
    HighLevelEventConstructor(gc.get_password(), graph, gc.get_entity_labels(), gc.get_action_lifecycle_labels()) \
        .construct()

# [1.b] CLUSTERING
if step_construct_clusters:
    tcm = TaskClusterModule(graph, gc.get_password(), ac.get_analysis_directory(), ac.get_pattern_filter_description(),
                            ac.get_pattern_filter_cypher(), ac.get_encoding(), ac.get_num_clusters())
    cc = ClusterConstructor(gc.get_password(), graph, gc.get_entity_labels(), gc.get_action_lifecycle_labels())
    cc.remove_cluster_constructs()
    cc.construct_clusters(tcm.get_patterns_clustered())

# [2] PROCESS VISUALIZATION using TASK DFGs
dfg_vis = DFGVisualizer(graph, gc.get_password(), gc.get_name_data_set(), gc.get_entity_labels(),
                        gc.get_action_lifecycle_labels(), ac.get_analysis_directory(), ac.get_exclude_clusters())
if step_create_inter_task_DFG:
    dfg_vis.visualize_inter_task_DFG(entity_type, df_show_threshold, start_end_date=start_end_date, resources=resources,
                                  print_description=print_description)
if step_create_intra_task_DFG:
    dfg_vis.visualize_intra_task_DFG(14)
if step_create_DFG_concept_drift_comparison:
    dfg_vis.visualize_cluster_DFG_concept_drift_comparison(entity_type, df_show_threshold, start_end_dates)
if step_create_inter_task_DFG_resource_specific:
    dfg_vis.visualize_cluster_DFG_resources(df_show_threshold_under, df_show_threshold_over, resources_lists_over,
                                            start_end_date=start_end_date, resources=resources)
