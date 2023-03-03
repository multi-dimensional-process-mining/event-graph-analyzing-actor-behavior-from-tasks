from PerformanceRecorder import PerformanceRecorder
from neo4j import GraphDatabase


class HighLevelEventConstructor:

    def __init__(self, password, name_data_set, entity_labels, action_lifecycle_label):
        self.driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", password))
        self.name_data_set = name_data_set
        self.entity_labels = entity_labels
        self.entity_labels[0].append('rID')
        self.entity_labels[1].append('cID')
        self.action_lifecycle_label = action_lifecycle_label
        self.max_cases = ""
        self.max_resources = ""

    def construct(self):
        # create performance recorder
        pr = PerformanceRecorder(self.name_data_set, 'constructing_task_instance_nodes')
        # combine resource and case directly follows relationships
        query_combine_df_joint = f'''
            MATCH (e1:Event)-[:DF {{EntityType:'{self.entity_labels[0][0]}'}}]->(e2:Event)
            WHERE (e1)-[:DF {{EntityType:'{self.entity_labels[1][0]}'}}]->(e2)
            CREATE (e1)-[:DF_JOINT]->(e2)
            '''
        run_query(self.driver, query_combine_df_joint)
        pr.record_performance('combine_df_joint')

        # check if the transactional lifecycle is recorded and combine with activity classifier into single property
        if len(self.action_lifecycle_label) == 2:
            query_set_activity_lifecycle_property = f'''
                MATCH (e:Event)
                SET e.activity_lifecycle = e.{self.action_lifecycle_label[0]}+'+'+e.{self.action_lifecycle_label[1]}
                '''
            run_query(self.driver, query_set_activity_lifecycle_property)
            pr.record_performance('set_activity_lifecycle_property')
            self.action_lifecycle_label[0] = 'activity_lifecycle'

        # query and materialize task instances and relationships with events
        query_create_ti_nodes = f'''
            CALL {{
            MATCH (e1:Event)-[:DF_JOINT]->() WHERE NOT ()-[:DF_JOINT]->(e1)
            MATCH ()-[:DF_JOINT]->(e2:Event) WHERE NOT (e2)-[:DF_JOINT]->()
            MATCH p=(e1)-[:DF_JOINT*]->(e2)
            RETURN p, e1, e2
            UNION
            MATCH (e:Event) WHERE e.{self.entity_labels[0][1]} IS NOT NULL
            AND NOT ()-[:DF_JOINT]->(e) AND NOT (e)-[:DF_JOINT]->()
            MATCH p=(e) RETURN p, e AS e1, e AS e2
            }}
            WITH [event in nodes(p) | event.{self.action_lifecycle_label[0]}] AS path, 
                e1.{self.entity_labels[0][1]} AS resource, e1.{self.entity_labels[1][1]} AS case_id, 
                nodes(p) AS events, e1.timestamp AS start_time, e2.timestamp AS end_time
            CREATE (ti:TaskInstance {{path:path, rID:resource, cID:case_id, start_time:start_time,
                end_time:end_time, r_count: 1, c_count: 1}})
            WITH ti, events
            UNWIND events AS e
            CREATE (e)<-[:CONTAINS]-(ti)
            '''
        run_query(self.driver, query_create_ti_nodes)
        pr.record_performance('create_ti_nodes')

        # set task instance ids
        query_set_ti_ids = f'''
            MATCH (ti:TaskInstance)
            WITH DISTINCT ti.path AS path, count(*) AS count
            ORDER BY count DESC
            WITH collect(path) as paths, collect(count) as counts
            UNWIND range(0, size(paths)-1) as pos
            WITH paths[pos] AS path, pos+1 AS rank
            MATCH (ti:TaskInstance) WHERE ti.path = path
            SET ti.ID = rank
            '''
        run_query(self.driver, query_set_ti_ids)
        pr.record_performance('set_task_instance_id')

        for entity in self.entity_labels:
            # correlate task instances to entities
            query_correlate_ti_to_entity = f'''
                MATCH (ti:TaskInstance)
                MATCH (n:Entity {{EntityType:"{entity[0]}"}}) WHERE ti.{entity[2]} = n.ID
                CREATE (ti)-[:CORR]->(n)
                '''
            run_query(self.driver, query_correlate_ti_to_entity)
            pr.record_performance(f'correlate_ti_to_entity_({entity[0]})')

            # create DF-relationships between task instances
            query_create_df_ti = f'''
                MATCH (n:Entity) WHERE n.EntityType="{entity[0]}"
                MATCH (ti:TaskInstance)-[:CORR]->(n)
                WITH n, ti AS nodes ORDER BY ti.start_time, ID(ti)
                WITH n, COLLECT (nodes) as nodeList
                UNWIND range(0, size(nodeList)-2) AS i
                WITH n, nodeList[i] as ti_first, nodeList[i+1] as ti_second
                MERGE (ti_first)-[df:DF_TI {{EntityType:n.EntityType}}]->(ti_second)
                '''
            run_query(self.driver, query_create_df_ti)
            pr.record_performance(f'create_df_ti_({entity[0]})')

        pr.record_total_performance()
        pr.save_to_file()


def run_query(driver, query):
    with driver.session() as session:
        result = session.run(query).single()
        if result:
            return result.value()
        else:
            return None
