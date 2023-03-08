import pandas as pd
from PerformanceRecorder import PerformanceRecorder
from neo4j import GraphDatabase


class EventGraphConstructor:

    def __init__(self, password, import_directory, filename):
        self.driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", password))
        self.filename = filename
        self.file_name = f'{filename}.csv'
        self.csv_data_set = pd.read_csv(f'{import_directory}{filename}.csv')
        self.event_attributes = self.csv_data_set.columns
        self.data_entities = ['case', 'resource']

    def construct(self):
        pr = PerformanceRecorder(self.filename, 'constructing_event_graph')
        query_create_event_nodes = f'LOAD CSV WITH HEADERS FROM \"file:///{self.file_name}\" as line\n'
        query_create_event_nodes += 'CALL {\n'
        query_create_event_nodes += ' WITH line\n'
        for attr in self.event_attributes:
            if attr == 'idx':
                value = f'toInteger(line.{attr})'
            elif attr in ['timestamp', 'start', 'end']:
                value = f'datetime(line.{attr})'
            else:
                value = 'line.' + attr
            if self.event_attributes.get_loc(attr) == 0:
                new_line = f' CREATE (e:Event {{{attr}: {value},'
            elif self.event_attributes.get_loc(attr) == len(self.event_attributes) - 1:
                new_line = f' {attr}: {value}, LineNumber: linenumber()}})'
            else:
                new_line = f' {attr}: {value},'
            query_create_event_nodes = query_create_event_nodes + new_line
        query_create_event_nodes += '\n'
        query_create_event_nodes += '} IN TRANSACTIONS OF 1000 ROWS;'
        run_query(self.driver, query_create_event_nodes)
        pr.record_performance("import_event_nodes")

        # query_filter_events = f'MATCH (e:Event) WHERE e.lifecycle in ["SUSPEND","RESUME", "ATE_ABORT", "SCHEDULE", "WITHDRAW"] DELETE e'
        query_filter_events = f'CALL {{MATCH (e:Event) WHERE e.lifecycle in ["SUSPEND","RESUME"] DELETE e}} IN TRANSACTIONS OF 1000 ROWS'
        run_query(self.driver, query_filter_events)
        pr.record_performance(f"filter_events_SUSPEND_RESUME")

        for entity in self.data_entities:
            query_create_entity_nodes = f'''
                CALL {{
                    MATCH (e:Event) 
                    WITH DISTINCT e.{entity} AS id
                    CREATE (n:Entity {{ID:id, EntityType:"{entity}"}})
                }} IN TRANSACTIONS OF 1000 ROWS'''
            run_query(self.driver, query_create_entity_nodes)
            pr.record_performance(f"create_entity_nodes_({entity})")

            query_correlate_events_to_entity = f'''
                CALL {{
                    MATCH (e:Event) WHERE e.{entity} IS NOT NULL
                    MATCH (n:Entity {{EntityType: "{entity}"}}) WHERE e.{entity} = n.ID
                    CREATE (e)-[:CORR]->(n)
                }} IN TRANSACTIONS OF 1000 ROWS'''
            run_query(self.driver, query_correlate_events_to_entity)
            pr.record_performance(f"correlate_events_to_{entity}s")

            query_create_directly_follows = f'''
                CALL {{
                    MATCH (n:Entity) WHERE n.EntityType="{entity}"
                    MATCH (n)<-[:CORR]-(e)
                    WITH n, e AS nodes ORDER BY e.timestamp, ID(e)
                    WITH n, collect(nodes) AS event_node_list
                    UNWIND range(0, size(event_node_list)-2) AS i
                    WITH n, event_node_list[i] AS e1, event_node_list[i+1] AS e2
                    MERGE (e1)-[df:DF {{EntityType:n.EntityType}}]->(e2)
                }} IN TRANSACTIONS OF 1000 ROWS'''
            run_query(self.driver, query_create_directly_follows)
            pr.record_performance(f"create_directly_follows_({entity})")
        pr.record_total_performance()
        pr.save_to_file()


def run_query(driver, query):
    with driver.session() as session:
        result = session.run(query).single()
        if result:
            return result.value()
        else:
            return None
