from utils.implementation import *
from utils.gap_property import *

query_rich = """
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
SELECT ?entity (COUNT(?property) AS ?total) {

  SELECT DISTINCT ?entity ?property
    WHERE {
    ?entity wdt:P31 wd:Q3624078 .
    ?entity ?property ?value .
} 

} GROUP BY ?entity
ORDER BY DESC(?total)
LIMIT 20
"""

query_poor = """
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
SELECT ?entity (COUNT(?property) AS ?total) {

  SELECT DISTINCT ?entity ?property
    WHERE {
    ?entity wdt:P31 wd:Q3624078 .
    ?entity ?property ?value .
} 

} GROUP BY ?entity
ORDER BY ?total
LIMIT 20
"""

sparql_endpoint = "https://query.wikidata.org/sparql"
prop_entity_pairs = [('P31', 'Q3624078')]

rich_entity = query_entity(sparql_endpoint, query_rich)
poor_entity = query_entity(sparql_endpoint, query_poor)
rich_db, poor_db, rich_and_poor_db = get_property_labels_two_lists(rich_entity, poor_entity, sparql_endpoint)
rich_and_poor_df, rich_and_poor_value_list, rich_and_poor_properties = preprocess_db(rich_and_poor_db)
rich_and_poor_df_updated, rich_and_poor_prop_label = fetch_labels_and_update_columns(rich_and_poor_df,
                    rich_and_poor_value_list, rich_and_poor_properties)
calculate_rule_metrics(rich_and_poor_df_updated, rich_and_poor_prop_label,
                       "rich", "example.csv")
df = pd.read_csv("example.csv")
filter_and_export("example.csv", "gap_properties_example.xlsx")