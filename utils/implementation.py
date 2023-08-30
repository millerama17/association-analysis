import pandas as pd
from mlxtend.preprocessing import TransactionEncoder
from SPARQLWrapper import SPARQLWrapper, JSON


def query_entity(sparql_endpoint, query):
    sparql = SPARQLWrapper(sparql_endpoint)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    query_result = sparql.query().convert()

    entity_ids = []
    for results in query_result["results"]["bindings"]:
        entity = str(results["entity"]["value"]).split('/')
        entity_ids.append(entity[-1])

    return entity_ids


def get_property_labels_two_lists(entity_ids_1, entity_ids_2, sparql_endpoint, external_properties):
    sparql = SPARQLWrapper(sparql_endpoint)
    entity_1_db = []
    entity_merge_db = []
    entity_2_db = []

    for entity_id in entity_ids_1:
        query_string = """
        PREFIX wd: <http://www.wikidata.org/entity/>
        PREFIX wdt: <http://www.wikidata.org/prop/direct/>
        PREFIX wikibase: <http://wikiba.se/ontology#>
          
        SELECT ?prop WHERE {
          wd:""" + entity_id + """ ?prop ?value .
        }
        """
        sparql.setQuery(query_string)
        sparql.setReturnFormat(JSON)
        results_entity = sparql.query().convert()

        prop_label = [results["prop"]["value"] for results in results_entity["results"]["bindings"]]

        codes = [url.split('/')[-1] for url in prop_label]
        unique_codes = list(set(codes))

        filtered_list = [code for code in unique_codes if code.startswith('P') and code[1:].isdigit() and code not in
                         external_properties]

        entity_1_db.append(filtered_list)
        entity_merge_db.append(filtered_list)

    for entity_id in entity_ids_2:
        query_string = """
        PREFIX wd: <http://www.wikidata.org/entity/>
        PREFIX wdt: <http://www.wikidata.org/prop/direct/>
        PREFIX wikibase: <http://wikiba.se/ontology#>

        SELECT ?prop WHERE {
          wd:""" + entity_id + """ ?prop ?value .
        }
        """

        sparql.setQuery(query_string)
        sparql.setReturnFormat(JSON)
        results_entity = sparql.query().convert()

        prop_label = [results["prop"]["value"] for results in results_entity["results"]["bindings"]]

        codes = [url.split('/')[-1] for url in prop_label]
        unique_codes = list(set(codes))

        filtered_list = [code for code in unique_codes if code.startswith('P') and code[1:].isdigit() and code not in
                         external_properties]

        entity_2_db.append(filtered_list)
        entity_merge_db.append(filtered_list)

    return entity_1_db, entity_2_db, entity_merge_db


def preprocess_db(entity_merge_db):
    rich_and_poor_count = len(entity_merge_db) // 2
    # Append labels
    for i in range(len(entity_merge_db)):
        if i < rich_and_poor_count:
            entity_merge_db[i].append('rich')
        if i >= rich_and_poor_count:
            entity_merge_db[i].append('poor')

    # Transaction Encoding
    te = TransactionEncoder()
    te_ary = te.fit(entity_merge_db).transform(entity_merge_db)
    rich_and_poor_df = pd.DataFrame(te_ary, columns=te.columns_)

    # Extract property list and split
    rich_and_poor_prop_list = rich_and_poor_df.columns.tolist()

    # Separate properties and values
    rich_and_poor_value_list = []
    rich_and_poor_properties = []
    for i in rich_and_poor_prop_list:
        if 'Q' in i:
            rich_and_poor_value_list.append(i)
        else:
            rich_and_poor_properties.append(i)

    return rich_and_poor_df, rich_and_poor_value_list, rich_and_poor_properties


def fetch_labels_and_update_columns(rich_and_poor_df, rich_and_poor_value_list, rich_and_poor_properties,
                                    endpoint="https://query.wikidata.org/sparql"):
    sparql = SPARQLWrapper(endpoint)

    rich_and_poor_prop_label = []

    for i in (range(len(rich_and_poor_properties))):
        query_string = """
        SELECT DISTINCT ?propLabel {
          VALUES ?p {wdt:""" + rich_and_poor_properties[i] + """}
          SERVICE wikibase:label { bd:serviceParam wikibase:language "en". } 
          ?prop wikibase:directClaim ?p .
        }
        """

        sparql.setQuery(query_string)
        sparql.setReturnFormat(JSON)
        results_prop = sparql.query().convert()
        for results in results_prop["results"]["bindings"]:
            rich_and_poor_prop_label.append(results["propLabel"]["value"])

    rich_and_poor_value_label = []

    for i in (range(len(rich_and_poor_value_list))):
        query_string = """
        SELECT DISTINCT ?pLabel {
          VALUES ?p {wd:""" + rich_and_poor_value_list[i] + """}
          SERVICE wikibase:label { bd:serviceParam wikibase:language "en". } 
        }
        """

        sparql.setQuery(query_string)
        sparql.setReturnFormat(JSON)
        results_prop = sparql.query().convert()
        for results in results_prop["results"]["bindings"]:
            rich_and_poor_value_label.append(results["pLabel"]["value"])

    rich_and_poor_column = rich_and_poor_value_label.copy()
    rich_and_poor_column.extend(rich_and_poor_prop_label)

    rich_and_poor_column.append('poor')
    rich_and_poor_column.append('rich')
    rich_and_poor_df.columns = rich_and_poor_column

    return rich_and_poor_df, rich_and_poor_prop_label


def calculate_rule_metrics(rich_and_poor_df, rich_and_poor_prop_label, antecedent_value, csv_file_name):
    column = ['antecedents', 'consequents', 'antecedent support', 'consequent support',
              'support', 'confidence', 'lift', 'leverage', 'conviction']

    df_manual = pd.DataFrame(columns=column)

    for i in range(len(rich_and_poor_prop_label)):
        antecedent = antecedent_value
        consequent = rich_and_poor_prop_label[i]
        total_transactions = len(rich_and_poor_df)
        antecedent_support = rich_and_poor_df[antecedent].value_counts()[True]/total_transactions
        consequent_support = rich_and_poor_df[consequent].value_counts()[True]/total_transactions
        x_and_y = rich_and_poor_df.index[(rich_and_poor_df[antecedent] == True) &
                                         (rich_and_poor_df[consequent] == True)].tolist()
        x = rich_and_poor_df.index[(rich_and_poor_df[antecedent] == True)].tolist()
        if len(x_and_y) != 0:
            support = len(x_and_y) / total_transactions
            leverage = support - (antecedent_support * consequent_support)
        else:
            support = 0
            leverage = 0
        confidence = len(x_and_y) / len(x)
        lift = confidence / consequent_support
        if confidence < 1:
            conviction = (1 - consequent_support) / (1 - confidence)
        else:
            conviction = 'inf'
        new_row = {'antecedents': antecedent, 'consequents': consequent,
                   'antecedent support': antecedent_support,
                   'consequent support': consequent_support, 'support': support,
                   'confidence': confidence, 'lift': lift, 'leverage': leverage,
                   'conviction': conviction}
        df_manual = df_manual.append(new_row, ignore_index=True)

        # Sort by lift in descending order
        df_manual = df_manual.sort_values('lift', ascending=False)

        # Export to CSV
        df_manual.to_csv(csv_file_name, index=False)
