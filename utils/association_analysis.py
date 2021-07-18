import numpy as np
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
from mlxtend.preprocessing import TransactionEncoder

def query(classId):
  sparql = SPARQLWrapper("http://localhost:3030/memory/sparql")

  sparql.setQuery("""
  PREFIX wd: <http://www.wikidata.org/entity/>
  PREFIX wdt: <http://www.wikidata.org/prop/direct/>
  SELECT ?CS (COUNT(?prop) AS ?total) {
    
    SELECT DISTINCT ?CS ?prop
      WHERE {
      ?CS wdt:P31 wd:""" + classId + """ .
      ?CS ?prop ?value .
  } 

  } GROUP BY ?CS
  ORDER BY DESC(?total) 
  """)
  sparql.setReturnFormat(JSON)
  results_query = sparql.query().convert()

  queryRes = []
  for results in results_query["results"]["bindings"]:
    entity = str(results["CS"]["value"]).split('/')
    queryRes.append(entity[-1])
  
  return queryRes

def richPoor(queryRes, div, richPortion, poorPortion):
  divLen = len(queryRes)//div

  rich = queryRes[divLen*(richPortion-1):divLen*(richPortion)]

  if poorPortion == div:
    poor = queryRes[divLen*(poorPortion-1):]
  else:
    poor = queryRes[divLen*(poorPortion-1):divLen*(richPortion)]

  return rich, poor

def properties(rich, poor):
  richDb = []
  richAndpoorDb = []

  for i in range(len(rich)):
    query_string = """
    PREFIX wd: <https://www.wikidata.org/wiki/Special:EntityData/>
    PREFIX wikibase: <http://wikiba.se/ontology#>
    SELECT DISTINCT ?OS ?prop {
    VALUES ?OS {wd:""" + rich[i] + """}
    ?OS ?prop ?value .
    }
    """

    sparql.setQuery(query_string)
    sparql.setReturnFormat(JSON)
    results_entity = sparql.query().convert()
    propLabel = []
    for results in results_entity["results"]["bindings"]:
      propLabel.append(results["prop"]["value"])
    richDb.append(propLabel)
    richAndpoorDb.append(propLabel)

  poorDb = []

  for i in range(len(rich)):
    query_string = """
    PREFIX wd: <https://www.wikidata.org/wiki/Special:EntityData/>
    PREFIX wikibase: <http://wikiba.se/ontology#>
    SELECT DISTINCT ?OS ?prop {
    VALUES ?OS {wd:""" + poor[i] + """}
    ?OS ?prop ?value .
    }
    """
    sparql.setQuery(query_string)
    sparql.setReturnFormat(JSON)
    results_entity = sparql.query().convert()
    propLabel = []
    for results in results_entity["results"]["bindings"]:
      propLabel.append(results["prop"]["value"])
    
    poorDb.append(propLabel)
    richAndpoorDb.append(propLabel)

  return richAndpoorDb

def propertyLabel(rich, richPoorDf):
  wikidata = SPARQLWrapper("https://query.wikidata.org/sparql")

  for i in range(len(richAndPoordb)):
    if(i<len(rich)):
      richAndPoordb[i].append('rich')
    if(i>=len(rich)):
      richAndPoordb[i].append('poor')
  
  te = TransactionEncoder()
  te_ary = te.fit(richAndpoorDb).transform(richAndpoorDb)
  df_richAndpoor = pd.DataFrame(te_ary, columns=te.columns_)
  
  richAndpoor_prop_list = df_richAndpoor.columns.tolist()
  richAndpoor_prop_list = richAndpoor_prop_list[:-2]

  richAndpoorPropLabel = []
  richAndPoorColumnCheck = []

  for i in tqdm(range(len(richAndpoor_prop_list))):
    query_string = """
    SELECT DISTINCT ?propLabel {
      VALUES ?p {wdt:""" + richAndpoor_prop_list[i] + """}
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". } 
      ?prop wikibase:directClaim ?p .
    }
    """

    wikidata.setQuery(query_string)
    wikidata.setReturnFormat(JSON)
    results_prop = wikidata.query().convert()
    for results in results_prop["results"]["bindings"]:
      richAndpoorPropLabel.append(results["propLabel"]["value"])
      richAndPoorColumnCheck.append(richAndpoor_prop_list[i])

  missingProp = list(set(richAndpoor_prop_list) - set(richAndPoorColumnCheck))
  df_richAndpoor.drop(missingProp, axis = 1)

  richAndpoorPropLabel.append('poor')
  richAndpoorPropLabel.append('rich')
  df_richAndpoor.columns = richAndpoorPropLabel

  return df_richAndpoor

def association_analysis(df_richAndpoor):
  column = ['antecedents', 'consequents', 'antecedent support', 'consequent support', 'support', 'confidence', 
            'lift', 'leverage', 'conviction']
  df_manual_rich = pd.DataFrame(columns=column)
  columnLabel = df_richAndpoor.columns.tolist()

  for i in range(len(columnLabel)):
    antecedent = 'rich'
    consequent = columnLabel[i]
    total_transactions = len(df_richAndpoor)
    antecedent_support = df_richAndpoor[antecedent].value_counts()[True]/total_transactions
    consequent_support = df_richAndpoor[consequent].value_counts()[True]/total_transactions
    x_and_y = df_richAndpoor.index[(df_richAndpoor[antecedent] == True) & (df_richAndpoor[consequent] == True)].tolist()
    x = df_richAndpoor.index[(df_richAndpoor[antecedent] == True)].tolist()
    y = df_richAndpoor.index[(df_richAndpoor[consequent] == True)].tolist()
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
    new_row = {'antecedents':antecedent, 'consequents':consequent, 'antecedent support':antecedent_support, 
               'consequent support':consequent_support, 'support':support, 'confidence':confidence,
               'lift':lift, 'leverage':leverage, 'conviction':conviction}
    df_manual_rich = df_manual_rich.append(new_row, ignore_index=True)

  return df_manual_rich

def gap_properties(associationRuleDf):
  filtered = associationRuleDf[(associationRuleDf['support']>=0.1) & (associationRuleDf['lift']>=1.5)]

  return filtered

def gap_property_ratio(associationRuleDf):
  filtered = associationRuleDf[(associationRuleDf['support']>=0.1) & (associationRuleDf['lift']>=1.5)]

  totalProperties = associationRuleDf[(associationRuleDf['support']>=0.1)]

  gpr = filtered/totalProperties
  return gpr

