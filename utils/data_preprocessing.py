import pandas as pd
from csv import reader

def csv_processing(filename, targetFile):
  extCSV = pd.read_csv('external_prop.csv')
  extProp = extCSV['external_properties'].tolist()

  f = open(filename, "w")

  # open file in read mode
  with open(targetFile, 'r', encoding = 'ISO-8859-1') as read_obj:
    # pass the file object to reader() to get the reader object
    csv_reader = reader(read_obj)
    # Iterate over each row in the csv using reader object
    count = 0
    for row in tqdm(csv_reader):
      if count == 0:
        count += 1
        continue

        preprocessed = "".join(row)
        #process triples  
        raw = row[0].split(";")
        propRaw = raw[1].split('/')
        prop = propRaw[-1][:-1]

        if prop in extProp:
          continue
              

        if '<http://www.wikidata.org/entity/' not in raw[2]:
          raw[2] = "\"\"@en ."
          if len(raw) > 3:
            raw = raw[:3]
        
        triple = " ".join(raw)
        f.write(triple + "\n")
        count += 1
        
        f.close()

