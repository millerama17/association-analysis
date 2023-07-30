import pandas as pd
from csv import reader


def process_csv_and_write_nt(csv_file_name, nt_file_name, external_properties_csv='external_prop.csv'):

    # Load external properties
    ext_csv = pd.read_csv(external_properties_csv)
    ext_prop = ext_csv['external_properties'].tolist()

    with open(nt_file_name, "w") as f:
        # Open file in read mode
        with open(csv_file_name, 'r', encoding='utf-8') as read_obj:
            # Pass the file object to reader() to get the reader object
            csv_reader = reader(read_obj)
            next(csv_reader, None)  # Skip header

            # Iterate over each row in the csv using reader object
            for row in csv_reader:
                raw = row[0].split(";")
                prop = raw[1].split('/')[-1][:-1]  # Extract the property

                # Skip if property in external properties
                if prop in ext_prop:
                    continue

                # Replace values not containing 'http://www.wikidata.org/entity/' with "\"\"@en ."
                if '<http://www.wikidata.org/entity/' not in raw[2]:
                    raw[2] = "\"\"@en ."
                    raw = raw[:3]

                # Write the triple to file
                f.write(" ".join(raw) + "\n")

    f.close()
