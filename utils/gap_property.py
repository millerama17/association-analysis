import pandas as pd


def filter_and_export(rich_properties_csv, filtered_excel_file):
    # Read the CSV file into a dataframe
    df = pd.read_csv(rich_properties_csv)

    # Remove rows where the consequent is 'rich' or 'poor'
    df_drop = df[df.consequents != 'rich']
    df_drop = df_drop[df_drop.consequents != 'poor']

    # Filter dataframe based on lift and consequent support
    filtered = df_drop.loc[(df_drop['lift'] >= 1.5) & (df_drop['consequent support'] >= 0.1)]

    # Export the filtered dataframe to an Excel file
    filtered.to_excel(filtered_excel_file, index=False)


def gap_property_ratio(properties_excel):
    # Read the CSV file into a dataframe
    df_properties = pd.read_excel(properties_excel)

    all_relevant_properties = len(df_properties.loc[(df_properties['consequent support'] >= 0.1)])
    gap_properties = len(df_properties.loc[(df_properties['lift'] >= 1.5) &
                                            (df_properties['consequent support'] >= 0.1)])

    return gap_properties/all_relevant_properties
