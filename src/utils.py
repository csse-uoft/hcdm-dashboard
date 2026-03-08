from src.sparql_client import *
import gradio as gr
import numpy as np
def process_service_data(endpoint,prefixes,pid,progress=gr.Progress()):
    """Retrieves the services available to a given parcel IRI.
    Note: currently only maps services with site locations
    TBD whether we also want to display catchment areas (when available)
    Stage 1: Get classes. Stage 2: Loop through classes for details."""
    # --- STAGE 1: Get List of Service leaf classes defined in the graph ---
    progress(0, desc="Identifying Service Types...")
    class_names = fetch_service_classes(endpoint,prefixes)
    all_dfs = [] # List to hold individual service DataFrames
    map_features = [] 

    for i, row in class_names.iterrows():
        try:
            servicetype = row['servicetype'] # Get the actual URI string
            # Run a second query using both the parcel URI and the specific class name
            progress((i + 1) / len(class_names), desc=f"Querying Service: {servicetype}")
            service_df = fetch_service_data(endpoint,prefixes,pid,servicetype)
            new_features = []
            if not service_df.empty:
                # Extract map features before we modify the DF
                # Filter the DataFrame to only include rows with valid WKTs
                valid_wkts = service_df[service_df['swkt'].notna() & (service_df['swkt'] != '-')]
                
                # Use a list comprehension to build map_features instantly
                #include the service name for use as a hover label
                new_features = [
                    {"wkt": row['swkt'], "label": row['servicelabel'], "servicename": row['servicename']} 
                    for _, row in valid_wkts.iterrows()
                ]

                all_dfs.append(service_df)
                # Append the new batch to your master list
                map_features.extend(new_features)
        except Exception as e:
            print(f"Loop Error: {e}")
            return pd.DataFrame(), []
    #initialize dataframe
    final_df = pd.DataFrame()
    # Final Aggregation
    if all_dfs:
        # Combine all service DataFrames into one
        final_df = pd.concat(all_dfs, ignore_index=True)
        # Convert numeric column
        final_df['cap_avail'] = pd.to_numeric(final_df['cap_avail'], errors='coerce')
        return final_df, map_features
    else:
        return pd.DataFrame(), []
    
def process_neighbourhood_demographics(endpoint,prefixes,pid,census_characteristics):
    """processes results of demographic query for display"""
    #initialize list of map features
    map_features=[]
    #query results dataframe
    demo_df = fetch_neighbourhood_demographics(endpoint,prefixes,pid,census_characteristics)
    #process location data
    if not demo_df.empty:
        # Extract map features before we modify the DF
        # Filter the DataFrame to only include rows with valid WKTs (cwkt column), drop any duplicates (no need to display the same census tract twice)
        valid_wkts = demo_df[demo_df['cwkt'].notna() & (demo_df['cwkt'] != '-')].drop_duplicates(subset=['cwkt', 'ct'])
        
        # Use a list comprehension to build map_features instantly
        map_features = [
            {"wkt": row['cwkt'], "label": row['ct']} 
            for _, row in valid_wkts.iterrows()
        ]
    return demo_df,map_features

def process_compliance_properties(endpoint,prefixes):
    """returns a list of properties constrained by the zoning bylaw regulations in a format suitable for dropdown creation:
    [(att1 label, att1 uri),(att2 label, att2 uri),...]"""
    df = fetch_compliance_properties(endpoint,prefixes)
    property_list = list(zip(df['cp_label'], df['cp']))
    return property_list

def process_zoning_compliance(endpoint,prefixes,pid,property):
    """Returns a pandas dataframe that lists the zoning regulations that apply to nearby (within 200m) parcels, and the corresponding actual values (if available).
    Includes a list of map features of all of the "nearby" properties colour coded-based on compliance (if available)."""
    #initialize list of map features
    map_features=[]
    #query results dataframe
    df = fetch_zoning_compliance(endpoint,prefixes,pid,property)
    #add nearby parcels to list
    #label parcels as "Noncompliant" or "Compliant" depending on the value of the attribute "isviolated"
    #add a secondary label for the parcel ID (namespace stripped)
    nearbypcol = df['nearbyp'].str.extract(r'([^/#]+)$', expand=False) # This regex looks for the last / or # and takes everything following it
    df.insert(0,'nearbyp_short',nearbypcol)
    if not df.empty:
        # Extract map features 
        # Filter the DataFrame to only include rows with valid WKTs (no NAs)
        valid_wkts = df.dropna(subset=['nearbypwkt']).copy()

        # Use a list comprehension to build map_features
        map_features = [
            {"wkt": row['nearbypwkt'], "label": row['compliancestatus'], "att_label": "Parcel ID", "att_value": row['nearbyp_short']} 
            for _, row in valid_wkts.iterrows()
        ]
        
        

    return df, map_features
def process_df_col_to_markdown(df,colname):
    """Returns a string representation of a column as a markdown list"""
    list = "\n".join([f"* {x}" for x in df[colname]])
    markdown_output = f"""## {colname}
        \n{list}"""
    return markdown_output