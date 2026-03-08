#query templates
from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import gradio as gr
def fetch_parcel_attributes(endpoint,pid):
    """Returns the formatted SPARQL query string to retrieve a set of parcel attributes based on a given parcel IRI."""
    table_vars = ['attribute', 'value', 'unit']
    query =  f"""PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX tor: <http://ontology.eil.utoronto.ca/Toronto/Toronto#>
PREFIX i72: <http://ontology.eil.utoronto.ca/ISO21972/iso21972#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
PREFIX hp: <http://ontology.eil.utoronto.ca/HPCDM/>      
PREFIX loc: <https://standards.iso.org/iso-iec/5087/-1/ed-1/en/ontology/SpatialLoc/>
PREFIX genprop: <https://standards.iso.org/iso-iec/5087/-1/ed-1/en/ontology/GenericProperties/>

SELECT ?attribute ?value ?unit WHERE {{
	<{pid}> a hp:Parcel;
		?att ?q.
    ?q i72:hasValue [i72:hasNumericalValue ?value;
								i72:hasUnit ?u].
    ?att rdfs:label ?attribute.
    ?u rdfs:label ?unit.
  # Filter out ?attribute if there exists a more specific sub-property (?sub) that defines the value for the parcel
  FILTER NOT EXISTS {{
    ?sub rdfs:subPropertyOf+ ?att .
    <{pid}> ?sub ?q .
    FILTER (?sub != ?att)
  }}
}} """

    return run_sparql_to_data(query,endpoint, pid, table_vars)

def fetch_neighbourhood_demographics(endpoint,pid):
    """Returns neighbourhood demographic data based on a given parcel IRI"""
    query = f""" ...
    """
    return run_sparql_to_data(query,endpoint,pid)

def fetch_service_data(endpoint,pid,progress=gr.Progress()):
    """Returns the services available to a given parcel IRI.
    Note: currently only maps services with site locations
    TBD whether we also want to display catchment areas (when available)
    Stage 1: Get classes. Stage 2: Loop through classes for details."""
    sparql = SPARQLWrapper(endpoint)
    
    # --- STAGE 1: Get List of Service leaf classes defined in the graph ---
    progress(0, desc="Identifying Service Types...")
    class_query = f"""PREFIX time: <http://www.w3.org/2006/time#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX service: <https://standards.iso.org/iso-iec/5087/-2/ed-1/en/ontology/CityService/>
PREFIX oz: <http://www.theworldavatar.com/ontology/ontozoning/OntoZoning.owl#>
PREFIX i72: <http://ontology.eil.utoronto.ca/ISO21972/iso21972#>
PREFIX hp: <http://ontology.eil.utoronto.ca/HPCDM/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX tor: <http://ontology.eil.utoronto.ca/Toronto/Toronto#>
PREFIX loc: <https://standards.iso.org/iso-iec/5087/-1/ed-1/en/ontology/SpatialLoc/>
PREFIX loc_old: <http://ontology.eil.utoronto.ca/5087/1/SpatialLoc/>
PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
PREFIX uom: <http://www.opengis.net/def/uom/OGC/1.0/>
PREFIX cacensus: <http://ontology.eil.utoronto.ca/tove/cacensus#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX res: <https://standards.iso.org/iso-iec/5087/-1/ed-1/en/ontology/Resource/>
PREFIX opr: <http://www.theworldavatar.com/ontology/ontoplanningregulation/OntoPlanningRegulation.owl#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>

SELECT distinct ?servicetype WHERE {{

        #service types (TBD: what level should we capture?)
        ?servicetype rdfs:subClassOf* hp:Service.
    #filter any classes that have subclasses
      FILTER NOT EXISTS {{
    ?sub rdfs:subClassOf* ?servicetype .
    FILTER (?sub != ?servicetype && ?sub !=owl:Nothing)
  }}
}}"""
    sparql.setQuery(class_query)
    sparql.setReturnFormat(JSON)
    
    all_dfs = [] # List to hold individual service DataFrames
    map_features = [] 
    query_vars = ["servicelabel", "cap_type","cap_avail", "cap_unit", "swkt"]
    try:
        class_rows = sparql.query().convert()["results"]["bindings"]
        class_names = [row['servicetype']['value'] for row in class_rows]
        
        # --- STAGE 2: Loop through each service class ---
        for i,servicetype in enumerate(class_names):
            # Run a second query using both the parcel URI and the specific class name
            progress((i + 1) / len(class_names), desc=f"Querying Service: {servicetype}")
            detail_query = f"""
            PREFIX time: <http://www.w3.org/2006/time#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX service: <https://standards.iso.org/iso-iec/5087/-2/ed-1/en/ontology/CityService/>
PREFIX oz: <http://www.theworldavatar.com/ontology/ontozoning/OntoZoning.owl#>
PREFIX i72: <http://ontology.eil.utoronto.ca/ISO21972/iso21972#>
PREFIX hp: <http://ontology.eil.utoronto.ca/HPCDM/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX tor: <http://ontology.eil.utoronto.ca/Toronto/Toronto#>
PREFIX loc: <https://standards.iso.org/iso-iec/5087/-1/ed-1/en/ontology/SpatialLoc/>
PREFIX loc_old: <http://ontology.eil.utoronto.ca/5087/1/SpatialLoc/>
PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
PREFIX uom: <http://www.opengis.net/def/uom/OGC/1.0/>
PREFIX cacensus: <http://ontology.eil.utoronto.ca/tove/cacensus#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX res: <https://standards.iso.org/iso-iec/5087/-1/ed-1/en/ontology/Resource/>
PREFIX opr: <http://www.theworldavatar.com/ontology/ontoplanningregulation/OntoPlanningRegulation.owl#>

SELECT ?servicelabel ?cap_type ?cap_avail ?cap_unit ?swkt WHERE {{
{{
#services with suitable catchment areas
    <{pid}> hp:servicedBy ?s;
    	a hp:AdministrativeArea.
    ?s a <{servicetype}>;
    	a hp:Service.
    <{servicetype}> rdfs:label ?servicelabel.
    #service site location, if defined
    OPTIONAL {{
        ?s hp:providedFromSite ?site.		
    ?site loc:hasLocation ?sloc.
    ?sloc geo:asWKT ?swkt.}}
    
    #service capacity
    ?s res:hasAvailableCapacity ?cap.
    ?cap i72:hasValue [i72:hasNumericalValue ?cap_avail;
       									i72:hasUnit ?u];
    					rdf:type ?cap_type_class.
	?cap_type_class rdfs:label ?cap_type.
    ?u rdfs:label ?cap_unit.
    # Filter out the "Generics" (owl:Thing and owl:Nothing)
    FILTER(?cap_type_class != owl:Thing && ?cap_type_class != owl:Nothing)
    FILTER(!isBlank(?cap_type_class))

    # The Leaf Constraint: 
    # Ensure there isn't another type on this node that is a SUBCLASS of our candidate.
    FILTER NOT EXISTS {{
        ?cap rdf:type ?moreSpecific .
        ?moreSpecific rdfs:subClassOf+ ?cap_type_class .
        
        # Standard safety filters
        FILTER(?moreSpecific != ?cap_type_class)
        FILTER(?moreSpecific != owl:Nothing)
    }}
}}
UNION
{{
	#services with suitable service radius
	#parcel location
    <{pid}> loc:hasLocation [geo:asWKT ?pwkt];
    	a hp:AdministrativeArea.
    	
    #service site location(s)
    ?s a <{servicetype}>;
    	a hp:Service;
    	hp:providedFromSite ?site.		
    ?site loc:hasLocation ?sloc.
    ?sloc geo:asWKT ?swkt.
    <{servicetype}> rdfs:label ?servicelabel.
    
    #service-defined radius, in metres
    ?s hp:hasServiceRadius [i72:hasValue [i72:hasNumericalValue ?max_d;
    																i72:hasUnit i72:metre]].

	#(shortest) distance between the edge of the parcel and the service network 
	BIND(geof:distance(?pwkt, ?swkt, uom:metre) AS ?distance)
	#limit distance to within the defined service radius
	FILTER (?distance <= ?max_d)
	
    #service capacity
    ?s res:hasAvailableCapacity ?cap.
    ?cap i72:hasValue [i72:hasNumericalValue ?cap_avail;
       									i72:hasUnit ?u];
    					rdf:type ?cap_type_class.
    ?u rdfs:label ?cap_unit.
	?cap_type_class rdfs:label ?cap_type.
    # Filter out the "Generics" (owl:Thing and owl:Nothing)
    FILTER(?cap_type_class != owl:Thing && ?cap_type_class != owl:Nothing)
    FILTER(!isBlank(?cap_type_class))

    # The Leaf Constraint: 
    # Ensure there isn't another type on this node that is a SUBCLASS of our candidate.
    FILTER NOT EXISTS {{
        ?cap rdf:type ?moreSpecific .
        ?moreSpecific rdfs:subClassOf+ ?cap_type_class .
        
        # Standard safety filters
        FILTER(?moreSpecific != ?cap_type_class)
        FILTER(?moreSpecific != owl:Nothing)
    }}
}}
}}"""

            # Fetch and append results for each servce
            service_df = run_sparql_to_data(detail_query,endpoint, pid, query_vars)
            new_features = []
            # Clean the service name for the table and map label
            #service_label = servicetype.split('/')[-1].split('#')[-1]
            if not service_df.empty:
                # Extract map features before we modify the DF
                # Filter the DataFrame to only include rows with valid WKTs
                valid_wkts = service_df[service_df['swkt'].notna() & (service_df['swkt'] != '-')]
                
                # Use a list comprehension to build map_features instantly
                new_features = [
                    {"wkt": row['swkt'], "label": row['servicelabel']} 
                    for _, row in valid_wkts.iterrows()
                ]

                # Add the 'Service' column to the front of this DataFrame
                #service_df.insert(0, 'Service', service_label)
                all_dfs.append(service_df)
                # Append the new batch to your master list
                map_features.extend(new_features)

    except Exception as e:
        print(f"Loop Error: {e}")
        return pd.DataFrame(query_vars), []
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
        return pd.DataFrame(columns=query_vars), []

def fetch_zoning_data(pid,endpoint, progress=gr.Progress()):
    """Returns the zoning regulations applicable to a given parcel IRI.
    Stage 1: Get classes. Stage 2: Loop through classes for details."""
    table_vars=["constraint_type","constrained_property","limit","unit","regwkt"]
    map_features = []
    query = f"""
    PREFIX time: <http://www.w3.org/2006/time#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX service: <https://standards.iso.org/iso-iec/5087/-2/ed-1/en/ontology/CityService/>
    PREFIX oz: <http://www.theworldavatar.com/ontology/ontozoning/OntoZoning.owl#>
    PREFIX i72: <http://ontology.eil.utoronto.ca/ISO21972/iso21972#>
    PREFIX hp: <http://ontology.eil.utoronto.ca/HPCDM/>
    PREFIX geo: <http://www.opengis.net/ont/geosparql#>
    PREFIX tor: <http://ontology.eil.utoronto.ca/Toronto/Toronto#>
    PREFIX loc: <https://standards.iso.org/iso-iec/5087/-1/ed-1/en/ontology/SpatialLoc/>
    PREFIX loc_old: <http://ontology.eil.utoronto.ca/5087/1/SpatialLoc/>
    PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
    PREFIX uom: <http://www.opengis.net/def/uom/OGC/1.0/>
    PREFIX cacensus: <http://ontology.eil.utoronto.ca/tove/cacensus#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX res: <https://standards.iso.org/iso-iec/5087/-1/ed-1/en/ontology/Resource/>
    PREFIX opr: <http://www.theworldavatar.com/ontology/ontoplanningregulation/OntoPlanningRegulation.owl#>
    
    SELECT ?constraint_type ?constrained_property ?limit ?unit ?regwkt WHERE {{
        #if we are looking for the neighbourhood of a specific parcel
        <{pid}> loc:hasLocation ?ploc.
    
        #regulations defined in law
        ?reg hp:definedIn ?source.
        ?source a hp:ZoningBylaw.
        #the regulation that designates the zoning type for an area
        ?reg a hp:Regulation;
        hp:appliesTo [loc:hasLocation ?loc];
       	hp:specifiesConstraint [i72:hasValue [i72:hasNumericalValue ?limit;
            								i72:hasUnit ?unit];
                                hp:constrains [i72:parameter_of_var [i72:hasName ?constrained_property];
                                            i72:description_of ?p];
        						#constraint type
        						rdf:type ?constraint_type].
    
        #quantity constraint subtype (allowance, requirement, ...) to clarify the nature of the regulation
        ?constraint_type rdfs:subClassOf hp:QuantityConstraint.
        FILTER (?constraint_type != hp:QuantityConstraint)
    
        #only regulations that apply to the area that the parcel is located in
        ?ploc geo:sfIntersects ?loc.
    
        ?loc geo:asWKT ?regwkt.
    }}
    """
    #empty dataframe
    df = pd.DataFrame(columns=table_vars)
    #run sparql query; convert to table and add features to map
    df = run_sparql_to_data(query, endpoint, pid,table_vars)
    new_features=[]
    if not df.empty:
        # Extract map features -- what label do we give the regulation area?
        # Filter the DataFrame to only include rows with valid WKTs
        valid_wkts = df[df['regwkt'].notna() & (df['regwkt'] != '-')]

        # Use a list comprehension to build map_features instantly
        new_features = [
            {"wkt": row['regwkt'], "label": row['constrained_property']} 
            for _, row in valid_wkts.iterrows()
        ]
        # Append features 
        map_features.extend(new_features)

    return df, map_features

#transformation of query results for display
def run_sparql_to_data(query, endpoint, pid, columns):
    """
    Fetches raw data for a given SPARQL query and returns a pandas DataFrame.
    columns: List of strings matching the ?variables in the SELECT clause.
    """
    if not pid:
        return pd.DataFrame(columns=columns)

    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)

    try:
        results = sparql.query().convert()
        bindings = results.get("results", {}).get("bindings", [])
		# Create list of dicts for easy DataFrame conversion
        data = []
        for row in bindings:
        	#Extract only the variables in 'columns
            data.append({col: row.get(col, {}).get('value', None) for col in columns})

        df = pd.DataFrame(data, columns=columns)

        # Attempt numeric conversion per column
        for col in df.columns:
            try:
                # Attempt to convert the column to numeric
                # errors='raise' is the default; if it fails, it hits the 'except'
                df[col] = pd.to_numeric(df[col])
            except (ValueError, TypeError):
                # If conversion fails (e.g., it's a WKT or Unit string), 
                # we just leave the column as it is.
                continue

        # Intelligent conversion to best-fit nullable types (String, Int64, etc.)
        df = df.convert_dtypes()

        return df

    except Exception as e:
        print(f"SPARQL Error: {e}")
        return pd.DataFrame(columns=columns)