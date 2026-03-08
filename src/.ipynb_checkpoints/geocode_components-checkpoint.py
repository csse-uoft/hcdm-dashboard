from geopy.geocoders import Nominatim
from arcgis.gis import GIS
from arcgis.geocoding import geocode
import plotly.graph_objects as go
import plotly.express as px
from shapely.geometry import Point
from shapely import wkt
from SPARQLWrapper import SPARQLWrapper, JSON
from src.ui_components import *
def geocode_logic(address):
    """Tries ArcGIS with restriction first, then falls back to Nominatim."""
    # 1. Initialize ArcGIS (No key required for basic public geosearch)
    public_gis = GIS() 

    # 2. Initialize Nominatim fallback
    nominatim_geolocator = Nominatim(user_agent="megan.katsumi@utoronto.ca")

    # Toronto Bounding Box for ArcGIS: [min_lon, min_lat, max_lon, max_lat]
    TORONTO_EXTENT = "-79.6393,43.5810,-79.1159,43.8554"
    # Try Official ArcGIS Library
    try:
        results = geocode(
            address=address, 
            search_extent=TORONTO_EXTENT, 
            max_locations=1,
            location_type="rooftop"
        )
        if results:
            loc = results[0]['location']
            return loc['y'], loc['x'], results[0]['address']
    except Exception as e:
        print(f"ArcGIS Error: {e}")

    # Fallback to Nominatim Toronto Restriction
    try:
        location = nominatim_geolocator.geocode({"street": address, "city": "Toronto", "country": "Canada"})
        if location:
            return location.latitude, location.longitude, location.address
    except Exception as e:
        print(f"Nominatim Error: {e}")

    return None, None, None

def process_address(endpoint,address):
    """Returns a parcel ID, referenced address, SPARQL lookup query, and map given user address input"""
    if not address:
        return None, "Please enter an address.", "", go.Figure()

    lat, lon, full_address = geocode_logic(address)

    if lat is None:
        return None, "Address not found in Toronto.", "", go.Figure()

    # 1. Generate WKT and Query
    wkt_point = Point(lon, lat).wkt
    query_text = f"""PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
PREFIX hp: <http://ontology.eil.utoronto.ca/HPCDM/>      
   PREFIX loc: <https://standards.iso.org/iso-iec/5087/-1/ed-1/en/ontology/SpatialLoc/>
    PREFIX genprop: <https://standards.iso.org/iso-iec/5087/-1/ed-1/en/ontology/GenericProperties/>

SELECT ?p ?wkt WHERE {{
  ?p a hp:Parcel ;
     loc:hasLocation ?loc .
    ?loc geo:asWKT ?wkt.
    BIND("{wkt_point}"^^geo:wktLiteral AS ?pwkt)
   ?loc geo:sfIntersects ?pwkt #according to googleAI, graphDB's plugin specifically allows ?feature geo:sfIntersects ?wktLiteral to support "on-the-fly" spatial filtering without requiring you to insert temporary data.
}} LIMIT 5"""

    # 2. SPARQL Execution
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(query_text)
    sparql.setReturnFormat(JSON)

    parcel_uri = "No parcels found."
    fig = go.Figure()

    # Map Search Marker
    fig.add_trace(go.Scattermap(
        lat=[lat], 
        lon=[lon],
        mode='markers', 
        marker=dict(size=15, color='#FF0000'), #red
        name="Search Location"
    ))

    try:
        bindings = sparql.query().convert()["results"]["bindings"]
        if bindings:
            ids = []
            for res in bindings:
                parcel_uri = res['p']['value']
                ids.append(parcel_uri)

                # Use the new helper
                add_wkt_to_fig(
                    fig, 
                    res['wkt']['value'], 
                    f"Parcel: {parcel_uri.split('/')[-1]}", 
                    color='#FFA500', #orange
                    opacity=0.4)
    except Exception as e:
        parcel_uri = f"Query Error: {e}"

    fig.update_layout(
        map_style="streets",
        map=dict(center=dict(lat=lat, lon=lon), zoom=17),
        margin={"r":0,"t":0,"l":0,"b":0},
        legend=dict(
        orientation="h",  # Make the items stack horizontally
        yanchor="bottom",
        y=1.02,           # Position just above the top of the plot area
        xanchor="right",
        x=1
    )
    )
    return parcel_uri, f"Geocoded: {full_address}", query_text, fig