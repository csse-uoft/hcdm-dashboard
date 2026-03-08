import gradio as gr
import json
import plotly.graph_objects as go
from shapely import wkt
import plotly.express as px
from src.sparql_client import *
def add_wkt_to_fig(fig, wkt_str, name, color='blue', opacity=0.3, show_in_legend=True, group_id=None):
    """
    Parses WKT (Point, Polygon, MultiPolygon) and adds the correct trace to the Plotly figure.
    """
    try:
        clean_wkt = wkt_str.split('>')[-1].strip()
        geom = wkt.loads(clean_wkt)

        # Use the label name as the group ID if no specific group_id is provided
        gid = group_id if group_id else name

        legend_args = dict(
            name=name,
            legendgroup=gid,      # All items in this group toggle together
            showlegend=show_in_legend,
            marker=dict(size=12, color=color)
        )

        # Handle Point
        if geom.geom_type == 'Point':
            fig.add_trace(go.Scattermap(
                lat=[geom.y], lon=[geom.x],
                mode='markers',
                **legend_args # Unpack common legend settings
            ))

        # Handle Polygons
        elif geom.geom_type in ['Polygon', 'MultiPolygon']:
            geoms = geom.geoms if geom.geom_type == 'MultiPolygon' else [geom]
            for g in geoms:
                lons, lats = g.exterior.xy
                fig.add_trace(go.Scattermap(
                    mode="lines", 
                    fill="toself",
                    lon=list(lons), 
                    lat=list(lats),
                    fillcolor=f'rgba({color_to_rgb(color)}, {opacity})',
                    line=dict(width=2, color=color),
                    **legend_args # Unpack common legend settings
                ))
    except Exception as e:
        print(f"Error parsing WKT for {name}: {e}")

def color_to_rgb(color_name):
    """Simple helper for fill colors."""
    mapping = {'blue': '0, 100, 255', 'green': '0, 200, 100', 'orange': '255, 165, 0'}
    return mapping.get(color_name, '128, 128, 128')

def query_router(selected_option, endpoint, parcel_uri,current_fig,progress=gr.Progress()):
    """ Generates the appropriate query and output based on selected_option. """
    if not parcel_uri or selected_option == "Select...":
        headers=["", "", ""]
        data = [["No parcel found. Please search for an address first.", "", ""]]
        return gr.Dataframe(value=data, headers=headers, visible=True)

    # Manually unpack Gradio's PlotData object (to create a copy of the map so that we can add to it)
    try:
        # current_fig.plot is a JSON string containing the 'data' and 'layout'
        fig_json = json.loads(current_fig.plot)
        new_fig = go.Figure(fig_json)
    except Exception as e:
        print(f"Figure Restoration Error: {e}")
        # If it fails, we start a fresh figure to avoid crashing
        new_fig = go.Figure()

    if selected_option == "Parcel Attributes":
        # Query 1: Returns Attribute, Value, Unit
        headers = ["Attribute", "Value", "Unit of Measure"]
        data = fetch_parcel_attributes(endpoint,parcel_uri)
        data.columns=headers
        #Style values with lower precision
        displaydata = data.style.format(precision=2)
        results_table = gr.Dataframe(value=displaydata, visible=True)    
        return results_table, current_fig

    elif selected_option == "Placeholder Query":
        # Query 2: Returns neighbourhood demographic data
        headers = ["Census Characteristic", "Value"]
        data = fetch_neighbourhood_demographics(endpoint,parcel_uri)
        return gr.Dataframe(value=data, headers=headers, visible=True), current_fig

    elif selected_option == "Available Services":
        #Returns available service and capacities
        headers = ["Service", "Capacity Type", "Capacity", "Capacity Unit"]
        data, map_data = fetch_service_data(endpoint, parcel_uri, progress=progress)
        #remove wkt from results table
        data = data.drop(columns=['swkt'])
        #Set new headers for display
        data.columns=headers
        #Style values with lower precision
        displaydata = data.style.format(precision=2)
        results_table = gr.Dataframe(value=displaydata, visible=True)     

        # For service points on the map
        # Count occurrences of each service type
        from collections import Counter
        counts = Counter([item['label'] for item in map_data])

        # 2. Map each unique service type to a color
        unique_services = list(counts.keys())
        # Use Plotly's built-in qualitative palette (e.g., Plotly, D3, or G10)
        palette = px.colors.qualitative.Plotly 
        color_map = {srv: palette[i % len(palette)] for i, srv in enumerate(unique_services)}

        #add new map points for services, don't display an additional legend element if it's already been listed
        legend_tracker = set()
        for item in map_data:
            #labels for service types
            label = item['label']
            count = counts[label]
            display_name = f"{label} ({count})" # e.g., "Library (3)"

            is_first = label not in legend_tracker
            if is_first:
                legend_tracker.add(label)
            # The helper handles Points and Polygons automatically
            add_wkt_to_fig(
                new_fig, 
                item['wkt'], 
                display_name, 
                color=color_map[label], 
                show_in_legend=is_first,
                group_id=label, # Keep the internal group ID the same for toggling
                opacity=0.2)
            # Re-apply the layout to ensure 'map' properties are preserved
            new_fig.update_layout(
                map_style="streets",
                margin={"r":0,"t":0,"l":0,"b":0}
            )
        return results_table, new_fig
    #...and so on...
    elif selected_option == "Applicable Zoning":
        #Returns available service and capacities
        headers = ["Constraint", "Constrained Property", "Limit", "Limit Unit"]
        data, map_data = fetch_zoning_data(endpoint, parcel_uri, progress=progress)
        #remove wkt from results table
        data = data.drop(columns=['regwkt'])
        #Set new headers for display
        data.columns=headers
        #Style values with lower precision
        displaydata = data.style.format(precision=2)
        results_table = gr.Dataframe(value=displaydata, visible=True)

        #update map
        for item in map_data:
            #labels for service types
            label = item['label']

            display_name = f"{label}"

            # The helper handles Points and Polygons automatically
            add_wkt_to_fig(
                new_fig, 
                item['wkt'], 
                display_name, 
                # color=color_map[label], 
                opacity=0.2)
            # Re-apply the layout to ensure 'map' properties are preserved
            new_fig.update_layout(
                map_style="streets",
                margin={"r":0,"t":0,"l":0,"b":0}
            )
        return results_table, new_fig
    return gr.Dataframe(visible=False), new_fig