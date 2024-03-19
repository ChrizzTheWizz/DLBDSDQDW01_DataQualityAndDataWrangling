from dash import Dash, dcc, Output, Input, html, dash_table
import dash_bootstrap_components as dbc
from datetime import datetime

from utils.hdf5_file_output import *
from utils.crawl_setup import *
from utils.app_data_processing import *

# ----------------------------------------------------------------------------------------------------------------------

# read config file
config = read_config_file()

# determine date
current_datetime = datetime.now()

# read list of essential files
main_files = config['main_files']
# check if main files do exist
status_main_files = check_data_files(main_files)

# assign hdf5-filepath
h5_file = main_files['hdf5_file']

# ----------------------------------------------------------------------------------------------------------------------

# LOAD DATA FROM HDF5 FILE
df_air_quality = read_air_quality_data(h5_file)

df_traffic = read_traffic_data(h5_file)
df_traffic_sensors = read_traffic_sensors(h5_file, 'traffic')

df_constructions = read_construction_data(h5_file)
df_constructions['Coordinates'] = df_constructions.apply(lambda row:
                                                         row['Coordinates'] if isinstance(row['Coordinates'], list)
                                                         else (row['Geometries'][0]['coordinates']
                                                               if isinstance(row['Geometries'], list) else np.NaN),
                                                         axis=1)

df_car_registrations = read_car_registration_data(h5_file)
df_car_registrations.insert(1, 'Month', pd.NA)

df_new_car_registrations = read_new_car_registration_data(h5_file)

df_weather = read_weather_data(h5_file)

# ----------------------------------------------------------------------------------------------------------------------

# Create the Dash application
app = Dash(__name__, external_stylesheets=[dbc.themes.UNITED], title='Air Quality')

# Definiere das Layout
app.layout = dbc.Container(
    html.Div([

        dbc.Row(
            dbc.Col(
                html.Div(children=[
                    html.H1(children='Air Quality in Berlin',
                            id='00_header_title',
                            style={'color': 'black', 'text-align': 'center',
                                   'paddingTop': 42}),
                    html.H4(children='Visualization of available data on air quality and dependant factors',
                            id='00_header_subtitle',
                            style={'color': 'black', 'text-align': 'center',
                                   'paddingTop': 21, 'paddingBottom': 21})
                ]),
                width=12),
            id='00_header'),

        # 01.4 FIGURE WORLD HEATMAP (GLOBAL TEMPERATURE ANOMALIES)
        dbc.Row([
            dbc.Col(
                html.Div([
                    html.Hr(style={'height': 5}),
                    html.H5('Select an air quality station'),
                    dcc.Dropdown(
                        id='air_quality_station_code',
                        options=df_air_quality.columns.get_level_values(0).unique(),
                        value=df_air_quality.columns.get_level_values(0).unique()[0],
                        placeholder='Select a station'
                    ),
                    html.Hr(style={'height': 5}),
                ]),
                width=12
            )
        ]),

        dbc.Row([
            dbc.Col(
                html.Div([
                    html.H5('Station info:'),
                    html.H6(id='air_quality_station_info'),
                    dcc.Graph(id='air_quality_data', style={'height': '60vh'}, config={'displayModeBar': False}),
                ]),
                width=12
            )
        ]),

        dbc.Row([
            dbc.Col(
                html.Div([
                    html.H5('Total car registrations'),
                    html.Hr(style={'height': 5}),
                    html.H6('Car Registrations (annual count)'),
                    dash_table.DataTable(
                        id='car_registrations_table',
                        columns=[{"name": i, "id": i} for i in df_car_registrations.columns],
                        data=df_car_registrations.to_dict('records'),
                    ),
                    html.Br(),
                    html.H6('New Car Registrations (monthly count)'),
                    dash_table.DataTable(
                        id='new_car_registrations_table',
                        columns=[{"name": i, "id": i} for i in df_new_car_registrations.columns],
                        data=df_new_car_registrations.to_dict('records'),
                    )
                ]),
                width=6
            ),
            dbc.Col(
                html.Div([
                    html.H5('Geographical position of station, sensors and constructions'),
                    html.Hr(style={'height': 5}),
                    html.Table([
                        html.Tr([html.Td("Anzahl Baustellen in 500m Umkreis von Station",
                                         style={'text-align': 'center'}),
                                 html.Td("Anzahl Verkehrssensoren in 500m Umkreis von Station",
                                         style={'text-align': 'center'})]),
                        html.Tr([html.Td(id='number_constructions', style={'text-align': 'center'}),
                                 html.Td(id='number_sensors', style={'text-align': 'center'})])
                    ], style={'width': '100%', 'margin': 'auto'}),
                    dcc.Graph(id='berlin_map', config={'displayModeBar': False}),
                ]),
                width=6
            )

        ]),
    ]),
    fluid=True)

# ----------------------------------------------------------------------------------------------------------------------


@app.callback(
    Output('air_quality_data', 'figure'),
    Output('air_quality_station_info', 'children'),
    Output('berlin_map', 'figure'),
    Output('number_constructions', 'children'),
    Output('number_sensors', 'children'),
    [Input('air_quality_station_code', 'value')],
)
def air_quality_data(station):
    # initialize marker list
    coordinates_marker = []

    # filter air quality data from specified station
    df_air_quality_data = df_air_quality[station]

    # extract geographical data of air quality station
    station_address, station_lat, station_lng = read_air_quality_station_data(h5_file, station)

    station_info = 'Code: ' + station + ' - Address: ' + station_address

    # add air quality station coordinates as station type
    coordinates_marker.append((station_lat, station_lng, 'station'))

    # count constructions and add construction coordinates to marker list which are nearby the station
    construction_count, coordinates_marker = constructions_nearby_station(df_constructions, station_lat,
                                                                          station_lng, coordinates_marker)

    # extract sensor IDs and add construction coordinates to marker list which are nearby the station
    sensors, coordinates_marker = traffic_nearby_station(df_traffic_sensors, df_traffic,
                                                         station_lat, station_lng, coordinates_marker)

    # extract traffic data from sensors nearby station
    df_traffic_data = df_traffic.loc[(sensors, slice(None)), :]

    # restructure the DataFrame to get one column per sensor
    df_traffic_reshaped = df_traffic_data.unstack(level=0)

    # extract sensor IDs with valid (not NA) values
    sensor_codes = df_traffic_reshaped.columns.get_level_values(1)

    # creating air quality + traffic figure
    fig_station = create_air_quality_traffic_figure(df_air_quality_data, df_traffic_reshaped, df_weather, sensor_codes)

    fig_berlin = create_map_figure(coordinates_marker)

    return fig_station, station_info, fig_berlin, construction_count, len(sensors)


app.run_server(port=8051, debug=True)
# app.run_server(port=8051)
