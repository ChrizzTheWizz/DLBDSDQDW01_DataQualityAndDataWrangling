import pandas as pd
import numpy as np
from math import radians, cos, sin, sqrt, atan2
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def haversine(lat1, lng1, lat2, lng2):
    """
    Haversine function to determine distance between to coordinates on earth

    :param lat1: latitude of coordinates 1
    :param lng1: longitude of coordinates 1
    :param lat2: latitude of coordinates 2
    :param lng2: longitude of coordinates 2
    :return: distance in meters
    """
    # radius of the earth in kilometers
    R = 6371.0

    # conversion from degree to radian
    dlat = radians(lat2 - lat1)
    dlon = radians(lng2 - lng1)

    a = sin(dlat / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c

    # conversion from kilometers to meters
    return distance * 1000


def temp_to_color(temperature):
    """
    Assigns a color to temperature

    :param temperature: temperature (between -20 and 60)
    :return: RGB color
    """
    # linear color transition from -20°C (dark blue) to +40°C (dark red)
    norm_temp = (temperature + 20) / 60  # Normalisierung zwischen 0 und 1
    color = np.array([255, 0, 0]) * norm_temp + np.array([0, 0, 255]) * (1 - norm_temp)  # Rot zu Blau

    return f'rgb({color[0]}, {color[1]}, {color[2]})'


def constructions_nearby_station(data_constructions, station_lat, station_lng, marker_list):
    """
    Counts constructions which are within a 500m radius to the air quality station

    :param data_constructions: construction dataframe
    :param station_lat: latitude of station
    :param station_lng: longitude of station
    :param marker_list: list of markers to be edited
    :return: number of constructions and updated marker list
    """
    # initialize construction count
    construction_count = 0

    for index, row in data_constructions.iterrows():
        # extract coordinates
        construction_lng, construction_lat = row['Coordinates']

        # calculation and condition check
        if haversine(station_lat, station_lng, construction_lat, construction_lng) <= 500:
            marker_list.append((construction_lat, construction_lng, 'construction'))
            construction_count += 1

    return construction_count, marker_list


def traffic_nearby_station(data_sensors, data_traffic, station_lat, station_lng, marker_list):
    """
    Extract traffic sensor codes which are within a 500m radius to the air quality station

    :param data_sensors: traffic sensor dataframe
    :param data_traffic: traffic data dataframe
    :param station_lat: latitude of station
    :param station_lng: longitude of station
    :param marker_list: list of markers to be edited
    :return: list of traffic sensors and updated marker list
    """
    sensors = []

    for index, row in data_sensors.iterrows():
        if index in data_traffic.index.get_level_values(0):

            # !!! BE AWARE OF THE FOLLOWING COMMENTS

            # THIS (SWAPPED) FALSE ASSIGNMENT IS JUST DUE TO A MISTAKE
            sensor_lng = row['location_latitude']
            sensor_lat = row['location_longitude']

            # CHANGE IT TO THAT IF YOU RE-INITIALIZE EVERYTHING!
            # sensor_lng = row['location_longitude']
            # sensor_lat = row['location_latitude']

            distanz = haversine(station_lat, station_lng, sensor_lat, sensor_lng)

            if distanz <= 500:
                marker_list.append((sensor_lat, sensor_lng, 'sensor'))
                sensors.append(index)

    return sensors, marker_list


def create_air_quality_traffic_figure(air_quality_data, traffic_data, weather_data, sensors, group_size=6):
    """
    Creates a line figure visualizing both air quality and traffic data as well as the temperature (as background color)

    :param air_quality_data: air quality data dataframe
    :param traffic_data: traffic data dataframe
    :param weather_data: weather data dataframe
    :param sensors: list of traffic sensor IDs within 500m range to air quality station
    :param group_size: number of summarized (mean value calculation) hours for faster calculation of the weather
    :return:
    """
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # add air quality data to figure
    for col in air_quality_data.columns:
        fig.add_trace(
            go.Scatter(x=air_quality_data.index, y=air_quality_data[col], name=col.split('_')[0]),
            secondary_y=False,
        )

    # add a line for each sensor actually present in the restructured Traffic DataFrame
    for sensor in sensors:
        fig.add_trace(
            go.Scatter(x=traffic_data.index, y=traffic_data['Traffic'][sensor], name=f'Traffic {sensor}'),
            secondary_y=True,
        )

    # update of the layout for the plot
    fig.update_layout(
        title_text='Values for air quality and traffic'
    )

    # set X-axis and Y-axis titles
    fig.update_xaxes(title_text="Timestamp")
    fig.update_yaxes(title_text="Air Quality Measurements in µg / m³", secondary_y=False)
    fig.update_yaxes(title_text="Traffic (vehicles / h)", secondary_y=True)

    # prepare weather data according to group size
    group_weather_ids = np.arange(len(weather_data)) // group_size
    # grouping the DataFrame and calculating the mean value for the temperature
    grouped_weather_data = weather_data.groupby(group_weather_ids).agg({'Temperature': 'mean'})

    for group_id, group_data in grouped_weather_data.iterrows():
        # selection of the corresponding index for the group
        start_index = group_id * group_size
        end_index = start_index + group_size - 1
        start_timestamp = weather_data.index[start_index]
        end_timestamp = weather_data.index[min(end_index, len(weather_data)-1)]

        temp_color = temp_to_color(group_data['Temperature'])
        fig.add_shape(
            type="rect",
            x0=start_timestamp, x1=end_timestamp + pd.Timedelta(minutes=60),
            y0=0, y1=1,
            xref="x", yref="paper",
            fillcolor=temp_color,
            opacity=0.4,
            layer="below",
            line_width=0,
        )

    return fig


def create_map_figure(coordinates):
    """
    Creates map figure with station as center and additional markers for constructions and traffic sensors

    :param coordinates: coordinates and type for markers
    :return: scattermapbox figure
    """

    # initialize marker styles
    marker_styles = {
        'station': {'color': 'blue', 'size': 20},
        'construction': {'color': 'red', 'size': 15},
        'sensor': {'color': 'green', 'size': 10},
    }

    fig = go.Figure()

    for lat, lon, marker_type in coordinates:
        fig.add_trace(go.Scattermapbox(
            lat=[lat],
            lon=[lon],
            mode='markers',
            marker=go.scattermapbox.Marker(
                size=marker_styles[marker_type]['size'],
                color=marker_styles[marker_type]['color'],
            ),
            name=marker_type,
        ))

    # extract only the coordinates where the type is 'station'
    station_coordinates = [(lat, lng) for lat, lng, marker_type in coordinates if marker_type == 'station']
    station_lat, station_lng = station_coordinates[0]

    fig.update_layout(
        mapbox_style="open-street-map",
        mapbox=dict(
            center=dict(lat=station_lat, lon=station_lng),
            zoom=13
        )
    )

    return fig
