# Course: Data Quality and Data Wrangling
# Scraping and visualizing data on Berlin's air quality  SCRAPING AND VISUALIZING DATA ON BERLIN'S AIR QUALITY

## Table of Contents
1. [About the project](#About-the-project)
2. [Requirements](#Requirements)
3. [Installation](#Installation)
4. [Usage / How to](#Usage-/-How-to)
5. [Contributing](#Contributing)


## About the project
This code refers to freely available data on air quality in the city of Berlin as well as a few dependent factors (traffic, roadworks, vehicle registrations and weather). The data is obtained via API interfaces or web scraping and visualized afterwards via Dash / Plotly.
The code is written in such a way that it can run as an hourly routine to collect the relevant data.

The aforementioned data is obtained via API or web scraping. A significant part of the code therefore deals in detail with the use of bs4 and requests. 

The data is then written to an HDF5 file. 

IMPORTANT:
This app was developed as part of a case-study course for my Data Science degree and contains the basic requirements according to the assignment. Further development is not planned.

## Requirements

**General:** 
This app was written in Python. Make sure you have Python 3.8+ installed on your device. 
You can download the latest version of Python [here](https://www.python.org/downloads/). 

**Packages:**
* [dash](https://dash.plotly.com) (install via "pip install dash")
* [plotly](https://plotly.com/python/) (install via "pip install plotly)
* [numpy](https://numpy.org) (install via "pip install numpy")
* [pandas](https://pandas.pydata.org/about/index.html) (install via "pip install pandas")
* [pathlib](https://docs.python.org/3/library/pathlib.html) (install via "pip install pathlib")
* [yaml](https://python.land/data-processing/python-yaml) (install via "pip install yaml")*
* [dateutil](https://pypi.org/project/python-dateutil/) (install via "pip install python-dateutil")
* [datetime](https://docs.python.org/3/library/datetime.html) (install via "pip install datetime")
* [os](https://docs.python.org/3/library/os.html) (install via "pip install os")
* [math](https://docs.python.org/3/library/math.html) (install via "pip install math")
* [bs4](https://pypi.org/project/beautifulsoup4/) (install via "pip install beautifulsoup4")
* [requests](https://pypi.org/project/requests/) (install via "pip install requests")
* [re](https://docs.python.org/3/library/re.html) (install via "pip install re")
* [logging](https://docs.python.org/3/library/logging.html) (install via "pip install logging")
* [h5py](https://docs.h5py.org/en/stable/) (install via "pip install h5py")
* [json](https://docs.python.org/3/library/json.html) (install via "pip install json")
* [subprocess](https://docs.python.org/3/library/subprocess.html) (install via "pip install subprocess")
* [platform](https://docs.python.org/3/library/platform.html) (install via "pip install platform")

**Data:**

Data has already been collected for most of February and can be used accordingly. The data can be found in "./data/analysis.h5".

**IMPORTANT**<br>
Due to a small error regarding the coordinates of the constructions (longitude and latitude swapped), please note the following:<br>
The code as uploaded here can be left as it is if the data stored here ("./data/analysis.h5") is to be viewed exclusively via app.py.

If this data is to be supplemented (not recommended!), the code must be adapted at the following point:
"./utils/crawl_data_extraction.py"
Function: extract_traffic_sensors
See "!!!" comment

If new data is to be collected from the start (recommended), the code must be adapted at the following point:
"./utils/app_data_processing.py"
Function: traffic_nearby_station
See "!!!" comment


## Installation

**How To:**<br>
The easiest way to use this app is to download the complete repository.
IF you want to collect new data, you can skip to download the "./data/analysis.h5" file.


## Usage / How to

You only have to run crawl.py to collect the data. 
Logging file give you an insight what happend. The code is optimized to run on an hourly basis. A script for creating a cron job or a Windows task is also provided.

Run app.py to get a visualization of the data.

Make sure that you have downloaded the required data.

## Contributing 
With reference to the fact that this app was created in the course of my studies and I am therefore in a constant learning process, I am happy to receive any feedback.
So please feel free to contribute pull requests or create issues for bugs and feature requests.
