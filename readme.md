# pytebis Python Connector for TeBIS from Steinhaus

pytebis is a connector for interacting with a TeBIS Server.

The connector can return structured data in a defined timespan with defined measuring points.
There are function to get the data as structured NumPy Array, Pandas or as json.

## Install the package

```python
pip install pytebis
```

## Usage

### Import the package

```python
from pytebis import tebis
```

### Basic configuration

With the basic configuration it is possible to read data and to load the measuring point names and ids.
The advanced Configuration is needed to additional load the groups and tree config.

```python
configuration = {
    'host': '192.168.1.1', # The tebis host IP Adr
    'configfile': 'd:/tebis/Anlage/Config.txt' # Tebis config file loaction on the server -> ask your admin
}
teb = tebis.Tebis(configuration=configuration)
```

### Advanced configuration

```python
configuration = {
            'host': '192.168.1.1', # The tebis host IP Adr
            'port': 4712, # Tebis Port [4712]
            'configfile': 'd:/tebis/Anlage/Config.txt', # Tebis config file location on the server -> ask your admin
            'useOracle': None,  # Optional: can be True or False - False to Switch off the DB usage
            'OracleDbConn': { # The Oracle Connection
                'host': '192.168.1.1', # IP Adr
                'port': 1521, # Port [1521]
                'user': None, # user
                'psw': None, #pwd
                'service': 'XE'
            }
        }
teb = tebis.Tebis(configuration=configuration)
```

### read Data from TeBIS

There are different functions to read data from the TeBIS Server. All functions have the some parameters. Only the return are specific to the function.
Parameters:

`result = teb.getDataAsJson(names, start, end, rate=1)`

- names = Array of all mst-names to read
- start = Unix-Timestamp where to start the read
- end = Unix-Timestamp where to end the read
- rate = What reduction should be used for the read

The Data which is returned by the TeBIS-Server is vectorized into a structured numpy array. Which is working super fast and totally comparable with the performance of the TeBIS A Client. You can use different functions to get the data in std. Python formats for further analysis.

#### as Numpy structured array

```python
resNP = teb.getDataAsNP(['My_mst_1','My_mst_2'], 1581324153, 1581325153, 10)
```

A structured Numpy Array is returned. There is a Column per mst-name, additional a column with the timestamp is added with index 0.
You can directly access the elemnets e.g. by indexing them by name `resNP["timestamp"]`

#### as Pandas

```python
df = teb.getDataAsPD(['My_mst_1','My_mst_2'], 1581324153, 1581325153, 10)
```

The Pandas DataFrame will not return a column with the timestamp. But a DateTimeIndex. So you can directly use this for TimeSeries Operations,

#### as Json

```python
resJSON = teb.getDataAsJson(['My_mst_1','My_mst_2'], 1581324153, 1581325153, 10)
```
