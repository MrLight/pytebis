# pytebis Python Connector for TeBIS from Steinhaus

pytebis is a connector for interacting with a TeBIS Server.

The connector can return structured data in a defined timespan with defined measuring points.
There are function to get the data as structured NumPy Array, Pandas or as json.
For further interaction it is possible to load the measuring points, the groups and the tree.
Alarms are currently not supported.

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
    'host': '192.168.1.10', # The tebis host IP Adr
    'configfile': 'd:/tebis/Anlage/Config.txt' # Tebis config file loaction on the server -> ask your admin
}
teb = tebis.Tebis(configuration=configuration)
```

### Advanced configuration

```python
configuration = {
            'host': '192.168.1.10', # The tebis host IP Adr
            'port': 4712, # Tebis Port [4712]
            'configfile': 'd:/tebis/Anlage/Config.txt', # Tebis config file location on the server -> ask your admin
            'useOracle': None,  # Optional: can be True or False - False to Switch off the DB usage
            'OracleDbConn': { # The Oracle Connection
                'host': '192.168.1.10', # IP Adr
                'port': 1521, # Port [1521]
                'user': None, # Oracle username
                'psw': None, #Oracle pwd
                'service': 'XE'
            }
        }
teb = tebis.Tebis(configuration=configuration)
```

### read Data from TeBIS

There are different functions to read data from the TeBIS Server. All functions have the some parameters. Only the return is specific to the function.
Parameters:

`result = teb.getDataAsJson(names, start, end, rate=1)`

- names = Array of all mst-names to read. You can pass a array of IDs, names, TebisMst-Objects or Group-Objects (even mixed).
- start = Unix-Timestamp where to start the read (must be in the same timezone as the server is)
- end = Unix-Timestamp where to end the read (must be in the same timezone as the server is)
- rate = What reduction should be used for the read

The Data which is returned by the TeBIS-Server is vectorized into a structured numpy array. Which is working super fast and is totally comparable with the performance of the TeBIS A Client. You can use different functions to get the data in std. Python formats for further analysis.

#### as Numpy structured array

```python
resNP = teb.getDataAsNP(['My_mst_1','My_mst_2'], 1581324153, 1581325153, 10)
```

A structured Numpy Array is returned. There is a Column per mst-name, additional a column with the timestamp is added with index 0.
You can directly access the elements e.g. by indexing them by name `resNP["timestamp"]`

#### as Pandas

```python
df = teb.getDataAsPD(['My_mst_1','My_mst_2'], 1581324153, 1581325153, 10)
```

The Pandas DataFrame will not return a column with the timestamp. But a DateTimeIndex. So you can directly use this for TimeSeries Operations. The creation of the Pandas Dataframe is a bit slower than the generic NumPy function, as the DataFrame and the DateTimeIndex is generated afterwards.

#### as Json

```python
resJSON = teb.getDataAsJson(['My_mst_1','My_mst_2'], 1581324153, 1581325153, 10)
```

#### Example

```python
import time
import matplotlib.pyplot as plt
from pytebis import tebis

def example():
    configuration = {
        'host': '192.168.1.10',  # The tebis host IP Adr
        'configfile': 'd:/tebis/Anlage/Config.txt' # Tebis config file loaction on the server -> ask your admin
    }
    teb = tebis.Tebis(configuration=configuration)
    df = teb.getDataAsPD([492, 123], time.time() - 3600, time.time(), 10)
    df.plot()
    plt.show()

if __name__ == "__main__":
    example()
    pass
```

### Working with measuring points, groups and the tree

The measuring points and the virtual measuring points are loaded once at startup. This is always possible so you don't need to specify a db Connection.

If you want to load the Groups, Group Members and the Tree as it is configured in the TeBIS A client you must have a working db Connection.

If you have a long running service it is a good idea to reload the information in a regular interval. (e.g. all 10min)

Just call ```teb.refreshMsts()``` to reload the data.


### Logging

The package is implementing a logger using the std. logging framework of Python. The loggername is: ```pytebis```. There is no handler configured. To setup a specific log-level for the package use a config like this after ```logging.basicConfig()``` e.g. ```logging.getLogger('pytebis').setLevel(logging.INFO)``` 
