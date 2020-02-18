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
    'host': '192.168.178.1', # The tebis host IP Adr
    'configfile': 'd:/tebis/Anlage/Config.txt' # Tebis config file loaction on the server -> ask your admin
}
teb = tebis.Tebis(configuration=configuration)
```

### Advanced configuration

```python
configuration = {
            'host': None, # The tebis host IP Adr
            'port': 4712, # Tebis Port [4712]
            'configfile': 'd:/tebis/Anlage/Config.txt', # Tebis config file location on the server -> ask your admin
            'useOracle': None,  # Optional: can be True or False - False to Switch off the DB usage
            'OracleDbConn': { # The Oracle Connection
                'host': None, # IP Adr
                'port': 1521, # Port [1521]
                'user': None, # user
                'psw': None, #pwd
                'service': 'XE'
            }
        }
teb = tebis.Tebis(configuration=configuration)
```

### read Data:

#### as Numpy structured array:
```python
resJSON = teb.getDataAsJson(['PG2_H12_I_ALR_XXX_DF','PG2_H12_I_BAHN_XXX_PO1'], 1581324153, 1581325153, 10)
    
```
#### as Pandas:
```python
resJSON = teb.getDataAsJson(['PG2_H12_I_ALR_XXX_DF','PG2_H12_I_BAHN_XXX_PO1'], 1581324153, 1581325153, 10)
    
```
#### as Json:
```python
resJSON = teb.getDataAsJson(['PG2_H12_I_ALR_XXX_DF','PG2_H12_I_BAHN_XXX_PO1'], 1581324153, 1581325153, 10)
    
```