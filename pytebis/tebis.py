import time
import socket
import struct
import zlib
import numpy as np
import numbers
import pandas as pd
import json
import datetime
from json import JSONEncoder
import simplejson
from io import StringIO
import csv
from pytebis.lazyloader import LazyLoader
import logging
from dateutil.parser import parse
logging.getLogger('pytebis').addHandler(logging.NullHandler())


class Tebis():
    '''Tebis Communication class
    '''

    def __init__(self, configfile=None, sock=None, host=None, port=None, dbConn=None, configuration=None):
        default_conf = {
            'host': None,
            'port': 4712,
            'configfile': 'd:/tebis/Anlage/Config.txt',
            'useOracle': None,  # use Oracle true / false, Opt. if not defined a defined OracleDbConn.Host will set it to true. Use false to deactive Oracle usage
            'OracleDbConn': {
                'host': None,  # Host IP-Adr
                'port': 1521,  # host port [1521]
                'schema': None,  # schema name opt. if not set user is used as schema name
                'user': None,  # db user
                'psw': None,  # db pwd
                'service': 'XE'  # Oracle service name
            },
            'liveValues': {
                'enable': False,    # Use LiveValue Feature - This is used to compensate possible timedrifts between the Tebis Server and the Client.
                'recalcTimeOffsetEvery': 600,  # When using LiveValues recalc TimeOffset every x Seconds
                'offsetMstId': 100025,  # This is the Mst which is used to calculate the last available Timestamp. Use a always available mst.
            }
        }
        self.config = selective_merge(default_conf, configuration)
        # Setup some basics in the config dict
        if self.config['OracleDbConn']['host'] is not None and (self.config['useOracle'] is True or self.config['useOracle'] is None):
            if self.config['useOracle'] is None:
                self.config['useOracle'] = True
        
        if configfile is not None:
            self.config['configfile'] = configfile
        if host is not None:
            self.config['host'] = host
        if port is not None:
            self.config['port'] = port
        self.refreshMsts()
        if self.config['liveValues']['enable'] == True:
            self.setupLiveValues()
        

    def getDataAsNP(self, names, start, end, rate=1):
        ids = []
        # find Mst with id as a number, id as MST name a str, id
        for name in names:
            id = None
            if isinstance(name, numbers.Number):
                id = self.getMst(id=name).id
            elif isinstance(name, str):
                id = self.getMst(name=name).id
            elif isinstance(name, TebisMST):
                id = name.id
            elif isinstance(name, TebisGroupElement):
                for member in name.members:
                    ids.append(member.mst.id)
            elif isinstance(name, TebisGroupMember):
                id = name.mst.id
            if id is not None:
                ids.append(id)
        if isinstance(start, datetime.datetime):
            start = start.timestamp()*1000.0
        elif isinstance(start, float):
            start = start*1000.0
        elif isinstance(start, int) and start < 100000000000: #and start > 100000000000 and start < 100000000000000:
            start = start*1000.0
        elif isinstance(start, str):
            start = datetime.datetime.strptime(start, '%Y-%m-%d %H:%M:%S.%f').timestamp()*1000.0
        if isinstance(end, datetime.datetime):
            end = end.timestamp()*1000.0
        elif isinstance(end, float):
            end = end*1000.0
        elif isinstance(end, int) and end < 100000000000: #and start > 100000000000 and start < 100000000000000:
            end = end*1000.0
        elif isinstance(end, str):
            end = datetime.datetime.strptime(end, '%Y-%m-%d %H:%M:%S.%f').timestamp()*1000.0

        nCT = rate*1000.0
        nTimeR = end
        nTimeR = (int(float(nTimeR)) / int(nCT)) * int(nCT)

        nTimeL = start
        nTimeL = (int(float(nTimeL)) / int(nCT)) * int(nCT)
        nNmbX = int(nTimeR - nTimeL) / int(nCT)
        if nNmbX <= 0:
            nNmbX = 1
        
        return self.__getBinData(ids=ids, nNmbX=nNmbX, TimeR=nTimeR, nCT=nCT/1000.0)


    def getDataAsJson(self, names, start, end, rate=1):
        return getDataSeries_as_Json(self.getDataAsNP(names, start, end, rate))

    # returns RawData for Client based Converters like Javascript
    def getDataRAW(self,filepath, names, start, end, rate=1):
        ids = []
        # find Mst with id as a number, id as MST name a str, id
        for name in names:
            id = None
            if isinstance(name, numbers.Number):
                id = self.getMst(id=name).id
            elif isinstance(name, str):
                id = self.getMst(name=name).id
            elif isinstance(name, TebisMST):
                id = name.id
            elif isinstance(name, TebisGroupElement):
                for member in name.members:
                    ids.append(member.mst.id)
            elif isinstance(name, TebisGroupMember):
                id = name.mst.id
            if id is not None:
                ids.append(id)
        if isinstance(start, datetime.datetime):
            start = start.timestamp()*1000.0
        elif isinstance(start, float):
            start = start*1000.0
        elif isinstance(start, int) and start < 100000000000: #and start > 100000000000 and start < 100000000000000:
            start = start*1000.0
        elif isinstance(start, str):
            start = datetime.datetime.strptime(start, '%Y-%m-%d %H:%M:%S.%f').timestamp()*1000.0
        if isinstance(end, datetime.datetime):
            end = end.timestamp()*1000.0
        elif isinstance(end, float):
            end = end*1000.0
        elif isinstance(end, int) and end < 100000000000: #and start > 100000000000 and start < 100000000000000:
            end = end*1000.0
        elif isinstance(end, str):
            end = datetime.datetime.strptime(end, '%Y-%m-%d %H:%M:%S.%f').timestamp()*1000.0

        nCT = rate*1000.0
        nTimeR = end
        nTimeR = (int(float(nTimeR)) / int(nCT)) * int(nCT)

        nTimeL = start
        nTimeL = (int(float(nTimeL)) / int(nCT)) * int(nCT)
        nNmbX = int(nTimeR - nTimeL) / int(nCT)
        if nNmbX <= 0:
            nNmbX = 1
        
        return self.getBinDataRAW(filepath,ids=ids, nNmbX=nNmbX, TimeR=nTimeR, nCT=nCT/1000.0)

    def getDataAsPD(self, names, start, end = None, rate=1):
        if isinstance(start, list) and all(isinstance(elem, list) for elem in start):
            df = None
            for tuple in start:
                if df is None:
                    df = pd.DataFrame(self.getDataAsNP(names, tuple[0], tuple[1], rate))            
                else:
                    df = df.append(pd.DataFrame(self.getDataAsNP(names, tuple[0], tuple[1], rate)))
            df = df.set_index(pd.DatetimeIndex(pd.to_datetime(df['timestamp'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('Europe/Berlin').dt.tz_localize(None)))
            df.drop(columns=['timestamp'], inplace=True)
            # df['timestamp'] = df.index
            df.sort_index(inplace=True)
        elif end is not None:
            df = pd.DataFrame(self.getDataAsNP(names, start, end, rate))
            df = df.set_index(pd.DatetimeIndex(pd.to_datetime(df['timestamp'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('Europe/Berlin').dt.tz_localize(None)))
            df.drop(columns=['timestamp'], inplace=True)
        # df['timestamp'] = df.index
        return df

    def getMapTreeGroupById(self, id):
        if self.config['useOracle'] is True:
            return self.tebisMapTreeGroupById.get(id)
        else:
            raise TebisOracleDBException(
                'no DbConnection specified - you need to specifiy a valid OracleDbConn in config')

    def getMst(self, id=None, name=None):
        if id is not None:
            return self.mstById.get(id)
        if name is not None:
            return self.mstByName.get(name)

    def getMsts(self, ids=None, names=None):
        retval = []
        if ids is not None:
            for id in ids:
                retval.append(self.mstById.get(id))
        if names is not None:
            for name in names:
                retval.append(self.mstByName.get(name))
        return retval

    def getTree(self):
        return self.tebisTree

    def getTreeAsJson(self):
        return json.dumps(self.tebisTree, cls=tebisTreeEncoder, separators=(',', ':'))

    def getGroupsByTreeId(self, id):
        return self.getGroupsByTreeId(int(id))

    def getGroupsByTreeIdAsJson(self, id):
        return json.dumps(self.getGroupsByTreeId(int(id)), cls=tebisTreeEncoder, separators=(',', ':'))

    def refreshMsts(self):
        self.loadReductions()  # We load the reductions to double check if a valid nCT is asked
        if self.config['useOracle'] is True:
            self.loadTree()
        else:
            self.loadMstsnVMstsFromSocket()
            self.loadGroupsFromSocket()

    def setupLiveValues(self):
        self.config['liveValues']['lastTimeOffsetCalculation'] = None
        self.config['liveValues']['timeOffset'] = None
        self.getCurrentTime()

    """
    berechnet den Offset anhand einer Messstelle idealerweise ist diese Messtelle nie nan
    Es wird ein größerer Zeitraum angefragt und die letzte Stelle die nicht nan ist als aktuelle Systemzeit angenommen
    """

    def calcTimeOffset(self):
        now = time.time()
        timeseries = self.__getBinData(
            ids=[self.config['liveValues']['offsetMstId']], nNmbX=120, TimeR=int(now), nCT=1)
        lastMeasuredTime = timeseries[~np.isnan(
            np.array(timeseries[timeseries.dtype.names[1]]))][-1][0]
        now = int(time.time())
        self.config['liveValues']['timeOffset'] = now - (lastMeasuredTime - 1)
        self.config['liveValues']['lastTimeOffsetCalculation'] = now
        None
    """
    Versucht den aktuellen SystemOffset zu bestimmen. Die TebisDaten sind teilweise leicht verzögert
    """

    def getCurrentTime(self):
        if self.config['liveValues']['timeOffset'] is None or self.config['liveValues']['lastTimeOffsetCalculation'] < (time.time() - self.config['liveValues']['recalcTimeOffsetEvery']):
            self.calcTimeOffset()
        return time.time() - int(self.config['liveValues']['timeOffset'])

    """
    Gitb den aktuellen Messwert der in msts genannten messtellen zurück
    msts= Array mit Messtellen
    """

    def readCurrentValue(self, msts, howmany = 1):
        ids = []
        for mst in msts:
            if mst.name is not None:
                ids.append(mst.id)
        test = time.time()
        timeseries = self.__getBinData(
            ids=ids, nNmbX=howmany, TimeR=int(self.getCurrentTime()), nCT=1)
        res = ''
        timestamp = timeseries['timestamp'][(howmany*-1):]
        for mst in msts:
            if mst.name is not None:
                mst.currentValues = None
                if howmany > 1:
                    mst.currentValues = timeseries[mst.name][(howmany*-1):]
                mst.currentValue = timeseries[mst.name][-1]
                mst.currenTime = timestamp
        None

# region Config Data
    def loadReductions(self):
        array = np.dtype([('ID', (np.int64)), ('Reduction', (np.int64))])
        data = self.getConfigData("RsRedCTs", array)
        self.reductions = np.floor_divide((data)['Reduction'], 1).tolist()
        None

    def checkIfReductionAvailable(self, reduction):
        if reduction in self.reductions:
            return reduction
        else:
            raise TebisException('Reduction not available')

# region Config Data from DB
    """
    lädt den gesamten Tree inkl. Gruppen und Messstellen
    #TODO: Events in der DB registrieren um bei Änderungen neu einzulesen
    """

    def loadTree(self):
        msts = []
        CONN_STR = '{user}/{psw}@{host}:{port}/{service}'.format(
            **self.config['OracleDbConn'])
        if self.config['OracleDbConn']['schema'] is None:
            SCHEMA = self.config['OracleDbConn']['user']
        else:
            SCHEMA = self.config['OracleDbConn']['schema']
        # for DB Access
        # you need an actual version of instaclient installed. For Tebis Communication  e.g. instantclient_18_3
        # see: https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html
        try:
            cx_Oracle = LazyLoader('cx_Oracle', globals(), 'cx_Oracle')
            conn = cx_Oracle.connect(
                CONN_STR, encoding='UTF-8', nencoding='UTF-8')
        except ModuleNotFoundError:
            raise TebisOracleDBException(
                'No Module for OracleDB found. Do "pip install cx_oracle" and install Oracle instant-client! (https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html)')
        cursor = conn.cursor()
        mstsQuery = cursor.execute(
            f'SELECT * FROM {SCHEMA}.TB_MSTS order by MSTINDEX', {}).fetchall()
        cursor.close()
        for mst in mstsQuery:
            msts.append(TebisRMST(mst))
        cursor = conn.cursor()
        vmstsQuery = cursor.execute(
            f'SELECT * FROM {SCHEMA}.TB_VMSTS order by MSTINDEX', {}).fetchall()
        cursor.close()
        for mst in vmstsQuery:
            msts.append(TebisVMST(mst))
        self.msts = msts
        self.mstByName = build_dict(self.msts, key="name")
        self.mstById = build_dict(self.msts, key="id")
        cursor = conn.cursor()
        treeQuery = cursor.execute(
            f'SELECT * FROM {SCHEMA}.TB_HI order by HIINDEX, HIPARENT, HIPOS', {}).fetchall()
        cursor.close()
        self.tebisTree = []
        self.tebisTree.append(TebisTreeElement(treeQuery[0]))
        for result in treeQuery[1:]:
            actElem = TebisTreeElement(result)
            self.tebisTree[0].findNodeByID(
                actElem.parent).childs.append(actElem)
        cursor = conn.cursor()
        groupQuery = cursor.execute(
            f'SELECT * FROM {SCHEMA}.TB_GRPS ORDER BY GRPINDEX', {}).fetchall()
        cursor.close()
        self.tebisGrps = []
        for group in groupQuery:
            self.tebisGrps.append(TebisGroupElement(group))
        cursor = conn.cursor()
        groupMembersQuery = cursor.execute(
            f'SELECT * FROM {SCHEMA}.TB_GRP_ELEMS ORDER BY GRPINDEX, GRPPOS', {}).fetchall()
        cursor.close()
        i = 0
        for grp in self.tebisGrps:
            for member in groupMembersQuery[i:]:
                member = TebisGroupMember(member)
                if grp.id == member.groupId:
                    i += 1
                    member.mst = self.getMst(id=member.mstID)
                    grp.members.append(member)
                else:
                    break
        self.tebisGrpsById = build_dict(self.tebisGrps, key="id")

        cursor = conn.cursor()
        groupQuery = cursor.execute(
            f'SELECT * FROM {SCHEMA}.TB_MAP_GRPS ORDER BY HIINDEX,HIPOS', {}).fetchall()
        cursor.close()
        self.tebisMapTreeGroups = []
        id = -1
        for group in groupQuery:
            if id != group[0]:
                id = group[0]
                treegroup = TebisMapTreeGroup(group)
                self.tebisMapTreeGroups.append(treegroup)
            treegroup.groups.append(self.tebisGrpsById.get(group[2]))

        self.tebisMapTreeGroupById = build_dict(
            self.tebisMapTreeGroups, key="treeId")
        None
        conn.close()

# endregion

# region Config Data from Socket

    # TODO: Add Data to Object
    def loadRsCtsNmbX(self):
        array = np.dtype(
            [('res', (np.int64)), ('NmbX', (np.int64))])
        data = self.getConfigData("RsCtsNmbX", array)

    # TODO: Groups to Object
    def loadGroupsFromSocket(self):
        array = np.dtype([('ID', (np.int64)), ('GrpName', np.unicode_, 100),
                          ('GroupDesc', np.unicode_, 100), ('Group1', np.unicode_, 100)])
        data = self.getConfigData("Grps", array)

    def loadMstsnVMstsFromSocket(self):
        self.msts = []
        msts = self.loadMstsFromSocket()
        for i in range(0, len(msts)):
            self.msts.append(TebisRMST().setValuesFromSocketInterface(msts[i]))
        vmsts = self.loadVmstsFromSocket()
        for i in range(0, len(vmsts)):
            self.msts.append(TebisVMST().setValuesFromSocketInterface(vmsts[i]))
        self.mstByName = build_dict(self.msts, key="name")
        self.mstById = build_dict(self.msts, key="id")

    def loadMstsFromSocket(self):
        array = np.dtype([('ID', (np.int64)), ('MSTName', np.unicode_, 100), ('UNIT', np.unicode_, 10), ('MSTDesc', np.unicode_, 255), (
            'Val1', (np.float32)), ('Val2', (np.float32)), ('Val3', (np.float32)), ('Val4', (np.float32)), ('Val5', (np.float32))])
        data = self.getConfigData("Msts", array)
        return data

    def loadVmstsFromSocket(self):
        array = np.dtype([('ID', (np.int64)), ('MSTName', np.unicode_, 100), ('UNIT', 'U10'), (
            'MSTDesc', np.unicode_, 255), ('Rate', (np.int64)), ('Formula', np.unicode_, 255), ('refresh', (np.int64))])
        data = self.getConfigData("VMsts", array)
        return data

    """
    lädt die Messstellen direkt über die SocketVerbindung
    hier wird kein DB Zugriff benötigt
    Der Tree und die Gruppen kommen hier allerdings nicht zurück
    """

    def getConfigData(self, type, npArray):
        strRequest = "<tebis>\n"
        strRequest += "<szConfigFile>" + \
            self.config['configfile'] + "</szConfigFile>\n"
        strRequest += "<szProcedure>GetConfig</szProcedure>\n"
        strRequest += "<szTebObjType>" + type + "</szTebObjType>\n"
        strRequest += "<tebis>"
        self.socketConnect()
        # Send Request
        self.sendOnSocket(strRequest)
        # Recieve MSTS Packet
        raw = self.receiveOnSocket()
        self.socketClose()
        f = StringIO(str(raw, encoding='iso-8859-1'))
        reader = csv.reader(f, delimiter=',', quotechar="'")
        rawSplit = []
        for row in reader:
            for item in row:
                rawSplit.append(item.replace("'", ""))
        return self.__checkResultHeader(rawSplit, npArray)

# endregion
# endregion

# region socketData to results

# region string result handling
    """
    der Result-Converter für nicht binäre Daten.
    Wird verwendet zum Einlesen der Mestellen und der virtuellen Messstellen, wenn dies nicht über die OracleDB erfolgt.
    Außerdem beim langsamen zeichenbasieten lesen der Messreihen
    """
    def __checkResultHeader(self, result, dtype):
        result = np.array(result)
        m_intPos = 0
        m_intNmbResultSet = int(result[m_intPos])
        m_intPos += 1
        m_intLengthResultSet = int(result[m_intPos])
        m_intPos += m_intNmbResultSet

        intMagic0 = int(result[m_intPos])
        m_intPos += 1
        intMagic1 = int(result[m_intPos])
        m_intPos += 1
        intMagic2 = int(result[m_intPos])
        m_intPos += 1
        intMagic3 = int(result[m_intPos])
        m_intPos += 1
        if (intMagic0 != -1 or intMagic1 != 463453 or intMagic2 != 756543 or intMagic3 != -1):
            return False
        intVersion = int(result[m_intPos])
        m_intPos += 1
        if intVersion != 3:
            return False
        m_intNmbCols = int(result[m_intPos])
        m_intPos += 1
        m_intNmbRows = int(result[m_intPos])
        m_intPos += 1
        if(m_intNmbCols < 0 or m_intNmbRows < 0):
            return False
        resultarr = np.empty(m_intNmbRows, dtype=dtype)
        find = np.nonzero(np.logical_or(result == 'i', result == 'd'))
        findindex = 0
        for x in range(0, m_intNmbCols):
            intColType = int(result[m_intPos])
            m_intPos += 1
            intColHasName = int(result[m_intPos])
            m_intPos += 1
            if intColHasName:
                None
            y = 0
            while y < m_intNmbRows:

                """Differentialfunktion
                //Bsp.: d,4,2,1
                /*
                d: 	Zeigt die Funktion an
                4:	Stacklänge
                2: 	Start
                1:	Differenz zwischen den einzelnen Stacks

                Daraus ergibt sich: [2,3,4,5]
                """
                if result[m_intPos] == "d":
                    findindex += 1
                    m_intPos += 1
                    intStackLen = np.int64(result[m_intPos])
                    m_intPos += 1
                    if np.issubdtype(resultarr[resultarr.dtype.names[x]].dtype, np.integer):
                        intStart = np.int64(result[m_intPos])
                        m_intPos += 1
                        intInc = np.int64(result[m_intPos])
                        m_intPos += 1
                    elif np.issubdtype(resultarr[resultarr.dtype.names[x]].dtype, np.floating):
                        intStart = np.float64(result[m_intPos])
                        m_intPos += 1
                        intInc = np.float64(result[m_intPos])
                        m_intPos += 1
                    resultarr[resultarr.dtype.names[x]][y:(y + intStackLen)] = np.linspace(
                        intStart, intStart + (intStackLen * intInc) - intInc, num=intStackLen)
                    y += intStackLen - 1
                elif result[m_intPos] == "i":
                    findindex += 1
                    m_intPos += 1
                    intStackLen = int(result[m_intPos])
                    m_intPos += 1
                    intValue = self.__getValue(
                        result[m_intPos], resultarr[resultarr.dtype.names[x]].dtype)
                    m_intPos += 1

                    resultarr[resultarr.dtype.names[x]
                              ][y:y + intStackLen] = intValue
                    y += intStackLen - 1
                else:
                    if len(find[0]) > findindex and not np.issubdtype(resultarr[resultarr.dtype.names[x]].dtype, np.dtype(str).type):
                        pos_to_next = find[0][findindex] - 1
                        endy = y + (pos_to_next - m_intPos)
                        if endy >= m_intNmbRows:
                            endy = m_intNmbRows - 1
                            pos_to_next = m_intPos + (endy - y)
                        resultarr[resultarr.dtype.names[x]][y:endy] = self.__getValue(
                            result[m_intPos:pos_to_next], resultarr[resultarr.dtype.names[x]].dtype)
                        m_intPos = pos_to_next + 1
                        y = endy
                    else:
                        resultarr[resultarr.dtype.names[x]][y] = self.__getValue(
                            result[m_intPos], resultarr[resultarr.dtype.names[x]].dtype)
                        m_intPos += 1
                y += 1

        intMagic0 = int(result[m_intPos])
        m_intPos += 1
        intMagic1 = int(result[m_intPos])
        m_intPos += 1
        intMagic2 = int(result[m_intPos])
        m_intPos += 1
        intMagic3 = int(result[m_intPos])
        m_intPos += 1
        if(intMagic0 != -1 or intMagic1 != 463453 or intMagic2 != 756543 or intMagic3 != -1):
            return False
        return resultarr
    
    def __getValue(self, value, var_dtype):
        if np.issubdtype(var_dtype, np.dtype(float).type):
            if isinstance(value, (np.ndarray)):
                try:
                    val = value
                    find = np.nonzero(value == '')
                    for res in find[0]:
                        value[res] = np.NaN
                    value = np.array(value, dtype=np.float64)
                except ValueError as e:
                    None
            else:
                if value == '':
                    return np.NaN
                value = float(value)
            try:
                return value
            except Exception:
                return None
        elif np.issubdtype(var_dtype, np.integer):
            return np.int64(value)
        elif np.issubdtype(var_dtype, np.dtype(str).type):
            return value
# endregion

# region binary result handling
    def __checkBinaryResultHeader(self, raw, dtype, resultarr=None, offset=0):
        m_intPos = 0
        m_intNmbResultSet = int(raw[m_intPos])
        m_intPos += 2
        nextComma = m_intPos + raw[m_intPos:].find(b',')
        m_intLengthResultSet = int(raw[m_intPos:nextComma])
        m_intPos = nextComma + 1
        intHeader = struct.unpack('>iiiiiiiii', raw[m_intPos:m_intPos + 36])
        m_intPos += 36
        intFooter = struct.unpack('>iiii', raw[-16:])
        if(intHeader[0] != -1 or intHeader[1] != 463453 or intHeader[2] != 756543 or intHeader[3] != -1 or intFooter[0] != -1 or intFooter[1] != 463453 or intFooter[2] != 756543 or intFooter[3] != -1):
            return False
        if intHeader[4] != 2:
            return False
        m_intNmbCols = intHeader[5]
        m_intNmbRows = intHeader[6]
        m_int1 = intHeader[7]
        m_int2 = intHeader[8]
        if m_int2 != -1:
            data = zlib.decompress(raw[m_intPos:-16])
            datalen = len(data)
        else:
            data = raw[m_intPos:-16]
        if(m_intNmbCols < 0 or m_intNmbRows < 0):
            return False
        m_intPos = 0
        if resultarr is None:
            resultarr = np.empty(m_intNmbRows, dtype=dtype)
        for x in range(0 + offset, m_intNmbCols + offset):
            column_name = resultarr.dtype.names[x]
            col = struct.unpack('>hh', data[m_intPos:m_intPos + 4])
            m_intPos += 4
            intZero = col[0]  # ?
            intColType = col[1]  # ?  301 == Timestamp?  | 8 = WertSpalte
            precount = 0
            segments = []
            while precount < m_intNmbRows:  # Schauen ob die Spalte in mehrere Blöcke aufgeteilt ist
                Length = struct.unpack('>B', data[m_intPos:m_intPos + 1])[0]
                m_intPos += 1
                # Die Anzahl ist größer als ein Byte dann kommt die Anzahl als Int (4Byte)
                if Length == 255:
                    m_intLength = struct.unpack(
                        '>I', data[m_intPos:m_intPos + 4])[0]
                    m_intPos += 4
                else:
                    m_intLength = Length
                m_isNAN = struct.unpack('>B', data[m_intPos:m_intPos + 1])[0]
                m_intPos += 1
                if m_isNAN == 0:
                    segments.append([precount, m_intLength])
                elif m_isNAN == 255:
                    resultarr[column_name][precount:precount +
                                           m_intLength] = np.nan
                else:
                    None
                precount += m_intLength
            # Die Breite des Datentyps in Bytes
            m_intByteCount = struct.unpack(
                '>B', data[m_intPos:m_intPos + 1])[0]
            m_intPos += 1
            # Die Funktion 111 = Ein Wert in allen Zeilen |
            m_intFunction = struct.unpack('>B', data[m_intPos:m_intPos + 1])[0]
            m_intPos += 1
            if intColType == 301:  # TimeStamp Col
                for segment in segments:
                    y = segment[0]
                    m_intLength = segment[1]
                    value = struct.unpack('>qq', data[m_intPos:m_intPos + 16])
                    m_intStepSize = value[1]
                    if offset != 0:
                        x -= 1
                    else:
                        resultarr[column_name][y:y + m_intLength] = np.linspace(value[0], value[0] + (
                            m_intLength * m_intStepSize) - m_intStepSize, num=m_intLength)
                    m_intPos += 16
            elif intColType == 8:  # Wert Col
                if m_intFunction == 109:  # ?  alle Werte nan?
                    for segment in segments:
                        y = segment[0]
                        m_intLength = segment[1]
                        if m_isNAN == 255:
                            resultarr[column_name][y:y + m_intLength] = np.nan
                        else:
                            None
                elif m_intFunction == 110:  # alle Werte sind unterschiedlich
                    for segment in segments:
                        y = segment[0]
                        m_intLength = segment[1]
                        values = self.__getValueFromBinArray(
                            data, m_intPos, m_intByteCount, arraycount=m_intLength)
                        resultarr[column_name][y:y + m_intLength] = values[0]
                        m_intPos = int(values[1])
                elif m_intFunction == 111:  # Alle Werte gleich
                    value = self.__getValueFromBin(
                        data, m_intPos, m_intByteCount)
                    m_intPos = int(value[1])
                    step = self.__getValueFromBin(
                        data, m_intPos, m_intByteCount)
                    m_intPos = int(step[1])
                    for segment in segments:
                        y = segment[0]
                        m_intLength = segment[1]
                        resultarr[column_name][y:y + m_intLength] = np.linspace(
                            value[0], value[0] + (m_intLength * step[0]) - step[0], num=m_intLength)
                    None
                elif m_intFunction == 112:  # Gruppen gleicher Werte
                    m_intGroupCount = 0
                    for segment in segments:
                        y = segment[0]
                        m_intSegmentLength = segment[1]
                        valcount = 0
                        while valcount < m_intSegmentLength:
                            if m_intGroupCount == 0:
                                m_intGroupCount = struct.unpack(
                                    '>B', data[m_intPos:m_intPos + 1])[0]
                                m_intPos += 1
                                if m_intGroupCount == 0:
                                    break
                                value = self.__getValueFromBin(
                                    data, m_intPos, m_intByteCount)
                                m_intPos = int(value[1])
                            length = m_intGroupCount
                            if valcount + length > m_intSegmentLength:
                                length = m_intSegmentLength - valcount
                            m_intGroupCount -= length

                            resultarr[column_name][y + valcount:y +
                                                   valcount + length] = value[0]
                            valcount += length
                else:
                    None
            None
        None
        return resultarr

    def __getValueFromBin(self, data, pos, bytecount, type=None):
        result = [0.0, pos]
        if bytecount == 8:
            result[0] = struct.unpack('>d', data[pos:pos + bytecount])[0]
            result[1] += bytecount
        elif bytecount == 4:
            result[0] = struct.unpack('>i', data[pos:pos + (bytecount)])[0]
            result[1] += bytecount
        elif bytecount == 2:
            result[0] = struct.unpack('>h', data[pos:pos + (bytecount)])[0]
            result[1] += bytecount
        elif bytecount == 1:
            result[0] = struct.unpack('>b', data[pos:pos + (bytecount)])[0]
            result[1] += bytecount
        return result

    def __getValueFromBinArray(self, data, pos, bytecount, arraycount=1, type=None):
        result = [0.0, pos]
        if bytecount == 8:
            result[0] = struct.unpack(
                f'>{arraycount}d', data[pos:pos + (bytecount * arraycount)])
            result[1] += bytecount * arraycount
        elif bytecount == 4:
            result[0] = struct.unpack(
                f'>{arraycount}i', data[pos:pos + (bytecount * arraycount)])
            result[1] += bytecount * arraycount
        elif bytecount == 2:
            result[0] = struct.unpack(
                f'>{arraycount}h', data[pos:pos + (bytecount * arraycount)])
            result[1] += bytecount * arraycount
        elif bytecount == 1:
            result[0] = struct.unpack(
                f'>{arraycount}b', data[pos:pos + (bytecount * arraycount)])
            result[1] += bytecount * arraycount
        return result

# endregion

# endregion

# region Socket handling

    def socketConnect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.config['host'], self.config['port']))
        logging.debug(f"Connect to Tebis-Socket {self.config['host']}:{self.config['port']}")

    def socketClose(self):
        self.sock.shutdown(1)
        self.sock.close()

    def sendOnSocket(self, msg):
        totalsent = 0
        while totalsent < len(msg):
            sent = self.sock.send((msg[totalsent:]).encode('latin-1'))
            if sent == 0:
                raise RuntimeError("socket connection broken")
            totalsent = totalsent + sent

    def receiveOnSocket(self):
        chunks = []
        bytes_recd = 0
        header = self.sock.recv(16)
        if header == '':
            raise RuntimeError("socket connection broken")
        header = header.rstrip(b'\x00').split(b' ')
        version = int(header[0])
        error = int(header[1])
        size = int(header[2])
        if error == 1:
            raise TebisException
        while bytes_recd < size:
            chunk = self.sock.recv(min(size - bytes_recd, 4096))
            if chunk == '':
                raise RuntimeError("socket connection broken")
            chunks.append(chunk)
            bytes_recd = bytes_recd + len(chunk)
        return b''.join(chunks)

# endregion
    def getBinDataRAW(self, filepath, ids=None, nCT=1, nNmbX=1, TimeR=time.time()):
        
        start = round(time.time() * 1000)
        #TimeR = TimeR * 1000.0
        nCT = int(nCT*1000.0)
        nCT = self.checkIfReductionAvailable(nCT)
        if TimeR > start:
            dif = int(TimeR - start) / int(nCT)
            TimeR = int(start)
            nNmbX = int(nNmbX - dif)
        timeR_new = int(int(int(int(TimeR) / int(nCT)) * int(nCT)))
        dif = int(int(int(TimeR) - timeR_new) / int(nCT))
        nNmbX = int(nNmbX - dif)
        n = 50
        data = None
        types = [('timestamp', (np.int64))]
        for id in ids:
            mst = self.getMst(id=id)
            types.append((str(mst.name), (np.float32)))
        x = [ids[i:i + n] for i in range(0, len(ids), n)]
        offset = 0
        
        for ids in x:
            rawdata = bytearray()
            arrMsts = ""
            rawdata.extend(len(ids).to_bytes(8,'big'))
            for id in ids:
                arrMsts += str(id) + ', '
                rawdata.extend(id.item().to_bytes(8,'big'))
            arrMsts = arrMsts[:-2]
            #print(arrMsts)
            if nNmbX > 0:
                strRequest = "<tebis>\n"
                strRequest += "<szConfigFile>" + \
                    self.config['configfile'] + "</szConfigFile>\n"
                strRequest += "<szProcedure>LoadData</szProcedure>\n"
                strRequest += "<arrMsts>" + arrMsts + "</arrMsts>\n"
                strRequest += "<nNmbX>" + str(nNmbX) + "</nNmbX>\n"
                strRequest += "<nCT>" + str(int(nCT)) + "</nCT>\n"
                strRequest += "<nTimeR>" + \
                    str(timeR_new) + "</nTimeR>\n"
                strRequest += "<tebis>"
                try:
                    self.socketConnect()
                    # Send Request
                    self.sendOnSocket(strRequest)
                    # Recieve MSTS Packet
                    MSTSRaw = self.receiveOnSocket()
                except TebisException:
                    self.socketConnect()
                    # Send Request
                    self.sendOnSocket(strRequest)
                    # Recieve MSTS Packet
                    MSTSRaw = self.receiveOnSocket()  
                rawdata.extend(len(MSTSRaw).to_bytes(8,'big'))
                rawdata.extend(MSTSRaw)
                #print(len(ids)) 
                #print(MSTSRaw)
                #data = self.__checkBinaryResultHeader(
                #    MSTSRaw, types, data, offset)
                #offset += len(ids)
            #data['timestamp'] = data['timestamp'] / 1000.0
            with open(filepath, 'ab') as fpout:
                fpout.write(rawdata)
            #print(os.path.getsize(filepath))
        return rawdata
    
    """
    schnelles Lesen von Messreihen
    ids= Array mit den Messtellen-Namen
    nCT = Auflösung der Messwerte
    nNmbX= Anzahl der Messpunkt rückwärts ab TimeR
    TimeR= Unixtimestamp rechte Seite der Daten
    """

    def __getBinData(self, ids=None, nCT=1, nNmbX=1, TimeR=time.time()):
        start = round(time.time() * 1000)
        #TimeR = TimeR * 1000.0
        nCT = int(nCT*1000.0)
        nCT = self.checkIfReductionAvailable(nCT)
        if TimeR > start:
            dif = int(TimeR - start) / int(nCT)
            TimeR = int(start)
            nNmbX = int(nNmbX - dif)
        timeR_new = int(int(int(int(TimeR) / int(nCT)) * int(nCT)))
        dif = int(int(int(TimeR) - timeR_new) / int(nCT))
        nNmbX = int(nNmbX - dif)
        n = 100
        data = None
        types = [('timestamp', (np.int64))]
        for id in ids:
            mst = self.getMst(id=id)
            types.append((str(mst.name), (np.float32)))
        x = [ids[i:i + n] for i in range(0, len(ids), n)]
        offset = 0
        for ids in x:
            arrMsts = ""
            for id in ids:
                arrMsts += str(id) + ', '
            arrMsts = arrMsts[:-2]
            if nNmbX > 0:
                strRequest = "<tebis>\n"
                strRequest += "<szConfigFile>" + \
                    self.config['configfile'] + "</szConfigFile>\n"
                strRequest += "<szProcedure>LoadData</szProcedure>\n"
                strRequest += "<arrMsts>" + arrMsts + "</arrMsts>\n"
                strRequest += "<nNmbX>" + str(nNmbX) + "</nNmbX>\n"
                strRequest += "<nCT>" + str(int(nCT)) + "</nCT>\n"
                strRequest += "<nTimeR>" + \
                    str(timeR_new) + "</nTimeR>\n"
                strRequest += "<tebis>"
                try:
                    self.socketConnect()
                    # Send Request
                    self.sendOnSocket(strRequest)
                    # Recieve MSTS Packet
                    MSTSRaw = self.receiveOnSocket()
                except TebisException:
                    self.socketConnect()
                    # Send Request
                    self.sendOnSocket(strRequest)
                    # Recieve MSTS Packet
                    MSTSRaw = self.receiveOnSocket()
                data = self.__checkBinaryResultHeader(
                    MSTSRaw, types, data, offset)
                offset += len(ids)
            #data['timestamp'] = data['timestamp'] / 1000.0
        return data

    """
    lädt die Daten als Zeichenkette
    Die Funktion ist wesentlich langsamer als getBinData und sollte nicht verwendet werden...
    """

    def __getData(self, ids=None, nCT=1, nNmbX=1, TimeR=time.time()):
        start = time.time()
        types = [('timestamp', (np.int64))]
        arrMsts = ""
        for id in ids:
            types.append((str(id), (np.float32)))
            arrMsts += str(id) + ', '
        arrMsts = arrMsts[:-2]
        strRequest = "<tebis>\n"
        strRequest += "<szConfigFile>" + \
            self.config['configfile'] + "</szConfigFile>\n"
        strRequest += "<szProcedure>JLoadData</szProcedure>\n"
        strRequest += "<arrMsts>" + arrMsts + "</arrMsts>\n"
        strRequest += "<nNmbX>" + str(nNmbX) + "</nNmbX>\n"
        strRequest += "<nCT>" + str(nCT * 1000) + "</nCT>\n"
        strRequest += "<nTimeR>" + \
            str((int(TimeR) / int(nCT)) * int(nCT) * 1000) + "</nTimeR>\n"
        strRequest += "<tebis>"
        self.socketConnect()
        # Send Request
        self.sendOnSocket(strRequest)
        # Recieve MSTS Packet
        MSTSRaw = self.receiveOnSocket()
        self.socketClose()
        MSTSRawSplit = str(
            MSTSRaw, encoding='iso-8859-1').replace("'", "").split(',')
        temp = self.__checkResultHeader(MSTSRawSplit, types)
        return temp


class TebisMST:
    def __init__(self, id, name, unit=None, desc=None):
        self.id = id
        self.name = name
        self.unit = unit
        self.desc = desc
        self.currentValue = None


class TebisRMST(TebisMST):
    def __init__(self, elem=None):
        if elem is not None:
            self.mode = elem[4]
            self.elunit = elem[5]
            self.elFrom = elem[6]
            self.elTo = elem[7]
            self.phyFrom = elem[8]
            self.phyTo = elem[9]
            TebisMST.__init__(
                self, id=elem[0], name=elem[1], unit=elem[2], desc=elem[3])

    def setValuesFromSocketInterface(self, elem):
        id = elem[0]
        name = elem[1]
        unit = testUnicodeError(elem, 2)
        desc = testUnicodeError(elem, 3)
        TebisMST.__init__(self, id=id, name=name, unit=unit, desc=desc)
        return self


class TebisVMST(TebisMST):
    def __init__(self, elem=None):
        if elem is not None:
            self.reduction = elem[4]
            self.formula = elem[5]
            self.recalc = elem[6]
            TebisMST.__init__(
                self, id=elem[0], name=elem[1], unit=elem[2], desc=elem[3])

    def setValuesFromSocketInterface(self, elem):
        id = elem[0]
        name = elem[1]
        unit = testUnicodeError(elem, 2)
        desc = testUnicodeError(elem, 3)
        self.reduction = elem[4]
        self.formula = elem[5]
        self.recalc = elem[6]
        TebisMST.__init__(self, id=id, name=name, unit=unit, desc=desc)
        return self


class TebisMapTreeGroup:
    def __init__(self, elem):
        self.treeId = elem[0]
        self.groups = []


class TebisGroupMember:
    def __init__(self, elem):
        self.groupId = elem[0]
        self.pos = elem[1]
        self.mstID = elem[2]
        self.grpFrom = elem[3]
        self.grpTo = elem[4]
        self.grpColor = elem[5]
        self.grpWidth = elem[6]
        self.grpVisiblw = elem[7]
        self.grpMode = elem[8]
        self.grpScale = elem[9]


class TebisGroupElement:
    def __init__(self, elem):
        self.id = elem[0]
        self.members = []
        self.name = elem[1]
        self.desc = elem[2]


class TebisTreeElement:
    def __init__(self, elem):
        self.id = elem[0]
        self.childs = []
        self.grps = []
        self.parent = elem[1]
        self.order = elem[2]
        self.name = elem[3]

    def findNodeByID(self, x):
        if self.id == x:
            return self
        for node in self.childs:
            n = node.findNodeByID(x)
            if n:
                return n
        return None


class TebisOracleDBException(Exception):
    ''' raise if try to get DB-Information without a DB Connection specifeied '''


class TebisException(Exception):
    pass


# BUG: UnicodeDecodeError on Numpy...
def testUnicodeError(elem, id):
    res = ''
    try:
        res = elem[id]
    except UnicodeDecodeError:
        None
    return res


# SUPPORT for DB Query
def build_dict(seq, key):
    return dict((getattr(d, key), d) for (index, d) in enumerate(seq))


# Json Converter
# TODO: die FLOAT_REPR geht in Python >3.6 nicht mehr. Siehe https://stackoverflow.com/questions/32521823/json-encoder-float-repr-changed-but-no-effect
def getDataSeries_as_Json(data):
    dic = {}
    for name in data.dtype.names:
        dic[name] = data[name].tolist()
    simplejson.encoder.FLOAT_REPR = lambda o: format(o, '.3f')
    j = simplejson.dumps(dic, ignore_nan=True)
    return j


class tebisTreeEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, TebisTreeElement):
            return {"id": obj.id, "text": obj.name, "nodes": obj.childs}
        if isinstance(obj, TebisMapTreeGroup):
            return {"treeId": obj.treeId, "groups": obj.groups}
        if isinstance(obj, TebisGroupElement):
            return {"id": obj.id, "name": obj.name, "desc": obj.desc, "members": obj.members}
        if isinstance(obj, TebisGroupMember):
            return {"id": obj.groupId, "name": obj.mst.name, "desc": obj.mst.desc, "unit": obj.mst.unit}
        return json.JSONEncoder.default(self, obj)


# config helper
def selective_merge(base_obj, delta_obj):
    if not isinstance(base_obj, dict):
        return delta_obj
    common_keys = set(base_obj).intersection(delta_obj)
    new_keys = set(delta_obj).difference(common_keys)
    for k in common_keys:
        base_obj[k] = selective_merge(base_obj[k], delta_obj[k])
    for k in new_keys:
        base_obj[k] = delta_obj[k]
    return base_obj
