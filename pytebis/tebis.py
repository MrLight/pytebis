import time
import socket
import struct
import zlib
import numpy as np
import pandas as pd
import json
from json import JSONEncoder
import simplejson
from pytebis.lazyloader import LazyLoader


class Tebis():
    '''Tebis Communication class
    '''
    def __init__(self, configfile=None, sock=None, host=None, port=None, dbConn=None, configuration=None):
        default_conf = {
            'host': None,
            'port': 4712,
            'configfile': 'd:/tebis/Anlage/Config.txt',
            'useOracle': None,
            'OracleDbConn': {
                'host': None,
                'port': 1521,
                'user': None,
                'psw': None,
                'service': 'XE'
            }
        }
        self.config = selective_merge(default_conf, configuration)
        if self.config['OracleDbConn']['host'] is not None and (self.config['useOracle'] is True or self.config['useOracle'] is None):
            if self.config['useOracle'] is None:
                self.config['useOracle'] = True
        if configfile is not None:
            self.config['configfile'] = configfile
        if host is not None:
            self.config['host'] = host
        if port is not None:
            self.config['port'] = port
        if self.config['useOracle'] is True:
            self.loadTree()
        else:
            self.loadMSTS()

    def getDataAsNP(self, names, start, end, rate=1):
        ids = []
        for name in names:
            id = self.getMst(name=name).id
            if id is not None:
                ids.append(id)
        nCT = rate
        nTimeR = end
        nTimeR = (int(float(nTimeR)) / int(nCT)) * int(nCT)

        nTimeL = start
        nTimeL = (int(float(nTimeL)) / int(nCT)) * int(nCT)
        nNmbX = int(nTimeR - nTimeL) / int(nCT)
        if nNmbX <= 0:
            nNmbX = 1
        return self.getBinData(ids=ids, nNmbX=nNmbX, TimeR=nTimeR, nCT=nCT)

    # TODO: implement return RawData for Client based Converters like Javascript
    def getRawData(self, names, start, end, rate=1):
        return None

    def getDataAsJson(self, names, start, end, rate=1):
        return getDataSeries_as_Json(self.getDataAsNP(names, start, end, rate))

    def getDataAsPD(self, names, start, end, rate=1):
        df = pd.DataFrame(self.getDataAsNP(names, start, end, rate))
        # df = df.set_index('timestamp')
        # df['timestamp'] = df.index
        return df

    def getMapTreeGroupById(self, id):
        if self.config['useOracle'] is True:
            return self.tebisMapTreeGroupById.get(id)
        else:
            raise TebisOracleDBException('no DbConnection specified - you need to specifiy a valid OracleDbConn in config')

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

    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.config['host'], self.config['port']))

    def close(self):
        self.sock.shutdown(1)
        self.sock.close()

    def send_on_socket(self, msg):
        totalsent = 0
        while totalsent < len(msg):
            sent = self.sock.send((msg[totalsent:]).encode('latin-1'))
            if sent == 0:
                raise RuntimeError("socket connection broken")
            totalsent = totalsent + sent

    def receive_on_socket(self):
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
            raise TebisReceiveException
        while bytes_recd < size:
            chunk = self.sock.recv(min(size - bytes_recd, 4096))
            if chunk == '':
                raise RuntimeError("socket connection broken")
            chunks.append(chunk)
            bytes_recd = bytes_recd + len(chunk)
        return b''.join(chunks)

    def getValue(self, value, var_dtype):
        if np.issubdtype(var_dtype, np.dtype(float).type):
            if isinstance(value, (np.ndarray)):
                try:
                    val = value
                    find = np.nonzero(value == '')
                    for res in find[0]:
                        value[res] = np.NaN
                    value = np.array(value, dtype=np.float)
                except ValueError as e:
                    print(e)
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

    def __getValueFromBin(self, data, pos, bytecount, type=None):
        result = [0.0, pos]
        if (bytecount == 8):
            result[0] = struct.unpack('>d', data[pos:pos + bytecount])[0]
            result[1] += bytecount
        elif (bytecount == 4):
            result[0] = struct.unpack('>i', data[pos:pos + (bytecount)])[0]
            result[1] += bytecount
        elif (bytecount == 2):
            result[0] = struct.unpack('>h', data[pos:pos + (bytecount)])[0]
            result[1] += bytecount
        elif (bytecount == 1):
            result[0] = struct.unpack('>b', data[pos:pos + (bytecount)])[0]
            result[1] += bytecount
        return result

    def __getValueFromBinArray(self, data, pos, bytecount, arraycount=1, type=None):
        result = [0.0, pos]
        if (bytecount == 8):
            result[0] = struct.unpack(f'>{arraycount}d', data[pos:pos + (bytecount * arraycount)])
            result[1] += bytecount * arraycount
        elif (bytecount == 4):
            result[0] = struct.unpack(f'>{arraycount}i', data[pos:pos + (bytecount * arraycount)])
            result[1] += bytecount * arraycount
        elif (bytecount == 2):
            result[0] = struct.unpack(f'>{arraycount}h', data[pos:pos + (bytecount * arraycount)])
            result[1] += bytecount * arraycount
        elif (bytecount == 1):
            result[0] = struct.unpack(f'>{arraycount}b', data[pos:pos + (bytecount * arraycount)])
            result[1] += bytecount * arraycount
        return result

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
        if(intHeader[4] != 2):
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
                if Length == 255:  # Die Anzahl ist größer als ein Byte dann kommt die Anzahl als Int (4Byte)
                    m_intLength = struct.unpack('>I', data[m_intPos:m_intPos + 4])[0]
                    m_intPos += 4
                else:
                    m_intLength = Length
                m_isNAN = struct.unpack('>B', data[m_intPos:m_intPos + 1])[0]
                m_intPos += 1
                if m_isNAN == 0:
                    segments.append([precount, m_intLength])
                elif m_isNAN == 255:
                    resultarr[column_name][precount:precount + m_intLength] = np.nan
                else:
                    None
                precount += m_intLength
            m_intByteCount = struct.unpack('>B', data[m_intPos:m_intPos + 1])[0]  # Die Breite des Datentyps in Bytes
            m_intPos += 1
            m_intFunction = struct.unpack('>B', data[m_intPos:m_intPos + 1])[0]  # Die Funktion 111 = Ein Wert in allen Zeilen |
            m_intPos += 1
            if (intColType == 301):  # TimeStamp Col
                for segment in segments:
                    y = segment[0]
                    m_intLength = segment[1]
                    value = struct.unpack('>qq', data[m_intPos:m_intPos + 16])
                    m_intStepSize = value[1]
                    if offset != 0:
                        x -= 1
                    else:
                        resultarr[column_name][y:y + m_intLength] = np.linspace(value[0], value[0] + (m_intLength * m_intStepSize) - m_intStepSize, num=m_intLength)
                    m_intPos += 16
            elif (intColType == 8):  # Wert Col
                if (m_intFunction == 109):  # ?  alle Werte nan?
                    for segment in segments:
                        y = segment[0]
                        m_intLength = segment[1]
                        if m_isNAN == 255:
                            resultarr[column_name][y:y + m_intLength] = np.nan
                        else:
                            None
                elif (m_intFunction == 110):  # alle Werte sind unterschiedlich
                    for segment in segments:
                        y = segment[0]
                        m_intLength = segment[1]
                        values = self.__getValueFromBinArray(data, m_intPos, m_intByteCount, arraycount=m_intLength)
                        resultarr[column_name][y:y + m_intLength] = values[0]
                        m_intPos = int(values[1])
                elif (m_intFunction == 111):  # Alle Werte gleich
                    value = self.__getValueFromBin(data, m_intPos, m_intByteCount)
                    m_intPos = int(value[1])
                    step = self.__getValueFromBin(data, m_intPos, m_intByteCount)
                    m_intPos = int(step[1])
                    for segment in segments:
                        y = segment[0]
                        m_intLength = segment[1]
                        resultarr[column_name][y:y + m_intLength] = np.linspace(value[0], value[0] + (m_intLength * step[0]) - step[0], num=m_intLength)
                    None
                elif (m_intFunction == 112):  # Gruppen gleicher Werte
                    m_intGroupCount = 0
                    for segment in segments:
                        y = segment[0]
                        m_intSegmentLength = segment[1]
                        valcount = 0
                        while valcount < m_intSegmentLength:
                            if m_intGroupCount == 0:
                                m_intGroupCount = struct.unpack('>B', data[m_intPos:m_intPos + 1])[0]
                                m_intPos += 1
                                if m_intGroupCount == 0:
                                    break
                                value = self.__getValueFromBin(data, m_intPos, m_intByteCount)
                                m_intPos = int(value[1])
                            length = m_intGroupCount
                            if valcount + length > m_intSegmentLength:
                                length = m_intSegmentLength - valcount
                            m_intGroupCount -= length

                            resultarr[column_name][y + valcount:y + valcount + length] = value[0]
                            valcount += length
                else:
                    print(m_intFunction)
                    None
            None
        None
        return resultarr

    """
    berechnet den Offset anhand einer Messstelle idealerweise ist diese Messtelle nie nan
    Es wird ein größerer Zeitraum angefragt und die letzte Stelle die nicht nan ist als aktuelle Systemzeit angenommen
    """
    def calcTimeOffset(self):
        now = time.time()
        timeseries = self.getBinData(ids=[100025], nNmbX=60, TimeR=int(now), nCT=1)
        lastMeasuredTime = timeseries[~np.isnan(np.array(timeseries[timeseries.dtype.names[1]]))][-1][0]
        self.timeOffset = now - lastMeasuredTime
        print(self.timeOffset)
        None
    """
    Versucht den aktuellen SystemOffset zu bestimmen. Die TebisDaten sind teilweise leicht verzögert
    """
    def getCurrentTime(self):
        if self.timeOffset is None:
            self.calcTimeOffset()
        return time.time() - self.timeOffset - 2
    
    """
    Gitb den aktuellen Messwert der in msts genannten messtellen zurück
    msts= Array mit Messtellen
    """
    def readCurrentValue(self, msts):
        ids = []
        for mst in msts:
            if (mst.name is not None):
                ids.append(mst.id)
        timeseries = self.getBinData(ids=ids, nNmbX=1, TimeR=int(self.getCurrentTime()), nCT=1)
        res = ''
        timestamp = timeseries['timestamp'][-1]
        for mst in msts:
            if mst.name is not None:
                mst.currentValue = timeseries[mst.name][-1]
                mst.currenTime = timestamp
                if len(res) < 200:
                    res += f' - {mst.currentValue:.2f}'
        print(f'{time.time()-msts[0].currenTime:.2f} {msts[0].currenTime} {res}')
        None

    """
    lädt den gesamten Tree inkl. Gruppen und Messstellen
    #TODO: Events in der DB registrieren um bei Änderungen neu einzulesen
    """    
    def loadTree(self):
        msts = []
        CONN_STR = '{user}/{psw}@{host}:{port}/{service}'.format(**self.config['OracleDbConn'])
        # for DB Access
        # you need an actual version of instaclient installed. For Tebis Communication  e.g. instantclient_18_3
        # see: https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html
        try:
            cx_Oracle = LazyLoader('cx_Oracle', globals(), 'cx_Oracle')
            conn = cx_Oracle.connect(CONN_STR, encoding='UTF-8', nencoding='UTF-8')
        except ModuleNotFoundError:
            raise TebisOracleDBException('No Module for OracleDB found. Do "pip install cx_oracle" and install Oracle instant-client! (https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html)')
        cursor = conn.cursor()
        mstsQuery = cursor.execute('SELECT * FROM TB_TWO.TB_MSTS order by MSTINDEX', {}).fetchall()
        cursor.close()
        for mst in mstsQuery:
            msts.append(_TebisRMST(mst))
        cursor = conn.cursor()
        vmstsQuery = cursor.execute('SELECT * FROM TB_TWO.TB_VMSTS order by MSTINDEX', {}).fetchall()
        cursor.close()
        for mst in vmstsQuery:
            msts.append(_TebisVMST(mst))
        self.msts = msts
        self.mstByName = build_dict(self.msts, key="name")
        self.mstById = build_dict(self.msts, key="id")
        cursor = conn.cursor()
        treeQuery = cursor.execute('SELECT * FROM TB_TWO.TB_HI order by HIINDEX, HIPARENT, HIPOS', {}).fetchall()
        cursor.close()
        self.tebisTree = []
        self.tebisTree.append(_TebisTreeElement(treeQuery[0]))
        for result in treeQuery[1:]:
            actElem = _TebisTreeElement(result)
            self.tebisTree[0].findNodeByID(actElem.parent).childs.append(actElem)
        cursor = conn.cursor()
        groupQuery = cursor.execute('SELECT * FROM TB_TWO.TB_GRPS ORDER BY GRPINDEX', {}).fetchall()
        cursor.close()
        self.tebisGrps = []
        for group in groupQuery:
            self.tebisGrps.append(_TebisGroupElement(group))
        cursor = conn.cursor()
        groupMembersQuery = cursor.execute('SELECT * FROM TB_TWO.TB_GRP_ELEMS ORDER BY GRPINDEX, GRPPOS', {}).fetchall()
        cursor.close()
        i = 0
        for grp in self.tebisGrps:
            for member in groupMembersQuery[i:]:
                member = _TebisGroupMember(member)
                if grp.id == member.groupId:
                    i += 1
                    member.mst = self.getMst(id=member.mstID)
                    grp.members.append(member)
                else:
                    break
        self.tebisGrpsById = build_dict(self.tebisGrps, key="id")

        cursor = conn.cursor()
        groupQuery = cursor.execute('SELECT * FROM TB_TWO.TB_MAP_GRPS ORDER BY HIINDEX,HIPOS', {}).fetchall()
        cursor.close()
        self.tebisMapTreeGroups = []
        id = -1
        for group in groupQuery:
            if (id != group[0]):
                id = group[0]
                treegroup = _TebisMapTreeGroup(group)
                self.tebisMapTreeGroups.append(treegroup)
            treegroup.groups.append(self.tebisGrpsById.get(group[2]))

        self.tebisMapTreeGroupById = build_dict(self.tebisMapTreeGroups, key="treeId")
        None
        conn.close()
        
    
    """
    lädt die Messstellen direkt über die SocketVerbindung
    hier wird kein DB Zugriff benötigt
    Der Tree und die Gruppen kommen hier allerdings nicht zurück
    """
    def loadMSTS(self):
        strRequest = "<tebis>\n"
        strRequest += "<szConfigFile>" + self.config['configfile'] + "</szConfigFile>\n"
        strRequest += "<szProcedure>GetConfig</szProcedure>\n"
        strRequest += "<szTebObjType>Msts</szTebObjType>\n"
        strRequest += "<tebis>"
        self.connect()
        # Send Request
        self.send_on_socket(strRequest)
        # Recieve MSTS Packet
        MSTSRaw = self.receive_on_socket()
        self.close()
        msts = []
        MSTSRawSplit = str(MSTSRaw, encoding='iso-8859-1').replace("'", "").split(',')
        dtMSTS = np.dtype([('ID', (np.int64)), ('MSTName', np.unicode_, 100), ('UNIT', np.unicode_, 10), ('MSTDesc', np.unicode_, 255), ('Val1', (np.float32)), ('Val2', (np.float32)), ('Val3', (np.float32)), ('Val4', (np.float32)), ('Val5', (np.float32))])
        MSTS = self.__checkResultHeader(MSTSRawSplit, dtMSTS)
        for i in range(0, len(MSTS)):
            msts.append(_TebisVMST().setValuesFromSocketInterface(MSTS[i])) 
        strRequest = "<tebis>\n"
        strRequest += "<szConfigFile>" + self.config['configfile'] + "</szConfigFile>\n"
        strRequest += "<szProcedure>GetConfig</szProcedure>\n"
        strRequest += "<szTebObjType>VMsts</szTebObjType>\n"
        strRequest += "<tebis>"
        self.connect()
        # Send Request
        self.send_on_socket(strRequest)
        # Recieve MSTS Packet
        VMSTSRaw = self.receive_on_socket()
        self.close()
        VMSTSRawSplit = str(VMSTSRaw, encoding='iso-8859-1').replace("'", "").split(',')
        dtVMSTS = np.dtype([('ID', (np.int64)), ('MSTName', np.unicode_, 100), ('UNIT', 'U10'), ('MSTDesc', np.unicode_, 255), ('Rate', (np.int)), ('Formula', np.unicode_, 255), ('refresh', (np.int))])
        self.VMSTS = self.__checkResultHeader(VMSTSRawSplit, dtVMSTS)

        self.msts = msts
        None

    """
    schnelles Lesen von Messreihen
    ids= Array mit den Messtellen-Namen
    nCT = Auflösung der Messwerte
    nNmbX= Anzahl der Messpunkt rückwärts ab TimeR
    TimeR= Unixtimestamp rechte Seite der Daten
    """
    def getBinData(self, ids=None, nCT=1, nNmbX=1, TimeR=time.time()):
        start = time.time()
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
                strRequest += "<szConfigFile>" + self.config['configfile'] + "</szConfigFile>\n"
                strRequest += "<szProcedure>LoadData</szProcedure>\n"
                strRequest += "<arrMsts>" + arrMsts + "</arrMsts>\n"
                strRequest += "<nNmbX>" + str(nNmbX) + "</nNmbX>\n"
                strRequest += "<nCT>" + str(int(nCT) * 1000) + "</nCT>\n"
                strRequest += "<nTimeR>" + str(timeR_new * 1000) + "</nTimeR>\n"
                strRequest += "<tebis>"
                try:
                    self.connect()
                    # Send Request
                    self.send_on_socket(strRequest)
                    # Recieve MSTS Packet
                    MSTSRaw = self.receive_on_socket()   
                except TebisReceiveException:
                    self.connect()
                    # Send Request
                    self.send_on_socket(strRequest)
                    # Recieve MSTS Packet
                    MSTSRaw = self.receive_on_socket()
                data = self.__checkBinaryResultHeader(MSTSRaw, types, data, offset)
                offset += len(ids)
        data['timestamp'] = data['timestamp'] / 1000
        return data

    """
    lädt die Daten als Zeichenkette
    Die Funktion ist wesentlich langsamer als getBinData und sollte nicht verwendet werden...
    """
    def getData(self, ids=None, nCT=1, nNmbX=1, TimeR=time.time()):
        start = time.time()
        types = [('timestamp', (np.int64))]
        arrMsts = ""
        for id in ids:
            types.append((str(id), (np.float32)))
            arrMsts += str(id) + ', '
        arrMsts = arrMsts[:-2]
        strRequest = "<tebis>\n"
        strRequest += "<szConfigFile>" + self.config['configfile'] + "</szConfigFile>\n"
        strRequest += "<szProcedure>JLoadData</szProcedure>\n"
        strRequest += "<arrMsts>" + arrMsts + "</arrMsts>\n"
        strRequest += "<nNmbX>" + str(nNmbX) + "</nNmbX>\n"
        strRequest += "<nCT>" + str(nCT * 1000) + "</nCT>\n"
        strRequest += "<nTimeR>" + str((int(TimeR) / int(nCT)) * int(nCT) * 1000) + "</nTimeR>\n"
        strRequest += "<tebis>"
        self.connect()
        # Send Request
        self.send_on_socket(strRequest)
        # Recieve MSTS Packet
        MSTSRaw = self.receive_on_socket()
        self.close()
        MSTSRawSplit = str(MSTSRaw, encoding='iso-8859-1').replace("'", "").split(',')
        temp = self.__checkResultHeader(MSTSRawSplit, types)
        return temp
    
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
        if(intMagic0 != -1 or intMagic1 != 463453 or intMagic2 != 756543 or intMagic3 != -1):
            return False
        intVersion = int(result[m_intPos])
        m_intPos += 1
        if(intVersion != 3):
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
            if(intColHasName):
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
                if(result[m_intPos] == "d"):
                    findindex += 1
                    m_intPos += 1
                    intStackLen = np.int64(result[m_intPos])
                    m_intPos += 1
                    if np.issubdtype(resultarr[resultarr.dtype.names[x]].dtype, np.integer):
                        intStart = np.int64(result[m_intPos])
                        m_intPos += 1
                        intInc = np.int64(result[m_intPos])
                        m_intPos += 1
                    elif np.issubdtype(resultarr[resultarr.dtype.names[x]].dtype, float):
                        intStart = float(result[m_intPos])
                        m_intPos += 1
                        intInc = float(result[m_intPos])
                        m_intPos += 1
                    resultarr[resultarr.dtype.names[x]][y:(y + intStackLen)] = np.linspace(intStart, intStart + (intStackLen * intInc) - intInc, num=intStackLen)
                    y += intStackLen - 1
                elif(result[m_intPos] == "i"):
                    findindex += 1
                    m_intPos += 1
                    intStackLen = int(result[m_intPos])
                    m_intPos += 1
                    intValue = self.getValue(result[m_intPos], resultarr[resultarr.dtype.names[x]].dtype)
                    m_intPos += 1

                    resultarr[resultarr.dtype.names[x]][y:y + intStackLen] = intValue
                    y += intStackLen - 1
                else:		
                    if len(find[0]) > findindex and not np.issubdtype(resultarr[resultarr.dtype.names[x]].dtype, np.dtype(str).type):
                        pos_to_next = find[0][findindex] - 1
                        endy = y + (pos_to_next - m_intPos)
                        if endy >= m_intNmbRows:
                            endy = m_intNmbRows - 1
                            pos_to_next = m_intPos + (endy - y)
                        resultarr[resultarr.dtype.names[x]][y:endy] = self.getValue(result[m_intPos:pos_to_next], resultarr[resultarr.dtype.names[x]].dtype)
                        m_intPos = pos_to_next + 1
                        y = endy
                    else:
                        resultarr[resultarr.dtype.names[x]][y] = self.getValue(result[m_intPos], resultarr[resultarr.dtype.names[x]].dtype)
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


class _TebisMST:
    def __init__(self, id, name, unit=None, desc=None):
        self.id = id
        self.name = name
        self.unit = unit
        self.desc = desc
        self.currentValue = None


class _TebisRMST(_TebisMST):
    def __init__(self, elem):
        self.mode = elem[4]
        self.elunit = elem[5]
        self.elFrom = elem[6]
        self.elTo = elem[7]
        self.phyFrom = elem[8]
        self.phyTo = elem[9] 
        _TebisMST.__init__(self, id=elem[0], name=elem[1], unit=elem[2], desc=elem[3])  


class _TebisVMST(_TebisMST):
    def __init__(self, elem=None):
        if elem is not None:
            self.reduction = elem[4]
            self.formula = elem[5]
            self.recalc = elem[6]
            _TebisMST.__init__(self, id=elem[0], name=elem[1], unit=elem[2], desc=elem[3]) 
    
    @classmethod
    def setValuesFromSocketInterface(cls, elem):
        unit = elem[2]
        _TebisMST.__init__(cls, id=elem[0], name=elem[1], desc=elem[3]) 
        return cls


class _TebisMapTreeGroup:
    def __init__(self, elem):
        self.treeId = elem[0]
        self.groups = []
        

class _TebisGroupMember:
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


class _TebisGroupElement:
    def __init__(self, elem):
        self.id = elem[0]
        self.members = []
        self.name = elem[1]
        self.desc = elem[2]


class _TebisTreeElement:
    def __init__(self, elem):
        self.id = elem[0]
        self.childs = []
        self.grps = []
        self.parent = elem[1]
        self.order = elem[2]
        self.name = elem[3]

    def findNodeByID(self, x):
        if self.id is x: 
            return self
        for node in self.childs:
            n = node.findNodeByID(x)
            if n: 
                return n
        return None


class TebisOracleDBException(Exception):
    ''' raise if try to get DB-Information without a DB Connection specifeied '''


class TebisReceiveException(Exception):
    pass


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
        if isinstance(obj, _TebisTreeElement): 
            return {"id": obj.id, "text": obj.name, "nodes": obj.childs}
        if isinstance(obj, _TebisMapTreeGroup):
            return {"treeId": obj.treeId, "groups": obj.groups}
        if isinstance(obj, _TebisGroupElement):
            return {"id": obj.id, "name": obj.name, "desc": obj.desc, "members": obj.members}
        if isinstance(obj, _TebisGroupMember):
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