# -*- coding: utf-8 -*-
"""
Created on Thu Aug 30 07:40:03 2018

Módulo que permite hacer una conexión odbc a Impala con funciones
que ayudan a la calendarización de procesos.

@author: RLARIOS
"""

import pyodbc
import time
import pandas as pd
import io
import random as rd
import string 
import json
from Hdfs import HDFS
#from sftp import sftp
import getpass
import ast
import os

import inspect

#%%

CHANGELOG = """

20201103:  Add BigDataFrame
20201104:  Fix String Cast in BigDataframe
20201112:  Add validator on instance
20201119:  Refix String Cast in BigDataframe and add cleanup
20210218:  Add execute option modifier 
20210225:  Add executeFolder function
20210413:  Add Queries as params
20210414:  Add Describe Formatted Function
20210421:  Fix getBigDataFrame on empty queries
20210512:  Add count function
20210518:  Minor cleanup, no version change
20210520:  Remove F-Strings for backwards compat

"""

#%%


class Helper():
    def __init__(self , cache , logger = None ):
        """Inicializa el conector a bases de Datos ODBC - Impala

        Argumentos:
        cache -- Diccionario con los parámetros para crear el Helper 
        
            Valores Obligatorios en el Diccionario:
                connStr = Cadena de conexión a la base de datos o configuración DSN
                
            Valores no Obligatorios con valores por defecto:
                infoLeng = tamaño para mostrar queries , por defecto 50
                fetchSize = tamaño de la cantidad de filas a traer, por defecto 10K 
                db = base de datos por defecto a usar, si no se envía valor es "proceso"
                refresh = tasa de refresco de conexión.  Por defecto son 5 minutos
                verbose = (Boolean) Indica si imprime cada accion que ejecuta.  Por defecto es False
                
        logger -- Objeto tipo logger para realizar el loggeo del servicio
        """
        self.log = logger
        
        if type(cache) != dict:
            msg = "Debe enviarse un diccionario como parámetro para la creación del Helper.  Tipo Enviado: " + str(type(cache))
            self.printE(msg)
            raise Exception(msg)
            
        self.cache = cache
        self.con = None
        self.cursor = None
        self.CONN = self.cache["connStr"]
        self.currentDB = self.validInput("db","proceso",str)
        self.refreshTime = self.validInput("refresh" , 5*60 , int )
        self.verbose = self.validInput("verbose" , False , bool )
        self.lastCon = 0
        self.lengthMSG = self.validInput("infoLeng", 50 , int)
        self.fetchSize = self.validInput("fetchSize" , 10000 , int )
        self.maxTries  = self.validInput("maxTries" , 5 , int )
        self.waitPeriod = self.validInput("waitPeriod" , 1 , int )
        self.executeOpts = self.validInput("executeOptions" , False , bool)
        self.fileSizeBDF = self.validInput("fileSizeBDF" , 1 , int) # Tamaño en GB para crear el archivo parquet
        
        if self.fileSizeBDF > 1:
            raise Exception("El parámetro de fileSizeBDF debe ser menor a 2gb")
        
        self.queryDict = self.__loadQueryDict()
        
        VERSION_NUMBER = "1.10.20210520"
        
        self.printI("Impala Helper Instanciado.  Versión: " + VERSION_NUMBER)
        
    def __load_config_json( self , conf_path ):
        """Carga un archivo json en un diccionario de python
        recibe como parámetro la ruta de un archivo json
        """
        if conf_path is None or conf_path == '':
            raise RuntimeError('Archivo de Configuración no puede ser Nulo')
        
        with open( conf_path ) as f_in :
            json_str = f_in.read()
            return json.loads( json_str )
        
    def __loadQueryDict(self):
        filename = inspect.getframeinfo(inspect.currentframe()).filename
        pathQ    = os.path.dirname(os.path.abspath(filename))
        return self.__load_config_json( pathQ + os.sep + "queriesParams.json" )
        
        
    def validInput( self , param , default , theType ):
        value = self.cache.get(param , default)
        if type(value) != theType:
            msg = "Error de instanciación de Helper para el parámetro '{0}': Tipo Enviado: {1}, tipo Esperado: {2}".format(param,type(value),theType)
            self.printE(msg)
            raise Exception(msg)
            
        return value
        

    def useDataBase(self , dbName ):
        """Genera un query a Impala para que use una base de datos específica por defecto."""
        try:
            self.currentDB = dbName
            cursor = self.getConn().cursor()
            cursor.execute("use " + self.currentDB)
            self.printI("Cambió a BD: " + self.currentDB)
        except Exception as e:
            self.printE("Problemas con useDataBase!: " + str(e))
            raise

    def getConn(self  ):
        """Trae una conexión a la base de datos específica."""
        try:
            actual = int(round(time.time()))
            elapsed = (actual - self.lastCon)
            if self.lastCon == 0 or elapsed > self.refreshTime:
                #Entra aqui en el caso que sea una nueva conexión o la ultima conexión que se trae
                #fue hace mayor que el tiempo de refresco
                
                self.printI("Transcurrido: {0}, Tiempo de Refresco = {1}".format(elapsed , self.refreshTime))
                self.printI("Refrescando Conexión")
                self.close()
                self.con = pyodbc.connect(self.CONN, autocommit=True)
                self.cursor = self.con.cursor()
                self.lastCon = int(round(time.time()))
                self.cursor.execute("use " + self.currentDB)
                if self.verbose:
                    self.printI("Cambió a BD: " + self.currentDB)
                
                if self.executeOpts:
                    self.executeOptions()
                    if self.verbose:
                        self.printI("Ejecutó Opciones")
                
                return self.con
            else:
                return self.con
        except Exception as e:
            self.printE("Problemas con getConn!: " + str(e))
            raise
            
    def executeOptions(self):
        """Setea opciones obligatorias para Impala."""
        try:
            self.cursor.execute(self.queryDict["options1"])
        except Exception as e:
            self.printE("Problemas con executeOptions!: " + str(e))
            raise



    def removeComment( self , text):
        """Remueve los posibles comentarios que el query enviado pudiera tener."""
        try:
            a = text.find("--")
            if a > -1:
                return text[:a]
            else:
                return text
        except Exception as e:
            self.printE("Problemas con removeComment!: " + str(e))
            raise



    def getQueries(self , path , params = None):
        """Trae los queries sin comentarios de un archivo y devuelve una lista con cada uno"""
        try:
            qs = []
            tex = ''
            with io.open (path , encoding = 'UTF-8') as fi:
                for line in fi:
                    tex += self.removeComment(line)

            spli = tex.split(";")
            for el in spli:
                if el.strip() != '':
                    # Se convierten los parámetros
                    if params is not None:
                        el = el.format(**params)

                    qs.append( el.strip() )

            if self.verbose:
                self.printI("Retornando {0} consultas".format(len(qs)))

            return qs
        except Exception as e:
            self.printE("Problemas con getQueries!: " + str(e))
            raise


    def executeFile(self ,  filePath , params = None ):
        """Ejecuta un archivo especificado en filePath"""
        i = 1
        try:
            tic = time.time()
            self.printI('Ejecutando Archivo: {0}'.format(filePath ))
            #lenMsg = self.lengthMSG
            queries = self.getQueries( filePath , params )
            
            for q in queries:
                if self.verbose:
                    self.printI(' -> Ejecutando Consulta {0} del archivo'.format(i))
                self.execute(q)

                i += 1
            toc = time.time()
            self.printI('Duración de Archivo (s): {0}'.format( int(toc-tic) ))
        except Exception as e:
            self.printE("Problemas con executeFile!, Query {0}, Error: {1} ".format( i , e))
            raise
            
    
    def executeFolder(self , folder , params = None):
        """Ejecuta todos los archivos SQL de un folder recorriendo recursivamente
        las carpetas dentro del folder.   Solo ejecuta archivos con extensión .sql.
        Recorre recursivamente primero las carpetas y luego los archivos dentro de ellas.
        """
        def getOrder(folder):
            dirlist = [x for x in os.listdir(folder) if os.path.isdir(os.path.join(folder, x))]
            filelist = [x for x in os.listdir(folder) if not os.path.isdir(os.path.join(folder, x))]
            return dirlist + filelist
        
        def recorrer(folder ):
            if not os.path.isfile(folder):
                filesIn = getOrder(folder)
                for file in filesIn:
                    recorrer(folder + os.sep + file )
                 
            else:
                filename, file_extension = os.path.splitext(folder)
                if file_extension.lower() == ".sql":
                    self.executeFile(folder , params)
        
        
        self.printI("Ejecutando Directorio: {}".format(folder))
        if not os.path.isdir(folder):
            msg = "Problemas con executeFolder: Error en el directorio enviado.  No es un Directorio: " + folder
            self.printE(msg)
            raise Exception(msg)
        else:
            try:
                tic = time.time()
                filesIn = getOrder(folder)
                for file in filesIn:
                    recorrer(folder + os.sep + file)
                    
                toc = time.time()
                msg = "Directorio ejecutado correctamente, duración(s): {}".format(round(toc-tic,2))
                self.printI(msg)
            except Exception as e:
                self.printE("Problemas con executeFolder!, Directorio: {0}, Error: {1} ".format( folder , e))
                raise   


    def toCSV(self , query ,  fileName , params = None , sepa = "," , encode = "utf-8"  ):
        """Ejecuta un query específico y lo guarda en una ruta especificada en fileName"""
        try:
            if self.verbose:
                self.printI('Creando CSV: {0}'.format( fileName ))

            df = self.getDataFrame(query , params)
            df.to_csv(fileName , sep = sepa , encoding = encode , index=False)
            self.printI('Archivo CSV Creado: {0}'.format( fileName ))
        except Exception as e:
            self.printE("Problemas con toCSV!: " + str(e))
            raise

    def fromCSV(self , fileName , tablename , serverOpts = None,  sep = "," , encod = "UTF-8" ,  fromDF = None ):
        """carga un archivo csv a una tabla de impala, Esto lo hace creando un archivo parquet, 
        lo envía a hdfs y crea la tabla de impala a través de este formato
        """
        def loadConfig(conf_path):
            if conf_path is None or conf_path == '':
                raise RuntimeError('Archivo de Configuración no puede ser Nulo')
            
            with open( conf_path ) as f_in :
                json_str = f_in.read()
                
            return json.loads( json_str )
            
        def createImpala(df , table):
            cfg = None
            
            if serverOpts is not None:
                if type(serverOpts) == str:
                    cfg = loadConfig(serverOpts)
                elif type(serverOpts) == dict:
                    cfg = serverOpts     
                else:
                    raise RuntimeError('serverOpts debe ser una ruta a archivo de configuración o un dict')
                    
            # Ubicación aleatoria
            #seq = string.ascii_uppercase + string.digits
            
            if cfg is None:
                user = getpass.getuser().strip().lower()
            else:
                user = cfg.get("user","rlarios").lower().strip()       
            
            #rndloc = ''.join(rd.choice(seq) for _ in range(20))
            location = "/user/{0}/{1}/".format(user , tablename.replace(" ","").strip())
            
            cr = self.queryDict["fromCSV1"].format(table,location)
            return cr , location      
        
        try:
            if self.verbose:
                if fromDF is None:
                    self.printI('Leyendo CSV: {0}'.format( fileName ))
                else:
                    self.printI('Leyendo Pandas!!')
            
            if fromDF is None:
                df = pd.read_csv(fileName , sep = sep , encoding = encod , low_memory=False)
            else:
                df = fromDF
            
            crt_stc , hdfs_path = createImpala(df , tablename)
            newFile = "file.parq"
            
            df.to_parquet( newFile , index=False)
            
            if serverOpts is not None:
                # Se envia si esta en modo remoto
                from importlib import import_module
                sftp = getattr(import_module('sftp'), 'sftp')
                trp = sftp(serverOpts ,  logger = self.log)
                trp.put( newFile , newFile)
            
            
            hdfs = HDFS(logger = self.log , remoteOpts = serverOpts)
            
            if serverOpts is not None:
                hdfs.kinit( )
                
            try:
                hdfs.rm(hdfs_path , recursive = True , skipTrash = True)
            except:
                self.printI("Ruta no existe anteriormente")
                
            hdfs.mkdir(hdfs_path)
            hdfs.put(newFile , hdfs_path + newFile)
            hdfs.close()
            
            if serverOpts is not None:
                trp.remove( newFile )
                trp.close()
                
            os.remove(newFile)
            if self.verbose:
                self.printI("Archivo local eliminado")
            
            self.drop(tablename )
            self.execute( crt_stc )
            self.computeStats(tablename)
            
            tipo = "DataFrame" if fromDF is not None else "Archivo"
           
            self.printI("Se ha cargado el {} satisfactoriamente".format(tipo))

        except Exception as e:
            self.printE("Problemas con fromCSV!: " + str(e))
            raise

    def fromPandasDF(self , df , tablename , serverOpts = None ):
            """carga un archivo DF de pandas a una tabla de impala, crea un archivo parquet a partir
            de pyarrow y este archivo se carga como tabla externa"""
            
            try:
                fileName = "temp.csv"
                #df.to_csv( fileName , sep = "," , encoding = "utf-8" , index=False )
                self.fromCSV( fileName , tablename , serverOpts , fromDF = df  )

            except Exception as e:
                self.printE("Problemas con fromPandasDF!: " + str(e))
                raise
                
                
    def reStarter(self, cursor , q , tries = 1 ):
        try:
            if self.waitPeriod > 0:
                time.sleep(self.waitPeriod)

            return cursor , cursor.execute( q )
        
        except pyodbc.ProgrammingError as e:
            raise e
        
        except:    
            if tries > self.maxTries:
                raise

            tri = tries + 1
            self.printI("Falló, reintentando por vez No. {0} ".format(tries))
            return self.reStarter(cursor , q , tries = tri)
        
        

    def getIterator(self , query , params = None):
        """Ejecuta un query en específico.  Devuelve un Iterador a los resultados.
        Usar este cuando queremos hacer un loop con fetchMany."""
        try:
            q = query
            if params is not None:
                q = q.format(**params)

            cursor = self.getConn().cursor()
        
        except Exception as e:
            self.printE("Problemas con getIterator!: " + str(e))
            raise
        
        try:
            return self.reStarter(cursor , q)
        except Exception as e:
            self.printE("Problemas con getIterator!: " + str(e))
            raise

    def getRows(self , query ,  params = None  ):
        """Ejecuta un query en específico.  Una lista con los resultados. 
        Usar este cuando se quieren traer pocas filas"""
        try:
            lenMsg = self.lengthMSG
            if self.verbose:
                self.printI('Retornando filas para consulta: {0} ...'.format(query[:lenMsg].replace("\n"," ")))

            cursor , iterator = self.getIterator( query , params )
            header = ",".join([column[0] for column in cursor.description])
            if self.verbose:
                self.printI('Header: {0}'.format(header) )
            res = []
            i = 0;
            while True:
                records = iterator.fetchmany( self.fetchSize )
                i = i + len(records)
                if len(records) == 0:
                    break;
                res = res + records

            if self.verbose:
                self.printI('Retornando {0} filas'.format( i ))
            return header , res
        except Exception as e:
            self.printE("Problemas con getRows!: " + str(e))
            raise



    def execute(self , query , params = None):
        """Ejecuta un query en específico.  especifico para lanzar consultas
        sin devolución de resultados"""
        try:
            lenMsg = self.lengthMSG
            if self.verbose:
                self.printI('Ejecutando consulta: {0} ...'.format(query[:lenMsg].replace("\n"," ")))

            cursor , iterator = self.getIterator( query , params )
            return cursor , iterator
        except Exception as e:
            self.printE("Problemas con execute!: " + str(e))
            raise


    def getDataFrame(self , query ,  params = None  ):
        """Ejecuta un query en específico.  Devuelve un DataFrame de Pandas
        con los resultados. Usar este cuando se quieren traer pocas filas"""
        try:
            lenMsg = self.lengthMSG
            if self.verbose:
                self.printI('Generando DF para Consulta: {0} ...'.format(query[:lenMsg].replace("\n"," ")))
            header , res = self.getRows( query , params )
            cols = header.split(",")
            dfRet = pd.DataFrame.from_records(res, columns=cols)

            return dfRet
        except Exception as e:
            self.printE("Problemas con getDataFrame!: " + str(e))
            raise
            
    def getBigDataFrame(self , query ,  params = None , serverOpts = None , printCols = False):
        """Ejecuta un query en específico y se trae "a mano" el archivo subyacente. 
        Este proceso es más eficiente para tablas con un gran tamaño (Más de 10MM de registros)
        Retorna el dataframe generado con el Query o un dataframe vacio si el query genera cero registros"""
        try:
            if self.verbose:
                self.printI('Generando DF para Consulta: {0} ...'.format(query[:self.lengthMSG].replace("\n"," ")))
                
            tic = time.time()
            seq = string.ascii_uppercase
            tabla = ''.join(rd.choice(seq) for _ in range(20)).lower()
            
            #Crea una tabla con características especiales
            self.execute(self.queryDict["getBigDataFrame1"])
            self.execute(self.queryDict["getBigDataFrame2"].format(self.fileSizeBDF))
            if params is not None:
                query = query.format(**params)
            self.execute(self.queryDict["getBigDataFrame3"].format(tabla , query) )
            self.execute(self.queryDict["getBigDataFrame4"])
            
            files = []
            h , rows = self.getRows(self.queryDict["getBigDataFrame5"] + tabla)
            

            for r in rows:
                files.append(r[0])
                
            hdfs = HDFS(logger = self.log , remoteOpts = serverOpts)
            
            
            for idx , file in enumerate(files):
                hdfs.get(file , tabla + "_" + str(idx) + ".parq")

            if serverOpts is not None:
                # Se recibe si esta en modo remoto
                from importlib import import_module
                sftp = getattr(import_module('sftp'), 'sftp')
                trp = sftp(serverOpts ,  logger = self.log)
                #trp = sftp(serverOpts)
                for idx , file in enumerate(files):
                    archivo = tabla + "_" + str(idx) + ".parq"
                    trp.get( archivo , archivo)
                    trp.remove(archivo)
                
                trp.close()

            frames = []        
            for idx , file in enumerate(files):
                archivo = tabla + "_" + str(idx) + ".parq"
                frames.append(pd.read_parquet(archivo ))
                os.remove(archivo)
                
            if len(frames) != 0:
                df = pd.concat(frames, ignore_index=True)
            else:
                campos = [field[0] for field in self.describe(tabla)]
                df = pd.DataFrame(columns=campos)            

            
            self.drop(tabla)

            groups = df.columns.to_series().groupby(df.dtypes).groups
            for k , v in groups.items():
                if str(k) == "object":
                    for column in v:
                        if printCols:
                            self.printI("Object a String Columna: {}".format(column))
                        #df[column]  = df[column].astype(str).str.decode('utf-8')
                        df[column]  = df[column].astype(str).apply(ast.literal_eval).str.decode("utf-8")

             
            toc = time.time()
            self.printI('BigDataFrame Generado en {0} segundos'.format(round(toc-tic,2)))
            return df
        
        except Exception as e:
            self.printE("Problemas con getBIGDataFrame!: " + str(e))
            raise
        
            
    def drop(self , table ):
        """Borra una tabla de impala"""
        try:
            self.execute(self.queryDict["drop1"].format(table))

        except Exception as e:
            self.printE("Problemas con drop!: " + str(e))
            raise   
            
    def count(self , table , tableOpts = None):
        """Cuenta los registros de una tabla y devuelve su valor"""
        try:
            h , rows = self.getRows(self.queryDict["count1"].format(table))
            cnt = 0
            for r in rows:
                cnt = r[0]
                
            return cnt

        except Exception as e:
            self.printE("Problemas con count!: " + str(e))
            raise 
            
    
    def describe(self , table ):
        """trae los campos de una tabla de impala, su nombre y su tipo"""
        try:

            head , rows = self.getRows( self.queryDict["describe1"].format(table))
            return [[r[0] , r[1]] for r in rows]

        except Exception as e:
            self.printE("Problemas con describe!: " + str(e))
            raise 
            
    def describeFormatted( self , tabla ):
        """Devuelve los campos de una tabla de impala con las propiedades de la misma
        El primer elemento a devlver es una lista de columnas, donde cada elemento es una tupla con nombre y tipo de columnas
        El segundo elemento es un dicionario donde cada llave es el nombre de la propiedad y su valor es el texto de la propiedad
        """
        try:
            if self.verbose:
                self.printI('Generando describe formateado para: {0} ...'.format(tabla))
                
            h , rows = self.getRows(self.queryDict["describe2"].format(tabla))
            exceptions = ["# col_name" , "# Detailed Table Information","Table Parameters:","# Storage Information"]
            columns = []
            info = {}
            nullCounter = 0
            for r in rows:
                if r[1] is None:
                    nullCounter += 1
                else:
                    if nullCounter > 0 and nullCounter < 2:
                        columns.append((r[0],r[1]))
                    else:
                        if r[0].strip() not in exceptions:
                            if r[0].strip()  != "":
                                info[r[0].replace(":","").strip()] = r[1].strip()
                            elif r[1].strip() != "":
                                info[r[1].replace(":","").strip()] = r[2].strip()
                            else:
                                print("OJO ->" , r ) #does Nothing
                            
            return columns , info
        
        except Exception as e:
            self.printE("Problemas con Decribe Formatted!: " + str(e))
            raise  
        
            
    def computeStats(self , table ):
        """Calcula las estadísticas de una tabla de impala"""
        try:
            self.execute(self.queryDict["compute1"].format(table))

        except Exception as e:
            self.printE("Problemas con Compute Stats!: " + str(e))
            raise  


    def oneHot(self , table , colsOHE , drop = False):
        """Funcion para generar OneHotEncodings para una lista de columnas"""
        try:
            def getDistinct(column , table):
                q = self.queryDict["oneHot1"].format(column , table)
                head , res = self.getRows( q  )
                return [x[0] for x in res]

            def getColumns(drop , colsOHE , table) :
                if drop:
                    q = self.queryDict["describe1"].format(table)
                    head , res = self.getRows( q  )
                    fields = []
                    for row in res:
                        if row[0] not in colsOHE:
                            fields.append(row[0])

                    return ",".join(fields)
                else:
                    return "*"

            colsOHE = [x.lower() for x in colsOHE]
            self.printI('Creación de OneHotEncodings para columnas {0}, en tabla {1}'.format(colsOHE,table))

            q = self.queryDict["oneHot2"].format(table , getColumns(drop , colsOHE , table))
            mappings = {}
            for i in range(len(colsOHE)):
                values =   getDistinct(colsOHE[i] , table)
                cases = []
                for val in range(len(values)):
                    col_name = "{0}_{1}".format(colsOHE[i], val )
                    cases.append( self.queryDict["oneHot3"].format(colsOHE[i],values[val],col_name))
                    mappings[col_name] = values[val]
                q = q + ",".join(cases) + ","

            q = q[:len(q)-1] + "FROM {0} ".format( table )

            self.execute (self.queryDict["oneHot4"].format(table))
            self.execute( q )
            self.execute( self.queryDict["oneHot5"].format(table))
            if self.verbose:
                self.printI("Se crean los siguientes mapeos de columnas:")
                self.printI("COLUMNA -> VALOR ")
                self.printI("******************")
                for idx in mappings:
                    self.printI("{0} -> {1}".format(idx,mappings[idx]))
            
            self.printI('Creación de tabla {0}_ohe satisfactoria'.format( table ))
            return mappings

        except Exception as e:
            self.printE("Problemas con OneHotEncoding!: " + str(e))
            raise



    def transpose(self , table , cols_group , cols_trans , cols_sum):
        """FUNCION ALPHA: USAR BAJO TU PROPIO RIESGO!"""
        try:
            def getDistinct(column , table):
                q = self.queryDict["transpose1"].format(column , table)
                head , res = self.getRows( q  )
                return [x[0] for x in res]

            def getTypes(cols_trans , table):
                q = self.queryDict["describe1"].format(table)
                head , res = self.getRows( q )
                cols = [(x[0] , x[1]) for x in res] #(columna , tipo)
                tipos = {}
                for tup in cols:
                    if tup[0] in cols_trans:
                        tipos[tup[0]] = tup[1]

                return tipos

            def returnValue(colum , tipos , val):
                retVal = tipos[colum]
                if retVal in ["string","varchar","char"]:
                    return "'" + val + "'"
                else:
                    return val

            def columnName(value , column):
                x = str(column).lower()
                chars = [("á" ,"a") , ("é" ,"e") , ("í" ,"i") , ("ó" ,"o") , ("ú" ,"u") , ("ñ" ,"n") , (" " ,"_") , ("/" ,"_")]
                for tup in chars:
                    x = x.replace(tup[0] , tup[1])

                return str(value) + "_" + str(x)
            
            if self.verbose:
                self.printI('Creación de tabla a partir de {0}, con columnas de agrupación {1}, y columnas a transponer {2}'.format(table,cols_group,cols_trans))

            q = self.queryDict["transpose2"].format(table , ",".join(cols_group))
            tipos = getTypes(cols_trans , table)
            for col in range(len(cols_trans)):
                columna = cols_trans[col]
                dist_vals = getDistinct(columna , table)
                cases = []
                for val in dist_vals:
                    cases.append( self.queryDict["transpose3"].format(columna, returnValue(columna,tipos,val) ,cols_sum[col],columnName(cols_sum[col],val)) )
                q = q + ",".join(cases) + ","

            q = q[:len(q)-1] + self.queryDict["transpose4"].format( table , ",".join(cols_group))

            self.execute(self.queryDict["transpose5"].format(table))
            self.execute( q )
            self.execute(self.queryDict["transpose6"].format(table))
            self.printI('Tabla {0}_trans creada con éxito!'.format(table))


        except Exception as e:
            self.printE("Problemas con transpose!: " + str(e))
            raise

    def recreate(self , table):
        """Recrea una tabla como parquet en caso de ser necesario"""
        try:
            self.printI('Re-Creación de tabla a partir de {0}'.format(table))
            tableX = ""
            if table.find(".") != -1:
                tableX =  table[table.find(".")+1:]
            else:
                tableX = table
            self.execute( self.queryDict["recreate1"].format(tableX) )
            self.execute( self.queryDict["recreate2"].format(tableX))
            self.execute( self.queryDict["recreate3"].format(table))
            self.execute( self.queryDict["recreate4"].format( table , tableX))
            self.execute( self.queryDict["recreate5"].format( table ))

        except Exception as e:
            self.printE("Problemas con recreate!: " + str(e))
            raise
   
    def muestraAleatoria(self ,  table , pct = 0.01):
        """Genera una muestra aleatoria de una tabla , por defecto del 1% de los datos"""
        qMuestra = self.queryDict["muestraAleatoria1"]
        if self.verbose:
            self.printI("Generación de una muestra aleatoria simple de {}% de los datos".format(pct*100))
        desc = self.describe(table)
        campos = [ d[0] for d in desc ]
        
        self.drop(table + "_muestra")
        self.execute( qMuestra.format(table , ",".join(campos) , pct) )
        self.computeStats(table + "_muestra")
        
        
    def muestraEstratificada(self ,  table , estratos , pct = 0.01):
        """Genera una muestra aleatoria estratificada de una tabla , por defecto del 1% de los datos
        toma todos los campos de estrato y saca una muestra de cada estrato"""
        
        qMuestra = self.queryDict["muestraEstratificada1"]
        if self.verbose:
            self.printI("Generación de una muestra aleatoria estratificada de {}% de los datos".format(pct*100))
      
        desc = self.describe(table)
        campost = [ "t."+ d[0] for d in desc ]
        joiner = " and ".join([ "t." + x + " = " + "x." + x for x in estratos ]    )
        self.drop(table + "_muestra_est")
        self.execute( qMuestra.format(table , ",".join(estratos), ",".join(campost) , joiner , pct) )
        self.computeStats(table + "_muestra_est")
        
        
    def tableSize(self ,  table , sizeFmt = "B"):
        """Trae el tamaño de una tabla en las unidades específicadas"""
        def getFileSize( bts ):
            """ Toma el tamaño en bytes y lo devuelve en el formato especificado) """
            sizes = { "YB":8, "ZB":7, "EB":6, "PB":5, "TB":4, "GB":3, "MB":2, "KB":1, "B":0 }
            return bts / (1024**sizes[sizeFmt])
        
        def stripBytes( size ):
            """toma un tamaño en texto ej, 20MB y lo deja en bytes)"""
            sizes = ["YB","ZB","EB","PB","TB","GB","MB","KB"]
            for id , s in enumerate( sizes ):
                if size.find(s) > 0:
                    return float(size[:size.find(s)]) * (1024**(len(sizes)-id))
            return  float(size[:size.find("B")])
        
        if self.verbose:
            self.printI("Calculando el tamaño de la tabla {0} en tamaño {1}: ".format( table , sizeFmt ))
            
        size = 0
        h , rows = self.getRows(self.queryDict["tableSize1"] + table)
        for r in rows:
            size = size +  stripBytes( r[1] )
        
        res = getFileSize( size )
        
        if self.verbose:
            self.printI("Tamaño de la tabla: {0}{1}".format( round(res,2) , sizeFmt ))
            
        return res
    
    
    def estabVariables(self , tabla , groupCols , varbl = None , exclude = None):
        """Genera estadísticas de estabilidad de variables para los modelos"""
        self.printI('Generación de estabilidad de variables para {0} '.format(tabla))
        #Funciones utiles
        def missing( table , group , vs ):
            """Genera el porcentaje de nulos por periodo para cada variable.
            La tabla resultante es {nombre_tabla}_miss
            """
            if self.verbose:
                self.printI('Estabilidad -> Generando tabla de missing')
            
            joiner = " and ".join(["t."+a+ " = " + "p." + a for a in groupCols])
            creat = self.queryDict["missing1"]
            x = ""
            for v in vs:
                x = x + self.queryDict["missing2"].format(v)
            
            t = ["t."+a for a in group]
            crt = creat.format(table , ",".join(t) , x , joiner)
            self.drop(table + "_miss")
            self.execute(crt)
            self.computeStats(table + "_miss")
            
        def averages( table , group , vs ):
            """Genera el valor promedio para cada variable.
            La tabla resultante es {nombre_tabla}_av
            """
            if self.verbose:
                self.printI('Estabilidad -> Generando tabla de promedios')
                
            creat = self.queryDict["averages1"]
            x = ""
            for v in vs:
                x = x + self.queryDict["averages2"].format(v)
            
            crt = creat.format(table , ",".join(group) , x )

            self.drop(table + "_av")
            self.execute(crt)
            self.computeStats(table + "_av")
            
        def medians( table , group , vs ):
            """Genera la mediana para cada variable.
            La tabla resultante es {nombre_tabla}_med
            """
            if self.verbose:
                self.printI('Estabilidad -> Generando tabla de medianas')
                
            creat = self.queryDict["medians1"]
            x = ""
            for v in vs:
                x = x + self.queryDict["medians2"].format(v)
            
            crt = creat.format(table , ",".join(group) , x )
            
            self.drop(table + "_med")
            self.execute(crt)
            self.computeStats(table + "_med")
            
            
        def totals( table , vs ):
            """Genera para cada variable el total (no por periodo) de
                - promedio
                - mediana
                - desviación estándar
                - promedio de nulos
            La tabla resultante es {nombre_tabla}_full
            """
            if self.verbose:
                self.printI('Estabilidad -> Generando tabla de totales')
                
            creat = self.queryDict["totals1"]
            
            x = []
            for v in vs:
                x.append(self.queryDict["totals2"].format(v))
                x.append(self.queryDict["totals3"].format(v))
                x.append(self.queryDict["totals4"].format(v))
                x.append(self.queryDict["totals5"].format(v))
            
            crt = creat.format(table , ",".join(x) )
            
            self.drop(table + "_full")
            self.execute(crt)
            self.computeStats(table + "_full")
            
            
        def limiteControl( table , group , vs , desv , n , conf , tipo):
            """Genera un limite de control para la mediana de los valores basado en
            valor actual por fuera de : promedio_full +- conf*(desv_est/sqrt(num_periodos))
            La tabla resultante es {nombre_tabla}_lcm_{conf}
            """
            if self.verbose:
                msg = 'Estabilidad -> Limite de Control de la media con desv={0} , Result={1}_lcm_{2}_{3}'
                self.printI(msg.format(desv , table , conf , tipo))
                
            base =  self.queryDict["limiteControl1"]            
            creat = self.queryDict["limiteControl2"]
            
            x = []
            for v in vs:
                x.append(base.format(v,desv,n,tipo))

            crt = creat.format(table , ",".join(group) , "".join(x) , conf , tipo )
            
            tblx = "{0}_lcm_{1}_{2}".format(table , conf ,tipo)
            
            self.drop( tblx )
            self.execute(crt)
            self.computeStats( tblx )
            
            
        def completitud( table , group , vs , desv , n , conf ):
            """Genera un valor de completitud para los nulos basado en
            valor actual por fuera de : promedio_full +- conf*sqrt(promedio_full*(1-promedio_full)/num_periodos)
            La tabla resultante es {nombre_tabla}_comp_{conf}
            """
            if self.verbose:
                msg = 'Estabilidad -> Porcentaje de no completitud con desv={0} , Result={1}_comp_{2}'
                self.printI(msg.format(desv , table , conf))
                
            base = self.queryDict["completitud1"]
            
            creat = self.queryDict["completitud2"]
            
            x = []
            for v in vs:
                x.append(base.format(v,desv,n))

            crt = creat.format(table , ",".join(group) , "".join(x) , conf )
            
            tblx = "{0}_comp_{1}".format(table , conf )
            
            self.drop( tblx )
            self.execute(crt)
            self.computeStats( tblx )
            
        def alertaPeriodo( table , group , vs , conf ):
            """Genera una tabla de alertas con base en las tablas de límites
            y de completitud.  Suma el número de veces por periodo que la tabla 
            supera cada limite por intervalo de confianza    
            La tabla resultante es {nombre_tabla}_alerta
            """
            if self.verbose:
                msg = 'Estabilidad -> Alertas por periodo'
                self.printI( msg )
            
            types = ["lcm" , "comp"]
            w = []
            sumVar = " + ".join(vs)
            withs = self.queryDict["alertaPeriodo1"]
            for t in types:
                for c in conf:
                    if t == "lcm":
                        lcmt = ["av","med"]
                        for l in lcmt:
                            tablex = "{0}_{1}_{2}_{3}".format(table,t,c,l)
                            w.append( withs.format( t+"_"+c+"_"+l , ",".join(group) , sumVar , tablex ) )
                    else:
                        tablex = "{0}_{1}_{2}".format(table,t,c)
                        w.append( withs.format( t+"_"+c , ",".join(group) , sumVar , tablex ) )  
                    
            
            first = "l90_med"
            tab = ["l90_av" , "l95_med" , "l95_av" ,
                              "l99_med" , "l99_av" ,
                    "c90" , "c95", "c99"]
            j = {}
            for t in tab:
                grupos = []
                for g in group:
                    grupos.append( first + "." + g + " = " + t + "." + g )
                j[t] = " and ".join(grupos)  
                
            finale = self.queryDict["alertaPeriodo2"].format(**j)
                
            create = self.queryDict["alertaPeriodo3"]
            
            t90 =  ",".join(["l90_med." + g for g in group])
            crt = create.format( table , ",".join(w) , t90 , finale )
            
            tblx = "{0}_alerta".format( table )
            
            self.drop( tblx )
            self.execute(crt)
            self.computeStats( tblx )

            
        def getSize( table ):
            h , rows = self.getRows(self.queryDict["getSize1"].format(table))
            n = 0
            for r in rows:
                n = r[0]
            return n
        
        def baseForAlerta(table , group ):
            q = self.queryDict["baseForAlerta1"].format( table , ",".join(group))
            head , rows = self.getRows(q)
            return rows
        
        def alertaPorTabla(table , group , vs , rows):
            """Genera un informe de Alertas por cada tabla generada     
            la tabla que recibe es la tabla con el sufijo de lcm o comp
            correspondiente (comp , lcm)
            Devuelve para cada tabla y periodo las variables alertadas.
            """
            reto = []
            if self.verbose:
                msg = 'Estabilidad -> Detalle de alertas, tabla {0}'.format(table)
                self.printI( msg )
            
            lgr = len(group) # tamaño de los campos de agruupación
            for r in rows:
                per = []
                for i in range(lgr):
                    per.append(group[i] + ": " +  str(r[i]))
                
                nr = r[lgr:] #se trae el resto de variables
                varx = []
                for i in range(len(vs)):
                    if nr[i] != 0:
                        varx.append(vs[i])
                        
                if len(varx) > 0:
                    reto.append([table , " , ".join(per) , " , ".join(varx)])
            
            return reto    
        
        
        def alertaPorTablaTrans(table , group , vs , rows ):
            """Genera un informe de Alertas por cada tabla generada     
            la tabla que recibe es la tabla con el sufijo de lcm o comp
            correspondiente (comp , lcm)
            Devuelve para cada tabla y variable, los periodos alertados.
            """
            reto = []
            if self.verbose:
                msg = 'Estabilidad -> Detalle de alertas Transpuesta, tabla {0}'.format(table)
                self.printI( msg )
            
            lgr = len(group) # tamaño de los campos de agruupación
            numPer = len(rows) # Número de periodos
            
            # Se genera lista de periodos 1 sola vez.
            periods = []
            for r in rows:
                x = []
                for i in range(lgr):
                    x.append(group[i] + "=" + str(r[i]))
                periods.append(" , ".join(x))
                    
            for j in range(len(vs)):
                #Para cada  variable se recorre toda la columna y se toman los periodos.
                perx = []
                variable = vs[j]
                for i in range(numPer):
                    if rows[i][lgr + j] != 0:
                        perx.append( periods[i] )
                        
                if len(perx) > 0:
                    reto.append([table , variable , " -- ".join(perx)])
                        
            return reto 
        
        
        def baseVarNtile( table , var ):
            """Trae la información de percentiles, máximos , mínimos para
            cada variable.
            """
            q = self.queryDict["baseVarNtile1"].format(var , table)
            head , rows = self.getRows(q)
            return rows[0]
         
        def generateSummary( table , variables):
            """Genera la tabla con los percentiles, missing, máximos y 
            mínimos de cada variable.  Retorna un dataframe.
            """
            if self.verbose:
                msg = 'Estabilidad -> Resumen de valores para variables para tabla {0}'.format(table)
                self.printI( msg )
                
            df = []
            for v in variables:
                row = baseVarNtile( table , v)
                df.append( row )
            
            cols = ["variable","pmiss","min","p1","p10","p25","p50","p75","p90","p99","max"]
            return pd.DataFrame.from_records(df, columns=cols)
        
      
            
        # ##############################################################
        # EL programa de estabilidad se ejecuta desde aqui
        # usando las funciones internas definidas directamente arriba
        # ##############################################################
        if tabla is None or groupCols is None:
            self.printE('Debe enviar la tabla y las columnas de agrupación')
            raise
        
        if varbl is None and exclude is None:
            self.printE('Debe enviar la lista de las variables a incluir o a excluir')
            raise
            
        tic = time.time()    
        varis = []
        if exclude is None:
            #Proceso Normal
            varis = varbl
        else:
            # se traen todas las columnas menos las excluidas y las de agrupación
            h , rows = self.getRows(self.queryDict["describe1"].format(tabla))
            exc = groupCols + exclude
            for r in rows:
                if r[0] not in exc:
                    varis.append(r[0])
        
        varis = [ x.strip().lower() for x in varis]
        if self.verbose:
            self.printI("Número de variables a evaluar: " + str(len(varis)))
            
        ##aqui se ejecutan las funciones para cada transformación
        
        missing( tabla , groupCols , varis)
        averages( tabla , groupCols , varis)
        medians( tabla , groupCols , varis)
        totals( tabla , varis)
        n = getSize( tabla )

        # Se generan tablas con variables por fuera de límites en
        # completitud de missing o por valores por fuera de intervalos de
        # confianza definidos (90 , 95 , 99)
        confidence = {
            "90" :  1.2816 ,
            "95" :  1.6449 ,
            "99" :  2.3263 ,
        }
        
        tipoLimite = ["med" , "av"]
        
        for k in confidence:
            completitud(tabla , groupCols , varis , confidence[k] , n , k)
            for tipo in tipoLimite:
                limiteControl(tabla , groupCols , varis , confidence[k] , n , k , tipo)
            
        alertaPeriodo( tabla , groupCols , varis , confidence )
        
        sufix = ['comp_90' , 'comp_95' , 'comp_99' 
               , 'lcm_90_av' , 'lcm_90_med' , 'lcm_95_av' 
               , 'lcm_95_med' , 'lcm_99_av' , 'lcm_99_med' ]
        
        build = []
        buildT = []
        
        for s in sufix:
            rows = baseForAlerta(tabla + "_" + s , groupCols )
            build = build + alertaPorTabla(tabla + "_" + s , groupCols , varis , rows )
            buildT = buildT + alertaPorTablaTrans(tabla + "_" + s , groupCols , varis , rows )
        
        dfRet = pd.DataFrame.from_records(build, columns=["tabla","periodo","variables"])
        dfRetT = pd.DataFrame.from_records(buildT, columns=["tabla","variable","periodos"])
        
        dfSum = generateSummary( tabla , varis )
       
        toc = time.time()
        d = round(toc-tic , 2) 
        self.printI('Finalizó proceso de estabilidad de variables.  Duración: {0}s'.format(d))
        return dfRet , dfRetT , dfSum
    
        
    def enmascarar(self , tabla , variables , saltI = None , saltT = None):
        """Enmascaramiento de variables en una tabla.
        Usa funciones propias de impala para generar nuevos valores.
        Puede utilizar sales enviadas o generadas automáticamente.
        la tabla resultado es tabla_mask.
        Retorna las sales utilizadas
        """
        
        def genSeeds():
            seq = string.ascii_uppercase + string.digits
            salti = saltI if saltI != None else rd.randint(0,1000000)
            # ejemplo en https://stackoverflow.com/questions/2257441/random-string-generation-with-upper-case-letters-and-digits
            saltt = saltT if saltT != None else ''.join(rd.choice(seq) for _ in range(10))
            return salti , saltt
        
        self.printI('Inicia proceso de enmascaramiento para tabla {0}'.format(tabla))
        tic = time.time()
        
        si , st = genSeeds()
        self.printI('Usando sales generadas o enviadas.  {0} para enteros y {1} para textos'.format(si,st))
        
        vbls = [ x.lower().strip() for x in variables ]
        q = self.queryDict["enmascarar1"]
        fields = []
        h , rows = self.getRows(self.queryDict["describe1"].format(tabla) )
        for r in rows:
            if r[0] in vbls:
                limit = r[1].find("(")
                tipo =  r[1][:limit] if limit > -1 else r[1]
                    
                if tipo in ["tinyint" , "smallint" , "int", "bigint"]:
                    fields.append(" default.mask_int( {0} , {1} ) as {0} ".format(r[0] , si ) )
                
                elif tipo in ["string" , "char" , "varchar"]:
                    fields.append(" default.mask_text( cast({0} as string) , '{1}' ) as {0} ".format(r[0] , st ) )
                    
                else:
                    self.printI("WARNING: Campo {0} no enmascarable (tipo={1}), campo se ha ignorado ".format(r[0],tipo))
                    fields.append(r[0])
                    
            else:
                fields.append(r[0])

        self.drop(tabla + "_mask")
        self.execute( q.format(tabla , " , ".join(fields)) )
        self.computeStats(tabla + "_mask")
        toc = time.time()
        d = round(toc-tic , 2) 
        self.printI('Finalizó proceso de enmascaramiento. Duración: {0}s'.format(d))
        return si , st
        
        
    def enmascararLong(self , tabla , variables , tabla_original = None):
        """EXPERIMENTAL - Enmascaramiento de variables en una tabla, teniendo en cuenta longitudes

        Usa 'tablas de verdad' con el valor original y el generado para cada campo
        La idea es que el campo enmascarado mantenga una longitud igual o menor al campo original
        la tabla resultado es tabla_mask
        las tablas de verdad (1 x campo) quedan como campo_tabla_truth.
        
        Si se envía como parámetro una tabla_original, es porque estoy generando una tabla nueva
        con base en atributos que ya han sido calculados.  por lo tanto no se deben calcular de nuevo
        las tablas de verdad y usar las anteriores (para aquellos campos que ya estén en
        una tabla de verdad.  Los campos a enmascarar deberían tener el mismo nombre en todas 
        las tablas que se vana enmascarar).
        
        """
        def getTypes( table , fields ):
            h , rows = self.getRows(self.queryDict["describe1"].format(table))
            realfields = []
            for r in rows:
                if r[0] in fields:
                    limit = r[1].find("(")
                    tipo =  r[1][:limit] if limit > -1 else r[1]
                  
                    if tipo in ["tinyint" , "smallint" , "int", "bigint"]:
                        realfields.append( (r[0] , "entero") )
                    elif tipo in ["string" , "char" , "varchar"]:
                        realfields.append( (r[0] , "string") )
                    else:
                        self.printI("WARNING: Campo {0} no enmascarable (tipo={1}), campo se ha ignorado ".format(r[0],tipo))
            
            return realfields
            
        
        def genTablaVerdad( table , field ):
            campo = field[0]
            if field[1] == "entero":
                q = self.queryDict["enmascararLg1"].format(campo , table)
            else:
                q = self.queryDict["enmascararLg2"].format(campo , table)
                
            newt  = "{0}_{1}_truth".format(campo , table)
            self.drop(newt)
            self.execute( q ) 
            self.computeStats(newt)
            
            return (newt,campo)
        
        
        self.printI('Inicia proceso de enmascaramiento (long) para tabla {0}'.format(tabla))
        tic = time.time()
        vbls = [ x.lower().strip() for x in variables ]
        
        fields = getTypes( tabla , vbls )
        
        auxTables = []
        self.printI('Generación de tablas de verdad para cada variable en tabla {0}'.format(tabla))
        for v in fields:
            auxTables.append( genTablaVerdad(tabla , v) )
                
        creat = self.queryDict["enmascararLg3"]
        flds = [v[0] for v in fields]
        
        h , rows = self.getRows(self.queryDict["describe1"].format(tabla))
        slct = []
        for r in rows:
            if r[0] in flds:
                slct.append("new_{0} as {0}".format(r[0]))
            else:
                slct.append(r[0])
        
        j = []
        for i in range(len(auxTables)):
            t = auxTables[i][0]
            f = auxTables[i][1]
            j.append(" {0} t{1} on x.{2} = t{1}.{2} ".format(t,i,f))
            
        self.drop(tabla + "_mask")
        self.execute( creat.format(tabla , " , ".join(slct) , " left join ".join(j) ) )
        self.computeStats(tabla + "_mask")
        
        toc = time.time()
        d = round(toc-tic , 2) 
        self.printI('Finalizó proceso de enmascaramiento. Duración: {0}s'.format(d))
            
        
    def close(self):
        """cierra la conexión, en el caso que ya se haya conectado al menos una vez"""
        if self.lastCon != 0 and self.con is not None:
            self.printI('Cerrando Conexión')
            try:
                self.con.close()
            except Exception as e:
                self.printE("Problemas Cerrando conexion: " + str(e))
            
                


    def printI(self , msg ):
        if self.log is None:
            print(msg)
        else:
            self.log.Info(msg)

    def printE(self , msg ):
        if self.log is None:
            print(msg)
        else:
            self.log.Error(msg)
