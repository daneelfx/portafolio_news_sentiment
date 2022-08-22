# -*- coding: utf-8 -*-
"""
Created on Thu May  2 14:03:16 2019

@author: rlarios
"""

import pysftp
import time
import json
import getpass
import os

class sftp():
    def __init__(self , cache , logger = None ):
        """Inicializa el conector a bases de Datos ODBC - Impala

        Argumentos:
        cache -- Diccionario con los parámetros para crear el Helper 
        
            Valores Obligatorios en el Diccionario:
                user = usuario de conexión
                password = contraseña del usuario
                server = servidor a donde se conectará
                
        logger -- Objeto tipo logger para realizar el loggeo del servicio
        """
        
        cfg = {}
        if type(cache) == str:
            cfg = self.__loadConfig(cache)
        elif type(cache) == dict:
            cfg = cache     
        else:
            raise RuntimeError('Cache debe ser una ruta a archivo de configuración o un dict')
        
        self.cache = cfg
        self.log = logger
        self.usr = self.cache.get("user" , getpass.getuser().lower().strip() )
        self.pwd = self.cache.get("password" , None)
        self.pkey = self.cache.get("private_key" , None)
        self.srv = self.cache.get("server" , "sbmdeblze004")
        self.port = self.cache.get("port" , 22 )
        
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        self.con = None
        if self.pwd is not None:
            self.con = pysftp.Connection(self.srv, username=self.usr, password=self.pwd , port = self.port , cnopts=cnopts)
        elif self.pkey is not None:
            self.con = pysftp.Connection(self.srv, username=self.usr, private_key=self.pkey , port = self.port , cnopts=cnopts)
        else:
            raise RuntimeError('Los parámetros enviados deben tener contraseña o llave privada')
            
        self.con._transport.set_keepalive(30)
        self.printI("SFTP Instanciado")
            
        
    def __loadConfig(self, conf_path ):
        """Carga un archivo json en un diccionario de python
        recibe como parámetro la ruta de un archivo json
        """
        if conf_path is None or conf_path == '':
            raise RuntimeError('Archivo de Configuración no puede ser Nulo')
        
        with open( conf_path ) as f_in :
            json_str = f_in.read()
            return json.loads( json_str )
        
    
    def put(self , objeto_local , objeto_remoto = None):
        """Envia un archivo local al servidor remoto"""
        try:
            #Support for BigFiles
            self.printI("Enviando " + objeto_local)
            channel = self.con.sftp_client.get_channel()
            channel.lock.acquire()
            size = channel.out_window_size + os.stat(objeto_local).st_size

            channel.out_window_size = size
            channel.in_window_size  = size
            channel.in_max_packet_size  = size
            channel.out_max_packet_size = size
            channel.transport.packetizer.REKEY_BYTES = pow(2, 40)
            channel.transport.packetizer.REKEY_PACKETS  = pow(2, 40)
            channel.out_buffer_cv.notifyAll()
            channel.lock.release()
            
            tic = time.time()
            if objeto_remoto is None:
                self.con.put( objeto_local )
            else:
                self.con.put( objeto_local , objeto_remoto)
            toc = time.time()
            d = round(toc-tic,2)
            self.printI("Archivo Enviado, {0}s".format(d))
        except Exception as e:
            self.printE("Problemas con envío de archivo {0}. Error = {1} ".format( objeto_local , e))
            raise
            
    def get(self , objeto_remoto , objeto_local):
        """Recibe un archivo remoto a un archivo local"""
        try:
            self.printI("Recibiendo " + objeto_remoto)
            tic = time.time()
            self.con.get( objeto_remoto , objeto_local )
            toc = time.time()
            d = round(toc-tic,2)
            self.printI("Archivo Recibido, {0}s".format(d))
        except Exception as e:
            self.printE("Problemas con recepción de archivo remoto {0}. Error = {1} ".format( objeto_remoto , e))
            raise
            
    def remove(self , objeto_remoto ):
        """Elimina un archivo en el servidor remoto"""
        try:
            tic = time.time()
            self.con.remove(objeto_remoto)
            toc = time.time()
            d = round(toc-tic,2)
            self.printI("Archivo {1} Eliminado, {0}s".format(d,objeto_remoto))
        except Exception as e:
            self.printE("Problemas con eliminación de archivo remoto {0}. Error = {1} ".format( objeto_remoto , e))
            raise
            
    def close(self ):
        try:
            self.printI("Cerrando SFTP")
            self.con.close()
        except Exception as e:
            self.printE("Problemas cerrando la conexión.  Error = {}".format(e))
            raise
        
        
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