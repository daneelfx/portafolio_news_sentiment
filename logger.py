# -*- coding: utf-8 -*-
"""
Created on Thu Nov 23 08:50:22 2017

Módulo que permite hacer loggeo de información en una ruta específica

@author: rlarios
"""

#%%

CHANGELOG = """

20201123:  ChangeLogName
20210224:  Minor adjustments , Close Log
20210708:  Print any type
"""

#%%

#import time
import io
import os

from datetime import datetime
from pytz import timezone

class logger():
    def __init__(self , pathlog = 'logs/' , logName = "process" , timeZone = timezone("America/Bogota") , extension = ".log"  ):
        """Inicializa el logger
        
        Argumentos:
        pathlog -- Ruta donde se escribirá el log
        logName -- Nombre del archivo log (el archivo tendrá un sufijo con la fecha de creación)  
                   Este nombre no nesesariamente debe tener extensión.
        extension -- extension, por defecto es .log        
        timezone  -- La zona horaria del log, por defecto zona horaria de Colombia
        """
        self.tz = timeZone
        self.path = pathlog
        self.filename = self.getName(logName , extension)
        
        if not os.path.exists( self.path ):
            print("Directorio de log no creado, Creándolo ........")
            os.mkdir(self.path)

        self.flog = io.open(self.filename , 'a' , encoding = 'UTF-8') 
        self.typeMsg = {
                "I" : "[INFO ] ",
                "E" : "[ERROR] ",
                "D" : "[DEBUG] "                
                }
        VERSION_NUMBER = "1.2.20210708"
        
        self.Info("Logger Instanciado.  Versión: " + VERSION_NUMBER)
        
    def getName(self , logName , extension):
        """Genera el nombre del archivo de acuerdo a los parámetros enviados, 
        se hace de esta manera para mantener Backwards Compatibility"""
        
        idx = logName.find(".")
        name = logName if idx < 0 else logName[:idx]
        ext = extension if idx < 0 else logName[idx:]
        return self.path + name + "_" + self.curDate() + ext
        
    def curDate(self):
        """Devuelve la decha en formato YYYYMMDDHHMMSS."""
        #time.ctime()
        return  datetime.now(self.tz).strftime('%Y%m%d%H%M%S')
    
    def curTime(self):
        """Devuelve la decha en formato YYYY-MM-DD HH:MM:SS."""
        #time.ctime()
        return  '[' + datetime.now(self.tz).strftime('%Y-%m-%d %H:%M:%S') + '] '
    
    def Info(self , message):
        """Escribe un mensaje de info en el log"""
        self.__writeLog(message , "I")
        
    def Error(self , message):
        """Escribe un mensaje de error en el log"""
        self.__writeLog(message , "E")
        
    def Debug(self , message):
        """Escribe un mensaje de Debug en el log"""
        self.__writeLog(message , "D")
    
    def __writeLog(self , message , typeM):
        """Función que escribe el mensaje con el tipo especificado"""
        msd = message.strip() if type(message) == str else f"{message}".strip()
        msx = self.curTime() + self.typeMsg[typeM] + msd
        print( msx ) 
        self.flog.writelines( msx + "\n")
        self.flog.flush()
        
    def close(self):
        """Cierra el archivo de log"""
        self.Info("Cerrando Log")
        self.flog.flush()
        self.flog.close()

