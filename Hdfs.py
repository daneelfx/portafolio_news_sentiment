# -*- coding: utf-8 -*-
"""
Created on Thu Mar 21 09:56:07 2019

@author: rlarios
"""

import subprocess
#import paramiko
import importlib
import json
import getpass

class HDFS():
    def __init__(self ,  logger = None , remoteOpts = None ):
        """Inicializa el ayudante de HDFS
        Argumentos:
        remoteOpts -- Diccionario con los parámetros para crear la conexión
                      a un servidor con HDFS
        
            Valores Obligatorios en el Diccionario:
                user = usuario de conexión
                password = contraseña del usuario
                server = servidor a donde se conectará
                
        logger -- Objeto tipo logger para realizar el loggeo del servicio
        """
        self.log = logger
        self.remote = False
        if remoteOpts is not None:
            if type(remoteOpts) == str:
                cfg = self.__loadConfig(remoteOpts)
            elif type(remoteOpts) == dict:
                cfg = remoteOpts     
            else:
                raise RuntimeError('remoteOpts debe ser una ruta a archivo de configuración o un dict')
                
            self.remote = True
            self.usr = cfg.get("user" , getpass.getuser().lower().strip() ) 
            self.pwd = cfg["password"]
            self.srv = cfg.get("server"  , "sbmdeblze004")
            
            paramiko = importlib.import_module("paramiko")
            
            self.cli =  paramiko.SSHClient()
            self.cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.cli.connect(self.srv, port=22, username=self.usr, password=self.pwd)
            self.printI("usando conexión remota a HDFS")
        else:
            self.usr = getpass.getuser().lower().strip()
            self.printI("usando conexión local a HDFS")
            
            
    def __loadConfig(self, conf_path ):
        """Carga un archivo json en un diccionario de python
        recibe como parámetro la ruta de un archivo json
        """
        if conf_path is None or conf_path == '':
            raise RuntimeError('Archivo de Configuración no puede ser Nulo')
        
        with open( conf_path ) as f_in :
            json_str = f_in.read()
            return json.loads( json_str )
            
            
        
    def __run_cmd(self , args_list):
        """
        Ejecuta acciones de línea de comandos.  Devuelve el codigo de salida de
        la aplicación , la salida estándar y la salida de error
        """
        # import subprocess
        comm = ' '.join(args_list)
        if "kinit" in comm:
            self.printI('Ejecutando Comando: {0}'.format( ' '.join(args_list[3:])))
        else:
            self.printI('Ejecutando Comando: {0}'.format( comm ))
        
        if self.remote:
            stdin, stdout, stderr = self.cli.exec_command(comm,timeout=100)
            err = stderr.read().decode("utf-8")
            out = stdout.read().decode("utf-8")
            #sin = stdin.read()
            if err:
                return 1 , out , err
            else:
                return 0 , out , err
        else:
            proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            s_output, s_err = proc.communicate()
            s_return =  proc.returncode
            return s_return, s_output, s_err 

    def __manage(self , ret , out , err):
        if ret != 0:
            # Se generá error en el comando
            self.printE("Error en el comando HDFS ejecutado. stdErr:" + err)
            raise
            
        self.printI("Comando ejecutado correctamente")
        return out
    
#%%
        
    def kinit( self , usr = None , pwd = None ):
        """Ejecuta la inicialización de usuario en caso de ser necesario
        """
        def escapePwd( pwd ):
            # Para hacer kinit se necesitan escapar agunos caracteres para bash
            symbols = [" " , "!" , '"' , "#" , "$" , "&" , "'" , "(" , ")" , "*" , 
                       "," , ";" , "<" , ">" , "?" , "[" , "]" , "^" , "`" ,
                       "{" , "|" , "}"]

            #symbols = ['&','"','!','$',')','(']
            for s in symbols:
                pwd = pwd.replace( s , "\\" + s)
            
            return pwd

        escape = False
        if usr is None and pwd is None:
            #Toma valores de server
            usr = self.usr
            pwd = escapePwd(self.pwd)
            escape = True

        if not escape:
            pwd = escapePwd(pwd)
        
        (ret, out, err) = self.__run_cmd(['echo ', pwd , '|' , 'kinit', usr.lower()])
        return self.__manage(ret , out , err)

    def put(self , local_path , hdfs_path):
        """LLeva archivos de manera local a hdfs a una ruta especificada
        """
        (ret, out, err) = self.__run_cmd(['hdfs', 'dfs', '-put', local_path, hdfs_path])
        return self.__manage(ret , out , err)
    
    def get(self , hdfs_path , local_path ):
        """trae archivos de HDFS a una ruta local especificada
        """
        (ret, out, err) = self.__run_cmd(['hdfs', 'dfs', '-get', hdfs_path , local_path])
        return self.__manage(ret , out , err)
    
    def __lister(self , output):
        """toma el resultaqdo del comando de ls y devuelve una lista con los directorios
        otra lista con los archivos de la carpeta actual y la salida raw.
        
        """
        dirs = []
        files = []
        lst = output.split("\n")
        for l in lst:
            parts = [ x for x in l.split(" ") if x != ""]
            if len(parts) > 0:
                if parts[0][:1] == "d":
                    # es directorio
                    dirs.append(" ".join(parts[7:]))
                else:
                    files.append(" ".join(parts[7:]))
            
        return dirs , [x for x in files if x != ""] , lst
    
    def ls(self , hdfs_path , human = False):
        """Lista los archivos en una ruta de hdfs.  Devuelve la lista de python de las rutas
        """
        cmd = []
        if human:
            cmd = ['hdfs', 'dfs', '-ls', '-h' , hdfs_path]
        else:
            cmd = ['hdfs', 'dfs', '-ls', hdfs_path]
        
        (ret, out, err) = self.__run_cmd(cmd)
        oupt = self.__manage(ret , out , err)
        return self.__lister( oupt )
    
    def mkdir(self , hdfs_path ):
        """Crea una carpeta de hdfs en una ruta especificada
        """
        (ret, out, err) = self.__run_cmd(['hdfs', 'dfs', '-mkdir', hdfs_path])
        return self.__manage(ret , out , err)
    
    def chown(self , hdfs_path , owner , recursive = False):
        """Cambia el dueño de un archivo.  Solo ejecutable por el superusuario de hdfs
        se puede definir si es recursivo o no
        """
        cmd = []
        if recursive:
            cmd = ['hdfs', 'dfs', '-chown', '-R' ,owner , hdfs_path]
        else:
            cmd = ['hdfs', 'dfs', '-chown', owner, hdfs_path]
            
        (ret, out, err) = self.__run_cmd( cmd )
        return self.__manage(ret , out , err)
    
    def rm(self , hdfs_path , recursive = False , skipTrash = True):
        """Elimina el archivo en el path.  Si es recursivo pone -r al comando
        por defecto borra todo y no manda a Trash
        """
        cmd = ["hdfs","dfs","-rm" ]
        if recursive:
            cmd = cmd + ["-r"]
        if skipTrash:
            cmd = cmd + ["-skipTrash"]
        
        cmd = cmd + [hdfs_path]
            
        (ret, out, err) = self.__run_cmd( cmd )
        return self.__manage(ret , out , err)
    

    def close(self):
        if self.remote:
            self.cli.close()
        
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