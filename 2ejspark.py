from __future__ import print_function
 
import time
import sys
#import logging
#import sqsloghander

from operator import add
 
from pyspark import SparkConf, SparkContext

import defIps

def fecha_de_sesion(datos):
    IP=datos[0]
    FECHA=datos[3]
    archivo = datos[6]
    if (len(datos)>2):
        fecha= time.mktime(time.strptime(FECHA,'[%d/%b/%Y:%H:%M:%S'))
        return (IP,(fecha,archivo))
        


def accesos_en_sesion(IP,valores):
    T=24*60*60 #Defino 1d como el tiempo entre sesiones para separar comportamientos de los usuarios
    hora=0
    i=0
    horas = []
    archivos = []
    for (fecha, archivo) in valores:
        if (fecha-hora>=0 and fecha-hora<T):
            archivos[i-1].append(archivo)
        else:
            hora=fecha
            horas.append(fecha)
            i=i+1
            archivos.append([archivo])
    for v in range(len(archivos)):
        return (IP,tuple((horas[v],list(set(archivos[v])),v+1)))


def tiempo_entre_conexiones(IP,valores):
    fecha=0
    diferencia_fecha=0
    for fecha_dada,lista,conn_realizadas in valores:
        diferencia_fecha=int(fecha_dada)-fecha
        fecha=int(fecha_dada)
        return ((IP,lista),tuple((diferencia_fecha,fecha_dada,conn_realizadas,1)))
        

def comportamientos_repetidos(clave,valores):
    i=0
    clAve=list(clave)
    vAlores=tuple(valores)
    #return valores
    for diferencia_fecha,fecha_dada,conn_realizadas,conn in valores:
        i=i+1
    return ((clAve[0]),(clAve[1],i))
    
        
        
if __name__=="__main__":
    tiempoInicio=time.time()
    if len(sys.argv)!=2:
        print("Usage:wordcount <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="Contar IPs") 
    ips = sc.textFile(sys.argv[1],1).map(lambda x: x.split(' ')) 
    ipsGood = ips.filter(lambda x: x[7]==('200')).sortBy(lambda x: x[3]).map(lambda x: (fecha_de_sesion(x))).groupByKey() 
    ips2 = ipsGood.map(lambda x: accesos_en_sesion(x[0],x[1])) 
    ips3 = ips2.groupByKey() 
    ips4 = ips3.map(lambda x: tiempo_entre_conexiones(x[0],x[1])).groupByKey()
    ips41= ips3.map(lambda x: tiempo_entre_conexiones(x[0],x[1])).collect()
    ips5 = ips4.map(lambda x: comportamientos_repetidos(x[0],x[1])).reduceByKey(add)
    ips6 = ips5.collect()
    ips5.saveAsTextFile("salida-ejemplo2")
    for x in ips6:
         print (":"+str(x)+":")
    
    
    tiempoFinal=time.time()-tiempoInicio
    print(str(tiempoFinal))
    sc.stop()
