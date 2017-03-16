from __future__ import print_function
 
import time
import datetime
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
        return (IP),(horas[v],list(set(archivos[v])),v)


        
if __name__=="__main__":
    tiempoInicio=time.time()
    if len(sys.argv)!=2:
        print("Usage:wordcount <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="Contar IPs")
    ips = sc.textFile(sys.argv[1],1).map(lambda x: x.split(' '))#.map(broadcast)

    ipsGood = ips.filter(lambda x: x[7]==('200')).sortBy(lambda x: x[3]).map(lambda x: (fecha_de_sesion(x))).groupByKey()

    ipsGoods = ipsGood.map(lambda x: accesos_en_sesion(x[0],x[1]))
    
    print ("c1")
    ipS=ipsGoods.collect()
    ipsAgrupadas=ipsGoods.groupByKey().count()
    print ("c2")
    
    
    # x.split(' ')[0] := IP
    # x.split(' ')[3] := FECHA en formato[03/Aug/1995:22:49:15
    # x.split(' ')[6] := WEB que solicita /images/MOSAIC-logosmall.gif"
    # x.split(' ')[7] := Exito o no
    ts =  datetime.datetime.fromtimestamp(time.time()).strftime("%Y%m%d%H%M%S")
    ipsGoods.saveAsTextFile(ts+"_salida")
    #for x in ipS:
         #print (":"+str(x)+":")
         #print (":"+str(x[0])+":-->: "+str(len(x[1][1]))+":")
    
    #print (":"+str(ipsAgrupadas)+":")
        #print (":" + str(ipsGoods.count())+":")
    tiempoFinal=time.time()-tiempoInicio
    hor=(int(tiempoFinal/3600))
    minu=int((tiempoFinal-hor*3600)/60)
    seg=tiempoFinal-((hor*3600)+(minu*60))
    print(str(hor)+"h:"+str(minu)+"m:"+str(seg)+"s")
    sc.stop()
