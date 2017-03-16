# -*- coding: utf-8 -*-
from mrjob.job import MRJob
from mrjob.step import MRStep
import time


class MRTrabajo(MRJob):
    SORT_VALUES = True

    def tf_mapper(self, _, line):
        line = line.split()
        #Comando utilizado para obtener cada conjunto de valores separados por espacios
        if len(line)>=2:
            IP=line[0]
            FECHA=line[3]
            Archivo=line[6]
            Exito=line[8]
            #Hago un segundo filtro, para que solo empiece a devolverme cosas
            #si el acceso a la pagina ha sido un exito. No sería excesivamente complicado
            #pedirle al programa que nos mostrase ese comportamiento final.
            if (Exito=='200'):
                #Con este codigo convierto la fecha del formato que me la han dado
                #dia/mes/ano:horas:minutos:segundos al tiempo que ha pasado desde el
                #"comienzo de la cuenta de los computadores" (alrededor de 1970) hasta
                #ahora
                fecha= time.mktime(time.strptime(FECHA,'[%d/%b/%Y:%H:%M:%S'))
                '''Devuelvo IP, la fecha a la que se ha accedido a determinado archivo y el archivo'''
                yield IP,(fecha,Archivo)

    def tf_reducer(self,IP,values):
        T=30*24*60*60 #defino T como el limite de tiempo entre sesiones.        
        hora=0
       
        i=0 #Contador
        horas=[]
        archivos=[]
        for fecha,archivo in values:            
            #Filtro de este 
            if (fecha-hora>=0 and fecha-hora<T):
                archivos[i-1].append(archivo)
            else:
                #Si ha pasado mas de una hora, actualizo el valor de
                #hora para que empiece a contar desde ahi en la 
                #siguiente vuelta
                hora=fecha
                #Como ha pasado una hora desde la ultima sesion, no
                #deberia encontrarse hora en la lista horas. La añado.
                horas.append([hora])
                #Actualizo el contador
                i=i+1
                archivos.append([archivo])
            
        for v in range(len(archivos)):
            yield (IP,list(set(archivos[v]))),(horas[v][0],i)
    def reducido(self,key,values):
        hora=0
        T=385000
        for horas,n in values:
            if horas-hora>T:
                hora=horas
                yield key[0],(key[1],horas,n)
    def steps(self):
        return [
            MRStep(mapper = self.tf_mapper,
                   reducer = self.tf_reducer),
            MRStep(reducer = self.reducido)
        ]


if __name__ == '__main__':
    tiempoInicio=time.time()
    MRTrabajo.run()
    tiempoFinal=time.time()-tiempoInicio
    print (str(tiempoFinal))
