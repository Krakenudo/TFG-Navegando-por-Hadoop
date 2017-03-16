# -*- coding: utf-8 -*-
from mrjob.job import MRJob
from mrjob.step import MRStep
import time

'''Importante. He realizado mi trabajo al reves. Que quiere decir eso?
Quiere decir que en lugar de hacer un programa para cada apartado, y luego
juntarlos, he hecho un unico programa que iba modificando. Con lo cual, al ver
esta mañana (jueves) que habia que entregar los problemas por separado, me he 
puesto a copiar y pegar las partes del ejercicio final que me interesaban 
para lograr los resultados deseados. Esto lo hice una vez realizados los comentarios.
Con esto, quiero pedirte que:
* No atiendas a los comentarios que leas en los ejercicios 1ejercicio,
    2ejercicio y 3ejercicio
*Los comentarios leelos del ejercicio finalejercicio'''
class MRTrabajo(MRJob):
    SORT_VALUES = True
    #En este primer mapper desmenuzamos cada linea para que nos devuelva en una
    #lista (por cada linea) lo que nos interesa. IP de clave,
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

    #Primer reducer, en el que juntaremos los valores de las sesiones
    def tf_reducer(self,IP,values):
        T=60*60 #defino T como el limite de tiempo entre sesiones. En este caso,
	#lo fijamos en 1h
        hora=0
	#Contador que utilizaremos para definir el tiempo que ha pasado
	#desde el ultimo acceso a internet. He definido que sea cada vez que
	#está sin acceder a la pagina web una hora entera, no para que loggee
	#cada vez que pasa una hora en internet, personalmente porque a mi ese
	#comportamiento de las webs me fastidia. No seria dificil modificarlo
	#para que fuese de la otra manera. Simplemente, añadiendo otro contador
	#HORA=0
        i=0 #Contador
        horas=[]
        archivos=[]

        #booleano que utilizaremos para comprobar si se repite
        #alguna hora, y evitar que se añada un duplicado. Podria hacerse con un
        #set, estoy pensando, pero cuando lo he programado no he caido en la
        #cuenta y, ahora que funciona, ¿para que cambiar algo tan nimio?

        for fecha,archivo in values:            
	    #Filtro de este 
            if (fecha-hora>=0 and fecha-hora<T):
		#Si ha transcurrido menos de una hora desde la ultima
		#consulta, añado el archivo visitado a la lista de 
		#listas de archivos vistados, en la posicion i-1
		#(que llevamos en el contador i, que nos marca la
		#sesion en la que nos encontramos)
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
		#añado el archivo, como una lista, a la lista "archivos".
		#Aparecerá como un nuevo elemento de la lista, en ultima
		#posicion, formado por [archivo1].
                archivos.append([archivo])                    
                    
        # IMPORTANTE: Como los archivos y las horas los he ido aumentando
	#------------
	#de tamaño a la vez (dentro de cada elemento de archivos habrá
	#listas, pero no se contará su tamaño sino el de "archivos"), deben
	#de tener el mismo tamaño. Lo utilizo en el siguiente bucle para
	#sacar los valores en un yield, de clave IP, valores la hora de cada
	#sesion, una lista ordenada sin repeticiones de los elementos que 
	#visita en cada sesion y el numero total de sesiones que ha tenido el
	#usuario
        for v in range(len(archivos)):
            yield IP,(horas[v][0],list(set(archivos[v])),i)
    #vemos los tiempos entre sesiones, para decir la frecuencia con la que se suele conectar
    def time_between_conn(self,key,values):
        fecha=0
        diferencia_fecha=0
        for FECHA,lista,conn_realizadas in values:
            diferencia_fecha=FECHA-fecha
            fecha=int(FECHA)
            #devuelvo la lista de archivos visitados, la IP, la fecha en
	    #segundos respecto de la ultima sesion, la fecha en la que se ha
	    #accedido a la pagina, el numero de conexiones realizadas y
	    #el numero de sesiones en el día (con valor 1, para despues sumar todos)
	    yield lista,(key,diferencia_fecha,FECHA,conn_realizadas,1)  
    
    #Con este programa voy a buscar los usuarios que han accedido al mismo archivo,
    #comportamientos parecidos. Este programa no va a hacer el trabajo definitivo,
    #de decirnos quienes visitan el mismo archivo de manera definitiva. Para el primer
    #archivo que nos llega nos sacará que el archivo ha sido visitado por el primer
    #usuario. Para el segundo archivo, por los dos primeros usuarios, y asi...

    #IMPORTANTE:
    #-----------
    #La clave es la lista de archivos visitados.
    def comport_repetidos(self,key,values):
        #Creo una lista donde iré guardando las IP's que tengan el mismo comportamiento
        LISTA_COMPORTAMIENTOS=[]
        #genero un contador
        i=0
        for (IP,time_between_conn,FECHA,conn_realizadas,repeticion_en_sesion) in values:
            if (not (IP in LISTA_COMPORTAMIENTOS)):
                #en cada vuelta, añado la IP a la lista
                LISTA_COMPORTAMIENTOS.append(IP)
                i=i+1
            else:
                i=i+1
                #y en cada vuelta devuelvo esa lista. Devuelvo como clave la IP y la lista de
                #archivos visitados. Una especie de "cada oveja con su pareja".
                #Como valores: 
                #la fecha en segundos, el numero de sesiones en el día y si se ha repetido el 
                #comportamiento alguna vez en general, la lista de los usuarios que han repetido ese 
                #comportamiento
            yield key,(IP,FECHA,time_between_conn,conn_realizadas,i-1,LISTA_COMPORTAMIENTOS)
            
    def agrupaciones(self,key,values):
        I=0	
        for IP,FECHA,time_between_conn,conn_realizadas,numero,LISTA in values:
            I=I+1
            #En este programa devuelvo lo mismo que en el anterior, solo que ahora
            #el valor I medirá las veces que el usuario j (por ejemplo) visita el
            #archivo k. Antes, "numero" nos devolvia el numero de repeticiones en
            #general. Al recorrer todos los archivos con la misma IP con el contador,
            #es sencillo contar las repeticiones de cada usuario
        yield key,(I,LISTA)

    def comportamientos(self,behaviour,values):
        #Este set lo voy a utilizar para guardar todos los usuarios que visiten
        #un determinado archivo, sin repeticiones, ordenado
        user_list=set()
        #voy a utilizar las siguientes listas para no tener listas grandes con
        #grandes elementos, sino unas pocas listas grandes con elementos mas pequeños
        USER=[]
        T_S=[]
        N_B=[]
        #Para cada valor que nos den en values
        for user,t_s,n_b,lista_usuarios in values:
            #para cada valor de la lista lista_usuarios
            for i in range(len(lista_usuarios)):
                #añado al set el usuario en concreto
                user_list.add(lista_usuarios[i])
                #añado las IP's, el numero de sesiones y el numero de repeticiones
                #(antes calculados) en sus respectivas listas
                USER.append(user)
                T_S.append(t_s)
                N_B.append(n_b)
                #al salir del bucle, tenemos 3 listas de la misma longitud y un set "final"
                #que nos interesa para devolver en forma de lista
        
        for i in range(len(USER)):
            #fin.
            yield '.-RESULTADOS-->',(behaviour,N_B[i],list(user_list))



    
    def steps(self):
        return [
	    #ordenamos con cuidado los mapper y los
	    #reducer. Con cuidado de no devolver en un
	    #mapper una IP cuando en el siguiente reducer
	    #deberia recoger una lista
            MRStep(mapper = self.tf_mapper,
                   reducer = self.tf_reducer),
            MRStep(reducer = self.time_between_conn),
            MRStep(reducer=self.comport_repetidos),
            MRStep(reducer=self.agrupaciones)
        ]

if __name__ == '__main__':
    MRTrabajo.run()