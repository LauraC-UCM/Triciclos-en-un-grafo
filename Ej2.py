'''
Considera que los datos, es decir, la lista de las aristas, no se encuentran en un único fichero sino en muchos. 
Escribe un programa paralelo que calcule los 3-ciclos de un grafo que se encuentra denido en múltiples cheros de entrada.
'''

import sys
from pyspark import SparkContext

spc = SparkContext()


# Ordena alfabeticamente las lineas del fichero, que corresponden a las aristas
def ordena_aristas(line):
    arista = line.strip().split(',')
    vert1  = arista[0]
    vert2  = arista[1]

    if vert1 < vert2:
        return (vert1, vert2)
    
    elif vert1 > vert2:
         
        return (vert2, vert1)
    
    else:
        pass 
        



# Genera la lista correspondiente de los que existen y los pendientes
def genera_lista(tupla): # tupla => (nodo, lista de adyacencia)
    result = []
    for i in range(len(tupla[1])):
        result.append(((tupla[0], tupla[1][i]), 'Existen')) 

        for j in range(i + 1, len(tupla[1])):

            if tupla[1][i] < tupla[1][j]:
                result.append(((tupla[1][i], tupla[1][j]), ('Ptes', tupla[0])))

            else:
                result.append(((tupla[1][j], tupla[1][i]), ('Ptes', tupla[0])))

    return result


# Comprueba si la tupla tiene forma de triciclo  
def filtro(tupla):
    return (len(tupla[1]) >= 2 and 'Existen' in tupla[1])



#Genera la terna de un triciclo    
def crea_ternas(tupla):
    result = []
    for elem in tupla[1]:
        if elem != 'Existen':
            result.append((elem[1], tupla[0][0], tupla[0][1]))

    return result


    
    
def sol(spc, filelist):
    # Halla el rdd con las aristas del grafo  
    rdd = spc.parallelize([])
    for file in filelist:
        file_rdd = spc.textFile(file)
        rdd = rdd.union(file_rdd)

    adjuntas  = rdd.map(ordena_aristas).filter(lambda x: x is not None).distinct()
    asociados = adjuntas.groupByKey().mapValues(list).flatMap(genera_lista)
    triciclos = asociados.groupByKey().mapValues(list).filter(filtro).flatMap(crea_ternas)
    
    print(triciclos.collect())
    
    return triciclos.collect()



if __name__ == "__main__":
    lista = []
    if len(sys.argv) <= 2:
        print(f"Funcionamiento: python3 {0} <file>")

    else:
        for i in range(len(sys.argv)):
            if i != 0:
                lista.append(sys.argv[i])

        sol(spc, lista)