from py4j.protocol import NULL_TYPE
from pyspark.sql import SparkSession

#Variables globales
global totalColumnas
global totalVacio

#Este metodo se encargara, de leer los archivos a analizar
def cogerDatos():
    spark = SparkSession.builder.getOrCreate()
    rutaArchivo = "data/prueba.csv"

    df = spark.read.option("header", True).option("delimiter", ";").csv(rutaArchivo)
    df.show()
    #Eleccion para filtrar por columna o archivo completo
    eleccion = input("¿Quieres comprobar una columna (1) o el archivo entero (2): ")
    if int(eleccion) == 1:
        columna = input("Ingresa la columna del dato a comprobar: ")
        comprobarCompletitud(columna,df)
    elif int(eleccion) == 2:
        comprobarCompletitudArchivo(df)



#Comprueba la cantidad de filas que esa columna esta llena frente a las totales, mostrando el porcentaje de completitud
# que tiene esa columna
def comprobarCompletitud(columna,df):
    df_filtrado = df.filter(df[columna] != '')
    print(df_filtrado.count())
    print("La completitud es del ", (df_filtrado.count() / df.count()) * 100, "%")

def comprobarCompletitudArchivo(df):
    totalColumnas = 0
    totalVacio = 0
    df.foreach(comprobarFila('',df))
    print("La completitud del archivo es del ", ((totalColumnas - totalVacio)/totalColumnas) * 100, "%")

#Pendiente arreglar
def comprobarFila(fila,df):
    for columna in df.columns:
        print(columna)
        valor = fila[columna]
        if valor is None:
            totalVacio += 1
        totalColumnas += 1


#Main
def main():
    cogerDatos()

if __name__ == "__main__":
    main()
