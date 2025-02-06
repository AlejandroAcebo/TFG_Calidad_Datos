import os
os.environ["SPARK_VERSION"] = "3.5"

from pydeequ.analyzers import *

#Clase
class ComprobarParametros:

    def __init__(self):
        #Creacion de sesion de spark
        self.spark = (SparkSession.builder
                 .appName("Prueba completitud CSV")
                 .config("spark.jars.packages", "com.amazon.deequ:deequ:2.0.7-spark-3.5")
                 .getOrCreate())

        #Carga del dataframe
        rutaCSV= "data/prueba.csv"
        self.df = self.spark.read.option("header", True).option("delimiter", ";").csv(rutaCSV)

        #Mostrar dataframe, cuando crezca la base de datos entonces eliminar para no sobrecargar
        self.df.show()

        #Prueba, esto se sustituira por un apartado gráfico en el que se integrara de manera que sea mas visible
        lista = list()
        lista.insert(0,"ID")
        lista.insert(1,"Nombre")
        self.comprobarCompletitud(lista)


    #Se le pasa una lista de columna/s a revisar la completitud
    def comprobarCompletitud(self,lista):
        if not lista:
            return False

        analisisResultado = (AnalysisRunner(self.spark)
                             .onData(self.df)
                             .addAnalyzer(Size()))
        for columna in lista:
            analisisResultado = analisisResultado.addAnalyzer(Completeness(columna))

        resultados = analisisResultado.run()
        analisisResultado_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, resultados)
        analisisResultado_df.show()
        return True

if __name__ == "__main__":
    ComprobarParametros()
