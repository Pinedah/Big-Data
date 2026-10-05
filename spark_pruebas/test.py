# PRÁCTICA 1: Introducción a Apache Spark 
# Parte 1: Conceptos Básicos (Sesión, DataFrames, Transformaciones y Acciones) 
# ======================================================== 
import sys 
from pyspark.sql import SparkSession 
# ----------------------------------------------------------------------------- 
# 1. INICIALIZAR LA SPARKSESSION (El "Motor") 
# ----------------------------------------------------------------------------- 
# En un script de Python, la sesión no existe por defecto. Debemos crearla. 
# .master("local[*]") indica que usaremos todos los núcleos de TU procesador. 
print(">>> Iniciando SparkSession...") 
spark = SparkSession.builder.appName("Practica1_Intro").master("local[*]").getOrCreate() 
# Ajustamos el nivel de logs para que no salgan tantas letras rojas de advertencia 
spark.sparkContext.setLogLevel("ERROR") 
print(">>> Sesión iniciada correctamente.") 
# ----------------------------------------------------------------------------- 
# 2. CREAR UN DATAFRAME (Estructura de Datos Distribuida) 
# ----------------------------------------------------------------------------- 
# Creamos un rango de números del 0 al 999. 
# .toDF("number") asigna el nombre "number" a la única columna. 
print("\n>>> Creando DataFrame con rango de 0 a 999...") 
myRange = spark.range(1000).toDF("number") 
# ACCIÓN: .show() 
# Mostramos los primeros 5 registros para verificar que se creó bien. 
print("--- Muestra de los primeros 5 números ---") 
myRange.show(5) 
# ----------------------------------------------------------------------------- 
# 3. TRANSFORMACIÓN (Inmutabilidad y Lazy Evaluation) 
# ----------------------------------------------------------------------------- 
# Queremos solo los números PARES. 
# Spark NO modifica 'myRange'. Crea un NUEVO DataFrame llamado 'divisBy2'. 
# La operación es "Lazy" (Perezosa): Aquí Spark solo planea, no ejecuta todavía. 
print(">>> Aplicando Transformación: Filtrar números pares (number % 2 = 0)...") 
divisBy2 = myRange.where("number % 2 = 0") 
# ----------------------------------------------------------------------------- 
# 4. ACCIÓN (Ejecución) 
# ----------------------------------------------------------------------------- 
# Para obtener un resultado real, necesitamos una acción. 
# .count() obliga a Spark a recorrer los datos, filtrar y contar. 
print(">>> Ejecutando Acción: Contar registros resultantes...") 
total_pares = divisBy2.count() 
# Imprimimos el resultado final en la consola 
print("------------------------------------------------") 
print(f" RESULTADO FINAL: Se encontraron {total_pares} números pares.") 
print("------------------------------------------------") 
# Opcional: Ver los primeros 5 pares encontrados 
print("--- Muestra de los números pares encontrados ---") 
divisBy2.show(5) 
# ----------------------------------------------------------------------------- 
# 5. FINALIZAR 
input()
# ----------------------------------------------------------------------------- 
# Es buena práctica detener la sesión para liberar memoria y recursos. 
print(">>> Deteniendo SparkSession...") 
spark.stop()