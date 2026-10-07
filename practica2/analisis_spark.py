import os
import sys

# Configurar intérprete de Python para workers en Windows
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def main():
    print("=" * 60)
    print("1. CONFIGURACIÓN E INICIALIZACIÓN DE SPARK")
    print("=" * 60)

    # 1. Configuración: Inicializar SparkSession con el nombre 'Análisis Censo'
    spark = SparkSession.builder \
        .appName("Análisis Censo") \
        .master("local[*]") \
        .getOrCreate()

    # Ajustar nivel de logs para evitar advertencias en consola
    spark.sparkContext.setLogLevel("ERROR")

    print(f"SparkSession creada con éxito.")
    print(f" - Nombre de la aplicación: {spark.sparkContext.appName}")
    print(f" - Versión de Spark: {spark.version}")
    print(f" - Master: {spark.sparkContext.master}")

    print("\n" + "=" * 60)
    print("2. INGESTA DE DATOS")
    print("=" * 60)

    # 2. Ingesta de Datos: lectura con detección de encabezados e inferencia de esquema
    csv_file = "adult.csv"
    df = spark.read.csv(csv_file, header=True, inferSchema=True)
    print(f"Dataset '{csv_file}' cargado correctamente.")

    print("\n" + "=" * 60)
    print("3. EXPLORACIÓN INICIAL")
    print("=" * 60)

    # 3.1 Imprimir el esquema del DataFrame para verificar tipos de datos
    print("--- Esquema del DataFrame ---")
    df.printSchema()

    # 3.2 Contar el número total de registros cargados
    total_registros = df.count()
    total_columnas = len(df.columns)
    print(f"Total de registros cargados: {total_registros:,}")
    print(f"Total de columnas: {total_columnas}")

    print("\n--- Primeros 5 registros ---")
    df.show(5, truncate=False)

    print("\n" + "=" * 60)
    print("4. ANÁLISIS DEMOGRÁFICO")
    print("=" * 60)

    # 4.1 Top 10 Ocupaciones
    print("--- 4.1 Top 10 Ocupaciones más comunes ---")
    top_10_ocupaciones = df.groupBy("occupation") \
        .count() \
        .orderBy(F.col("count").desc()) \
        .limit(10)
    top_10_ocupaciones.show(truncate=False)

    # 4.2 Trabajo Duro: Promedio de horas trabajadas por semana agrupado por género
    print("--- 4.2 Promedio de horas trabajadas por semana por género ---")
    promedio_horas_genero = df.groupBy("gender") \
        .agg(F.round(F.avg("hours-per-week"), 2).alias("promedio_horas_semanales")) \
        .orderBy(F.col("promedio_horas_semanales").desc())
    promedio_horas_genero.show()

    # 4.3 Fuga de Cerebros: Personas cuyo país de origen sea Mexico
    print("--- 4.3 Fuga de Cerebros: Personas originarias de México ---")
    personas_mexico = df.filter(F.col("native-country") == "Mexico")
    total_mexicanos = personas_mexico.count()
    print(f"Total de personas de México: {total_mexicanos:,}")
    personas_mexico.show(10, truncate=False)

    print("\n" + "=" * 60)
    print("5. PERSISTENCIA DE DATOS")
    print("=" * 60)

    # 5. Persistencia de Datos: Guardar Top 10 Ocupaciones como 'top10.csv'
    nombre_estudiante = "Francisco_Pineda"
    carpeta_salida = f"Reporte_Ocupaciones_{nombre_estudiante}"
    os.makedirs(carpeta_salida, exist_ok=True)

    ruta_archivo = os.path.join(carpeta_salida, "top10.csv")
    top_10_ocupaciones.toPandas().to_csv(ruta_archivo, index=False)

    print(f"Top 10 Ocupaciones guardado exitosamente en: '{ruta_archivo}'")
    if os.path.exists(carpeta_salida):
        for f in os.listdir(carpeta_salida):
            ruta = os.path.join(carpeta_salida, f)
            print(f" - {f} ({os.path.getsize(ruta)} bytes)")

    print("\n" + "=" * 60)
    print("6. CIERRE DE LA SPARK SESSION")
    print("=" * 60)
    input()
    spark.stop()
    print("SparkSession finalizada con éxito.")

if __name__ == "__main__":
    main()
