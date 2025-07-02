# Trabajo de Fin de Grado : Alejandro Acebo

En este trabajo se ha persiguido el desarrollo de una herramienta para analizar la calidad de los datos con objeto de que usuarios no expertos en la programación puedan poner bajo análisis la calidad de sus datos.

# 📚 Índice

1. [📁 Estructura del Proyecto](#-estructura-del-proyecto)  
2. [🛠️ DaqLity: Herramienta para el Análisis de la Calidad de los Datos](#daqlity-herramienta-para-el-análisis-de-la-calidad-de-los-datos)  
3. [🖼️ Imágenes de la herramienta](#imágenes-de-la-herramienta)  
4. [⚙️ Instalación y Puesta en Marcha](#️-instalación-y-puesta-en-marcha)  
5. [🧪 Documentación: Tests de Calidad de Datos](#-documentación-tests-de-calidad-de-datos)


## 📁 Estructura del Proyecto

```plaintext
TFG_Calidad_Datos/
├── Base de datos PostgreSQL/                # Base de datos para evaluación práctica
│   ├── ScriptFinal                          # Script de creación de base de datos e introuducción de datos
├── DaqLity/                                 # Herramienta desarrollada, posteriormente se explica su instalación
│   ├── ...
├── Desarrollo-GreatExpectations/            # Herramienta Great-Expectations analizada en la evaluación práctica
│   ├── great-expectations
|   |   ├── evaluacion-practica-ge.py        # Desarrollo de test para analizar la calidad de datos en Great-Expectations
|   |   ├── ...
│   ├── ...
|── Desarrollo-PyDeequ/
|   |── evaluacion-practica-pydeequ.py       # Desarrollo de test para analizar la cadlidad de datos en PyDeequ
|── README.me                                # Documentación del proyecto
└── conjunto_test_plan_de_calidad.josn       # Conjunto de pruebas definidas en el plan de calidad recogidas en archivo JSON
```

## 🛠️ DaqLity: Herramienta para el Análisis de la Calidad de los Datos

**DaqLity** es una herramienta interactiva desarrollada como parte de un Trabajo de Fin de Grado (TFG) en Ingeniería Informática. Su objetivo principal es facilitar el análisis y la visualización de la calidad de conjuntos de datos, empleando tecnologías como Apache Spark y Streamlit.

La aplicación permite detectar posibles problemas de calidad en los datos, presentar información sobre las diferentes dimensiones de la calidad del dato y ofrecer una interfaz sencilla para su uso por parte de analistas y científicos de datos no exportos en programación.

---

## 🖼️ Imágenes de la herramienta
![Formulario conexión fuente de datos](https://i.imgur.com/S8gJLGU.png)

![Interfaz principal herramienta](https://i.imgur.com/DKDGOxY.png)

![Visualización de la evolución de varios análisis](https://i.imgur.com/qsJbzfj.png)

---
## ⚙️ Instalación y Puesta en Marcha

### 1. Clonar el repositorio

Para obtener una copia local del proyecto, ejecutar:

```bash
git clone https://github.com/AlejandroAcebo/TFG_Calidad_Datos/
```

### 2. Acceder al directorio de la herramienta

Después de clonar el repositorio, accede al directorio principal de la herramienta:

```bash
cd ./TFG_Calidad_Datos/DaqLity
```

### OPCIÓN UBUNTU
### 3. Crear un entorno virtual (opcional pero recomendable)

Se recomienda crear un entorno virtual para aislar las dependencias del proyecto:

```bash
python3 -m venv venv
source venv/bin/activate
```

### 3.1. Instalar las dependencias necesarias

Ejecuta los siguientes comandos para actualizar el sistema, instalar Java y configurar las variables de entorno, y finalmente instalar las dependencias de Python necesarias:

```bash
sudo apt update
sudo apt install openjdk-17-jdk
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
pip install streamlit
pip install pyspark==3.5.0
pip install --upgrade setuptools wheel
pip install plotly
pip install pydeequ
pip install findspark
```

Descarga de los conectores jdbc de bases relacionales: PostgreSQL, MySQL, SQL Server y MariaDB
```bash
mkdir jars && cd jars/
wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.30/mysql-connector-java-8.0.30.jar
wget https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.2.0.jre8/mssql-jdbc-12.10.0.jre8.jar
wget https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/3.1.4/mariadb-java-client-3.1.4.jar

```

### OPCIÓN WINDOWS
### 4. Instalar dependencias necesarias

Guía de Instalación y Configuración de Java, Spark y Hadoop en Windows


Instalar Java JDK 17
```bash
Descargar desde:
https://www.oracle.com/java/technologies/javase/jdk17-archive-downloads.html

Instalar en:
C:\Program Files\Java\jdk-17

Configurar variables de entorno:
JAVA_HOME → C:\Program Files\Java\jdk-17

Agregar a Path:
%JAVA_HOME%\bin
```

Instalar Apache Spark
```bash
Descargar desde:
https://spark.apache.org/downloads.html

Versión: Spark 3.5.0
Package: Pre-built for Apache Hadoop 3

Descomprimir en:
C:\spark\spark-3.5.0-bin-hadoop3

Configurar variables de entorno:
SPARK_HOME → C:\spark\spark-3.5.0-bin-hadoop3

Agregar a Path:
%SPARK_HOME%\bin
```

Instalar winutils.exe (soporte Hadoop en Windows)

```bash
Crear carpeta:
C:\hadoop\bin

Descargar winutils.exe compatible con Hadoop 3.x:
https://github.com/cdarlint/winutils

Copiar winutils.exe en:
C:\hadoop\bin

Configurar variable de entorno:
HADOOP_HOME → C:\hadoop

Agregar a Path:
%HADOOP_HOME%\bin
```

Una vez instalado todo esto reiniciar la terminal y acceder desde tu IDE de python preferido a la terminal y ejecutar:

```bash
pip install streamlit
pip install pyspark==3.5.0
pip install --upgrade setuptools wheel
pip install plotly
pip install pydeequ
pip install findspark
```

Seguido instalar los archivos jar de los jdbc necesarios y meterlos en una carpeta llamada "jars" dentro del proyecto, que habrá que crear:

```bash

mkdir jars && cd jars
https://github.com/microsoft/mssql-jdbc/releases/download/v12.10.0/mssql-jdbc-12.10.0.jre8.jar
https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.2.0.jre8/mssql-jdbc-12.10.0.jre8.jar
https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/3.1.4/mariadb-java-client-3.1.4.jar
```

# Configuración Manual de TCP/IP en SQL Server mediante el Registro de Windows

### 4.1. Abrir el Editor del Registro

- Pulsa `Win + R` para abrir la ventana **Ejecutar**.  
- Escribe `regedit` y presiona **Enter** para abrir el **Editor del Registro**.

### 4.2. Navegar a la clave de SQL Server

- En el panel izquierdo, navega a la siguiente ruta:

```
HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Microsoft SQL Server\
```

- Dentro de esta clave encontrarás carpetas con nombres similares a:

  - `MSSQL15.SQLEXPRESS`  
  - `MSSQL16.MSSQLSERVER`

- Identifica cuál corresponde a tu instancia de SQL Server.

### 4.3. Configurar TCP/IP para la instancia

- Entra en la ruta:

```
[Tu instancia]\MSSQLServer\SuperSocketNetLib\Tcp
```

Ejemplo para la instancia por defecto SQL Server 2022:

```
HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQLServer\SuperSocketNetLib\Tcp
```

- Modifica las siguientes entradas:

| Clave            | Acción                                       |
|------------------|---------------------------------------------|
| `Enabled`        | Cambiar el valor a `1` (habilitar TCP/IP)  |
| `TcpPort`        | Establecer el puerto a `1433`                |
| `TcpDynamicPorts`| Dejar en blanco (vacío)                      |

- Realiza estos cambios en la sección **IPAll**, y si existen, también en **IP1** o **IP2**.

### 4.4. Cerrar el Editor del Registro

- Cierra el editor de registro para guardar los cambios.

### 4.5. Reiniciar el servicio de SQL Server

- Pulsa `Win + R`, escribe `services.msc` y presiona **Enter**.  
- En la ventana **Servicios**, busca el servicio de SQL Server correspondiente a tu instancia:

  - Por ejemplo:  
    `SQL Server (MSSQLSERVER)` para la instancia por defecto.  
    `SQL Server (SQLEXPRESS)` para una instancia nombrada.

- Haz clic derecho sobre el servicio y selecciona **Reiniciar**.

---

### 5. Ejecutar la herramienta

Para iniciar la aplicación, ejecuta el siguiente comando en el directorio de la herramienta:

```bash
streamlit run UI.py
```

### 6. Acceder a la interfaz web

Al ejecutar la herramienta, en la terminal aparecerán dos URLs:

- **Local URL:** permite acceder desde el navegador del equipo local, por ejemplo:
_http://localhost:8501_

- **Network URL:** Esta URL permite acceder a la aplicación desde otro dispositivo que esté conectado a la misma red local. Por ejemplo:
_http://192.168.X.X:8501_


## 🧪 Documentación: Tests de Calidad de Datos

A continuación se describe el significado y un ejemplo de cada tipo de test de calidad que dispone la herramienta:

### ✅ 1. Completitud
Verifica que no haya datos faltantes en uan columna determinada.  
**Ejemplo:** Asegura que todas las filas tengan un valor en la columna `ID_Cliente`.

### 🔍 2. Credibilidad
Evalúa si una columna contiene valores dentro de los esperados.
**Ejemplo:** Un código postal solo puede tener 5 cifras o 5 + 4 cifras.

### 📅 3. Actualidad
Comprueba si los datos están actualizados en función de un rango de tiempo esperado. Esta actualidad se puede analizar por fecha o fecha y hora.
**Ejemplo:** Una fecha de última actualización mayor a 12 meses.

### 🔗 4. Integridad Referencial
Asegura que las relaciones entre tablas o entidades estén completas y sean coherentes.  
**Ejemplo:** Un `ID_Producto` en la tabla de ventas debe existir en la tabla de productos.

### 🧾 5. Exactitud Sintáctica
Valida que los datos cumplan con el formato o patrón esperado.  
**Ejemplo:** Correos electrónicos deben seguir el patrón `nombre@dominio.com`.

### 🧠 6. Exactitud Semántica
Evalúa si los valores son lógicos y tienen sentido en su contexto.  
**Ejemplo:** El tipo de envío puede ser de un particular o de un profesional.

### 🎯 7. Precisión
Verifica el nivel de detalle o granularidad de los datos. 
**Ejemplo:** El peso de un envío debe tener al menos 2 decimales.



