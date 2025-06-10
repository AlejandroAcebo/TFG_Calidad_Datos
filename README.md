# Trabajo de Fin de Grado : Alejandro Acebo

En este trabajo se ha persiguido el desarrollo de una herramienta para analizar la calidad de los datos con objeto de que usuarios no expertos en la programación puedan poner bajo análisis la calidad de sus datos.

<<<<<<< HEAD
## Estructura del Proyecto
=======
## 📁 Estructura del Proyecto
>>>>>>> refs/remotes/origin/main

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

##  DaqLity: Herramienta para el Análisis de la Calidad de los Datos

**DaqLity** es una herramienta interactiva desarrollada como parte de un Trabajo de Fin de Grado (TFG) en Ingeniería Informática. Su objetivo principal es facilitar el análisis y la visualización de la calidad de conjuntos de datos, empleando tecnologías como Apache Spark y Streamlit.

La aplicación permite detectar posibles problemas de calidad en los datos, presentar información sobre las diferentes dimensiones de la calidad del dato y ofrecer una interfaz sencilla para su uso por parte de analistas y científicos de datos no exportos en programación.

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

### 3. Crear un entorno virtual (opcional pero recomendable)

Se recomienda crear un entorno virtual para aislar las dependencias del proyecto:

```bash
python3 -m venv venv
source venv/bin/activate
```

### 4. Instalar las dependencias necesarias

Ejecuta los siguientes comandos para actualizar el sistema, instalar Java y configurar las variables de entorno, y finalmente instalar las dependencias de Python necesarias:

```bash
sudo apt update
sudo apt install openjdk-17-jdk
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
pip install streamlit
pip install pyspark==3.5.0
pip install --upgrade setuptools wheel
```

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

