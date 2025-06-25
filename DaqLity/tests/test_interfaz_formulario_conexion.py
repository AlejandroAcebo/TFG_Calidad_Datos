import os
import subprocess
from seleniumbase import BaseCase
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class PageContentTest(BaseCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.app_process = subprocess.Popen(["streamlit", "run", "UI.py"])

    def test_cargar_bd_exito(self) -> None:
        self.open("http://localhost:8501/")
        self.assert_exact_text("FORMULARIO CONEXIÓN", "#formulario-conexion")
        self.type('[aria-label="**Host**"]', "localhost")
        self.type('[aria-label="**Usuario**"]', "sa")
        self.type('[aria-label="**Contraseña**"]', "root")
        self.type('[aria-label="**Base de Datos**"]', "AdventureWorksLT2022")
        self.click('[data-testid="stBaseButton-secondary"]')
        self.sleep(20)
        # Comprobacion de que cambia de pagina
        self.assert_exact_text("DEFINIR PLAN DE CALIDAD","#definir-plan-de-calidad")
        self.assert_exact_text("GESTIÓN VISUALIZACIÓN","#gestion-visualizacion")
        self.assert_exact_text("GESTIÓN PLAN DE CALIDAD","#gestion-plan-de-calidad")

    def test_cargar_bd_fallo(self) -> None:
        self.open("http://localhost:8501/")
        self.assert_exact_text("FORMULARIO CONEXIÓN", "#formulario-conexion")
        self.type('[aria-label="**Host**"]', "localhost")
        self.type('[aria-label="**Usuario**"]', "sa")
        self.type('[aria-label="**Contraseña**"]', "fallo")
        self.type('[aria-label="**Base de Datos**"]', "AdventureWorksLT2022")
        self.click('[data-testid="stBaseButton-secondary"]')
        self.sleep(20)
        self.assert_element("div:contains('Error al conectar para análisis')")

    def test_cargar_csv(self) -> None:
        self.open("http://localhost:8501/")
        self.assert_exact_text("FORMULARIO CONEXIÓN", "#formulario-conexion")

        wait = WebDriverWait(self.driver, 10)
        dropdown_xpath_fuente_datos = '//*[@id="root"]/div[1]/div[1]/div/div/div/section/div[1]/div/div[4]/div/div[2]/div/div/div/div[2]/div'
        dropdown_fuente_datos = wait.until(EC.element_to_be_clickable((By.XPATH, dropdown_xpath_fuente_datos)))
        dropdown_fuente_datos.click()

        opcion_xpath_csv = "//div[contains(text(),'Archivo CSV')]"
        opcion_csv = wait.until(EC.element_to_be_clickable((By.XPATH, opcion_xpath_csv)))
        opcion_csv.click()

        self.wait_for_element('input[type="file"]', timeout=10)
        path = os.path.abspath("tests/resources/datos_prueba.csv")
        self.choose_file('input[type="file"]', path)
        self.sleep(20)
        # Comprobacion de que cambia de pagina
        self.assert_exact_text("DEFINIR PLAN DE CALIDAD", "#definir-plan-de-calidad")
        self.assert_exact_text("GESTIÓN VISUALIZACIÓN", "#gestion-visualizacion")
        self.assert_exact_text("GESTIÓN PLAN DE CALIDAD", "#gestion-plan-de-calidad")

    def test_cargar_json(self) -> None:
        self.open("http://localhost:8501/")
        self.assert_exact_text("FORMULARIO CONEXIÓN", "#formulario-conexion")

        wait = WebDriverWait(self.driver, 10)
        dropdown_xpath_fuente_datos = '//*[@id="root"]/div[1]/div[1]/div/div/div/section/div[1]/div/div[4]/div/div[2]/div/div/div/div[2]/div'
        dropdown_fuente_datos = wait.until(EC.element_to_be_clickable((By.XPATH, dropdown_xpath_fuente_datos)))
        dropdown_fuente_datos.click()

        opcion_xpath_json = "//div[contains(text(),'Archivo JSON')]"
        opcion_json = wait.until(EC.element_to_be_clickable((By.XPATH, opcion_xpath_json)))
        opcion_json.click()

        self.wait_for_element('input[type="file"]', timeout=10)
        path = os.path.abspath("tests/resources/datos_prueba.json")
        self.choose_file('input[type="file"]', path)
        self.sleep(20)
        # Comprobacion de que cambia de pagina
        self.assert_exact_text("DEFINIR PLAN DE CALIDAD", "#definir-plan-de-calidad")
        self.assert_exact_text("GESTIÓN VISUALIZACIÓN", "#gestion-visualizacion")
        self.assert_exact_text("GESTIÓN PLAN DE CALIDAD", "#gestion-plan-de-calidad")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.app_process.terminate()
        cls.app_process.wait()