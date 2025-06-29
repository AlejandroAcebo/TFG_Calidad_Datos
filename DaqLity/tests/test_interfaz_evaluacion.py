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

    def test_cargar_resultados_exito(self) -> None:
        self.driver.maximize_window()
        self.open("http://localhost:8501/")
        self.assert_exact_text("FORMULARIO CONEXIÓN", "#formulario-conexion")
        self.type('[aria-label="**Host**"]', "localhost")
        self.type('[aria-label="**Usuario**"]', "sa")
        self.type('[aria-label="**Contraseña**"]', "root")
        self.type('[aria-label="**Base de Datos**"]', "AdventureWorksLT2022")
        self.click('[data-testid="stBaseButton-secondary"]')
        self.sleep(20)

        self.click(
            "#root > div:nth-child(1) > div.withScreencast > div > div > div > section > div.stMainBlockContainer.block-container.st-emotion-cache-zy6yx3.e1cbzgzq4 > div > div.st-emotion-cache-13o7eu2.eertqu02 > div > div:nth-child(1) > div > div:nth-child(3) > div > button")
        self.assert_exact_text("EVALUACIÓN - ANÁLISIS DE RESULTADOS", "#evaluacion-analisis-de-resultados")

        self.wait_for_element('input[type="file"]', timeout=10)

        path1 = os.path.abspath("tests/resources/resultados_test_evaluacion1.json")
        path2 = os.path.abspath("tests/resources/resultados_test_evaluacion2.json")
        self.driver.find_element("css selector", 'input[type="file"]').send_keys(
            path1 + "\n" + path2
        )

        self.sleep(3)
        self.assert_element('div.js-plotly-plot')

    def test_volver_atras(self) -> None:
        self.driver.maximize_window()
        self.open("http://localhost:8501/")
        self.assert_exact_text("FORMULARIO CONEXIÓN", "#formulario-conexion")
        self.type('[aria-label="**Host**"]', "localhost")
        self.type('[aria-label="**Usuario**"]', "sa")
        self.type('[aria-label="**Contraseña**"]', "root")
        self.type('[aria-label="**Base de Datos**"]', "AdventureWorksLT2022")
        self.click('[data-testid="stBaseButton-secondary"]')
        self.sleep(20)

        self.click(
            "#root > div:nth-child(1) > div.withScreencast > div > div > div > section > div.stMainBlockContainer.block-container.st-emotion-cache-zy6yx3.e1cbzgzq4 > div > div.st-emotion-cache-13o7eu2.eertqu02 > div > div:nth-child(1) > div > div:nth-child(3) > div > button")
        self.assert_exact_text("EVALUACIÓN - ANÁLISIS DE RESULTADOS", "#evaluacion-analisis-de-resultados")

        self.click("#root > div:nth-child(1) > div.withScreencast > div > div > div > section > div.stMainBlockContainer.block-container.st-emotion-cache-zy6yx3.e1cbzgzq4 > div > div.st-emotion-cache-13o7eu2.eertqu02 > div > div.stColumn.st-emotion-cache-awpdtr.eertqu01 > div > div:nth-child(2) > div > button")
        self.sleep(1)
        self.assert_exact_text("DEFINIR PLAN DE CALIDAD", "#definir-plan-de-calidad")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.app_process.terminate()
        cls.app_process.wait()