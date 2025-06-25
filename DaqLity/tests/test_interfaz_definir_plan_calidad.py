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

    def test_definir_eliminar_test_exito(self) -> None:
        self.driver.maximize_window()
        self.open("http://localhost:8501/")
        self.assert_exact_text("FORMULARIO CONEXIÓN", "#formulario-conexion")
        self.type('[aria-label="**Host**"]', "localhost")
        self.type('[aria-label="**Usuario**"]', "sa")
        self.type('[aria-label="**Contraseña**"]', "root")
        self.type('[aria-label="**Base de Datos**"]', "AdventureWorksLT2022")
        self.click('[data-testid="stBaseButton-secondary"]')
        self.sleep(5)

        # Comprobacion de que cambia de pagina
        self.assert_exact_text("DEFINIR PLAN DE CALIDAD","#definir-plan-de-calidad")
        self.type('[aria-label="**Nombre del plan de calidad:**"]',"PruebaPlanCalidad")

        wait = WebDriverWait(self.driver, 10)
        dropdown_xpath_esquema = '//*[@id="root"]/div[1]/div[1]/div/div/div/section/div[1]/div/div[4]/div/div[2]/div/div[3]/div'
        dropdown_esquema = wait.until(EC.element_to_be_clickable((By.XPATH, dropdown_xpath_esquema)))
        dropdown_esquema.click()

        opcion_xpath_esquema = "//div[contains(text(),'SalesLT')]"
        opcion_esquema = wait.until(EC.element_to_be_clickable((By.XPATH, opcion_xpath_esquema)))
        opcion_esquema.click()

        self.sleep(1)

        dropdown_xpath_tabla = '//*[@id="root"]/div[1]/div[1]/div/div/div/section/div[1]/div/div[4]/div/div[2]/div/div[4]/div'
        dropdown_tabla = wait.until(EC.element_to_be_clickable((By.XPATH, dropdown_xpath_tabla)))
        dropdown_tabla.click()

        opcion_xpath_tabla = "//div[contains(text(),'Customer')]"
        opcion_tabla = wait.until(EC.element_to_be_clickable((By.XPATH, opcion_xpath_tabla)))
        opcion_tabla.click()

        self.sleep(1)

        dropdown_xpath_columna = '//*[@id="root"]/div[1]/div[1]/div/div/div/section/div[1]/div/div[4]/div/div[2]/div/div[5]/div'
        dropdown_columna = wait.until(EC.element_to_be_clickable((By.XPATH, dropdown_xpath_columna)))
        dropdown_columna.click()

        opcion_xpath_columna = "//div[contains(text(),'MiddleName')]"
        opcion_columna = wait.until(EC.element_to_be_clickable((By.XPATH, opcion_xpath_columna)))
        opcion_columna.click()

        self.sleep(1)

        dropdown_xpath_tipo_test = '//*[@id="root"]/div[1]/div[1]/div/div/div/section/div[1]/div/div[4]/div/div[2]/div/div[6]/div'
        dropdown_tipo_test = wait.until(EC.element_to_be_clickable((By.XPATH, dropdown_xpath_tipo_test)))
        dropdown_tipo_test.click()

        opcion_xpath_tipo_test = "//div[contains(text(),'Completitud')]"
        opcion_tipo_test = wait.until(EC.element_to_be_clickable((By.XPATH, opcion_xpath_tipo_test)))
        opcion_tipo_test.click()

        self.sleep(1)

        self.click("#root > div:nth-child(1) > div.withScreencast > div > div > div > section > div.stMainBlockContainer.block-container.st-emotion-cache-zy6yx3.e1cbzgzq4 > div > div.st-emotion-cache-13o7eu2.eertqu02 > div > div:nth-child(2) > div > div.st-emotion-cache-13o7eu2.eertqu02 > div > div:nth-child(1) > div > div > div > button")
        self.assert_element("//*[contains(., 'guardada correctamente')]")
        self.sleep(3)

        self.click("#root > div:nth-child(1) > div.withScreencast > div > div > div > section > div.stMainBlockContainer.block-container.st-emotion-cache-zy6yx3.e1cbzgzq4 > div > div.st-emotion-cache-13o7eu2.eertqu02 > div > div:nth-child(2) > div > div.st-emotion-cache-13o7eu2.eertqu02 > div > div:nth-child(2) > div > div > div > button")
        self.assert_element("//*[contains(., 'Ultimo test eliminado correctamente.')]")
        self.sleep(1)

    def test_ir_home(self) -> None:
        self.driver.maximize_window()
        self.open("http://localhost:8501/")
        self.assert_exact_text("FORMULARIO CONEXIÓN", "#formulario-conexion")
        self.type('[aria-label="**Host**"]', "localhost")
        self.type('[aria-label="**Usuario**"]', "sa")
        self.type('[aria-label="**Contraseña**"]', "root")
        self.type('[aria-label="**Base de Datos**"]', "AdventureWorksLT2022")
        self.click('[data-testid="stBaseButton-secondary"]')
        self.sleep(5)

        self.click("#root > div:nth-child(1) > div.withScreencast > div > div > div > section > div.stMainBlockContainer.block-container.st-emotion-cache-zy6yx3.e1cbzgzq4 > div > div.st-emotion-cache-13o7eu2.eertqu02 > div > div:nth-child(1) > div > div:nth-child(2) > div > button")
        self.assert_exact_text("FORMULARIO CONEXIÓN", "#formulario-conexion")

    def test_ir_evaluacion(self) -> None:
        self.driver.maximize_window()
        self.open("http://localhost:8501/")
        self.assert_exact_text("FORMULARIO CONEXIÓN", "#formulario-conexion")
        self.type('[aria-label="**Host**"]', "localhost")
        self.type('[aria-label="**Usuario**"]', "sa")
        self.type('[aria-label="**Contraseña**"]', "root")
        self.type('[aria-label="**Base de Datos**"]', "AdventureWorksLT2022")
        self.click('[data-testid="stBaseButton-secondary"]')
        self.sleep(5)

        self.click("#root > div:nth-child(1) > div.withScreencast > div > div > div > section > div.stMainBlockContainer.block-container.st-emotion-cache-zy6yx3.e1cbzgzq4 > div > div.st-emotion-cache-13o7eu2.eertqu02 > div > div:nth-child(1) > div > div:nth-child(3) > div > button")
        self.assert_exact_text("EVALUACIÓN - ANÁLISIS DE RESULTADOS", "#evaluacion-analisis-de-resultados")

    def test_ejecucion_plan_exito(self) -> None:
        self.driver.maximize_window()
        self.open("http://localhost:8501/")
        self.assert_exact_text("FORMULARIO CONEXIÓN", "#formulario-conexion")
        self.type('[aria-label="**Host**"]', "localhost")
        self.type('[aria-label="**Usuario**"]', "sa")
        self.type('[aria-label="**Contraseña**"]', "root")
        self.type('[aria-label="**Base de Datos**"]', "AdventureWorksLT2022")
        self.click('[data-testid="stBaseButton-secondary"]')
        self.sleep(5)

        # Comprobacion de que cambia de pagina
        self.assert_exact_text("DEFINIR PLAN DE CALIDAD", "#definir-plan-de-calidad")
        self.type('[aria-label="**Nombre del plan de calidad:**"]', "PruebaPlanCalidad")

        wait = WebDriverWait(self.driver, 10)
        dropdown_xpath_esquema = '//*[@id="root"]/div[1]/div[1]/div/div/div/section/div[1]/div/div[4]/div/div[2]/div/div[3]/div'
        dropdown_esquema = wait.until(EC.element_to_be_clickable((By.XPATH, dropdown_xpath_esquema)))
        dropdown_esquema.click()

        opcion_xpath_esquema = "//div[contains(text(),'SalesLT')]"
        opcion_esquema = wait.until(EC.element_to_be_clickable((By.XPATH, opcion_xpath_esquema)))
        opcion_esquema.click()

        self.sleep(1)

        dropdown_xpath_tabla = '//*[@id="root"]/div[1]/div[1]/div/div/div/section/div[1]/div/div[4]/div/div[2]/div/div[4]/div'
        dropdown_tabla = wait.until(EC.element_to_be_clickable((By.XPATH, dropdown_xpath_tabla)))
        dropdown_tabla.click()

        opcion_xpath_tabla = "//div[contains(text(),'Customer')]"
        opcion_tabla = wait.until(EC.element_to_be_clickable((By.XPATH, opcion_xpath_tabla)))
        opcion_tabla.click()

        self.sleep(1)

        dropdown_xpath_columna = '//*[@id="root"]/div[1]/div[1]/div/div/div/section/div[1]/div/div[4]/div/div[2]/div/div[5]/div'
        dropdown_columna = wait.until(EC.element_to_be_clickable((By.XPATH, dropdown_xpath_columna)))
        dropdown_columna.click()

        opcion_xpath_columna = "//div[contains(text(),'MiddleName')]"
        opcion_columna = wait.until(EC.element_to_be_clickable((By.XPATH, opcion_xpath_columna)))
        opcion_columna.click()

        self.sleep(1)

        dropdown_xpath_tipo_test = '//*[@id="root"]/div[1]/div[1]/div/div/div/section/div[1]/div/div[4]/div/div[2]/div/div[6]/div'
        dropdown_tipo_test = wait.until(EC.element_to_be_clickable((By.XPATH, dropdown_xpath_tipo_test)))
        dropdown_tipo_test.click()

        opcion_xpath_tipo_test = "//div[contains(text(),'Completitud')]"
        opcion_tipo_test = wait.until(EC.element_to_be_clickable((By.XPATH, opcion_xpath_tipo_test)))
        opcion_tipo_test.click()

        self.sleep(1)

        self.click(
            "#root > div:nth-child(1) > div.withScreencast > div > div > div > section > div.stMainBlockContainer.block-container.st-emotion-cache-zy6yx3.e1cbzgzq4 > div > div.st-emotion-cache-13o7eu2.eertqu02 > div > div:nth-child(2) > div > div.st-emotion-cache-13o7eu2.eertqu02 > div > div:nth-child(1) > div > div > div > button")
        self.assert_element("//*[contains(., 'guardada correctamente')]")
        self.sleep(2)

        self.click("#root > div:nth-child(1) > div.withScreencast > div > div > div > section > div.stMainBlockContainer.block-container.st-emotion-cache-zy6yx3.e1cbzgzq4 > div > div.st-emotion-cache-13o7eu2.eertqu02 > div > div:nth-child(2) > div > div:nth-child(8) > div > button")
        self.assert_element("//*[contains(@class, 'stDataFrame')]")

    def test_ejecucion_plan_fallo(self) -> None:
        self.driver.maximize_window()
        self.open("http://localhost:8501/")
        self.assert_exact_text("FORMULARIO CONEXIÓN", "#formulario-conexion")
        self.type('[aria-label="**Host**"]', "localhost")
        self.type('[aria-label="**Usuario**"]', "sa")
        self.type('[aria-label="**Contraseña**"]', "root")
        self.type('[aria-label="**Base de Datos**"]', "AdventureWorksLT2022")
        self.click('[data-testid="stBaseButton-secondary"]')
        self.sleep(5)

        # Comprobacion de que cambia de pagina
        self.assert_exact_text("DEFINIR PLAN DE CALIDAD", "#definir-plan-de-calidad")
        self.type('[aria-label="**Nombre del plan de calidad:**"]', "PruebaPlanCalidad")

        self.click("#root > div:nth-child(1) > div.withScreencast > div > div > div > section > div.stMainBlockContainer.block-container.st-emotion-cache-zy6yx3.e1cbzgzq4 > div > div.st-emotion-cache-13o7eu2.eertqu02 > div > div:nth-child(2) > div > div:nth-child(8) > div > button")
        self.assert_element("div:contains('No hay tests guardados.')")

    def test_eliminar_plan_calidad_completo(self) -> None:
        self.driver.maximize_window()
        self.open("http://localhost:8501/")
        self.assert_exact_text("FORMULARIO CONEXIÓN", "#formulario-conexion")
        self.type('[aria-label="**Host**"]', "localhost")
        self.type('[aria-label="**Usuario**"]', "sa")
        self.type('[aria-label="**Contraseña**"]', "root")
        self.type('[aria-label="**Base de Datos**"]', "AdventureWorksLT2022")
        self.click('[data-testid="stBaseButton-secondary"]')
        self.sleep(5)

        # Comprobacion de que cambia de pagina
        self.assert_exact_text("DEFINIR PLAN DE CALIDAD", "#definir-plan-de-calidad")
        self.type('[aria-label="**Nombre del plan de calidad:**"]', "PruebaPlanCalidad")

        wait = WebDriverWait(self.driver, 10)
        dropdown_xpath_esquema = '//*[@id="root"]/div[1]/div[1]/div/div/div/section/div[1]/div/div[4]/div/div[2]/div/div[3]/div'
        dropdown_esquema = wait.until(EC.element_to_be_clickable((By.XPATH, dropdown_xpath_esquema)))
        dropdown_esquema.click()

        opcion_xpath_esquema = "//div[contains(text(),'SalesLT')]"
        opcion_esquema = wait.until(EC.element_to_be_clickable((By.XPATH, opcion_xpath_esquema)))
        opcion_esquema.click()

        self.sleep(1)

        dropdown_xpath_tabla = '//*[@id="root"]/div[1]/div[1]/div/div/div/section/div[1]/div/div[4]/div/div[2]/div/div[4]/div'
        dropdown_tabla = wait.until(EC.element_to_be_clickable((By.XPATH, dropdown_xpath_tabla)))
        dropdown_tabla.click()

        opcion_xpath_tabla = "//div[contains(text(),'Customer')]"
        opcion_tabla = wait.until(EC.element_to_be_clickable((By.XPATH, opcion_xpath_tabla)))
        opcion_tabla.click()

        self.sleep(1)

        dropdown_xpath_columna = '//*[@id="root"]/div[1]/div[1]/div/div/div/section/div[1]/div/div[4]/div/div[2]/div/div[5]/div'
        dropdown_columna = wait.until(EC.element_to_be_clickable((By.XPATH, dropdown_xpath_columna)))
        dropdown_columna.click()

        opcion_xpath_columna = "//div[contains(text(),'MiddleName')]"
        opcion_columna = wait.until(EC.element_to_be_clickable((By.XPATH, opcion_xpath_columna)))
        opcion_columna.click()

        self.sleep(1)

        dropdown_xpath_tipo_test = '//*[@id="root"]/div[1]/div[1]/div/div/div/section/div[1]/div/div[4]/div/div[2]/div/div[6]/div'
        dropdown_tipo_test = wait.until(EC.element_to_be_clickable((By.XPATH, dropdown_xpath_tipo_test)))
        dropdown_tipo_test.click()

        opcion_xpath_tipo_test = "//div[contains(text(),'Completitud')]"
        opcion_tipo_test = wait.until(EC.element_to_be_clickable((By.XPATH, opcion_xpath_tipo_test)))
        opcion_tipo_test.click()

        self.sleep(1)

        self.click(
            "#root > div:nth-child(1) > div.withScreencast > div > div > div > section > div.stMainBlockContainer.block-container.st-emotion-cache-zy6yx3.e1cbzgzq4 > div > div.st-emotion-cache-13o7eu2.eertqu02 > div > div:nth-child(2) > div > div.st-emotion-cache-13o7eu2.eertqu02 > div > div:nth-child(1) > div > div > div > button")
        self.assert_element("//*[contains(., 'guardada correctamente')]")
        self.sleep(2)

        self.click("#root > div:nth-child(1) > div.withScreencast > div > div > div > section > div.stMainBlockContainer.block-container.st-emotion-cache-zy6yx3.e1cbzgzq4 > div > div.st-emotion-cache-13o7eu2.eertqu02 > div > div:nth-child(3) > div > div:nth-child(3) > div > button")
        self.assert_element("//*[contains(., 'Todas las pruebas han sido eliminadas.')]")

    def test_cargar_plan_calidad(self) -> None:
        self.driver.maximize_window()
        self.open("http://localhost:8501/")
        self.assert_exact_text("FORMULARIO CONEXIÓN", "#formulario-conexion")
        self.type('[aria-label="**Host**"]', "localhost")
        self.type('[aria-label="**Usuario**"]', "sa")
        self.type('[aria-label="**Contraseña**"]', "root")
        self.type('[aria-label="**Base de Datos**"]', "AdventureWorksLT2022")
        self.click('[data-testid="stBaseButton-secondary"]')

        self.sleep(5)

        self.wait_for_element('input[data-testid="stFileUploaderDropzoneInput"]', timeout=10)
        path = os.path.abspath("tests/resources/conjunto_test_plan_de_calidad.json")
        self.choose_file('input[data-testid="stFileUploaderDropzoneInput"]', path)

        self.sleep(5)
        self.assert_element('[data-testid="stAlertContentSuccess"]', timeout=20)
        self.assert_text("Se han añadido 15 tests correctamente. Total: 15", '[data-testid="stAlertContentSuccess"]')

    @classmethod
    def tearDownClass(cls) -> None:
        cls.app_process.terminate()
        cls.app_process.wait()