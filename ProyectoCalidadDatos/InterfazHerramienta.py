import tkinter as tk
from tkinter import ttk

#Definicion de la ventana
ventana = tk.Tk()
ventana.title("Herramienta calidad de datos")
ventana.geometry("1080x720")

#Definicion de estilos
style = ttk.Style()
style.configure("Label", foreground="black", background="white")

#Texto superior
l1 = ttk.Label(ventana, text="Herramienta para la medida de la calidad de datos en desarrollo", style="Label", font=("Arial",20))
l1.pack(pady=30)

#Barra de progreso de como va el desarrollo de la app
barra = ttk.Progressbar(ventana, orient="horizontal", length=200, mode="determinate")
barra["value"] = 10
barra.pack(pady=30)

ventana.mainloop()