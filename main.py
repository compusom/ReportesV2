# report_generator_project/main.py

# ============================================================
# IMPORTS Y CONFIGURACIÓN INICIAL
# ============================================================
import sys
import os
import traceback

# --- INICIO DE MODIFICACIÓN PARA RESOLVER ModuleNotFoundError ---
current_script_directory = os.path.dirname(os.path.abspath(__file__))
if current_script_directory not in sys.path:
    sys.path.insert(0, current_script_directory)

print(f"DEBUG: main.py __file__: {__file__}")
print(f"DEBUG: current_script_directory (debe ser la raíz de tu proyecto): {current_script_directory}")
print("DEBUG: sys.path ANTES de importar data_processing:")
for p_path in sys.path:
    print(f"  - {p_path}")

# --- VERIFICACIÓN EXHAUSTIVA DE LA CARPETA DATA_PROCESSING ---
print("\nDEBUG: Verificando la carpeta 'data_processing'...")
data_processing_path_expected = os.path.join(current_script_directory, "data_processing")
print(f"DEBUG: Ruta esperada para data_processing: {data_processing_path_expected}")

if os.path.isdir(data_processing_path_expected):
    print("DEBUG: OK - La ruta es un directorio.")
    init_py_path_expected = os.path.join(data_processing_path_expected, "__init__.py")
    print(f"DEBUG: Ruta esperada para __init__.py: {init_py_path_expected}")
    if os.path.isfile(init_py_path_expected):
        print("DEBUG: OK - El archivo __init__.py existe.")
        try:
            import importlib.util
            spec = importlib.util.spec_from_file_location("data_processing", init_py_path_expected)
            if spec and spec.loader:
                dp_module_test = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(dp_module_test) # type: ignore
                print(f"DEBUG: OK - Prueba de carga de 'data_processing' (via __init__.py) exitosa: {dp_module_test}")
            else:
                print("FATAL DEBUG: No se pudo obtener la especificación o el cargador para data_processing/__init__.py.")
        except Exception as e_init_test:
            print(f"FATAL DEBUG: Error durante la prueba de carga de data_processing/__init__.py: {e_init_test}")
            traceback.print_exc()
    else:
        print("FATAL DEBUG: ERROR - El archivo __init__.py NO existe en la ruta esperada.")
        print("             Por favor, crea un archivo vacío llamado '__init__.py' (dos guiones bajos al inicio y al final)")
        print(f"             dentro de la carpeta: {data_processing_path_expected}")
else:
    print("FATAL DEBUG: ERROR - La carpeta 'data_processing' NO existe en la ruta esperada o no es un directorio.")
    print("             Por favor, asegúrate de que la carpeta se llama exactamente 'data_processing' (todo en minúsculas, con guion bajo)")
    print(f"             y está ubicada en: {current_script_directory}")

print("\nDEBUG: Listado de la carpeta raíz del proyecto:")
try:
    for item in os.listdir(current_script_directory):
        print(f"  - {item} {'(DIR)' if os.path.isdir(os.path.join(current_script_directory, item)) else '(FILE)'}")
except Exception as e_listdir_root:
    print(f"  ERROR listando directorio raíz: {e_listdir_root}")

if os.path.isdir(data_processing_path_expected):
    print(f"\nDEBUG: Listado de la carpeta '{data_processing_path_expected}':")
    try:
        for item in os.listdir(data_processing_path_expected):
            print(f"  - {item} {'(DIR)' if os.path.isdir(os.path.join(data_processing_path_expected, item)) else '(FILE)'}")
    except Exception as e_listdir_dp:
        print(f"  ERROR listando directorio data_processing: {e_listdir_dp}")
# --- FIN VERIFICACIÓN EXHAUSTIVA ---


# --- Intento de importación INMEDIATO ---
procesar_reporte_rendimiento_func = None
procesar_reporte_bitacora_func = None
try:
    print("\nDEBUG: Intentando: from data_processing.orchestrators import ...")
    from data_processing.orchestrators import procesar_reporte_rendimiento, procesar_reporte_bitacora
    procesar_reporte_rendimiento_func = procesar_reporte_rendimiento
    procesar_reporte_bitacora_func = procesar_reporte_bitacora
    print("DEBUG: Importación de orchestrators A NIVEL DE MODULO exitosa.")
except ModuleNotFoundError as e_mnfe:
    print(f"FATAL DEBUG (Importación Nivel Módulo): ModuleNotFoundError: {e_mnfe}")
except ImportError as e_ie:
    print(f"FATAL DEBUG (Importación Nivel Módulo): ImportError: {e_ie}")
    traceback.print_exc()
except Exception as e_ge:
    print(f"FATAL DEBUG (Importación Nivel Módulo): Exception: {e_ge}")
    traceback.print_exc()
# --- FIN Intento de importación INMEDIATO ---


import pandas as pd
import re
import csv
import warnings
import numpy as np
import threading
import queue
# traceback ya está importado
from datetime import datetime, date, timedelta

from file_io import find_date_column_name, get_dates_from_file
from config import norm_map
from utils import normalize


# Suprimir advertencias específicas
np.seterr(divide='ignore', invalid='ignore')
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
warnings.filterwarnings('ignore', r'Parsing dates in .* format when dayfirst=True was specified')
warnings.filterwarnings('ignore', r'divide by zero encountered')
warnings.filterwarnings('ignore', r'invalid value encountered')

try:
    from dateutil.relativedelta import relativedelta
    from dateutil.parser import parse as date_parse
    print("INFO: dateutil importado OK.")
except ImportError:
    print("¡ERROR! Falta 'python-dateutil'. Instala con: pip install python-dateutil")
    relativedelta = None
    date_parse = None

import tkinter as tk
from tkinter import ttk, simpledialog, filedialog, messagebox, scrolledtext

try:
    import sv_ttk
except ImportError:
    print("Advertencia: sv_ttk no encontrado.")
    sv_ttk = None

try:
    from tkcalendar import Calendar
    print("INFO: tkcalendar OK.")
except ImportError:
    print("¡ADVERTENCIA! Falta 'tkcalendar'. El selector de calendario no estará disponible.")
    print("Instala con: pip install tkcalendar")
    Calendar = None

try:
    _tk_test_root = tk.Tk()
    _tk_test_root.withdraw()
    _tk_test_root.destroy()
    print("INFO: tkinter OK.")
except NameError:
    print("FATAL: tkinter alias 'tk' no definido.");
    sys.exit()
except Exception as e:
    print(f"INFO: Problema tkinter ({e}). Continuando...")

log_summary_messages = []

# ============================================================
# INTERFAZ GRÁFICA DE USUARIO (GUI)
# ============================================================
class ReportApp:
    def __init__(self, root_window):
        self.root = root_window
        self.root.title("Generador Marketing Reports vNUEVA (Modular)")
        self.root.geometry("950x950")
        self.style = ttk.Style()
        global sv_ttk
        if 'sv_ttk' in globals() and sv_ttk:
            try:
                sv_ttk.set_theme("light")
                print("INFO: Tema sv-ttk 'light' aplicado.")
            except Exception as e_svttk:
                print(f"Adv: sv_ttk ({e_svttk}). Fallback.")
                self._apply_standard_theme()
        else:
            self._apply_standard_theme()

        self.input_files = []
        self.output_dir = tk.StringVar(value=os.getcwd())
        self.min_date_detected = None
        self.max_date_detected = None
        self.detected_date_col_names = {}
        self.report_type = tk.StringVar(value="Rendimiento")
        self.selected_campaign = tk.StringVar(value="--- Todas ---")
        self.selected_adset = tk.StringVar(value="--- Todos ---")
        self.all_campaign_adsets_pairs = []
        self.output_filename_var = tk.StringVar()
        self.is_processing = False
        
        # Para Bitácora Semanal
        self.bitacora_selected_week_start_date_var = tk.StringVar() # Fecha de inicio de la semana seleccionada
        self.bitacora_selected_week_end_date_var = tk.StringVar()   # Fecha de fin de la semana seleccionada
        
        self.bitacora_selected_monday_week_var = tk.StringVar() # Para el combobox de semanas detectadas (fallback)
        self.detected_mondays_for_bitacora_display = []
        self.detected_mondays_for_bitacora_date_obj = []
        self.valid_mondays_for_calendar = [] # Semanas con datos (Lunes) para destacar en calendario
        self.bitacora_comparison_type = tk.StringVar(value="Weekly")
        self.calendar_week_selection_mode = tk.StringVar(value="monday") # 'monday' o 'end_day'

        main_frame = ttk.Frame(root_window, padding=(20, 10))
        main_frame.pack(fill=tk.BOTH, expand=True)
        main_frame.columnconfigure(0, weight=1)

        report_type_frame = ttk.LabelFrame(main_frame, text="Tipo de Reporte", padding=(10,5)); report_type_frame.grid(row=0, column=0, columnspan=3, sticky="ew", padx=10, pady=5)
        ttk.Radiobutton(report_type_frame, text="Bitácora", variable=self.report_type, value="Bitácora", command=self._on_report_type_change).pack(side=tk.LEFT, padx=10)
        ttk.Radiobutton(report_type_frame, text="Rendimiento Detallado", variable=self.report_type, value="Rendimiento", command=self._on_report_type_change).pack(side=tk.LEFT, padx=10)
        
        input_frame = ttk.LabelFrame(main_frame, text="1. Archivos de Datos (Excel/CSV)", padding=(10, 5)); input_frame.grid(row=1, column=0, columnspan=3, sticky="ew", padx=10, pady=(5, 10)); input_frame.columnconfigure(0, weight=1)
        listbox_frame = ttk.Frame(input_frame); listbox_frame.grid(row=0, column=0, sticky="nsew", pady=5); listbox_frame.rowconfigure(0, weight=1); listbox_frame.columnconfigure(0, weight=1)
        self.listbox_files = tk.Listbox(listbox_frame, selectmode=tk.EXTENDED, height=5, borderwidth=1, relief="solid"); self.listbox_files.grid(row=0, column=0, sticky="nsew")
        scrollbar_files = ttk.Scrollbar(listbox_frame, orient=tk.VERTICAL, command=self.listbox_files.yview); scrollbar_files.grid(row=0, column=1, sticky="ns"); self.listbox_files.config(yscrollcommand=scrollbar_files.set)
        input_buttons_frame = ttk.Frame(input_frame); input_buttons_frame.grid(row=0, column=1, sticky="ns", padx=(10, 0))
        btn_add = ttk.Button(input_buttons_frame, text="Añadir Archivos...", command=self.select_input_files); btn_add.grid(row=0, column=0, pady=3, sticky="ew")
        btn_remove = ttk.Button(input_buttons_frame, text="Quitar Seleccionados", command=self.remove_selected_files); btn_remove.grid(row=1, column=0, pady=3, sticky="ew")
        btn_clear = ttk.Button(input_buttons_frame, text="Limpiar Lista", command=self.clear_file_list); btn_clear.grid(row=2, column=0, pady=3, sticky="ew")
        range_frame = ttk.Frame(input_frame, padding=(5, 2)); range_frame.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(5,0))
        ttk.Label(range_frame, text="Rango detectado:").pack(side=tk.LEFT, padx=(0, 5))
        self.lbl_min_date = ttk.Label(range_frame, text="Inicio: -", width=18, anchor="w"); self.lbl_min_date.pack(side=tk.LEFT, padx=5)
        self.lbl_max_date = ttk.Label(range_frame, text="Fin: -", width=18, anchor="w"); self.lbl_max_date.pack(side=tk.LEFT, padx=5)

        campaign_select_frame = ttk.LabelFrame(main_frame, text="2. Selección de Campaña / AdSet (Opcional)", padding=(10,5));
        campaign_select_frame.grid(row=2, column=0, columnspan=3, sticky="ew", padx=10, pady=(0, 5)); campaign_select_frame.columnconfigure(1, weight=1)

        ttk.Label(campaign_select_frame, text="Campaña:").grid(row=0, column=0, padx=(0,5), pady=3, sticky='w')
        self.campaign_combo = ttk.Combobox(campaign_select_frame, textvariable=self.selected_campaign, state='disabled', width=80);
        self.campaign_combo.grid(row=0, column=1, padx=5, pady=3, sticky='ew'); self.campaign_combo.set("--- Todas ---")
        self.campaign_combo.bind("<<ComboboxSelected>>", self._on_campaign_selected)

        ttk.Label(campaign_select_frame, text="AdSet:").grid(row=1, column=0, padx=(0,5), pady=3, sticky='w')
        self.adset_combo = ttk.Combobox(campaign_select_frame, textvariable=self.selected_adset, state='disabled', width=80);
        self.adset_combo.grid(row=1, column=1, padx=5, pady=3, sticky='ew'); self.adset_combo.set("--- Todos ---")


        self.bitacora_settings_frame = ttk.LabelFrame(main_frame, text="3. Configuración Bitácora", padding=(10, 5));
        self.bitacora_settings_frame.grid(row=3, column=0, columnspan=3, sticky="ew", padx=10, pady=10); self.bitacora_settings_frame.columnconfigure(0, weight=1); self.bitacora_settings_frame.columnconfigure(1, weight=1)

        bitacora_type_frame = ttk.Frame(self.bitacora_settings_frame)
        bitacora_type_frame.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 5))
        self.rb_weekly = ttk.Radiobutton(bitacora_type_frame, text="Comparación Semanal", variable=self.bitacora_comparison_type, value="Weekly", command=self._on_bitacora_comparison_change)
        self.rb_weekly.pack(side=tk.LEFT, padx=10)
        self.lbl_weekly_info = ttk.Label(bitacora_type_frame, text="", foreground="blue") 
        self.lbl_weekly_info.pack(side=tk.LEFT, padx=(0,20))

        self.rb_monthly = ttk.Radiobutton(bitacora_type_frame, text="Comparación Mensual", variable=self.bitacora_comparison_type, value="Monthly", command=self._on_bitacora_comparison_change)
        self.rb_monthly.pack(side=tk.LEFT, padx=10)
        self.lbl_monthly_info = ttk.Label(bitacora_type_frame, text="", foreground="blue") 
        self.lbl_monthly_info.pack(side=tk.LEFT, padx=(0,5))


        self.bitacora_options_container = ttk.Frame(self.bitacora_settings_frame)
        self.bitacora_options_container.grid(row=1, column=0, columnspan=2, sticky="ew", padx=10, pady=5)
        self.bitacora_options_container.columnconfigure(1, weight=1)

        self.bitacora_weekly_options_frame = ttk.Frame(self.bitacora_options_container)
        
        weekly_row1_frame = ttk.Frame(self.bitacora_weekly_options_frame)
        weekly_row1_frame.pack(fill=tk.X, pady=(0,5))
        
        ttk.Label(weekly_row1_frame, text="Semana de Referencia:").pack(side=tk.LEFT, padx=(0,5))
        self.lbl_bitacora_selected_week_start = ttk.Label(weekly_row1_frame, textvariable=self.bitacora_selected_week_start_date_var, width=12, relief="sunken", anchor="center")
        self.lbl_bitacora_selected_week_start.pack(side=tk.LEFT, padx=2)
        ttk.Label(weekly_row1_frame, text="a").pack(side=tk.LEFT, padx=2)
        self.lbl_bitacora_selected_week_end = ttk.Label(weekly_row1_frame, textvariable=self.bitacora_selected_week_end_date_var, width=12, relief="sunken", anchor="center")
        self.lbl_bitacora_selected_week_end.pack(side=tk.LEFT, padx=2)

        self.btn_open_calendar = ttk.Button(weekly_row1_frame, text="📅 Seleccionar Semana...", command=self._open_calendar_selector, state=tk.DISABLED)
        self.btn_open_calendar.pack(side=tk.LEFT, padx=10)

        weekly_row2_frame = ttk.Frame(self.bitacora_weekly_options_frame)
        weekly_row2_frame.pack(fill=tk.X, pady=(5,0))
        
        ttk.Label(weekly_row2_frame, text="O semana detectada (fallback):").pack(side=tk.LEFT, padx=(0,5))
        self.combo_bitacora_monday = ttk.Combobox(weekly_row2_frame, textvariable=self.bitacora_selected_monday_week_var, state='disabled', width=35);
        self.combo_bitacora_monday.pack(side=tk.LEFT, padx=5)
        self.lbl_bitacora_monday_info = ttk.Label(weekly_row2_frame, text="(Semanas con datos. Auto si no se selecciona por calendario.)")
        self.lbl_bitacora_monday_info.pack(side=tk.LEFT, padx=5)


        self.bitacora_monthly_options_frame = ttk.Frame(self.bitacora_options_container)
        self.lbl_bitacora_monthly_info_detail = ttk.Label(self.bitacora_monthly_options_frame, text="Compara los 2 últimos meses calendario completos con datos.", wraplength=500)
        self.lbl_bitacora_monthly_info_detail.pack(side=tk.LEFT, padx=5)

        output_frame_outer = ttk.LabelFrame(main_frame, text="4. Configuración de Salida", padding=(10, 5)); output_frame_outer.grid(row=4, column=0, columnspan=3, sticky="ew", padx=10, pady=10); output_frame_outer.columnconfigure(1, weight=1)
        ttk.Label(output_frame_outer, text="Directorio:").grid(row=0, column=0, sticky="w", padx=(0,5), pady=3)
        self.entry_output_dir = ttk.Entry(output_frame_outer, textvariable=self.output_dir); self.entry_output_dir.grid(row=0, column=1, sticky="ew", pady=3)
        btn_browse_dir = ttk.Button(output_frame_outer, text="Examinar...", command=self.select_output_dir); btn_browse_dir.grid(row=0, column=2, sticky="e", padx=(5,0), pady=3)
        ttk.Label(output_frame_outer, text="Archivo:").grid(row=1, column=0, sticky="w", padx=(0,5), pady=3)
        self.entry_output_filename = ttk.Entry(output_frame_outer, textvariable=self.output_filename_var); self.entry_output_filename.grid(row=1, column=1, columnspan=2, sticky="ew", pady=3)

        generate_frame = ttk.Frame(main_frame); generate_frame.grid(row=5, column=0, columnspan=3, pady=(15, 10))
        try: self.style.configure('Accent.TButton', font=('Segoe UI', 12, 'bold')); btn_style = 'Accent.TButton'
        except tk.TclError: btn_style = 'TButton'
        self.btn_generate = ttk.Button(generate_frame, text="🚀 GENERAR REPORTE 🚀", command=self.start_processing_thread, style=btn_style, padding=(10, 5)); self.btn_generate.pack()

        status_frame = ttk.LabelFrame(main_frame, text="Registro del Proceso", padding=(10, 5)); status_frame.grid(row=6, column=0, columnspan=3, sticky="nsew", padx=10, pady=(5, 10)); status_frame.columnconfigure(0, weight=1); status_frame.rowconfigure(0, weight=1)
        self.text_status = scrolledtext.ScrolledText(status_frame, wrap=tk.WORD, height=10, bd=0, state=tk.DISABLED, font=("Consolas", 9)); self.text_status.grid(row=0, column=0, sticky="nsew"); main_frame.rowconfigure(6, weight=1)

        self._set_default_filename()
        self._on_report_type_change()
        
        self.status_queue = queue.Queue()
        if hasattr(self, 'root') and self.root.winfo_exists():
            self.root.after(100, self.check_queue)


    def _apply_standard_theme(self):
        available=self.style.theme_names(); preferred=['vista','clam','default']
        for t_theme in preferred:
            if t_theme in available:
                try: self.style.theme_use(t_theme); print(f"INFO: Tema ttk: '{t_theme}'"); return
                except tk.TclError: continue
        print("Adv: No se pudo aplicar tema ttk preferido.")

    def _set_default_filename(self):
        rt = self.report_type.get()
        comp_type = self.bitacora_comparison_type.get() if rt == "Bitácora" else ""
        ts = datetime.now().strftime("%Y%m%d_%H%M")

        if rt == "Bitácora":
            fn = f"reporte_bitacora_{comp_type.lower()}_{ts}.txt"
        elif rt == "Rendimiento":
            fn = f"reporte_rendimiento_{ts}.txt"
        else:
            fn = f"reporte_desconocido_{ts}.txt"
        self.output_filename_var.set(fn)

    def _on_report_type_change(self):
        self._set_default_filename()
        is_bitacora = (self.report_type.get() == "Bitácora")

        if hasattr(self, 'bitacora_settings_frame') and self.bitacora_settings_frame.winfo_exists():
            if is_bitacora:
                self.bitacora_settings_frame.grid()
                self._on_bitacora_comparison_change() 
            else:
                self.bitacora_settings_frame.grid_remove()

        if self.all_campaign_adsets_pairs:
             campaigns = sorted(list(set(pair[0] for pair in self.all_campaign_adsets_pairs)))
             self._update_campaign_combo_ui(campaigns)
        else:
             self._update_campaign_combo_ui([])

    def _on_bitacora_comparison_change(self):
        is_weekly = (self.bitacora_comparison_type.get() == "Weekly")

        for widget in self.bitacora_options_container.winfo_children():
            widget.grid_remove()

        if is_weekly:
            self.bitacora_weekly_options_frame.grid(row=0, column=0, sticky="ew", padx=0, pady=0)
            self.btn_open_calendar.configure(state='normal') 
            self._update_bitacora_monday_selector_ui() 
            self.lbl_monthly_info.config(text="") 
        else: # Monthly
            self.bitacora_monthly_options_frame.grid(row=0, column=0, sticky="ew", padx=0, pady=0)
            if hasattr(self, 'btn_open_calendar') and self.btn_open_calendar.winfo_exists(): 
                self.btn_open_calendar.configure(state='disabled')
            if hasattr(self, 'combo_bitacora_monday') and self.combo_bitacora_monday.winfo_exists():
                self.combo_bitacora_monday.configure(state='disabled')
            
            self.bitacora_selected_week_start_date_var.set("") 
            self.bitacora_selected_week_end_date_var.set("") 
            self.bitacora_selected_monday_week_var.set("") 
            self.lbl_weekly_info.config(text="") 
            self._update_bitacora_monthly_info_ui() 

        self._set_default_filename() 

    def _update_bitacora_monthly_info_ui(self):
        if self.min_date_detected and self.max_date_detected and relativedelta:
            num_full_months = 0
            try:
                current_month_start = self.min_date_detected.replace(day=1)
                while True:
                    current_month_end = current_month_start + relativedelta(months=1) - timedelta(days=1)
                    if current_month_start >= self.min_date_detected and current_month_end <= self.max_date_detected:
                        num_full_months +=1
                    if current_month_end >= self.max_date_detected:
                        break
                    current_month_start += relativedelta(months=1)

                if num_full_months >= 2:
                    self.lbl_monthly_info.config(text=f"({num_full_months} meses con datos para potencial comparación)")
                elif num_full_months == 1:
                    self.lbl_monthly_info.config(text="(Solo ~1 mes con datos)")
                else:
                    self.lbl_monthly_info.config(text="(No hay suficientes datos para meses)")
            except Exception as e_month_count:
                print(f"Error contando meses para GUI: {e_month_count}")
                self.lbl_monthly_info.config(text="(No se pudo determinar el número de meses)")
        else:
             self.lbl_monthly_info.config(text="")

    def _set_widget_state_recursive(self, parent, state):
        try:
            if 'state' in parent.configure():
                 parent.configure(state=state)
            for child in parent.winfo_children(): self._set_widget_state_recursive(child, state)
        except tk.TclError: pass

    def _update_campaign_list(self, campaign_adset_pairs):
        self.all_campaign_adsets_pairs = campaign_adset_pairs 

        if not campaign_adset_pairs: 
            campaigns = []
        else: 
            campaigns = sorted(list(set(pair[0] for pair in self.all_campaign_adsets_pairs)))
        self._update_campaign_combo_ui(campaigns) 

    def _update_campaign_combo_ui(self, campaigns):
        if hasattr(self,'campaign_combo') and self.campaign_combo.winfo_exists():
            if campaigns: 
                vals=["--- Todas ---"]+campaigns; 
                self.campaign_combo['values'] = vals
                current_selection = self.selected_campaign.get()
                if current_selection not in vals: 
                     self.selected_campaign.set("--- Todas ---")
                self.campaign_combo.configure(state='readonly') 
                if campaigns:
                     self._update_status(f"Detectadas {len(campaigns)} campañas.")
            else: 
                self.campaign_combo['values'] = []
                self.selected_campaign.set("--- Todas ---")
                self.campaign_combo.configure(state='disabled') 
                if self.input_files: 
                    self._update_status("No se detectaron campañas válidas.")
        self._update_adset_list() 

    def _on_campaign_selected(self, event=None):
        self._update_adset_list() 

    def _update_adset_list(self):
        if not self.all_campaign_adsets_pairs or not hasattr(self, 'adset_combo') or not self.adset_combo.winfo_exists():
            if hasattr(self, 'adset_combo') and self.adset_combo.winfo_exists():
                self.adset_combo['values'] = []
                self.selected_adset.set("--- Todos ---")
                self.adset_combo.configure(state='disabled')
            return

        selected_camp_display = self.selected_campaign.get() 
        adsets_for_selected_camp = []

        if selected_camp_display == "--- Todas ---": 
            adsets_for_selected_camp = sorted(list(set(pair[1] for pair in self.all_campaign_adsets_pairs))) 
        else: 
            normalized_selected_camp = normalize(selected_camp_display) 
            adsets_for_selected_camp = sorted(list(set(pair[1] for pair in self.all_campaign_adsets_pairs if normalize(pair[0]) == normalized_selected_camp))) 

        if adsets_for_selected_camp: 
            vals = ["--- Todos ---"] + adsets_for_selected_camp 
            self.adset_combo['values'] = vals
            current_selection = self.selected_adset.get()
            if current_selection not in vals: 
                 self.selected_adset.set("--- Todos ---")
            self.adset_combo.configure(state='readonly') 
            if selected_camp_display != "--- Todas ---":
                 self._update_status(f"Detectados {len(adsets_for_selected_camp)} AdSets para la campaña '{selected_camp_display}'.")
            else:
                 self._update_status(f"Detectados {len(adsets_for_selected_camp)} AdSets en total.")
        else: 
            self.adset_combo['values'] = []
            self.selected_adset.set("--- Todos ---")
            self.adset_combo.configure(state='disabled') 
            if selected_camp_display != "--- Todas ---":
                self._update_status(f"No se detectaron AdSets válidos para la campaña '{selected_camp_display}'.")

    def select_input_files(self):
        init_dir=self.output_dir.get() if os.path.isdir(self.output_dir.get()) else os.getcwd()
        files=filedialog.askopenfilenames(title="Seleccionar Archivos",initialdir=init_dir,filetypes=[("Soportados","*.xlsx *.xls *.csv"),("Excel","*.xlsx *.xls"),("CSV","*.csv"),("Todos","*.*")])
        if files:
            added=0
            for f_path in files:
                if f_path not in self.input_files: self.input_files.append(f_path); self.listbox_files.insert(tk.END,os.path.basename(f_path)); added+=1
            if added>0: self._update_status(f"Añadidos {added}. Total: {len(self.input_files)}."); self._detect_date_range_and_mondays(); 
            else: self._update_status("No se añadieron nuevos archivos (ya estaban en la lista).")

    def remove_selected_files(self):
        indices=self.listbox_files.curselection()
        if not indices: messagebox.showwarning("Nada Seleccionado","Selecciona uno o más archivos de la lista para quitar."); return
        bases={self.listbox_files.get(i) for i in indices};
        for i_idx in reversed(indices): self.listbox_files.delete(i_idx)
        removed=0; new_list=[]
        for f_path in self.input_files:
            if os.path.basename(f_path) in bases:
                removed+=1;
                self.detected_date_col_names.pop(f_path,None) 
            else: new_list.append(f_path)
        self.input_files=new_list
        if removed>0: self._update_status(f"Eliminados {removed}. Total: {len(self.input_files)}."); self._detect_date_range_and_mondays(); 

    def clear_file_list(self):
        if not self.input_files: messagebox.showinfo("Lista Vacía","La lista de archivos ya está vacía."); return
        if messagebox.askyesno("Confirmar","¿Seguro que quieres limpiar la lista de archivos?"):
            self.input_files.clear(); self.listbox_files.delete(0,tk.END); self.detected_date_col_names.clear()
            self.all_campaign_adsets_pairs = [] 
            self.valid_mondays_for_calendar = [] 
            self._update_status("Lista de archivos limpiada."); self._detect_date_range_and_mondays(); 

    def _detect_date_range_and_mondays(self): 
        if not self.input_files:
            self._update_status("No hay archivos para detectar rango de fechas/lunes.");
            self.min_date_detected=None; self.max_date_detected=None;
            self.detected_mondays_for_bitacora_display = []
            self.detected_mondays_for_bitacora_date_obj = []
            self.valid_mondays_for_calendar = [] 
            self.all_campaign_adsets_pairs = []
            self._update_date_range_display();
            self._update_bitacora_monday_selector_ui()
            self._update_campaign_list([]) 
            return
        self._update_status("Detectando rango fechas, Lunes para Bitácora y entidades (hilo)...")
        thread = threading.Thread(target=self._detect_dates_mondays_and_entities_thread, daemon=True); thread.start()

    def _detect_dates_mondays_and_entities_thread(self): 
        min_dt,max_dt=None,None; count_files_with_dates=0; errors_date=[]
        all_dates_in_files = pd.Series(dtype='datetime64[ns]') 
        all_campaign_adsets = set() 
        all_unique_dates_from_files = set() 

        files_to_process = self.input_files[:] 
        for f_path in files_to_process: 
            try:
                col_name=self.detected_date_col_names.get(f_path) 
                if col_name is None: 
                    col_name=find_date_column_name(f_path);
                    self.detected_date_col_names[f_path]=col_name 
                if not col_name: errors_date.append(f"No col. fecha: {os.path.basename(f_path)}"); continue

                dates_found=get_dates_from_file(f_path,col_name).dropna() 
                if not dates_found.empty:
                    all_dates_in_files = pd.concat([all_dates_in_files, dates_found], ignore_index=True) 
                    for d_obj in dates_found.dt.date.unique(): 
                        all_unique_dates_from_files.add(d_obj)

                    count_files_with_dates+=1; cmin_dt=dates_found.min(); cmax_dt=dates_found.max() 
                    if min_dt is None or cmin_dt<min_dt: min_dt=cmin_dt
                    if max_dt is None or cmax_dt>max_dt: max_dt=cmax_dt

                if 'campaign' in norm_map and 'adset' in norm_map: 
                    df_temp_peek = None
                    try: 
                         ext = os.path.splitext(f_path)[1].lower()
                         if ext in ['.xlsx', '.xls']:
                             engine = 'openpyxl' if 'openpyxl' in sys.modules else None
                             if engine:
                                 try: df_temp_peek = pd.read_excel(f_path, engine=engine, dtype=str, nrows=20, skiprows=[1]) 
                                 except Exception: df_temp_peek = pd.read_excel(f_path, engine=engine, dtype=str, nrows=20) 
                         elif ext == '.csv': 
                             sep_peek = None; enc_peek = None;
                             try: 
                                 with open(f_path,'r',encoding='utf-8-sig', errors='ignore') as f_peek: header_line_peek = f_peek.readline()
                                 if not header_line_peek: 
                                      with open(f_path,'r',encoding='latin-1', errors='ignore') as f_peek: header_line_peek = f_peek.readline()
                                 if header_line_peek: 
                                     try: dialect = csv.Sniffer().sniff(header_line_peek, delimiters=',;\t|'); sep_peek = dialect.delimiter
                                     except csv.Error: 
                                         common = [',', ';', '\t', '|']
                                         counts = {s: header_line_peek.count(s) for s in common}
                                         sep_peek = max(counts, key=counts.get) if any(counts.values()) else ','
                             except Exception: pass 
                             for enc_try in ['utf-8-sig', 'latin-1', 'cp1252']:
                                try:
                                    with open(f_path, 'r', encoding=enc_try, errors='ignore') as f_peek_enc: f_peek_enc.read(100) 
                                    enc_peek = enc_try; break
                                except: pass
                             if enc_peek is None: enc_peek = 'latin-1' 

                             try: 
                                 try: df_temp_peek = pd.read_csv(f_path, dtype=str, sep=sep_peek, engine='python', encoding=enc_peek, on_bad_lines='skip', nrows=20, skiprows=[1])
                                 except Exception: df_temp_peek = pd.read_csv(f_path, dtype=str, sep=sep_peek, engine='python', encoding=enc_peek, on_bad_lines='skip', nrows=20)
                             except Exception: pass 

                    except Exception as e_peek: 
                         print(f"Adv: Error reading peek data for entities in {os.path.basename(f_path)}: {e_peek}")

                    if df_temp_peek is not None and not df_temp_peek.empty: 
                        file_cols_normalized = {c: normalize(c) for c in df_temp_peek.columns} 
                        normalized_to_original = {v: k for k, v in file_cols_normalized.items()} 

                        camp_col_orig = next((orig for norm_key in norm_map['campaign'] for orig in [normalized_to_original.get(norm_key)] if orig and orig in df_temp_peek.columns), None)
                        adset_col_orig = next((orig for norm_key in norm_map['adset'] for orig in [normalized_to_original.get(norm_key)] if orig and orig in df_temp_peek.columns), None)

                        if camp_col_orig and adset_col_orig: 
                            temp_df = df_temp_peek[[camp_col_orig, adset_col_orig]].copy()
                            temp_df.columns = ['Campaign', 'AdSet'] 
                            temp_df = temp_df.dropna(how='all') 
                            temp_df['Campaign'] = temp_df['Campaign'].astype(str).apply(normalize) 
                            temp_df['AdSet'] = temp_df['AdSet'].astype(str).apply(normalize)
                            temp_df = temp_df[
                                (~temp_df['Campaign'].str.startswith('(no', na=False)) &
                                (~temp_df['AdSet'].str.startswith('(no', na=False))
                            ]
                            for camp, adset in temp_df.drop_duplicates().itertuples(index=False): 
                                all_campaign_adsets.add((camp, adset))
                        elif camp_col_orig:
                             print(f"Adv: Columna Campaign '{camp_col_orig}' encontrada, pero AdSet no en '{os.path.basename(f_path)}' para detección de entidades.")
                        elif adset_col_orig:
                             print(f"Adv: Columna AdSet '{adset_col_orig}' encontrada, pero Campaign no en '{os.path.basename(f_path)}' para detección de entidades.")
                        else:
                             print(f"Adv: No se encontraron columnas Campaign/AdSet en '{os.path.basename(f_path)}' para detección de entidades.")
            except NameError as ne_date:
                 self.status_queue.put(f"Error Fatal Interno: Función '{ne_date.name}' no encontrada.");
                 self.root.after(0,self._update_dates_mondays_and_entities_ui,None,None,0,[],[],[]) 
                 self.root.after(0, self._update_campaign_list, [])
                 return
            except Exception as e_date: errors_date.append(f"Error procesando archivo {os.path.basename(f_path)}: {e_date}")

        all_valid_mondays_for_calendar_set = set() 
        if not all_dates_in_files.empty and min_dt and max_dt:
            unique_dates_sorted = sorted(list(all_unique_dates_from_files)) 
            if unique_dates_sorted: 
                min_data_date_obj = unique_dates_sorted[0]
                max_data_date_obj = unique_dates_sorted[-1]
                current_monday_candidate = min_data_date_obj - timedelta(days=min_data_date_obj.weekday())
                while current_monday_candidate <= max_data_date_obj + timedelta(days=7): # extend check slightly beyond max_data_date
                     week_dates_to_check = {current_monday_candidate + timedelta(days=i) for i in range(7)}
                     if any(d in all_unique_dates_from_files for d in week_dates_to_check if min_data_date_obj <= d <= max_data_date_obj):
                        all_valid_mondays_for_calendar_set.add(current_monday_candidate)
                     current_monday_candidate += timedelta(weeks=1)
        all_valid_mondays_for_calendar_list_sorted = sorted(list(all_valid_mondays_for_calendar_set))
        self.root.after(0,self._update_dates_mondays_and_entities_ui,min_dt,max_dt,count_files_with_dates,errors_date, sorted(list(all_campaign_adsets)), all_valid_mondays_for_calendar_list_sorted )

    def _update_dates_mondays_and_entities_ui(self,min_d,max_d,count_processed_files,errors_list, valid_campaign_adset_pairs, all_valid_mondays_for_calendar_list ):
        if errors_list: [self._update_status(f"Adv Fechas: {e_item}") for e_item in errors_list]

        if min_d is not None and max_d is not None and count_processed_files>0:
            self.min_date_detected=min_d; self.max_date_detected=max_d;
            self._update_status(f"Rango detectado en {count_processed_files} archivo(s).")
        elif count_processed_files==0 and self.input_files:
            self._update_status("No se encontraron fechas válidas en los archivos seleccionados.");
            self.min_date_detected=None; self.max_date_detected=None
        else:
            self.min_date_detected=None; self.max_date_detected=None;

        if self.input_files and not (self.min_date_detected and self.max_date_detected):
            self._update_status("No se pudo determinar el rango de fechas global.")

        self.valid_mondays_for_calendar = all_valid_mondays_for_calendar_list 
        self.detected_mondays_for_bitacora_date_obj = all_valid_mondays_for_calendar_list 

        self.detected_mondays_for_bitacora_display = []
        if self.detected_mondays_for_bitacora_date_obj:
            day_names_es = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]
            for md_date in sorted(self.detected_mondays_for_bitacora_date_obj, reverse=True): # Mas recientes primero
                sunday_date = md_date + timedelta(days=6)
                self.detected_mondays_for_bitacora_display.append(f"{day_names_es[md_date.weekday()]}, {md_date.strftime('%d/%m/%Y')} - {day_names_es[sunday_date.weekday()]}, {sunday_date.strftime('%d/%m/%Y')}")

        self._update_date_range_display()
        self._update_bitacora_monday_selector_ui()
        self._update_campaign_list(valid_campaign_adset_pairs)

    def _update_bitacora_monday_selector_ui(self):
        if hasattr(self, 'combo_bitacora_monday') and self.combo_bitacora_monday.winfo_exists():
            if self.detected_mondays_for_bitacora_display:
                self.combo_bitacora_monday['values'] = self.detected_mondays_for_bitacora_display
                current_selection = self.bitacora_selected_monday_week_var.get()
                if current_selection not in self.detected_mondays_for_bitacora_display:
                     # No preseleccionar si hay selección por calendario
                    if not self.bitacora_selected_week_start_date_var.get():
                        self.bitacora_selected_monday_week_var.set(self.detected_mondays_for_bitacora_display[0])
                    else:
                        self.bitacora_selected_monday_week_var.set("") # Clear combobox if calendar was used


                if self.report_type.get() == "Bitácora" and self.bitacora_comparison_type.get() == "Weekly":
                     self.combo_bitacora_monday.configure(state='readonly')
                     self.lbl_weekly_info.config(text=f"({len(self.detected_mondays_for_bitacora_display)} sem. con datos detectadas)")
                else:
                     self.combo_bitacora_monday.configure(state='disabled')
                     self.lbl_weekly_info.config(text="")
                self.lbl_bitacora_monday_info.configure(text="(Semanas con datos. Auto si no se selecciona por calendario.)")
            else:
                self.combo_bitacora_monday['values'] = []
                self.bitacora_selected_monday_week_var.set("")
                self.combo_bitacora_monday.configure(state='disabled')
                self.lbl_bitacora_monday_info.configure(text="(No se detectaron semanas con datos para selección.)")
                self.lbl_weekly_info.config(text="")
                if self.input_files:
                    self._update_status("No se detectaron Lunes adecuados para Bitácora Semanal (se usará lógica automática).")

    def _update_date_range_display(self):
        min_s=self.min_date_detected.strftime('%d/%m/%Y') if self.min_date_detected else "-"; max_s=self.max_date_detected.strftime('%d/%m/%Y') if self.max_date_detected else "-"
        try:
            if hasattr(self,'lbl_min_date') and self.lbl_min_date.winfo_exists(): self.lbl_min_date.config(text=f"Inicio: {min_s}")
            if hasattr(self,'lbl_max_date') and self.lbl_max_date.winfo_exists(): self.lbl_max_date.config(text=f"Fin: {max_s}")
        except tk.TclError: pass

    def select_output_dir(self):
        init_dir=self.output_dir.get() if os.path.isdir(self.output_dir.get()) else os.getcwd()
        dir_path=filedialog.askdirectory(title="Seleccionar Directorio de Salida",initialdir=init_dir)
        if dir_path: self.output_dir.set(dir_path); self._update_status(f"Directorio salida seleccionado: {dir_path}")

    def _update_status(self, msg):
        if hasattr(self,'text_status') and self.text_status and self.text_status.winfo_exists():
            try:
                timestamp = datetime.now().strftime('%H:%M:%S')
                full_msg = f"[{timestamp}] {msg}\n"
                self.text_status.config(state=tk.NORMAL)
                self.text_status.insert(tk.END, full_msg)
                self.text_status.see(tk.END)
                self.text_status.config(state=tk.DISABLED)
            except tk.TclError: pass

    def check_queue(self):
        try:
            while True:
                self._handle_queue_message(self.status_queue.get_nowait())
        except queue.Empty: pass
        except Exception as e: print(f"Error checking queue: {e}")
        finally:
            if hasattr(self,'root') and self.root and self.root.winfo_exists():
                 self.root.after(100, self.check_queue)

    def _handle_queue_message(self,msg):
        if msg=="---DONE---": self.processing_finished(success=True)
        elif msg=="---ERROR---": self.processing_finished(success=False)
        else: self._update_status(msg)

    def start_processing_thread(self):
        global procesar_reporte_rendimiento_func, procesar_reporte_bitacora_func 
        
        if procesar_reporte_rendimiento_func is None or procesar_reporte_bitacora_func is None:
            print("FATAL: Funciones de procesamiento no se importaron correctamente al inicio (start_processing_thread).")
            messagebox.showerror("Error Crítico", "Funciones de procesamiento no cargadas. Revise consola.")
            self._update_status("ERROR CRÍTICO: Fallo en importación inicial (detectado en start_processing_thread).")
            return

        if self.is_processing: self._update_status("ADVERTENCIA: Ya hay un proceso en curso."); return
        if not self.input_files: messagebox.showerror("Error","No hay archivos de entrada seleccionados."); return
        out_dir=self.output_dir.get(); out_file=self.output_filename_var.get()
        if not out_dir or not os.path.isdir(out_dir): messagebox.showerror("Error","Directorio de salida inválido."); return
        if not out_file: messagebox.showerror("Error","Especifica un nombre para el archivo de salida."); return
        if not out_file.lower().endswith(".txt"): out_file+=".txt"; self.output_filename_var.set(out_file)

        rep_type=self.report_type.get(); target_func=None; args_tuple=()
        camp_sel_display=self.selected_campaign.get(); camp_proc='__ALL__' if camp_sel_display in ["--- Todas ---",""] else camp_sel_display
        camp_log="Todas" if camp_proc=='__ALL__' else f"'{camp_sel_display}'"

        adset_sel_display = self.selected_adset.get()
        adsets_proc_list = ['__ALL__']
        if adset_sel_display and adset_sel_display not in ["--- Todos ---", ""]:
             adsets_proc_list = [adset_sel_display]
        adset_log = 'Todos' if adsets_proc_list[0] == '__ALL__' else f"'{adset_sel_display}'"

        try:
            if rep_type=="Bitácora":
                bitacora_comp_type = self.bitacora_comparison_type.get()
                
                selected_week_start_str = self.bitacora_selected_week_start_date_var.get()
                selected_week_end_str = self.bitacora_selected_week_end_date_var.get()

                if bitacora_comp_type == "Weekly":
                    # Usar la semana seleccionada del combobox como fallback si la del calendario está vacía
                    if not selected_week_start_str and not selected_week_end_str:
                        combo_date_str_display = self.bitacora_selected_monday_week_var.get()
                        if combo_date_str_display and " - " in combo_date_str_display:
                            try:
                                # Asumimos que el formato es "Lun, DD/MM/YYYY - Dom, DD/MM/YYYY"
                                monday_str_part = combo_date_str_display.split(" - ")[0].split(", ")[1]
                                selected_week_start_obj = date_parse(monday_str_part, dayfirst=True).date()
                                selected_week_end_obj = selected_week_start_obj + timedelta(days=6)
                                
                                selected_week_start_str = selected_week_start_obj.strftime('%d/%m/%Y')
                                selected_week_end_str = selected_week_end_obj.strftime('%d/%m/%Y')
                                self._update_status(f"Bitácora Semanal: Usando semana del combobox (fallback): {selected_week_start_str} a {selected_week_end_str}.")
                            except Exception as e_combo_parse_fallback:
                                self._update_status(f"Advertencia: Error parseando selección del combobox para fallback: {e_combo_parse_fallback}. Usando detección automática.")
                                selected_week_start_str = "" # Forzar detección automática en backend
                                selected_week_end_str = ""
                        else:
                            self._update_status("Bitácora Semanal: No hay selección de calendario ni de combobox. Usando detección automática.")
                            selected_week_start_str = "" # Forzar detección automática en backend
                            selected_week_end_str = ""
                    
                    if selected_week_start_str and selected_week_end_str:
                         self._update_status(f"Bitácora Semanal: Semana de referencia para proceso: {selected_week_start_str} a {selected_week_end_str}.")
                    else:
                         self._update_status("Bitácora Semanal: No se especificó semana válida. Se usará detección automática en backend.")
                
                target_func=procesar_reporte_bitacora_func; 
                args_tuple=(self.input_files.copy(), out_dir, out_file, self.status_queue, 
                            camp_proc, adsets_proc_list, 
                            selected_week_start_str, selected_week_end_str, 
                            bitacora_comp_type)

            elif rep_type=="Rendimiento":
                target_func=procesar_reporte_rendimiento_func; 
                args_tuple=(self.input_files.copy(),out_dir,out_file,self.status_queue,camp_proc, adsets_proc_list)
            else: messagebox.showerror("Error",f"Tipo reporte '{rep_type}' no reconocido."); return
        except NameError as ne_func: err=f"Error Código: Falta función principal: {ne_func}"; messagebox.showerror("Error Interno",err); self._update_status(f"ERROR CRÍTICO: {err}"); return
        except Exception as e_prep: err=f"Error preparando ejecución: {e_prep}"; messagebox.showerror("Error",err); self._update_status(f"ERROR CRÍTICO: {err}"); return

        self.is_processing=True; self.btn_generate.config(state=tk.DISABLED)
        try:
             self.text_status.config(state=tk.NORMAL); self.text_status.delete(1.0,tk.END); self.text_status.config(state=tk.DISABLED)
        except tk.TclError: pass
        self._update_status(f"🚀 Iniciando Reporte {rep_type} | Campaña: {camp_log} | AdSet(s): {adset_log}...");
        if rep_type == "Bitácora":
            self._update_status(f"   Tipo de comparación de Bitácora: {self.bitacora_comparison_type.get()}");

        self._update_status(f"   Salida: {os.path.join(out_dir,out_file)}"); self._update_status("   Por favor, espera...")
        self.processing_thread=threading.Thread(target=target_func,args=args_tuple,daemon=True); self.processing_thread.start()

    def processing_finished(self, success):
        self.is_processing=False
        try:
             if hasattr(self,'btn_generate') and self.btn_generate.winfo_exists():
                 self.btn_generate.config(state=tk.NORMAL)
        except tk.TclError: pass

        rep_type=self.report_type.get(); title=f"✅ ¡Éxito ({rep_type})!" if success else f"❌ ¡Error ({rep_type})!"
        final_log=f"\n🏁 ¡Proceso {'completado con éxito!' if success else 'terminó con ERRORES!'} Revisa el registro y el archivo de salida."; self._update_status(final_log)
        report_path=os.path.join(self.output_dir.get(),self.output_filename_var.get())
        msg=f"Reporte '{rep_type}' {'generado correctamente' if success else 'terminó con errores'}.\n\n{'Archivo guardado en' if success else 'Archivo (puede estar incompleto) en'}:\n{report_path}"
        if success: messagebox.showinfo(title,msg)
        else: messagebox.showerror(title,msg)
        
    def _open_calendar_selector(self):
        if Calendar is None: 
            messagebox.showerror("Error de Dependencia", "La librería 'tkcalendar' no está instalada o no se pudo importar.\nPor favor, instálala con: pip install tkcalendar")
            return

        calendar_window = tk.Toplevel(self.root)
        calendar_window.title("Seleccionar Fecha para Semana de Bitácora")
        calendar_window.transient(self.root)
        calendar_window.grab_set()

        mode_frame = ttk.Frame(calendar_window, padding=(10,5))
        mode_frame.pack(fill=tk.X)
        ttk.Label(mode_frame, text="Modo de selección:").pack(side=tk.LEFT, padx=(0,10))
        
        rb_monday = ttk.Radiobutton(mode_frame, text="Lunes de Semana (Lun-Dom)", variable=self.calendar_week_selection_mode, value="monday")
        rb_monday.pack(side=tk.LEFT, padx=5)
        rb_end_day = ttk.Radiobutton(mode_frame, text="Día Final de Semana (7 días hasta este día)", variable=self.calendar_week_selection_mode, value="end_day")
        rb_end_day.pack(side=tk.LEFT, padx=5)
        self.calendar_week_selection_mode.set("monday") # Default

        year_to_show, month_to_show, day_to_show = date.today().year, date.today().month, 1
        
        current_selection_date = None
        if self.bitacora_selected_week_start_date_var.get(): 
            try:
                parsed_entry_date = date_parse(self.bitacora_selected_week_start_date_var.get(), dayfirst=True).date()
                current_selection_date = parsed_entry_date
                year_to_show, month_to_show, day_to_show = parsed_entry_date.year, parsed_entry_date.month, parsed_entry_date.day
            except: pass
        elif self.max_date_detected:
            current_selection_date = self.max_date_detected.date()
            year_to_show, month_to_show, day_to_show = self.max_date_detected.year, self.max_date_detected.month, self.max_date_detected.day
        elif self.min_date_detected:
             current_selection_date = self.min_date_detected.date()
             year_to_show, month_to_show, day_to_show = self.min_date_detected.year, self.min_date_detected.month, self.min_date_detected.day
        
        cal = Calendar(calendar_window, selectmode='day', date_pattern='dd/mm/yyyy',
                       year=year_to_show, month=month_to_show, day=day_to_show, 
                       locale='es_ES' 
                       ) 
        if current_selection_date:
            try: cal.selection_set(current_selection_date)
            except Exception as e_cal_select_set: print(f"Advertencia: No se pudo preseleccionar fecha en calendario: {e_cal_select_set}")

        try: cal.config(firstweekday='monday')
        except tk.TclError: print("Advertencia: No se pudo configurar 'firstweekday' en tkcalendar. Usando default.")

        if self.min_date_detected: cal.config(mindate=self.min_date_detected.date())
        if self.max_date_detected: cal.config(maxdate=self.max_date_detected.date())

        if self.valid_mondays_for_calendar:
            cal.calevent_remove('all') 
            for monday_dt_obj in self.valid_mondays_for_calendar:
                cal.calevent_create(monday_dt_obj, 'Lunes Válido', 'valid_monday_tag')
                for i in range(1,7):
                    cal.calevent_create(monday_dt_obj + timedelta(days=i), 'Día en Semana Válida', 'valid_monday_tag')
            cal.tag_config('valid_monday_tag', background='lightgreen', foreground='black')

        cal.pack(pady=20, padx=20)

        def on_date_selected():
            selected_date_str = cal.get_date() 
            try:
                selected_date_obj = date_parse(selected_date_str, dayfirst=True).date()
                
                week_start_date = None
                week_end_date = None
                selection_mode = self.calendar_week_selection_mode.get()

                if selection_mode == "monday":
                    week_start_date = selected_date_obj - timedelta(days=selected_date_obj.weekday())
                    week_end_date = week_start_date + timedelta(days=6)
                elif selection_mode == "end_day":
                    week_end_date = selected_date_obj
                    week_start_date = week_end_date - timedelta(days=6)
                
                if week_start_date is None or week_end_date is None:
                    messagebox.showerror("Error de Modo", "Modo de selección de semana no reconocido.", parent=calendar_window)
                    return
                
                # Validar si la semana seleccionada tiene datos (basado en Lunes)
                monday_of_chosen_week = week_start_date # Ya es lunes
                if self.valid_mondays_for_calendar and monday_of_chosen_week not in self.valid_mondays_for_calendar:
                    # Chequear si algún día de la semana tiene datos, incluso si el Lunes no está marcado
                    has_data_in_chosen_week = False
                    for i in range(7):
                        day_to_check = week_start_date + timedelta(days=i)
                        if self.min_date_detected and self.max_date_detected:
                             if self.min_date_detected.date() <= day_to_check <= self.max_date_detected.date():
                                # Podríamos tener una lista de todos los días con datos si el rendimiento lo permite
                                # Por ahora, esta validación es solo sobre los Lunes marcados.
                                pass # Mejorar si es necesario.
                    
                    if not any(m <= week_start_date <= (m + timedelta(days=6)) for m in self.valid_mondays_for_calendar): # Chequeo más simple
                         messagebox.showwarning("Semana sin Datos Detectados", 
                                           f"La semana seleccionada ({week_start_date.strftime('%d/%m/%Y')} - {week_end_date.strftime('%d/%m/%Y')}) podría no contener datos en los archivos cargados. "
                                           "El reporte se generará, pero podría estar vacío para algunos períodos.", 
                                           parent=calendar_window)
                
                self.bitacora_selected_week_start_date_var.set(week_start_date.strftime('%d/%m/%Y'))
                self.bitacora_selected_week_end_date_var.set(week_end_date.strftime('%d/%m/%Y'))
                self._update_status(f"Semana de referencia para Bitácora actualizada: {week_start_date.strftime('%d/%m/%Y')} a {week_end_date.strftime('%d/%m/%Y')}")
                
                self.bitacora_selected_monday_week_var.set("") # Deseleccionar combobox
                
                calendar_window.destroy()
            except Exception as e_cal_sel:
                messagebox.showerror("Error de Fecha", f"Error procesando fecha del calendario: {e_cal_sel}", parent=calendar_window)

        ttk.Button(calendar_window, text="Seleccionar esta Semana", command=on_date_selected).pack(pady=10)
        calendar_window.wait_window()

    def _ask_day_of_week_for_ref_date(self):
        messagebox.showinfo("Información", "Utiliza el botón 'Seleccionar Semana...' para elegir el período de la Bitácora Semanal.")
        return None

# ============================================================
# PUNTO DE ENTRADA PRINCIPAL
# ============================================================
if __name__ == "__main__":
    print(f"DEBUG: __main__ block - os.getcwd(): {os.getcwd()}")
    try:
        if 'tk' not in globals(): raise NameError("'tk' no definido.")
        if relativedelta is None or date_parse is None:
             try:
                 _warn_root = tk.Tk(); _warn_root.withdraw()
                 messagebox.showwarning("Dependencia Faltante", "¡Advertencia! Falta 'python-dateutil'.\n\nInstala con: pip install python-dateutil\n\nSin esta librería, las funciones de comparación mensual y la Bitácora Semanal podrían no funcionar correctamente.")
                 _warn_root.destroy()
             except Exception as e_warn:
                 print("\nADVERTENCIA CRÍTICA: 'python-dateutil' no instalado o falló importación. Funciones de periodo (Bitácora, comp. mensual) podrían fallar.")
                 print(f"(Error al mostrar advertencia GUI: {e_warn})")

        root = tk.Tk(); app = ReportApp(root)
        root.update_idletasks()
        w_val=root.winfo_width();h_val=root.winfo_height();sw_val=root.winfo_screenwidth();sh_val=root.winfo_screenheight()
        cx_val=int(sw_val/2-w_val/2);cy_val=int(sh_val/2-h_val/2); root.geometry(f'{w_val}x{h_val}+{cx_val}+{cy_val}')
        root.mainloop()
    except NameError as e_name: print(f"ERROR FATAL NameError: {e_name}"); traceback.print_exc()
    except Exception as e_gui: print(f"ERROR FATAL GUI: {e_gui}"); traceback.print_exc()