#Archivo base.
#Solo utulizar como referencia 
#En caso de ser utulizado como referencia centrate en las partes logicas
#Partes Importantes PUT AND GET AND PAYLOADS 
#Las pertes importantes solo funcionan en ese codigo ya que esta diseñado para eso


import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
import threading
import queue
import requests
import pandas as pd
import re
from io import StringIO
import json
import time
import os
import sys
import base64
import logging
from datetime import datetime
from pathlib import Path
import concurrent.futures
import webbrowser
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
import importlib.util

class OptimizedInventoryApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Optimized Inventory Management")
        
        self.setup_custom_paths()
        
        self.user_id = self.get_user_id()
        if not self.user_id:
            self.root.quit()
            return
        
        self.root.attributes('-fullscreen', True)
        self.exit_fs_button = tk.Button(root, text="✕", command=self.toggle_fullscreen, 
                                      bg="red", fg="white", font=("Arial", 12, "bold"))
        self.exit_fs_button.place(x=10, y=10, width=30, height=30)
        
        self.setup_azure_theme()
        
        self.log_auto_save = True
        self.last_log_save = datetime.now()
        self.log_save_interval = 300
        
        self.setup_config()
        self.create_ui()
        self.setup_threading()
        
        self.data_cache = None
        self.filtered_data = None
        self.last_update = None
        #'PICKTO',
        self.UBICACIONES_ESPECIALES = ['BLOCK', "STAGE", 'JT', 'NPI', 'EOL', 'PICKTO','LST',
                                       '2-QUARANTINE', "2-BLOCK","T5-NPI","EOL","EOL-","T1",
                                       "T1-NPI","T2-NPI","T3-NPI","T4-NPI","T5-NPI","T7-NPI",
                                       "T8-NPI","T9-NPI","T10-NPI","T6-NPI","SOS","SOS-NPI",
                                       "EOL-8-1","EOL-8-2","EOL-8-3","EOL-8-4","EOL-8-5",
                                       "EOL-8-6","EOL-8-7","EOL-8-8","EOL-8-9","EOL-8-10",
                                       "EOL-8-11","EOL-8-12","EOL-8-13","EOL-8-14","EOL-8-15",
                                       "EOL-8-16","EOL-8-17","EOL-8-18","EOL-8-19","EOL-8-20",
                                       "EOL-8-21","EOL-8-22","EOL-8-23","EOL-8-24","EOL-8-25",
                                       "EOL-8-26","EOL-8-27","EOL-8-28","EOL-8-29","EOL-8-30",
                                       "EOL-8-31","EOL-8-32","EOL-8-33","EOL-8-34","EOL-8-35",
                                       "EOL-1-1","EOL-1-2","EOL-1-3","EOL-1-4","EOL-1-5",
                                       "EOL-1-6","EOL-1-7","EOL-1-8","EOL-1-9","EOL-1-10",
                                       "EOL-1-11","EOL-1-12","EOL-1-13","EOL-1-14","EOL-1-15",
                                       "EOL-1-16","EOL-1-17","EOL-1-18","EOL-1-19","EOL-1-20",
                                       "EOL-1-21","EOL-1-22","EOL-1-23","EOL-1-24","EOL-1-25",
                                       "EOL-1-26","EOL-1-27","EOL-1-28","EOL-1-29","EOL-1-30",
                                       "EOL-1-31","EOL-1-32","EOL-1-33","EOL-1-34","EOL-1-35",
                                       "EOL-9-1","EOL-9-2","EOL-9-3","EOL-9-4","EOL-9-5",
                                       "EOL-9-6","EOL-9-7","EOL-9-8","EOL-9-9","EOL-9-10",
                                       "EOL-9-11","EOL-9-12","EOL-9-13","EOL-9-14","EOL-9-15",
                                       "EOL-9-16","EOL-9-17","EOL-9-18","EOL-9-19","EOL-9-20",
                                       "EOL-9-21","EOL-9-22","EOL-9-23","EOL-9-24","EOL-9-25",
                                       "EOL-9-26","EOL-9-27","EOL-9-28","EOL-9-29","EOL-9-30",
                                       "EOL-9-31","EOL-9-32","EOL-9-33","EOL-9-34","EOL-9-35",
                                       "EOL-7-1","EOL-7-2","EOL-7-3","EOL-7-4","EOL-7-5",
                                       "EOL-7-6","EOL-7-7","EOL-7-8","EOL-7-9","EOL-7-10",
                                       "EOL-7-11","EOL-7-12","EOL-7-13","EOL-7-14","EOL-7-15",
                                       "EOL-7-16","EOL-7-17","EOL-7-18","EOL-7-19","EOL-7-20",
                                       "EOL-7-21","EOL-7-22","EOL-7-23","EOL-7-24","EOL-7-25",
                                       "EOL-7-26","EOL-7-27","EOL-7-28","EOL-7-29","EOL-7-30",
                                       "EOL-7-31","EOL-7-32","EOL-7-33","EOL-7-34","EOL-7-35",
                                       "EOL-6-1","EOL-6-2","EOL-6-3","EOL-6-4","EOL-6-5",
                                       "EOL-6-6","EOL-6-7","EOL-6-8","EOL-6-9","EOL-6-10",
                                       "EOL-6-11","EOL-6-12","EOL-6-13","EOL-6-14","EOL-6-15",
                                       "EOL-6-16","EOL-6-17","EOL-6-18","EOL-6-19","EOL-6-20",
                                       "EOL-6-21","EOL-6-22","EOL-6-23","EOL-6-24","EOL-6-25",
                                       "EOL-6-26","EOL-6-27","EOL-6-28","EOL-6-29","EOL-6-30",
                                       "EOL-6-31","EOL-6-32","EOL-6-33","EOL-6-34","EOL-6-35",
                                       "EOL-10-1","EOL-10-2","EOL-10-3","EOL-10-4","EOL-10-5",
                                       "EOL-10-6","EOL-10-7","EOL-10-8","EOL-10-9","EOL-10-10",
                                       "EOL-10-11","EOL-10-12","EOL-10-13","EOL-10-14","EOL-10-15",
                                       "EOL-10-16","EOL-10-17","EOL-10-18","EOL-10-19","EOL-10-20",
                                       "EOL-10-21","EOL-10-22","EOL-10-23","EOL-10-24","EOL-10-25",
                                       "EOL-10-26","EOL-10-27","EOL-10-28","EOL-10-29","EOL-10-30",
                                       "EOL-10-31","EOL-10-32","EOL-10-33","EOL-10-34","EOL-10-35",
                                       "T1-NPI","T2-NPI","T3-NPI","T4-NPI","T5-NPI","T7-NPI",
                                       "T8-NPI","T9-NPI","T10-NPI","T6-NPI","SOS-NPI","EOL-NPI",]
        
        self.processed_inventories = set()  
        
        self.thread_lock = threading.Lock()
        
        self.bulk_data = None
        self.bulk_groups = None
        self.bulk_running = False
        self.bulk_stop_requested = False
        
        self.rack_data = None
        self.rack_groups = None
        self.rack_running = False
        self.rack_stop_requested = False
        
        self.load_initial_data()
        
        self.root.after(self.log_save_interval * 1000, self.check_auto_save_logs)
    
    def setup_custom_paths(self):
        base_path = r"C:\Users\bryan.marcial\OneDrive - GXO\Documents\PYTHON\Code\Local\Teo PutAway\Juntar LPN"
        
        self.LOG_DIR = os.path.join(base_path, "LOG")
        self.PRINT_DIR = os.path.join(base_path, "print")
        self.REGISTRO_DIR = os.path.join(base_path, "REGISTRO")
        self.DATA_DIR = os.path.join(os.path.dirname(base_path), "Data")
        self.NAMES_CSV = os.path.join(self.DATA_DIR, "PutAway", "Requeriments", "Names.csv")
        
        for directory in [self.LOG_DIR, self.PRINT_DIR, self.REGISTRO_DIR]:
            if not os.path.exists(directory):
                try:
                    os.makedirs(directory)
                    print(f"Directorio creado: {directory}")
                except Exception as e:
                    print(f"Error creando directorio {directory}: {str(e)}")
        
        # Verificar que existe el archivo Names.csv
        if not os.path.exists(self.NAMES_CSV):
            print(f"¡Advertencia! No se encontró el archivo {self.NAMES_CSV}")
            
            # Intentar buscar en ubicaciones alternativas
            alt_paths = [
                os.path.join(os.path.dirname(base_path), "Data", "Names.csv"),
                os.path.join(base_path, "Names.csv"),
                "Names.csv"
            ]
            
            for alt_path in alt_paths:
                if os.path.exists(alt_path):
                    self.NAMES_CSV = alt_path
                    print(f"Se utilizará el archivo alternativo: {alt_path}")
                    break
    
    def toggle_fullscreen(self):
        is_fullscreen = self.root.attributes('-fullscreen')
        self.root.attributes('-fullscreen', not is_fullscreen)
        if is_fullscreen:
            self.root.geometry("1280x900")
            self.exit_fs_button.config(text="□")
        else:
            self.exit_fs_button.config(text="✕")
    
    def setup_azure_theme(self):
        try:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            azure_theme_dir = os.path.join(base_dir, "Azure-ttk-theme-main")
            azure_tcl_path = os.path.join(azure_theme_dir, "azure.tcl")

            if os.path.exists(azure_tcl_path):
                self.root.tk.call('source', azure_tcl_path)
                self.root.tk.call('set_theme', 'light')
                print(f"Tema Azure cargado d    desde: {azure_tcl_path}")
            else:
                print(f"Error: No se encontró azure.tcl en {azure_tcl_path}")

        except Exception as e:
            print(f"Error cargando tema: {str(e)}")

    def setup_config(self):
        # Importar dinámicamente los headers desde Config/URLS.py
        try:
            spec = importlib.util.spec_from_file_location("URLS", "Config/URLS.py")
            urls_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(urls_module)
            
            # Usar los headers actualizados de URLS.py
            headers_inventory = urls_module.headersWMX.copy()
            headers_move = urls_module.headersWMX.copy()
            
        except Exception as e:
            print(f"Error al importar headers de URLS.py: {e}")
            # Fallback a headers hardcodeados en caso de error
            headers_inventory = {
                "authorization": "Bearer eyJhbGciOiJodHRwOi8vd3d3LnczLm9yZy8yMDAxLzA0L3htbGRzaWctbW9yZSNyc2Etc2hhMjU2IiwidHlwIjoiSldUIn0.eyJodHRwOi8vc2NoZW1hcy5taWNyb3NvZnQuY29tL3dzLzIwMDgvMDYvaWRlbnRpdHkvY2xhaW1zL3JvbGUiOiJNT0JJTEVEWU5DWUNDLENGR1NFQ1RJT04sTU9CSUxFSU5WTU9WRSxJTlZMUE4sTU9CSUxFUkVQTEVOLE1PQklMRVRSQU5TRkVSUExBTixNT0JJTEVQSUNLQ0xVU1RFUkJBVENILE1PQklMRVBaUElDSyxUQVNLQVVESVRQTEFOLElOVkxPVCxDRkdET0NVTUVOVFMsQ09VTlRSRVFVRVNUUkVWSUVXLENZQ0xFQ09VTlRJTVBPUlQsTU9CSUxFSU5WTE9ULElOVk1PVkUsTU9CSUxFVVNFUlNFVFRJTkdTLENGR0NBUlJJRVJUUkFDS0lORyxDRkdDTElFTlQsTUFOSUZFU1QsQ0ZHTE9UVkFMSURBVElPTixNT0JJTEVPUEVORFJPUElEUyxDRkdDT01QQU5ZLENGR0RPQ0NPTkZJRyxNQk9MLENGR1JFUExFTklTSE1FTlQsQ09OVEFJTkVSTU9WRSxUQVNLUkVQTEVOWk9ORSxNT0JJTEVNT1ZFLE1BTklGRVNUUkVQUklOVCxMUE5BREpVU1QsTU9CSUxFUElDS0lORyxPUkRFUixWRVJJRklDQVRJT04sUElDS0JZQkFUQ0gsTU9CSUxFTFBOQURKVVNULE9SREVSRElTQ1JFUEFOQ1ksVEFTS0hJU1RPUlksTU9CSUxFQ1lDTEVDT1VOVCxDRkdTS1VMT0NBVElPTixJTlZTTixET0NSRVBSSU5ULElOVkhPTEQsVEFTS0FVRElULFJFQ0VJVkUsQ0ZHUk9VVEVSVUxFUyxJTlZUUkFOU0ZFUixOQ0ksTU9CSUxFUExBTklOVk1PVkUsQ0ZHQ09ERU1BUCxDRkdNTkZDT05GSUcsTU9CSUxFQUxMT1BFTkRST1BJRFMsUE8sTU9CSUxFTFBOTU9WRSxNT0JJTEVWQVNUUkFOU0lUQ09OVEFJTkVSLENGR0RFVklDRVBSSU5URVIsRE9DS0FDSyxUQVNLREFTSEJPQVJELFBSSU5UUVVFVUUsTU9CSUxFU0lOR0xFTElORVJDVCxDRkdXQVZFQ09ERSxDT05UQUlORVJTSU5HTEUsQ0ZHQUxMT0NBVElPTixUQVNLUkVQTEVOSVNITUVOVCxBVVRPUkVQTEVOLExQTkNSRUFURSxNT0JJTEVQWlBVVCxPUkRFUlZFUklGWUhJU1QsTU9CSUxFR09SRVBMRU4sQ0ZHU0tVLElOVkxPVEFUVFIsQ0ZHRVFVSVBNRU5ULElOVlNQRUNJQUxNT1ZFLElOVlRSQU5TQUNUSU9OUyxUQVNLSU5WTU9WRSxNT0JJTEVWQVNQVVQsQ09OVEFJTkVSTVVMVEksVEFTS1JFUExFTlFVRVVFLE1PQklMRVBVVEFXQVksQ0ZHQVRUUklCVVRFQ09ORklHLElOVkFESlVTVE1FTlQsTUFOSUZFU1RFT0QsTUFOSUZFU1RWT0lELElOVkhPTERSRU1PVkVRVUVVRSxXQVZFLFNVUFBPUlREQVNIQk9BUkQsU0hJUE9SREVSLFRSQUNLSU5HQUREVVBEQVRFLExQTlJFR1JPVVAsQ0ZHUFJJTlRFUixDRkdQQUNLUE9TSVRJT04sQ0ZHTE9DQVRJT04sTU9CSUxFSU5TUEVDVCxNT0JJTEVJTlZCQUxBTkNFLElOVkhPTERUUkFOU0FDVElPTixDT05UQUlORVJWRVJJRklDQVRJT04sQ1lDTEVDT1VOVFBMQU4sQ1lDTEVDT1VOVFJFVklFVyxDRkdTUFJFQURTSEVFVElNUE9SVCxDRkdaT05FLEFTTixDRkdDT0RFVFlQRSxDRkdDT05GSUcsQ0ZHQ09VTlRFUixJTlZCQUxBTkNFLENGR1BBQ0tDT0RFLENGR1BVVEFXQVkiLCJodHRwOi8vc2NoZW1hcy5taWNyb3NvZnQuY29tL3dzLzIwMDgvMDYvaWRlbnRpdHkvY2xhaW1zL3VzZXJkYXRhIjoiVE1TMDAxIiwiZXhwIjoxNzU2NDg4MDQwLCJpc3MiOiJodHRwOi8vYXBpLXNlY3VyaXR5LXdteHAwMDEtd214LmFtLmd4by5jb20ifQ.k04THFvrMCG8DQEMGcT0qECIICb6O6OYAJXUjNAccHo2shvNHVZiOusaB5ALrnXRUfeeln2lkugGdnBMvUr8vEoYpvP3wdoc9q07l0xzVaoegyzfbmi45EeOMDu1lT6Y0Kj7yXbjtSZgUnTu_SLHztLtDu1T9ys-Ml8aGWBVVrO8GEEIpDlQgkj-JRm33GSx7Fi0oteZFJAbJFcFm5GqEIR0oNQjp1hawNPYCYV34DIjgnGlA015sFBSGaltRuKRyTMzuKjbUG98ln8bbCsvEHf7JGlW7GXp_11RyoSui3AX-fXbUAw2XCXIfD3A7Bfdayt1dk8Z3cK1FT_Hvtiqlw",
                'xposc-clientid': '1824',
                'xposc-deviceid': 'SES754173',
                'xposc-language': 'ENG',
                'xposc-siteid': 'TMS001',
                'xposc-userid': 'juan.martinez017'
            }
            headers_move = headers_inventory.copy()

        self.API_CONFIG = {
            'data_url': "https://bax01.am.gxo.com/schneider/superset/csv/IanSKue4h",
            'inventory_url': 'http://api-cqrs-wmxp001-wmx.am.gxo.com/queryservice/inventory/{inv_id}',
            'move_url': 'http://api-inventory-wmxp001-wmx.am.gxo.com/Inventory/Move',
            'headers': {
                'data': {
                    'Cookie': '_client=schneider; session=.eJxNj0tuwzAMRO-idVBQomRJuYxB8YMY9QewnEVR9O5V2kW75LzBcObTcT9tvo533d3dWURGAUm1aCEflGvlnJKKhiSSMGTKuZi7udlO7Q93N1q73tyz67nTpiOknR-0v2108kLrcK4H0_oC48WvcV5knH6afI0VX2E_gqAXoIZsqD6gxZK5aRHmojlLDUZeE3CAoXCLUiIrYPIIQDmS5wxTNPZq2FoSz9gmbYBWAwSJjTIkbYkpjH0YTaRNIY0WIYcoaRTp2vty7HM_7JqvZdPjebm7DwB_7EGn_GMF4OsbTaJjgQ.aGLnlA.f5yAvAU7peg4y13M2E_zta7wqcE'
                },
                'inventory': headers_inventory,
                'move': headers_move
            },
            'timeout': 100,
            'delay_between_requests': 0.4,
            'max_threads': 6
        }
        
        # Guardar referencia al módulo para recargar headers
        self.urls_module = urls_module if 'urls_module' in locals() else None
        
    def reload_headers(self):
        """Recarga los headers desde Config/URLS.py para obtener las últimas actualizaciones"""
        try:
            if self.urls_module:
                importlib.reload(self.urls_module)
            else:
                spec = importlib.util.spec_from_file_location("URLS", "Config/URLS.py")
                self.urls_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(self.urls_module)
            
            # Actualizar headers con los más recientes
            self.API_CONFIG['headers']['inventory'] = self.urls_module.headersWMX.copy()
            self.API_CONFIG['headers']['move'] = self.urls_module.headersWMX.copy()
            
            self.log("Headers actualizados desde Config/URLS.py", "INFO")
            return True
            
        except Exception as e:
            self.log(f"Error al recargar headers: {e}", "ERROR")
            return False
    def create_ui(self):
        self.main_frame = ttk.Frame(self.root, padding="10")
        self.main_frame.pack(fill=tk.BOTH, expand=True)
        
        self.notebook = ttk.Notebook(self.main_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True)
        
        self.individual_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.individual_tab, text="Consolidación Individual")
        
        self.html_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.html_tab, text="Generación de Etiquetas")
        
        self.bulk_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.bulk_tab, text="Consolidación Masiva")
        
        self.rack_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.rack_tab, text="Consolidación por RACK")
        
        self.global_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.global_tab, text="Consolidación Global")
        
        self.analysis_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.analysis_tab, text="Análisis")
        
        self.setup_individual_tab()
        
        self.setup_html_tab()
        
        self.setup_bulk_tab()
        
        self.setup_rack_tab()
        
        self.setup_global_tab()
        
        #self.setup_analysis_tab()
        
        self.setup_log_panel()
    
    def setup_individual_tab(self):
        self.ind_paned = ttk.PanedWindow(self.individual_tab, orient=tk.VERTICAL)
        self.ind_paned.pack(fill=tk.BOTH, expand=True)
        
        self.top_frame = ttk.Frame(self.ind_paned)
        self.bottom_frame = ttk.Frame(self.ind_paned)
        self.ind_paned.add(self.top_frame, weight=3)
        self.ind_paned.add(self.bottom_frame, weight=1)
        
        self.setup_control_panel()
        self.setup_filter_panel()
        self.setup_results_panel()
        self.setup_action_panel()

    def setup_control_panel(self):
        control_frame = ttk.LabelFrame(self.top_frame, text="Controls", padding="10")
        control_frame.pack(fill=tk.X, pady=5)
        
        ttk.Button(control_frame, text="Reload Data", command=self.load_initial_data).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="Clear Filters", command=self.clear_filters).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="Reset Threads", command=self.reset_thread_counter).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="Eliminar HTML Duplicados", command=self.eliminar_html_duplicados).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="Recargar Headers", command=self.reload_headers).pack(side=tk.LEFT, padx=5)
        
        self.data_status = ttk.Label(control_frame, text="No data loaded", foreground="gray")
        self.data_status.pack(side=tk.RIGHT, padx=10)

    def setup_filter_panel(self):
        filter_frame = ttk.LabelFrame(self.top_frame, text="Filters", padding="10")
        filter_frame.pack(fill=tk.X, pady=5)
        
        filter_frame.grid_columnconfigure(1, weight=1)
        filter_frame.grid_columnconfigure(3, weight=1)
        filter_frame.grid_columnconfigure(5, weight=1)
        
        ttk.Label(filter_frame, text="SKU:").grid(row=0, column=0, padx=5, sticky=tk.W)
        self.sku_combobox = ttk.Combobox(filter_frame, width=25)
        self.sku_combobox.grid(row=0, column=1, padx=5, sticky=tk.EW)
        self.sku_combobox.bind("<Return>", lambda e: self.validate_and_apply_filters())
        self.sku_combobox.bind("<<ComboboxSelected>>", lambda e: self.on_sku_selected())
        self.sku_combobox.bind("<KeyRelease>", lambda e: self.on_combobox_keyrelease(e))
        
        ttk.Label(filter_frame, text="LOC:").grid(row=0, column=2, padx=5, sticky=tk.W)
        self.loc_combobox = ttk.Combobox(filter_frame, width=25)
        self.loc_combobox.grid(row=0, column=3, padx=5, sticky=tk.EW)
        self.loc_combobox.bind("<Return>", lambda e: self.validate_and_apply_filters())
        self.loc_combobox.bind("<<ComboboxSelected>>", lambda e: self.on_loc_selected())
        self.loc_combobox.bind("<KeyRelease>", lambda e: self.on_combobox_keyrelease(e))
        
        ttk.Label(filter_frame, text="LOT:").grid(row=0, column=4, padx=5, sticky=tk.W)
        self.lot_combobox = ttk.Combobox(filter_frame, width=25)
        self.lot_combobox.grid(row=0, column=5, padx=5, sticky=tk.EW)
        self.lot_combobox.bind("<Return>", lambda e: self.validate_and_apply_filters())
        self.lot_combobox.bind("<KeyRelease>", lambda e: self.on_combobox_keyrelease(e))
        
        ttk.Button(filter_frame, text="Apply Filters", command=self.validate_and_apply_filters).grid(row=0, column=6, padx=5)

    def setup_results_panel(self):
        result_frame = ttk.LabelFrame(self.top_frame, text="Results", padding="10")
        result_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        self.tree = ttk.Treeview(result_frame, columns=(
            'INVENTORYID', 'LOC', 'LOT', 'LPN', 'SKU', 'QTY', 
            'Estado_Consolidacion', 'Tipo_Ubicacion'
        ), show='headings')
        
        columns = {
            'INVENTORYID': 'Inventory ID', 
            'LOC': 'Location', 
            'LOT': 'Lot', 
            'LPN': 'LPN',
            'SKU': 'SKU',
            'QTY': 'Quantity',
            'Estado_Consolidacion': 'Consolidation',
            'Tipo_Ubicacion': 'Location Type'
        }
        
        for col, text in columns.items():
            self.tree.heading(col, text=text)
            self.tree.column(col, width=100, anchor=tk.W)
        
        y_scroll = ttk.Scrollbar(result_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=y_scroll.set)
        
        self.tree.tag_configure('processed', background='#f0f0f0', foreground='#999999')
        
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        y_scroll.pack(side=tk.RIGHT, fill=tk.Y)

    def setup_action_panel(self):
        action_frame = ttk.LabelFrame(self.bottom_frame, text="Actions", padding="10")
        action_frame.pack(fill=tk.X, pady=5)
        
        ttk.Button(action_frame, text="Move Inventory", command=self.start_move_process).pack(side=tk.LEFT, padx=5)
        ttk.Button(action_frame, text="Export to CSV", command=self.export_to_csv).pack(side=tk.LEFT, padx=5)
        ttk.Button(action_frame, text="Show Stats", command=self.show_stats).pack(side=tk.LEFT, padx=5)
        ttk.Button(action_frame, text="Check Inventory Status", command=self.check_inventory_status).pack(side=tk.LEFT, padx=5)
        ttk.Button(action_frame, text="Verificar LOT Formato", command=self.check_lot_formats).pack(side=tk.LEFT, padx=5)

    def setup_log_panel(self):
        log_frame = ttk.LabelFrame(self.bottom_frame, text="Log", padding="10")
        log_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        self.log_area = scrolledtext.ScrolledText(
            log_frame, 
            height=10,
            wrap=tk.WORD,
            font=('Consolas', 14)
        )
        self.log_area.pack(fill=tk.BOTH, expand=True)
        
        color_map = {
            "info": "black",
            "success": "green",
            "warning": "orange",
            "error": "red",
            "critical": "red"
        }
        
        for tag, color in color_map.items():
            self.log_area.tag_configure(tag, foreground=color)
        
        log_control_frame = ttk.Frame(log_frame)
        log_control_frame.pack(fill=tk.X)
        
        ttk.Button(log_control_frame, text="Clear Log", 
                command=self.clear_log).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(log_control_frame, text="Save Log", 
                command=self.save_log).pack(side=tk.LEFT, padx=5)
        
        self.auto_save_var = tk.BooleanVar(value=self.log_auto_save)
        ttk.Checkbutton(log_control_frame, text="Auto-guardado", 
                       variable=self.auto_save_var,
                       command=self.toggle_auto_save).pack(side=tk.LEFT, padx=15)
        
        self.log(f"Sesión iniciada - Usuario ID: {self.user_id}", "SUCCESS")

    def setup_threading(self):
        self.message_queue = queue.Queue()
        self.threads = []
        self.running_threads = 0
        self.root.after(100, self.process_queue)

    def load_initial_data(self):
        if self.running_threads >= self.API_CONFIG['max_threads']:
            self.log("Maximum threads reached. Please wait...", "WARNING")
            return
        
        self.log("Loading data from API...")
        self.data_status.config(text="Loading...", foreground="blue")
        
        self.processed_inventories.clear()
        self.log("Lista de inventarios procesados reiniciada", "INFO")
        
        thread = threading.Thread(target=self.fetch_data_thread, daemon=True)
        thread.start()
        self.threads.append(thread)
        self.running_threads += 1
        
    def queue_message(self, message):
        self.message_queue.put(message)

    def fetch_data_thread(self):
        try:
            response = requests.get(
                self.API_CONFIG['data_url'],
                headers=self.API_CONFIG['headers']['data'],
                timeout=self.API_CONFIG['timeout']
            )
            
            if response.status_code != 200:
                self.queue_message(("ERROR", f"API Error: {response.status_code}"))
                return
            
            data = pd.read_csv(StringIO(response.text))
            
            data = self.process_data(data)
            self.queue_message(("DATA_LOADED", data))
            
        except Exception as e:
            self.queue_message(("ERROR", f"Data load error: {str(e)}"))

    def process_data(self, data):
        data['Inventory_ID_HEX'] = data['INVENTORYID'].apply(
            lambda x: ''.join(f"{ord(c):02X}" for c in str(x)) + '20'
        )
        
        data = data.drop(columns=['ALTSKU', 'ALLOCQTY'], errors='ignore')
        data['LOC'] = data['LOC'].fillna('').astype(str)
        
        data = self.apply_business_rules(data)
        return data

    def apply_business_rules(self, data):
        data['LOC_sorted'] = data['LOC'].apply(self.sort_key)
        
        data['sort_helper'] = data['LOC_sorted'].apply(lambda x: ''.join(x))
        data = data.sort_values(by='sort_helper').drop(columns='sort_helper')
        
        data['ALTITUD'] = data.apply(self.check_altitude, axis=1)
        data['SubLoc'] = data.apply(self.check_location_type, axis=1)
        
        data['Estado_Consolidacion'] = 'Pendiente'
        consolidated_skus = data[~data['LOC'].isin(self.UBICACIONES_ESPECIALES)]
        consolidated_skus = consolidated_skus.groupby('SKU')['LOC'].nunique()
        consolidated_skus = consolidated_skus[consolidated_skus == 1].index
        data.loc[data['SKU'].isin(consolidated_skus), 'Estado_Consolidacion'] = 'Consolidado'
        
        data['Tipo_Ubicacion'] = 'Normal'
        dual_skus = data[~data['LOC'].isin(self.UBICACIONES_ESPECIALES)]
        dual_skus = dual_skus.groupby('SKU')['ALTITUD'].agg(set)
        dual_skus = dual_skus[dual_skus.apply(lambda x: 'UP' in x and 'Pick' in x)].index
        data.loc[data['SKU'].isin(dual_skus), 'Tipo_Ubicacion'] = 'DUAL'
        
        return data[["Inventory_ID_HEX", 'INVENTORYID', 'LOC', 'LOT', 'LPN', 'SKU', 'QTY',
                   'EDITWHO', 'LOC_sorted', 'ALTITUD', 'SubLoc', 'Estado_Consolidacion', 'Tipo_Ubicacion']]

    def sort_key(self, value):
        parts = re.split(r'(\d+)', str(value))
        return [f"{int(part):06d}" if part.isdigit() else part for part in parts if part]

    def check_altitude(self, row):
        loc_list = row['LOC_sorted']
        if row['LOC'] in self.UBICACIONES_ESPECIALES:
            return "Special"
        has_dash = False
        has_pdash = False
        has_hdash = False
        for item in loc_list:
            if item == '-':
                has_dash = True
            elif item == 'P-':
                has_pdash = True
            elif item == 'HD-':
                has_hdash = True
        if has_hdash and has_dash:
            last_num = None
            for element in reversed(loc_list):
                try:
                    last_num = int(element)
                    break
                except:
                    continue
            if last_num is not None:
                return "Pick" if last_num < 5 else "UP"
            return "UP"
        elif has_hdash:
            return "HD"
        elif has_pdash:
            return "Pick"
        elif has_dash:
            return "UP"
        else:
            return "Others"

    def check_location_type(self, row):
        joined_loc = ' '.join(row['LOC_sorted'])
        if 'HD-' in joined_loc:
            return "HD"
        elif 'P-' in joined_loc:
            return "P"
        elif '-' in joined_loc:
            return "-"
        else:
            return "Others"

    def validate_and_apply_filters(self):
        try:
            sku = self.sku_combobox.get().strip()
            loc = self.loc_combobox.get().strip()
            lot = self.lot_combobox.get()
            
            if lot:
                self.log("Verificando formato del lote ingresado:", "INFO")
                self.verify_lot_format(lot, normalize=False)
            
            filtered = self.data_cache.copy()
            
            if sku:
                filtered = filtered[filtered['SKU'] == sku]
                if filtered.empty:
                    raise ValueError(f"SKU no encontrado: {sku}")
                locs = filtered['LOC'].unique().tolist()
                self.loc_combobox['values'] = locs
            
            if loc:
                filtered = filtered[filtered['LOC'] == loc]
                if filtered.empty:
                    raise ValueError(f"LOC no válida: {loc}")
                lots = filtered['LOT'].astype(str).unique().tolist()
                self.lot_combobox['values'] = lots
            
            if lot:
                self.log("Lotes disponibles en el dataset:", "INFO")
                available_lots = filtered['LOT'].astype(str).unique().tolist()
                for available_lot in available_lots[:5]:
                    self.verify_lot_format(available_lot, normalize=False)
                
                filtered = filtered[filtered['LOT'].astype(str) == lot]
                if filtered.empty:
                    raise ValueError(f"Lote no válido: {lot}")
            
            self.filtered_data = filtered
            self.update_treeview(filtered)
            
            filters_applied = []
            if sku: filters_applied.append(f"SKU: {sku}")
            if loc: filters_applied.append(f"LOC: {loc}")
            if lot: filters_applied.append(f"LOT: {lot}")
            
            if filters_applied:
                self.log(f"Filtros aplicados: {', '.join(filters_applied)} → {len(filtered)} registros", "SUCCESS")
            
        except Exception as e:
            self.log(f"Error en filtros: {str(e)}", "ERROR")

    def start_move_process(self):
        if self.filtered_data is None or len(self.filtered_data) == 0:
            self.log("No hay datos filtrados para procesar", "ERROR")
            messagebox.showerror("Error", "No hay datos filtrados para procesar. Aplique filtros primero.")
            return
        
        if len(self.filtered_data) < 2:
            self.log("Se necesitan al menos 2 registros para consolidar", "ERROR")
            messagebox.showerror("Error", "Se necesitan al menos 2 registros para consolidar.")
            return
        
        special_locs = self.filtered_data[self.filtered_data['LOC'].isin(self.UBICACIONES_ESPECIALES)]
        if not special_locs.empty:
            special_loc_list = ", ".join(special_locs['LOC'].unique())
            warning_msg = f"Hay ubicaciones especiales que serán excluidas: {special_loc_list}"
            self.log(warning_msg, "WARNING")
            messagebox.showwarning("Advertencia", warning_msg)
        
        processed_data = self.filtered_data[self.filtered_data['INVENTORYID'].isin(self.processed_inventories)]
        if not processed_data.empty:
            processed_ids = list(processed_data['INVENTORYID'])[:5]
            processed_count = len(processed_data)
            warning_msg = f"Hay {processed_count} inventarios que ya han sido procesados anteriormente. "
            warning_msg += f"Ejemplos: {', '.join(map(str, processed_ids))}"
            warning_msg += "\n\n¿Desea excluir estos registros y continuar solo con los no procesados?"
            
            self.log(warning_msg, "WARNING")
            if messagebox.askyesno("Inventarios ya procesados", warning_msg):
                self.filtered_data = self.filtered_data[~self.filtered_data['INVENTORYID'].isin(self.processed_inventories)]
                self.log(f"Se excluyeron {processed_count} inventarios ya procesados", "INFO")
                
                self.update_treeview(self.filtered_data)
                
                if len(self.filtered_data) < 2:
                    self.log("No quedan suficientes registros para consolidar después de excluir los ya procesados", "ERROR")
                    messagebox.showerror("Error", "No quedan suficientes registros para consolidar.")
                    return
            else:
                self.log("Operación cancelada por el usuario", "INFO")
                return
        
        valid_data = self.filtered_data[~self.filtered_data['LOC'].isin(self.UBICACIONES_ESPECIALES)].copy()
        if len(valid_data) < 2:
            self.log("No hay suficientes registros válidos para consolidar después de excluir ubicaciones especiales", "ERROR")
            messagebox.showerror("Error", "No hay suficientes registros válidos para consolidar.")
            return
        
        try:
            valid_data['LPN_STR'] = valid_data['LPN'].astype(str)
            valid_data['LPN_INT'] = valid_data['LPN_STR'].apply(
                lambda x: int(x) if x.strip().isdigit() else float('inf')
            )
            
            min_idx = valid_data['LPN_INT'].idxmin()
            reference_lpn = valid_data.loc[min_idx, 'LPN_STR']
            
            self.log(f"Selección de LPN de referencia basada en valor numérico:", "INFO")
            top_lpns = valid_data.sort_values('LPN_INT').head(5)
            for idx, row in top_lpns.iterrows():
                self.log(f"  LPN: {row['LPN_STR']} → Valor numérico: {row['LPN_INT']}", "INFO")
            
            self.log(f"LPN de referencia seleccionado: {reference_lpn} (valor numérico más pequeño)", "SUCCESS")
            
        except Exception as e:
            self.log(f"Error seleccionando LPN por valor numérico: {str(e)}", "WARNING")
            reference_lpn = valid_data.loc[valid_data['LPN'].str.len().idxmin()]['LPN']
            self.log(f"Fallback: LPN de referencia seleccionado: {reference_lpn} (más corto)", "WARNING")
        
        num_payloads = len(valid_data[valid_data['LPN'] != reference_lpn])
        
        self.log(f"Resumen de consolidación:", "INFO")
        self.log(f"  - LPN destino: {reference_lpn}", "INFO")
        self.log(f"  - Total LPNs a consolidar: {num_payloads}", "INFO")
        self.log(f"  - SKU: {valid_data['SKU'].iloc[0]}", "INFO")
        self.log(f"  - Cantidad total: {valid_data['QTY'].sum()}", "INFO")
        
        if self.filtered_data['SKU'].nunique() > 1:
            warning_msg = "Hay múltiples SKUs diferentes en la selección. Se recomienda consolidar un solo SKU a la vez."
            self.log(warning_msg, "WARNING")
            if not messagebox.askyesno("Advertencia", f"{warning_msg}\n\n¿Desea continuar de todos modos?"):
                self.log("Proceso cancelado por el usuario", "INFO")
                return
        
        if self.running_threads >= self.API_CONFIG['max_threads']:
            self.log(f"Máximo de hilos alcanzado ({self.API_CONFIG['max_threads']}). Espere a que termine un proceso activo.", "WARNING")
            messagebox.showwarning("Advertencia", f"Máximo de hilos alcanzado ({self.API_CONFIG['max_threads']}). Espere a que termine un proceso activo.")
            return
        
        confirm_msg = (
            f"Detalles de la operación:\n"
            f"- LPN de destino: {reference_lpn}\n"
            f"- Número de LPNs a consolidar: {num_payloads}\n"
            f"- SKU: {valid_data['SKU'].iloc[0]}\n"
            f"- Cantidad total: {valid_data['QTY'].sum()}\n\n"
            f"¿Está seguro de proceder con la consolidación de {num_payloads} registros?\n"
            "Esta operación no se puede deshacer."
        )
        
        if not messagebox.askyesno("Confirmar Consolidación", confirm_msg):
            self.log("Operación cancelada por el usuario", "INFO")
            return
        
        self.log(f"Iniciando proceso de consolidación de {num_payloads} LPNs...", "INFO")
        thread = threading.Thread(target=self.move_inventory_thread, daemon=True)
        thread.start()
        self.threads.append(thread)
        self.running_threads += 1

    def move_inventory_thread(self):
        try:
            if self.log_auto_save:
                self.save_log(silent=True)
                
            start_time = time.time()
            
            valid_data = self.filtered_data[~self.filtered_data['LOC'].isin(self.UBICACIONES_ESPECIALES)].copy()
            
            if len(valid_data) < 2:
                self.log("Mínimo 2 registros requeridos", "ERROR")
                return
            
            valid_data['LOT'] = valid_data['LOT'].astype(str)
            valid_data['LPN'] = valid_data['LPN'].astype(str)
            valid_data['INVENTORYID'] = valid_data['INVENTORYID'].astype(str)
            valid_data['QTY'] = valid_data['QTY'].astype(float)

            
            try:
                valid_data['LPN_STR'] = valid_data['LPN'].astype(str)
                valid_data['LPN_INT'] = valid_data['LPN_STR'].apply(
                    lambda x: int(x) if x.strip().isdigit() else float('inf')
                )
                
                min_idx = valid_data['LPN_INT'].idxmin()
                reference = valid_data.loc[min_idx]
                
                self.log(f"Selección de LPN por valor numérico:", "INFO")
                top_lpns = valid_data.sort_values('LPN_INT').head(5)
                for idx, row in top_lpns.iterrows():
                    self.log(f"  LPN: {row['LPN_STR']} → Valor numérico: {row['LPN_INT']}", "INFO")
                    
                log_message = "LPN de referencia seleccionado (valor numérico más pequeño): {lpn}"
            except Exception as e:
                self.log(f"Error al seleccionar LPN por valor numérico: {e}", "WARNING")
                
                try:
                    valid_data['LPN_STR'] = valid_data['LPN'].astype(str)
                    valid_data['LPN_NUMERIC'] = valid_data['LPN_STR'].str.lstrip('0')
                    valid_data.loc[valid_data['LPN_NUMERIC'] == '', 'LPN_NUMERIC'] = '0'
                    min_idx = valid_data['LPN_NUMERIC'].str.len().idxmin()
                    min_len = valid_data.loc[min_idx, 'LPN_NUMERIC'].str.len()
                    min_len_lpns = valid_data[valid_data['LPN_NUMERIC'].str.len() == min_len]
                    
                    if len(min_len_lpns) > 1:
                        min_idx = min_len_lpns['LPN_NUMERIC'].astype(int).idxmin()
                    
                    reference = valid_data.loc[min_idx]
                    log_message = "LPN de referencia seleccionado (valor numérico más corto): {lpn}"
                except Exception as e2:
                    self.log(f"Error en método alternativo: {e2}", "WARNING")
                    min_idx = valid_data['LPN'].str.len().idxmin()
                    reference = valid_data.loc[min_idx]
                    log_message = "LPN de referencia seleccionado (más corto): {lpn}"

            first_lpn_original = str(reference['LPN'])
            first_lpn = first_lpn_original
            reference_loc = reference['LOC']
            self.log(log_message.format(lpn=first_lpn_original), "INFO")
            self.log(f"Verificando formato del LPN de referencia:", "INFO")
            first_lpn = self.verify_lot_format(first_lpn, normalize=True)
            
            if first_lpn != first_lpn_original:
                self.log(f"LPN de referencia normalizado: '{first_lpn_original}' → '{first_lpn}'", "WARNING")

            payloads = []
            targets = valid_data[valid_data['LPN'] != first_lpn_original]
            
            self.log(f"Número total de registros a procesar: {len(targets)}", "INFO")

            for idx, record in targets.iterrows():
                try:
                    if record['INVENTORYID'] in self.processed_inventories:
                        self.log(f"Saltando registro {idx+1}: Inventario {record['INVENTORYID']} ya fue procesado", "WARNING")
                        continue
                        
                    qty = int(float(record['QTY']))
                    if qty <= 0:
                        raise ValueError("Cantidad inválida")
                    
                    lot_original = str(record['LOT'])
                    lpn_original = str(record['LPN'])
                    
                    self.log(f"\nRegistro {idx+1}: Procesando SKU={record['SKU']}", "INFO")
                    
                    self.log(f"  - LOT original: '{lot_original}'", "INFO")
                    self.log(f"  - LPN original: '{lpn_original}'", "INFO")
                    
                    lot_value = self.verify_lot_format(lot_original, normalize=True)
                    lpn_value = self.verify_lot_format(lpn_original, normalize=True)
                    
                    inv_id_hex = record['Inventory_ID_HEX']
                    api_data = self.get_inventory_details(inv_id_hex)
                    
                    use_api_values = False
                    if 'error' not in api_data:
                        self.log(f"Comparación con API:", "INFO")
                        
                        if 'LOT' in api_data:
                            api_lot = str(api_data['LOT'])
                            api_lot_normalized = self.normalize_id(api_lot)
                            self.log(f"  - LOT en API: '{api_lot}' → Normalizado: '{api_lot_normalized}'", "INFO")
                            
                            # Usar valor normalizado de la API si es diferente
                            if api_lot_normalized != lot_value:
                                self.log(f"  ⚠️ Actualizando LOT a valor de API normalizado", "WARNING")
                                lot_value = api_lot_normalized
                                use_api_values = True
                        
                        if 'LPN' in api_data:
                            api_lpn = str(api_data['LPN'])
                            api_lpn_normalized = self.normalize_id(api_lpn)
                            self.log(f"  - LPN en API: '{api_lpn}' → Normalizado: '{api_lpn_normalized}'", "INFO")
                            
                            # Usar valor normalizado de la API si es diferente
                            if api_lpn_normalized != lpn_value:
                                self.log(f"  ⚠️ Actualizando LPN a valor de API normalizado", "WARNING")
                                lpn_value = api_lpn_normalized
                                use_api_values = True
                    
                    # Decidir qué valores usar (API o dataframe)
                    final_lot = lot_value
                    final_lpn = lpn_value
                    final_loc = record['LOC']
                    final_sku = record['SKU']
                    final_inv_id = record['INVENTORYID']
                    
                    if use_api_values and 'error' not in api_data:
                        self.log(f"  ℹ️ Utilizando valores normalizados de la API", "INFO")
                        if 'LOC' in api_data: final_loc = api_data['LOC']
                        if 'SKU' in api_data: final_sku = api_data['SKU']
                        if 'INVENTORYID' in api_data: final_inv_id = api_data['INVENTORYID']
                    
                    payloads.append({
                        'ClientId': '1824',
                        'DocKey': str(final_inv_id),
                        'DocLineNo': '',
                        'DocRefKey': '',
                        'DocType': 'MANUAL',
                        'FromLoc': final_loc,
                        'FromLot': final_lot,  # Valor normalizado a 10 dígitos
                        'FromLpn': final_lpn,  # Valor normalizado a 10 dígitos
                        'Sku': final_sku,
                        'ToLoc': reference_loc,
                        'ToLpn': first_lpn,  # Usar LPN normalizado
                        'TransCode': 'MOVE',
                        'Uom': 'EA',
                        'UomQty': qty,
                        'inventoryID': int(final_inv_id)
                    })
                except Exception as e:
                    self.log(f"Error en registro {record['LPN']}: {str(e)}", "ERROR")

            if not payloads:
                self.log("No hay movimientos válidos", "WARNING")
                return

            # Mostrar los payloads generados para verificación
            self.log(f"\nResumen de payloads generados ({len(payloads)}):", "INFO")
            for i, payload in enumerate(payloads[:3]):  # Mostrar solo los primeros 3 para no saturar el log
                self.log(f"  Payload {i+1}: SKU={payload['Sku']}, LOT={payload['FromLot']}, LPN={payload['FromLpn']}", "INFO")
            if len(payloads) > 3:
                self.log(f"  ... y {len(payloads) - 3} más", "INFO")

            # Solicitar confirmación al usuario
            if not messagebox.askyesno("Confirmar operación", 
                                      f"Se procesarán {len(payloads)} registros con LOT y LPN normalizados a 10 dígitos.\n\n"
                                      f"¿Desea continuar con la operación?"):
                self.log("Operación cancelada por el usuario", "INFO")
                return

            # 4. Configuración de esperas
            success = 0
            total = len(payloads)
            request_counter = 0
            processed_inv_ids = []  # Lista para almacenar los IDs procesados exitosamente

            # 5. Procesamiento de movimientos
            for idx, payload in enumerate(payloads):
                try:
                    self.log(f"Procesando movimiento {idx+1}/{total}: SKU {payload['Sku']}, Lote {payload['FromLot']}", "INFO")
                    
                    # Llamada a la API
                    response = requests.post(
                        self.API_CONFIG['move_url'],
                        json=payload,
                        headers=self.API_CONFIG['headers']['move'],
                        timeout=self.API_CONFIG.get('timeout', 10)
                    )
                    
                    if response.status_code in [200, 201]:
                        success += 1
                        self.log(f"Movimiento {idx+1} exitoso", "SUCCESS")
                        # Agregar el ID al conjunto de inventarios procesados
                        processed_inv_ids.append(str(payload['inventoryID']))
                    else:
                        error_text = response.text
                        self.log(f"Error {response.status_code}: {error_text}", "ERROR")
                    
                    # 6. Sistema de esperas inteligente
                    request_counter += 1
                    
                    # Espera base entre requests
                    if idx < total - 1:
                        base_delay = self.API_CONFIG.get('delay_between_requests', 1.0)
                        time.sleep(base_delay)
                    
                    # Espera aleatoria cada 30 requests
                    if request_counter % 30 == 0:
                        random_wait = random.uniform(5, 74)
                        self.log(f"Espera anti-saturación: {random_wait:.2f}s")
                        time.sleep(random_wait)
                    
                except requests.exceptions.RequestException as e:
                    self.log(f"Error de conexión: {str(e)}", "ERROR")
                except Exception as e:
                    self.log(f"Error inesperado: {str(e)}", "ERROR")

            # 7. Marcar inventarios como procesados
            with self.thread_lock:
                for inv_id in processed_inv_ids:
                    self.processed_inventories.add(inv_id)
                
                # Actualizar treeview para reflejar los inventarios procesados
                if self.filtered_data is not None:
                    self.update_treeview(self.filtered_data)
                
                # Mostrar información sobre los inventarios procesados
                self.log(f"Se han marcado {len(processed_inv_ids)} inventarios como procesados", "INFO")
                self.log(f"Total de inventarios procesados en esta sesión: {len(self.processed_inventories)}", "INFO")

            # 8. Generar etiqueta HTML para el LPN consolidado si hubo éxito
            label_filename = None
            if success > 0:
                try:
                    # Obtener detalles del LPN consolidado desde la API
                    self.log(f"Generando etiqueta para LPN consolidado: {first_lpn}", "INFO")
                    first_inv_id = self.filtered_data[self.filtered_data['LPN'] == first_lpn_original]['Inventory_ID_HEX'].iloc[0]
                    consolidated_data = self.get_inventory_details(first_inv_id)

                    if 'error' not in consolidated_data:
                        # Generar etiqueta HTML
                        label_filename = self.generate_label(consolidated_data)
                        if label_filename:
                            self.log(f"Etiqueta HTML generada exitosamente: {label_filename}", "SUCCESS")
                            # Preguntar si desea abrir la etiqueta en el navegador
                            if messagebox.askyesno("Etiqueta Generada", 
                                                f"Se ha generado la etiqueta: {label_filename}\n\n¿Desea abrirla en el navegador predeterminado?"):
                                self.open_html_in_browser(label_filename)
                    else:
                        self.log(f"No se pudo obtener datos para generar etiqueta", "WARNING")
                except Exception as e:
                    self.log(f"Error generando etiqueta HTML: {str(e)}", "ERROR")

            # 9. Reporte final
            total_time = time.time() - start_time
            self.log(f"Proceso completado: {success}/{total} éxitos")
            self.log(f"Tiempo total: {total_time:.2f}s")

            # Crear registro de movimientos en CSV
            if success > 0 and processed_inv_ids:
                self.log(f"Guardando registro de {len(processed_inv_ids)} movimientos en consolidación individual...", "INFO")
                self.log(f"Usuario ID: {self.user_id}", "INFO")
                self.save_movement_record(processed_inv_ids)

            # Recordatorio para recargar datos y mensaje sobre la etiqueta
            self.log("IMPORTANTE: Recargue los datos para actualizar el estado de los inventarios", "WARNING")

            # Guardar logs al finalizar un proceso crítico
            if self.log_auto_save:
                self.save_log(silent=True)

            # Mensaje emergente final
            completion_message = f"Proceso completado: {success}/{total} éxitos.\n\n"
            completion_message += f"Se han marcado {len(processed_inv_ids)} inventarios como procesados.\n\n"

            # Añadir información sobre la etiqueta generada si corresponde
            if success > 0 and 'label_filename' in locals() and label_filename:
                completion_message += f"Se ha generado una etiqueta HTML: {label_filename}\n"
                completion_message += "Puede abrirla en su navegador para imprimirla.\n\n"

            completion_message += "IMPORTANTE: Recargue los datos para actualizar el estado de los inventarios."

            messagebox.showinfo("Proceso Completado", completion_message)

        except Exception as e:
            self.log(f"Error crítico: {str(e)}", "CRITICAL")
        finally:
            with self.thread_lock:
                self.running_threads -= 1

    # Métodos auxiliares
    def update_treeview(self, data):
        """Actualiza el Treeview con datos"""
        self.tree.delete(*self.tree.get_children())
        for _, row in data.iterrows():
            # Verificar si el inventario ya ha sido procesado
            is_processed = row['INVENTORYID'] in self.processed_inventories

            # Establecer etiqueta para elementos procesados
            tags = ('processed',) if is_processed else ()

            self.tree.insert('', 'end', values=(
                row['INVENTORYID'], row['LOC'], row['LOT'], row['LPN'],
                row['SKU'], row['QTY'], row['Estado_Consolidacion'], row['Tipo_Ubicacion']
            ), tags=tags)

        # Configurar el estilo para elementos procesados
        self.tree.tag_configure('processed', background='#f0f0f0', foreground='#999999')

    def log(self, message, level="INFO"):
        """Registro de eventos"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Asignar colores según el nivel de log
        color_map = {
            "INFO": "black",
            "SUCCESS": "green",
            "WARNING": "orange",
            "ERROR": "red",
            "CRITICAL": "red"
        }

        color = color_map.get(level, "black")
        self.log_area.insert(tk.END, f"[{timestamp}] {level}: {message}\n", (level.lower(),))

        # Configurar tags para colores
        for tag, tag_color in color_map.items():
            self.log_area.tag_configure(tag.lower(), foreground=tag_color)

        self.log_area.see(tk.END)

        # Auto-guardar en casos críticos
        if level in ["ERROR", "CRITICAL"] and self.log_auto_save:
            self.save_log(silent=True)

    def clear_log(self):
        """Limpia el registro"""    
        if self.log_auto_save:
            self.save_log(silent=True)
        self.log_area.delete(1.0, tk.END)

    def save_log(self, silent=False):
        """Guarda el registro en archivo"""
        # Usar la carpeta LOG personalizada
        logs_dir = self.LOG_DIR
        if not os.path.exists(logs_dir):
            os.makedirs(logs_dir)

        # Crear nombre de archivo con fecha actual
        current_date = datetime.now().strftime('%Y%m%d')
        filename = os.path.join(logs_dir, f"inventory_log_{current_date}.txt")

        # Modo de apertura: append para no sobrescribir logs del mismo día
        with open(filename, 'a', encoding='utf-8') as f:
            # Añadir separador de tiempo para diferenciar entradas
            f.write(f"\n\n--- Sesión: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---\n\n")
            f.write(self.log_area.get(1.0, tk.END))

        self.last_log_save = datetime.now()

        if not silent:
            self.log(f"Log guardado: {filename}")

    def toggle_auto_save(self):
        """Activa o desactiva el guardado automático de logs"""
        self.log_auto_save = self.auto_save_var.get()
        self.log(f"Auto-guardado de logs {'activado' if self.log_auto_save else 'desactivado'}")
        
    def check_auto_save_logs(self):
        """Verifica si es necesario guardar los logs automáticamente"""
        try:
            if self.log_auto_save:
                # Si han pasado más de log_save_interval segundos desde el último guardado
                time_since_last_save = (datetime.now() - self.last_log_save).total_seconds()
                if time_since_last_save >= self.log_save_interval:
                    self.save_log(silent=True)
                    self.log("Auto-guardado de logs realizado", "INFO")
        finally:
            # Programar la próxima verificación
            self.root.after(self.log_save_interval * 1000, self.check_auto_save_logs)

    def export_to_csv(self):
        """Exporta datos a CSV"""
        data = self.filtered_data if self.filtered_data is not None else self.data_cache
        if data is None:
            self.log("No hay datos para exportar", "WARNING")
            return
        
        # Usar la carpeta REGISTRO personalizada
        if not os.path.exists(self.REGISTRO_DIR):
            os.makedirs(self.REGISTRO_DIR)
            
        filename = os.path.join(self.REGISTRO_DIR, f"inventory_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        data.to_csv(filename, index=False)
        self.log(f"Datos exportados: {filename}")
        
        # Guardar logs después de exportar
        if self.log_auto_save:
            self.save_log(silent=True)

    def show_stats(self):
        """Muestra estadísticas"""
        data = self.filtered_data if self.filtered_data is not None else self.data_cache
        if data is None:
            self.log("No hay datos para analizar", "WARNING")
            return
        
        stats = [
            f"Total registros: {len(data)}",
            f"SKUs únicos: {data['SKU'].nunique()}",
            f"Cantidad total: {data['QTY'].sum()}",
            f"Ubicaciones únicas: {data['LOC'].nunique()}",
            f"Consolidados: {len(data[data['Estado_Consolidacion'] == 'Consolidado'])}"
        ]
        
        self.log("\nEstadísticas:")
        for stat in stats:
            self.log(f"  {stat}")

    def clear_filters(self):
        """Limpia los filtros"""
        self.sku_combobox.set('')
        self.loc_combobox.set('')
        self.lot_combobox.set('')
        if self.data_cache is not None:
            self.update_treeview(self.data_cache)

    def process_queue(self):
        """Procesa mensajes en cola"""
        try:
            while True:
                msg_type, *content = self.message_queue.get_nowait()
                
                if msg_type == "DATA_LOADED":
                    self.data_cache = content[0]
                    self.last_update = datetime.now()
                    
                    # Actualizar treeview
                    self.update_treeview(self.data_cache)
                    
                    # Actualizar combobox SKU
                    skus = sorted(self.data_cache['SKU'].unique().tolist())
                    self.sku_combobox['values'] = skus
                    
                    # Actualizar estado y mostrar estadísticas
                    total_rows = len(self.data_cache)
                    total_skus = len(skus)
                    total_qty = self.data_cache['QTY'].sum()
                    total_locs = self.data_cache['LOC'].nunique()
                    consolidated = len(self.data_cache[self.data_cache['Estado_Consolidacion'] == 'Consolidado'])
                    
                    self.data_status.config(
                        text=f"Datos cargados: {total_rows} registros, {total_skus} SKUs", 
                        foreground="green"
                    )
                    
                    # Mostrar estadísticas en el log
                    self.log("Datos cargados correctamente:", "SUCCESS")
                    self.log(f"  - Total registros: {total_rows}", "SUCCESS")
                    self.log(f"  - SKUs únicos: {total_skus}", "SUCCESS")
                    self.log(f"  - Ubicaciones únicas: {total_locs}", "SUCCESS")
                    self.log(f"  - Cantidad total: {total_qty:,.2f}", "SUCCESS")
                    self.log(f"  - Consolidados: {consolidated} ({consolidated/total_rows*100:.1f}%)", "SUCCESS")
                
                elif msg_type == "ERROR":
                    self.log(content[0], "ERROR")
                    self.data_status.config(text="Error cargando datos", foreground="red")
                
                elif msg_type == "WARNING":
                    self.log(content[0], "WARNING")
                
        except queue.Empty:
            pass
        finally:
            self.root.after(100, self.process_queue)

    def get_inventory_details(self, inventory_id_hex):
        """Obtiene detalles del inventario desde la API"""
        try:
            response = requests.get(
                f'http://api-cqrs-wmxp001-wmx.am.gxo.com/queryservice/inventory/{inventory_id_hex}',
                headers=self.API_CONFIG['headers']['inventory'],
                timeout=self.API_CONFIG['timeout']
            )
            response.raise_for_status()
            data = response.json()
            data['inventory_id_hex'] = inventory_id_hex
            return data
        except Exception as e:
            self.log(f"Error al obtener detalles de inventario {inventory_id_hex}: {str(e)}", "ERROR")
            return {'inventory_id_hex': inventory_id_hex, 'error': str(e)}

    def generate_label(self, data, custom_folder=None):
        """Genera una etiqueta HTML para el inventario"""
        try:
            from barcode import Code128
            from barcode.writer import ImageWriter
            import base64
            import io
            import shutil

            def generate_barcode_base64(value):
                if not value:
                    return ""
                try:
                    value_str = str(value)
                    if not value_str.strip() or not all(32 <= ord(c) <= 127 for c in value_str):
                        return ""
                    barcode_buffer = io.BytesIO()
                    Code128(value_str, writer=ImageWriter()).write(
                        barcode_buffer,
                        options={'write_text': False, 'module_height': 12, 'module_width': 0.4}
                    )
                    barcode_buffer.seek(0)
                    return base64.b64encode(barcode_buffer.read()).decode('utf-8')
                except Exception as e:
                    self.log(f"Error generando código de barras para '{value}': {str(e)}", "ERROR")
                    return ""

            # Mantener LOT y LPN como strings para preservar los ceros a la izquierda
            # No aplicar strip() o int() para mantener el formato original
            lot_value = str(data.get('LOT', ''))
            lpn_value = str(data.get('LPN', ''))
            sku_value = str(data.get('SKU', ''))
            loc_value = str(data.get('LOC', ''))
            
            # Log para verificación
            self.log(f"Generando etiqueta con los siguientes valores:", "INFO")
            self.log(f"  - SKU: '{sku_value}'", "INFO")
            self.log(f"  - LOC: '{loc_value}'", "INFO")
            self.log(f"  - LOT: '{lot_value}'", "INFO")
            self.log(f"  - LPN: '{lpn_value}'", "INFO")
            
            # Verificar la carpeta destino
            if custom_folder:
                self.log(f"Se usará carpeta personalizada: {custom_folder}", "INFO")
            else:
                self.log(f"No se especificó carpeta personalizada, se usará predeterminada: {self.PRINT_DIR}", "INFO")
            
            self.log(f"NOTA: Los valores de LOT y LPN en la etiqueta se mostrarán sin ceros a la izquierda", "INFO")
            self.log(f"      para mejor visualización, pero los códigos de barras mantendrán el formato original", "INFO")
            
            # Generar códigos de barras
            sku_barcode = generate_barcode_base64(sku_value)
            loc_barcode = generate_barcode_base64(loc_value)
            lot_barcode = generate_barcode_base64(lot_value)
            lpn_barcode = generate_barcode_base64(lpn_value)
            
            # Generar HTML
            html_content = self.create_label_html(sku_value, sku_barcode, loc_value, loc_barcode,
                                              lot_value, lot_barcode, lpn_value, lpn_barcode)
            
            # Determinar la carpeta donde se guardará la etiqueta
            save_folder = custom_folder if custom_folder else self.PRINT_DIR
            
            # Asegurar que existe la carpeta principal
            if not os.path.exists(self.PRINT_DIR):
                os.makedirs(self.PRINT_DIR, exist_ok=True)
                
            # Asegurar que existe la carpeta personalizada
            if save_folder != self.PRINT_DIR and not os.path.exists(save_folder):
                try:
                    os.makedirs(save_folder, exist_ok=True)
                    self.log(f"Carpeta específica creada: {save_folder}", "SUCCESS")
                except Exception as folder_error:
                    self.log(f"Error creando carpeta específica: {str(folder_error)}", "ERROR")
                    save_folder = self.PRINT_DIR  # Usar carpeta por defecto en caso de error
            
            # Guardar archivo HTML con nombre que incluye timestamp para evitar sobrescritura
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Extraer información de locación para el ordenamiento
            import re
            try:
                # Buscar patrón como 208-37-4 o 208_37_4
                match = re.search(r'(\d+)[-_](\d+)[-_](\d+)', loc_value)
                if match:
                    seccion = int(match.group(1))
                    pasillo = int(match.group(2))
                    posicion = int(match.group(3))
                else:
                    seccion, pasillo, posicion = 999, 999, 999
            except Exception:
                seccion, pasillo, posicion = 999, 999, 999
            
            # Generar nombre con formato ordenable por locación
            base_filename = f"{seccion:03d}_{pasillo:03d}_{posicion:03d}_{sku_value}_{lot_value}_{lpn_value}_{timestamp}.html"
            
            self.log(f"Archivo generado con ordenamiento por locación: {seccion}-{pasillo}-{posicion}", "INFO")
            
            
            # Siempre guardar en la carpeta personalizada si está disponible
            if save_folder != self.PRINT_DIR:
                try:
                    # Guardar directamente en la carpeta personalizada
                    custom_filename = os.path.join(save_folder, base_filename)
                    
                    with open(custom_filename, "w", encoding="utf-8") as f:
                        f.write(html_content)
                    
                    self.log(f"Etiqueta HTML generada en carpeta específica: {custom_filename}", "SUCCESS")
                    
                    # También guardar copia en la carpeta predeterminada para compatibilidad
                    default_filename = os.path.join(self.PRINT_DIR, base_filename)
                    shutil.copy2(custom_filename, default_filename)
                    self.log(f"Copia de respaldo en carpeta predeterminada: {default_filename}", "INFO")
                    
                    return custom_filename  # Devolver la ruta a la carpeta personalizada
                except Exception as save_error:
                    self.log(f"Error guardando en carpeta específica: {str(save_error)}", "ERROR")
                    # En caso de error, guardar en la carpeta predeterminada
            
            # Si no hay carpeta personalizada o hubo un error, guardar en la predeterminada
            default_filename = os.path.join(self.PRINT_DIR, base_filename)
            with open(default_filename, "w", encoding="utf-8") as f:
                f.write(html_content)
            
            self.log(f"Etiqueta HTML generada en carpeta predeterminada: {default_filename}", "SUCCESS")
            return default_filename
        
        except ImportError:
            self.log("Error: Módulo 'barcode' no instalado. Instale con 'pip install python-barcode pillow'", "ERROR")
            return None
        except Exception as e:
            self.log(f"Error generando etiqueta: {str(e)}", "ERROR")
            return None

    def create_label_html(self, sku, sku_barcode, loc, loc_barcode, lot, lot_barcode, lpn, lpn_barcode):
        """Genera el contenido HTML para la etiqueta"""
        # Crear versiones sin ceros a la izquierda para visualización
        try:
            lot_display = str(int(lot)) if lot.strip().isdigit() else lot
            self.log(f"  - LOT para visualización: '{lot_display}' (original: '{lot}')", "INFO")
        except (ValueError, TypeError):
            lot_display = lot
            self.log(f"  - No se pudo convertir LOT: se usará valor original '{lot}'", "WARNING")
            
        try:
            lpn_display = str(int(lpn)) if lpn.strip().isdigit() else lpn
            self.log(f"  - LPN para visualización: '{lpn_display}' (original: '{lpn}')", "INFO")
        except (ValueError, TypeError):
            lpn_display = lpn
            self.log(f"  - No se pudo convertir LPN: se usará valor original '{lpn}'", "WARNING")
        
        html = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Etiqueta de Inventario</title>
            <style>
                @page {{
                    size: letter landscape;
                    margin: 0;
                }}
                body {{
                    font-family: Arial, sans-serif;
                    margin: 0;
                    padding: 0;
                    background-color: #fff;
                    width: 11in;
                    height: 8.5in;
                    display: flex;
                    justify-content: center;
                    align-items: center;
                }}
                .label-container {{
                    width: 10in;
                    border: 2px solid black;
                }}
                .row {{
                    width: 100%;
                    display: flex;
                    border-bottom: 1px solid black;
                }}
                .cell {{
                    padding: 5px;
                    text-align: center;
                }}
                .header-row {{
                    border-bottom: 1px solid black;
                }}
                .barcode-cell {{
                    width: 50%;
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    border-left: 1px solid black;
                }}
                .barcode-img {{
                    height: 80px;
                }}
                .title-cell {{
                    font-size: 35px;
                    font-weight: bold;
                    color: red;
                    text-align: left;
                    display: flex;
                    align-items: center;
                    padding-left: 10px;
                    height: 40px;
                    width: 80px;
                }}
                .value-cell-large {{
                    font-size: 100px;
                    font-weight: bold;
                    line-height: 1;
                    color: #00A7E1;
                    text-align: center;
                    padding: 10px;
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    height: 140px;
                    overflow: hidden;
                }}
                .half-width {{
                    width: 50%;
                }}
                .half-width-container {{
                    display: flex;
                    border-bottom: 1px solid black;
                }}
                .left-cell {{
                    border-right: 1px solid black;
                }}
            </style>
        </head>
        <body>
            <div class="label-container">
                <!-- SKU Row principal -->
                <div class="row header-row">
                    <div class="cell value-cell-large" style="width: 100%; text-align: center;">{sku}</div>
                </div>
                
                <!-- Fila con SKU y código de barras -->
                <div class="row">
                    <div class="title-cell">SKU</div>
                    <div class="barcode-cell">
                        <img class="barcode-img" src="data:image/png;base64,{sku_barcode}" alt="SKU Barcode">
                    </div>
                </div>
                
                <!-- LOC Row principal -->
                <div class="row header-row">
                    <div class="cell value-cell-large" style="width: 100%; text-align: center;">{loc}</div>
                </div>
                
                <!-- Fila con LOC y código de barras -->
                <div class="row">
                    <div class="title-cell">LOC</div>
                    <div class="barcode-cell">
                        <img class="barcode-img" src="data:image/png;base64,{loc_barcode}" alt="LOC Barcode">
                    </div>
                </div>
                
                <!-- Contenedor para LOT y LPN -->
                <div class="half-width-container">
                    <div class="half-width left-cell">
                        <div class="cell value-cell-large" style="font-size: 90px;">{lot_display}</div>
                    </div>
                    <div class="half-width">
                        <div class="cell value-cell-large" style="font-size: 90px;">{lpn_display}</div>
                    </div>
                </div>
                
                <!-- Fila de etiquetas LOT/LPN -->
                <div class="row" style="border-bottom: none; border-top: 1px solid black;">
                    <div class="title-cell" style="width: 50%; border-right: 1px solid black;">LOT</div>
                    <div class="title-cell" style="width: 50%;">LPN</div>
                </div>
                
                <!-- Fila de códigos de barra LOT/LPN -->
                <div class="row">
                    <div class="half-width" style="border-right: 1px solid black; display: flex; justify-content: center; padding: 5px 0;">
                        <img class="barcode-img" src="data:image/png;base64,{lot_barcode}" alt="LOT Barcode">
                    </div>
                    <div class="half-width" style="display: flex; justify-content: center; padding: 5px 0;">
                        <img class="barcode-img" src="data:image/png;base64,{lpn_barcode}" alt="LPN Barcode">
                    </div>
                </div>
            </div>
        </body>
        </html>
        """
        
        return html

    def format_lot(self, lot_value):
        """Mantiene el valor del lote exactamente como string para preservar ceros a la izquierda"""
        if lot_value is None:
            return ""
        
        # Simplemente convertir a string y quitar espacios
        return str(lot_value).strip()

    def verify_lot_format(self, lot_value, normalize=True):
        """
        Verifica y muestra el formato exacto de un valor LOT/LPN
        
        Args:
            lot_value: Valor a verificar
            normalize: Si se debe normalizar el valor a 10 dígitos
            
        Returns:
            String verificado y posiblemente normalizado si normalize=True,
            o el valor original sin cambios si normalize=False
        """
        original_value = lot_value
        string_value = str(lot_value)
        
        self.log(f"Verificación de formato:", "INFO")
        self.log(f"  - Valor original: '{original_value}' (tipo: {type(original_value).__name__})", "INFO")
        self.log(f"  - Como string: '{string_value}' (longitud: {len(string_value)})", "INFO")
        
        # Mostrar cada carácter y su código ASCII para verificar espacios o caracteres invisibles
        char_info = ", ".join([f"'{c}'({ord(c)})" for c in string_value])
        self.log(f"  - Análisis de caracteres: {char_info}", "INFO")
        
        # Normalizar a 10 dígitos solo si se solicita explícitamente
        if normalize:
            normalized = self.normalize_id(string_value, 10)
            if normalized != string_value:
                self.log(f"  - Normalizado a 10 dígitos: '{normalized}'", "WARNING")
                return normalized
        
        # Si no se debe normalizar, devolver el valor original sin cambios
        return string_value

    def normalize_id(self, value, target_length=10):
        """
        Normaliza un identificador (LOT o LPN) para asegurar que tenga exactamente 10 dígitos.
        Si ya tiene ceros a la izquierda o un formato específico, lo preserva.
        
        Args:
            value: El valor a normalizar
            target_length: Longitud objetivo (por defecto 10 dígitos)
            
        Returns:
            String normalizado con el formato correcto
        """
        # Convertir a string sin alterar
        str_value = str(value)
        
        # Si ya tiene la longitud correcta, no modificarlo
        if len(str_value) == target_length:
            return str_value
            
        # Si es un número y tiene menos de 10 dígitos, añadir ceros a la izquierda
        if str_value.isdigit() and len(str_value) < target_length:
            return str_value.zfill(target_length)
            
        # Si contiene caracteres especiales o ya está en un formato especial, mantenerlo
        if not str_value.isdigit():
            return str_value
            
        # Para cualquier otro caso, mantener el formato original
        return str_value

    def check_lot_formats(self):
        """Verifica y muestra el formato exacto de los lotes seleccionados"""
        if self.filtered_data is None or len(self.filtered_data) == 0:
            messagebox.showwarning("Advertencia", "No hay datos filtrados para verificar")
            return
            
        # Crear ventana para mostrar la información
        win = tk.Toplevel(self.root)
        win.title("Verificación de Formato de Lotes")
        win.geometry("800x600")
        
        # Frame principal
        main_frame = ttk.Frame(win, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Información general
        ttk.Label(main_frame, text="Análisis de formato de lotes en los datos seleccionados", 
                 font=("Arial", 12, "bold")).pack(anchor=tk.W, pady=5)
        
        ttk.Label(main_frame, text=f"Registros seleccionados: {len(self.filtered_data)}", 
                 font=("Arial", 10)).pack(anchor=tk.W)
        
        # Área de texto para resultados
        results_frame = ttk.LabelFrame(main_frame, text="Resultados del Análisis", padding="10")
        results_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        
        results_text = scrolledtext.ScrolledText(
            results_frame, 
            wrap=tk.WORD,
            font=('Consolas', 10)
        )
        results_text.pack(fill=tk.BOTH, expand=True)
        
        # Configurar etiquetas para colores
        results_text.tag_configure('header', font=('Consolas', 10, 'bold'))
        results_text.tag_configure('info', foreground='blue')
        results_text.tag_configure('warning', foreground='orange')
        results_text.tag_configure('error', foreground='red')
        results_text.tag_configure('success', foreground='green')
        
        # Analizar los lotes
        unique_lots = self.filtered_data['LOT'].astype(str).unique()
        
        results_text.insert(tk.END, f"Encontrados {len(unique_lots)} lotes únicos:\n\n", 'header')
        
        for idx, lot in enumerate(unique_lots):
            results_text.insert(tk.END, f"{idx+1}. Lote original: '{lot}' (tipo: {type(lot).__name__})\n", 'header')
            
            # Diferentes representaciones
            str_lot = str(lot)
            results_text.insert(tk.END, f"   - Como string: '{str_lot}' (longitud: {len(str_lot)})\n", 'info')
            
            # Mostrar cada carácter con su código ASCII
            char_info = ", ".join([f"'{c}'({ord(c)})" for c in str_lot])
            results_text.insert(tk.END, f"   - Análisis de caracteres: {char_info}\n", 'info')
            
            # Verificar si tiene ceros a la izquierda
            if str_lot.startswith('0'):
                zeros_count = len(str_lot) - len(str_lot.lstrip('0'))
                results_text.insert(tk.END, f"   - Tiene {zeros_count} ceros a la izquierda\n", 'warning')
                
                # Mostrar qué pasaría si se convierte a entero y luego a string
                int_repr = str(int(str_lot))
                if int_repr != str_lot:
                    results_text.insert(tk.END, f"   - ¡PRECAUCIÓN! Si se convierte a entero sería: '{int_repr}'\n", 'error')
            
            # Comparar con la API si hay pocos registros (máximo 5)
            if idx < 5:
                # Buscar un registro con este lote
                sample_record = self.filtered_data[self.filtered_data['LOT'].astype(str) == str_lot].iloc[0]
                try:
                    inv_id_hex = sample_record['Inventory_ID_HEX']
                    api_data = self.get_inventory_details(inv_id_hex)
                    
                    if 'error' not in api_data and 'LOT' in api_data:
                        api_lot = str(api_data['LOT'])
                        results_text.insert(tk.END, f"   - LOT en API: '{api_lot}'\n", 'info')
                        
                        if api_lot == str_lot:
                            results_text.insert(tk.END, f"   - ✅ Coincide exactamente con el valor de la API\n", 'success')
                        else:
                            results_text.insert(tk.END, f"   - ⚠️ No coincide con el valor de la API\n", 'error')
                except Exception as e:
                    results_text.insert(tk.END, f"   - Error verificando con API: {str(e)}\n", 'error')
            
            results_text.insert(tk.END, "\n")
        
        # Recomendaciones
        results_text.insert(tk.END, "\nRECOMENDACIONES:\n", 'header')
        results_text.insert(tk.END, "1. Siempre manejar LOT como string (str) para preservar ceros a la izquierda\n", 'info')
        results_text.insert(tk.END, "2. Nunca aplicar int() a los lotes, ya que elimina los ceros a la izquierda\n", 'warning')
        results_text.insert(tk.END, "3. Comparar siempre los valores exactos sin procesar con los de la API\n", 'info')
        
        # Botones
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=10)
        
        ttk.Button(button_frame, text="Exportar Análisis",  
                  command=lambda: self.export_lot_analysis(results_text.get(1.0, tk.END))).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(button_frame, text="Cerrar", 
                  command=win.destroy).pack(side=tk.RIGHT, padx=5)
    
    def export_lot_analysis(self, analysis_text):
        """Exporta el análisis de lotes a un archivo de texto"""
        # Usar la carpeta LOG personalizada
        if not os.path.exists(self.LOG_DIR):
            os.makedirs(self.LOG_DIR)
            
        filename = os.path.join(self.LOG_DIR, f"lot_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(analysis_text)
        self.log(f"Análisis de lotes exportado: {filename}", "SUCCESS")
        messagebox.showinfo("Exportación Exitosa", f"Análisis guardado como:\n{filename}")
        
        # Guardar logs después de exportar
        if self.log_auto_save:
            self.save_log(silent=True)

    def validate_lot(self, lot_number, inventory_id):
        """Valida que un número de lote exista en el sistema antes de intentar moverlo"""
        try:
            # Verificar y mostrar el formato exacto del lote
            self.log(f"Validando lote para inventario {inventory_id}:", "INFO")
            lot_string = self.verify_lot_format(lot_number)
            
            # Intentar obtener detalles del inventario
            inv_details = self.get_inventory_details(inventory_id)
            if 'error' in inv_details:
                return False, f"Error al obtener detalles: {inv_details['error']}"
            
            # Comprobar que el lote exista
            if 'LOT' in inv_details:
                # Convertir a string sin strip() para mantener formato original
                stored_lot = str(inv_details['LOT'])
                
                # Verificar y mostrar el formato del lote en la API
                self.log(f"Lote en API:", "INFO")
                api_lot_string = self.verify_lot_format(stored_lot)
                
                # Comparación directa de strings
                match = (lot_string == api_lot_string)
                self.log(f"Coincidencia exacta: {match}", "INFO" if match else "WARNING")
                
                if match:
                    return True, "Lote válido"
                
                # Si no hay coincidencia exacta, intentar normalizar y comparar
                normalized_input = self.normalize_id(lot_string)
                normalized_api = self.normalize_id(api_lot_string)
                normalized_match = (normalized_input == normalized_api)
                
                if normalized_match:
                    self.log(f"Coincidencia después de normalizar a 10 dígitos", "SUCCESS")
                    return True, "Lote válido después de normalización"
                
                return False, f"Lote en sistema ({stored_lot}) no coincide con el proporcionado ({lot_string})"
            else:
                return False, "No se pudo encontrar información del lote en el sistema"
        except Exception as e:
            return False, f"Error en validación: {str(e)}"

    def check_inventory_status(self):
        """Verifica el estado de un inventario específico para diagnóstico"""
        if self.filtered_data is None or len(self.filtered_data) == 0:
            messagebox.showwarning("Advertencia", "No hay datos filtrados para verificar")
            return
        
        inventory_ids = self.filtered_data['Inventory_ID_HEX'].tolist()
        
        if not inventory_ids:
            messagebox.showwarning("Advertencia", "No se encontraron IDs de inventario para verificar")
            return
        
        status_window = tk.Toplevel(self.root)
        status_window.title("Estado de Inventario")
        status_window.geometry("800x600")
        
        # Frame principal
        main_frame = ttk.Frame(status_window, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Área de información general
        info_frame = ttk.LabelFrame(main_frame, text="Información General", padding="10")
        info_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(info_frame, text=f"Registros seleccionados: {len(inventory_ids)}").pack(anchor=tk.W)
        ttk.Label(info_frame, text=f"SKU: {self.filtered_data['SKU'].iloc[0]}").pack(anchor=tk.W)
        ttk.Label(info_frame, text=f"Cantidad total: {self.filtered_data['QTY'].sum()}").pack(anchor=tk.W)
        
        # Área de resultados con pestañas
        notebook = ttk.Notebook(main_frame)
        notebook.pack(fill=tk.BOTH, expand=True, pady=10)
        
        # Pestaña para resultados de verificación
        verify_tab = ttk.Frame(notebook)
        notebook.add(verify_tab, text="Verificación")
        
        results_text = scrolledtext.ScrolledText(
            verify_tab,
            wrap=tk.WORD,
            font=('Consolas', 10)
        )
        results_text.pack(fill=tk.BOTH, expand=True)
        
        # Pestaña para problemas detectados
        issues_tab = ttk.Frame(notebook)
        notebook.add(issues_tab, text="Problemas")
        
        issues_text = scrolledtext.ScrolledText(
            issues_tab,
            wrap=tk.WORD,
            font=('Consolas', 10)
        )
        issues_text.pack(fill=tk.BOTH, expand=True)
        
        # Configurar etiquetas para colores
        for text_widget in [results_text, issues_text]:
            text_widget.tag_configure('error', foreground='red')
            text_widget.tag_configure('warning', foreground='orange')
            text_widget.tag_configure('success', foreground='green')
            text_widget.tag_configure('info', foreground='blue')
            text_widget.tag_configure('header', font=('Consolas', 10, 'bold'))
        
        # Procesar cada ID (limitado a 10 para no sobrecargar)
        max_to_check = min(10, len(inventory_ids))
        issues_found = 0
        
        for idx, inv_id in enumerate(inventory_ids[:max_to_check]):
            try:
                results_text.insert(tk.END, f"\n--- Verificando registro {idx+1}/{max_to_check} (ID: {inv_id}) ---\n", 'header')
                data = self.get_inventory_details(inv_id)
                
                if 'error' in data:
                    results_text.insert(tk.END, f"Error: {data['error']}\n", 'error')
                    issues_text.insert(tk.END, f"Registro {idx+1}: Error al obtener datos del ID {inv_id}\n", 'error')
                    issues_found += 1
                else:
                    # Mostrar atributos relevantes
                    relevant_fields = ['INVENTORYID', 'SKU', 'LOC', 'LOT', 'LPN', 'QTY', 'STATUS']
                    for field in relevant_fields:
                        if field in data:
                            results_text.insert(tk.END, f"{field}: {data[field]}\n")
                    
                    # Verificar si hay problemas
                    if 'STATUS' in data and data['STATUS'] != 'Available':
                        status_msg = f"ATENCIÓN: Estado del inventario es '{data['STATUS']}', no 'Available'.\n"
                        results_text.insert(tk.END, status_msg, 'error')
                        issues_text.insert(tk.END, f"Registro {idx+1}: {status_msg}", 'error')
                        issues_found += 1
                    
                    # Verificar cantidad
                    if 'QTY' in data and float(data['QTY']) <= 0:
                        qty_msg = f"ATENCIÓN: Cantidad es {data['QTY']}, debe ser mayor a 0.\n"
                        results_text.insert(tk.END, qty_msg, 'error')
                        issues_text.insert(tk.END, f"Registro {idx+1}: {qty_msg}", 'error')
                        issues_found += 1
                    
                    # Verificar formato del lote
                    if 'LOT' in data:
                        lot_value = str(data['LOT'])
                        results_text.insert(tk.END, f"Análisis de LOT: '{lot_value}' (longitud: {len(lot_value)})\n", 'info')
                        # Verificar si tiene ceros a la izquierda
                        if lot_value.startswith('0'):
                            zeros_count = len(lot_value) - len(lot_value.lstrip('0'))
                            results_text.insert(tk.END, f"LOT tiene {zeros_count} ceros a la izquierda\n", 'warning')
                        # Verificar longitud
                        if len(lot_value) != 10 and lot_value.isdigit():
                            results_text.insert(tk.END, f"LOT no tiene 10 dígitos (tiene {len(lot_value)})\n", 'warning')
                
            except Exception as e:
                results_text.insert(tk.END, f"Error al procesar: {str(e)}\n", 'error')
                issues_text.insert(tk.END, f"Registro {idx+1}: Error inesperado: {str(e)}\n", 'error')
                issues_found += 1
        
        # Actualizar la pestaña de problemas con resumen
        if issues_found > 0:
            issues_text.insert(tk.END, f"\n--- Resumen ---\n", 'header')
            issues_text.insert(tk.END, f"Se encontraron {issues_found} problemas en {max_to_check} registros verificados.\n", 'error')
            notebook.select(issues_tab)  # Mostrar la pestaña de problemas por defecto
        else:
            issues_text.insert(tk.END, "No se encontraron problemas en los registros verificados.\n", 'success')
        
        # Botones
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=10)
        
        ttk.Button(button_frame, text="Exportar Reporte", 
                  command=lambda: self.export_inventory_report(results_text.get(1.0, tk.END))).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(button_frame, text="Cerrar", 
                  command=status_window.destroy).pack(side=tk.RIGHT, padx=5)
        
        # Actualizar la interfaz de usuario
        status_window.update()

    def export_inventory_report(self, report_text):
        """Exporta el reporte de verificación de inventario a un archivo de texto"""
        # Usar la carpeta LOG personalizada
        if not os.path.exists(self.LOG_DIR):
            os.makedirs(self.LOG_DIR)
            
        filename = os.path.join(self.LOG_DIR, f"inventory_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(report_text)
        self.log(f"Reporte exportado: {filename}", "SUCCESS")
        messagebox.showinfo("Exportación Exitosa", f"Reporte guardado como:\n{filename}")
        
        # Guardar logs después de exportar
        if self.log_auto_save:
            self.save_log(silent=True)

    def open_html_in_browser(self, html_file):
        """Abre un archivo HTML en el navegador predeterminado del sistema"""
        try:
            import os
            import webbrowser
            
            # Obtener la ruta absoluta al archivo
            file_path = os.path.abspath(html_file)
            self.log(f"Abriendo archivo en navegador: {file_path}", "INFO")
            
            # Abrir el archivo en el navegador predeterminado
            webbrowser.open('file://' + file_path)
            return True
        except Exception as e:
            self.log(f"Error abriendo archivo en navegador: {str(e)}", "ERROR")
            return False

    def on_sku_selected(self):
        """Maneja la selección de un SKU en el combobox"""
        sku = self.sku_combobox.get().strip()
        if sku and self.data_cache is not None:
            # Filtrar por SKU
            filtered = self.data_cache[self.data_cache['SKU'] == sku]
            if not filtered.empty:
                # Actualizar el combobox de LOC
                locs = filtered['LOC'].unique().tolist()
                self.loc_combobox['values'] = locs
                
                # Obtener valores actuales para preservarlos
                current_loc = self.loc_combobox.get().strip()
                current_lot = self.lot_combobox.get()
                
                # Mostrar estadísticas del SKU seleccionado
                total_qty = filtered['QTY'].sum()
                total_locations = len(locs)
                self.log(f"SKU {sku}: {len(filtered)} registros, {total_qty} unidades en {total_locations} ubicaciones", "INFO")
                
                # Si el valor de LOC ya no es válido con el nuevo SKU, entonces sí lo borramos
                if current_loc and current_loc not in locs:
                    self.loc_combobox.set('')
                    self.lot_combobox.set('')
                    self.lot_combobox['values'] = []
                    self.log(f"Ubicación {current_loc} no disponible para SKU {sku}, filtro LOC reiniciado", "WARNING")
                elif current_loc:
                    # Si la LOC es válida, actualizar lotes disponibles para esa combinación
                    loc_filtered = filtered[filtered['LOC'] == current_loc]
                    if not loc_filtered.empty:
                        lots = loc_filtered['LOT'].astype(str).unique().tolist()
                        self.lot_combobox['values'] = lots
                        
                        # Si el lote ya no es válido, lo borramos
                        if current_lot and current_lot not in lots:
                            self.lot_combobox.set('')
                            self.log(f"Lote {current_lot} no disponible para SKU {sku} y LOC {current_loc}, filtro LOT reiniciado", "WARNING")

    def on_loc_selected(self):
        """Maneja la selección de una LOC en el combobox"""
        sku = self.sku_combobox.get().strip()
        loc = self.loc_combobox.get().strip()
        
        if sku and loc and self.data_cache is not None:
            # Filtrar por SKU y LOC
            filtered = self.data_cache[
                (self.data_cache['SKU'] == sku) & 
                (self.data_cache['LOC'] == loc)
            ]
            
            if not filtered.empty:
                # Obtener lote actual para preservarlo
                current_lot = self.lot_combobox.get()
                
                # Actualizar el combobox de LOT
                lots = filtered['LOT'].astype(str).unique().tolist()
                self.lot_combobox['values'] = lots
                
                # Mostrar estadísticas de la ubicación seleccionada
                total_qty = filtered['QTY'].sum()
                self.log(f"Ubicación {loc}: {len(filtered)} registros, {total_qty} unidades, {len(lots)} lotes", "INFO")
                
                # Si el lote actual ya no es válido con la nueva LOC, lo borramos
                if current_lot and current_lot not in lots:
                    self.lot_combobox.set('')
                    self.log(f"Lote {current_lot} no disponible para LOC {loc}, filtro LOT reiniciado", "WARNING")
    
    def on_combobox_keyrelease(self, event):
        """Maneja la liberación de teclas en combobox para búsqueda incremental"""
        # Identificar qué combobox generó el evento
        widget = event.widget
        
        if widget == self.sku_combobox:
            # Búsqueda incremental para SKU
            search_term = widget.get().strip().upper()
            if not search_term:
                return
                
            if self.data_cache is not None:
                # Filtrar SKUs que contienen el término de búsqueda
                matching_skus = [sku for sku in self.data_cache['SKU'].unique() 
                               if search_term in str(sku).upper()]
                
                # Actualizar valores del combobox
                if matching_skus:
                    widget['values'] = matching_skus
                    # No abrir la lista desplegable automáticamente para evitar distracciones
        
        elif widget == self.loc_combobox:
            # Búsqueda incremental para LOC
            search_term = widget.get().strip().upper()
            if not search_term:
                return
                
            # Solo buscar si hay un SKU seleccionado
            sku = self.sku_combobox.get().strip()
            if sku and self.data_cache is not None:
                # Primero filtrar por SKU
                sku_data = self.data_cache[self.data_cache['SKU'] == sku]
                
                # Luego buscar ubicaciones que contienen el término
                matching_locs = [loc for loc in sku_data['LOC'].unique() 
                               if search_term in str(loc).upper()]
                
                # Actualizar valores del combobox
                if matching_locs:
                    widget['values'] = matching_locs
        
        elif widget == self.lot_combobox:
            # Búsqueda incremental para LOT
            search_term = widget.get().strip().upper()
            if not search_term:
                return
                
            # Solo buscar si hay SKU y LOC seleccionados
            sku = self.sku_combobox.get().strip()
            loc = self.loc_combobox.get().strip()
            
            if sku and loc and self.data_cache is not None:
                # Filtrar por SKU y LOC
                filtered_data = self.data_cache[
                    (self.data_cache['SKU'] == sku) & 
                    (self.data_cache['LOC'] == loc)
                ]
                
                # Buscar lotes que contienen el término
                matching_lots = [str(lot) for lot in filtered_data['LOT'].unique() 
                               if search_term in str(lot).upper()]
                
                # Actualizar valores del combobox
                if matching_lots:
                    widget['values'] = matching_lots

    def save_movement_record(self, processed_ids):
        """Guarda un registro de los movimientos realizados en CSV"""
        try:
            # Mostrar los IDs que se procesarán
            self.log(f"IDs a procesar: {processed_ids[:5]}{'...' if len(processed_ids) > 5 else ''}", "INFO")
            
            # Verificar que existe la carpeta de REGISTRO
            if not os.path.exists(self.REGISTRO_DIR):
                try:
                    os.makedirs(self.REGISTRO_DIR)
                    self.log(f"Carpeta de registros creada: {self.REGISTRO_DIR}", "SUCCESS")
                except Exception as e:
                    self.log(f"Error creando carpeta de registros: {str(e)}", "ERROR")
                    # Intentar con ruta alternativa
                    alt_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Registros")
                    try:
                        os.makedirs(alt_dir, exist_ok=True)
                        self.REGISTRO_DIR = alt_dir
                        self.log(f"Usando carpeta alternativa: {self.REGISTRO_DIR}", "WARNING")
                    except Exception as e2:
                        self.log(f"Error creando carpeta alternativa: {str(e2)}", "ERROR")
                        return False
            
            # Crear nombre de archivo con fecha actual
            current_date = datetime.now().strftime('%Y%m%d')
            csv_path = os.path.join(self.REGISTRO_DIR, f"movement_record_{current_date}.csv")
            self.log(f"Ruta del archivo CSV: {csv_path}", "INFO")
            
            # Preparar datos para el registro
            movement_data = []
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Obtener información de referencia
            reference_info = getattr(self, 'last_reference_info', {}) or {}
            
            self.log(f"Preparando registro para {len(processed_ids)} inventarios procesados", "INFO")
            self.log(f"Usuario ID: {self.user_id}", "INFO")
            
            # Usar nombres guardados durante el login
            user_name = getattr(self, 'user_name', "")
            user_short_name = getattr(self, 'user_short_name', "")
            
            if user_name or user_short_name:
                self.log(f"Nombre del usuario: {user_name} ({user_short_name})", "INFO")
            
            self.log(f"Información de referencia: {reference_info}", "INFO")
            
            # Buscar en todas las fuentes de datos disponibles
            data_sources = []
            if hasattr(self, 'filtered_data') and self.filtered_data is not None:
                data_sources.append(('individual', self.filtered_data))
                self.log("Fuente de datos disponible: Consolidación Individual", "INFO")
            if hasattr(self, 'bulk_data') and self.bulk_data is not None:
                data_sources.append(('masiva', self.bulk_data))
                self.log("Fuente de datos disponible: Consolidación Masiva", "INFO")
            if hasattr(self, 'data_cache') and self.data_cache is not None:
                data_sources.append(('cache', self.data_cache))
                self.log("Fuente de datos disponible: Cache Global", "INFO")
            
            if not data_sources:
                self.log("No hay fuentes de datos disponibles para crear el registro", "ERROR")
                return False
                
            # Obtener detalles de los inventarios procesados
            for inv_id in processed_ids:
                record_found = False
                
                # Buscar en todas las fuentes de datos
                for source_name, source_data in data_sources:
                    try:
                        # Asegurarse de que INVENTORYID sea string para comparar
                        source_data_copy = source_data.copy()
                        source_data_copy['INVENTORYID'] = source_data_copy['INVENTORYID'].astype(str)
                        inv_id_str = str(inv_id)
                        
                        # Buscar el registro
                        inv_row = source_data_copy[source_data_copy['INVENTORYID'] == inv_id_str]
                        
                        if not inv_row.empty:
                            # Extraer información relevante
                            record = {
                                'Timestamp': timestamp,
                                'Usuario_ID': self.user_id,
                                'Usuario_Nombre': user_name,
                                'Usuario_Nombre_Corto': user_short_name,
                                'Usuario_API': self.API_CONFIG['headers']['move']['xposc-userid'],
                                'INVENTORYID': inv_id,
                                'SKU': inv_row['SKU'].iloc[0],
                                'LOC_ORIGEN': inv_row['LOC'].iloc[0],
                                'LOT': str(inv_row['LOT'].iloc[0]),
                                'LPN_ORIGEN': str(inv_row['LPN'].iloc[0]),
                                'QTY': inv_row['QTY'].iloc[0],
                                'LOC_DESTINO': reference_info.get('loc', ''),
                                'LPN_DESTINO': reference_info.get('lpn', ''),
                                'Tipo': f'Consolidación {source_name.capitalize()}'
                            }
                            movement_data.append(record)
                            record_found = True
                            self.log(f"Registro encontrado para inventario {inv_id} en fuente {source_name}", "SUCCESS")
                            break
                            
                    except Exception as e:
                        self.log(f"Error buscando inventario {inv_id} en fuente {source_name}: {str(e)}", "ERROR")
                
                if not record_found:
                    self.log(f"No se encontró información para el inventario {inv_id}", "WARNING")
            
            # Crear o actualizar el archivo CSV
            if movement_data:
                try:
                    # Preparar DataFrame
                    movement_df = pd.DataFrame(movement_data)
                    
                    # Organizar las columnas en un orden específico
                    column_order = [
                        'Timestamp', 'Usuario_ID', 'Usuario_Nombre', 'Usuario_Nombre_Corto', 'Usuario_API', 
                        'INVENTORYID', 'SKU', 'LOC_ORIGEN', 'LOC_DESTINO',
                        'LOT', 'LPN_ORIGEN', 'LPN_DESTINO',
                        'QTY', 'Tipo'
                    ]
                    
                    # Reordenar columnas (solo las existentes)
                    cols_to_use = [c for c in column_order if c in movement_df.columns]
                    movement_df = movement_df[cols_to_use]
                    
                    # Verificar si el archivo ya existe
                    file_exists = os.path.exists(csv_path)
                    
                    # Guardar en CSV
                    try:
                        if file_exists:
                            # Añadir a archivo existente sin repetir el encabezado
                            movement_df.to_csv(csv_path, mode='a', header=False, index=False)
                            self.log(f"Registro actualizado con {len(movement_data)} movimientos", "SUCCESS")
                        else:
                            # Crear nuevo archivo con encabezado
                            movement_df.to_csv(csv_path, index=False)
                            self.log(f"Nuevo registro creado con {len(movement_data)} movimientos", "SUCCESS")
                        
                        self.log(f"Registro CSV guardado en: {csv_path}", "SUCCESS")
                        
                        # Verificar que el archivo se creó correctamente
                        if os.path.exists(csv_path):
                            file_size = os.path.getsize(csv_path)
                            self.log(f"Archivo CSV verificado: {csv_path} (tamaño: {file_size} bytes)", "SUCCESS")
                        else:
                            self.log(f"¡ADVERTENCIA! El archivo debería existir pero no se encuentra", "WARNING")
                            
                    except Exception as csv_error:
                        self.log(f"Error guardando el archivo CSV: {str(csv_error)}", "ERROR")
                        # Intentar guardar en ubicación alternativa
                        alt_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"registro_{current_date}.csv")
                        try:
                            movement_df.to_csv(alt_path, index=False)
                            self.log(f"Registro guardado en ubicación alternativa: {alt_path}", "WARNING")
                        except Exception as alt_error:
                            self.log(f"Error guardando en ubicación alternativa: {str(alt_error)}", "ERROR")
                    
                    # Mostrar la estructura de datos guardada
                    self.log("Estructura del registro CSV:", "INFO")
                    for col in movement_df.columns:
                        self.log(f"  - {col}", "INFO")
                    
                    # Mostrar algunos registros para verificación
                    if len(movement_data) <= 3:
                        for i, record in enumerate(movement_data):
                            self.log(f"Registro {i+1}: SKU={record['SKU']}, LPN={record['LPN_ORIGEN']} → {record['LPN_DESTINO']}", "INFO")
                    else:
                        for i, record in enumerate(movement_data[:2]):
                            self.log(f"Registro {i+1}: SKU={record['SKU']}, LPN={record['LPN_ORIGEN']} → {record['LPN_DESTINO']}", "INFO")
                        self.log(f"... y {len(movement_data)-2} registros más", "INFO")
                    
                    return True
                    
                except Exception as df_error:
                    self.log(f"Error procesando datos para CSV: {str(df_error)}", "ERROR")
                    import traceback
                    self.log(traceback.format_exc(), "ERROR")
            else:
                self.log("No se generaron registros para guardar", "WARNING")
                return False
                
        except Exception as e:
            self.log(f"Error guardando registro de movimientos: {str(e)}", "ERROR")
            import traceback
            self.log(traceback.format_exc(), "ERROR")
            return False
    
    def get_reference_info(self):
        """Obtiene información del LPN/LOC de referencia para el registro"""
        info = {'lpn': '', 'loc': ''}
        
        try:
            # Si existe información de la última referencia, usarla
            if hasattr(self, 'last_reference_info') and self.last_reference_info:
                # Verificar si la información es reciente (menos de 10 minutos)
                last_time = datetime.strptime(self.last_reference_info['timestamp'], '%Y-%m-%d %H:%M:%S')
                time_diff = (datetime.now() - last_time).total_seconds()
                
                if time_diff < 600:  # 10 minutos
                    return {
                        'lpn': self.last_reference_info['lpn'],
                        'loc': self.last_reference_info['loc']
                    }

            # Si no hay información reciente, intentar obtenerla de los datos
            # Intentar obtener del último movimiento (de la consolidación individual)
            if hasattr(self, 'filtered_data') and self.filtered_data is not None and len(self.filtered_data) > 1:
                # Usar el LPN más pequeño numéricamente como referencia
                try:
                    temp_data = self.filtered_data.copy()
                    temp_data['LPN_INT'] = temp_data['LPN'].apply(
                        lambda x: int(x) if str(x).strip().isdigit() else float('inf')
                    )
                    min_idx = temp_data['LPN_INT'].idxmin()
                    info['lpn'] = temp_data.loc[min_idx, 'LPN']
                    info['loc'] = temp_data.loc[min_idx, 'LOC']
                except:
                    pass
                    
            # Si no hay datos en filtered_data, intentar con bulk_data
            if not info['lpn'] and hasattr(self, 'bulk_data') and self.bulk_data is not None:
                # Similar para consolidación masiva
                try:
                    # Aquí es más complejo porque hay múltiples grupos
                    # Para simplificar, usar el último LPN seleccionado como referencia
                    recent_entries = self.bulk_data.head(1)
                    if not recent_entries.empty:
                        info['lpn'] = recent_entries['LPN'].iloc[0]
                        info['loc'] = recent_entries['LOC'].iloc[0]
                except:
                    pass
        except Exception as e:
            self.log(f"Error obteniendo info de referencia: {str(e)}", "WARNING")
        
        return info

    def setup_bulk_tab(self):
        """Configura la pestaña de consolidación masiva por SKU"""
        # Frame principal
        main_frame = ttk.Frame(self.bulk_tab, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Panel superior
        top_frame = ttk.Frame(main_frame)
        top_frame.pack(fill=tk.X, pady=5)
        
        # Título
        ttk.Label(top_frame, text="Consolidación Masiva por SKU", 
                 font=("Arial", 14, "bold")).pack(side=tk.LEFT, padx=10)
        
        # Panel de configuración
        config_frame = ttk.LabelFrame(main_frame, text="Configuración", padding="10")
        config_frame.pack(fill=tk.X, pady=5)
        
        # Panel de filtros
        filter_frame = ttk.Frame(config_frame)
        filter_frame.pack(fill=tk.X, pady=5)
        
        # SKU Filter
        ttk.Label(filter_frame, text="SKU:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        self.bulk_sku_combobox = ttk.Combobox(filter_frame, width=25)
        self.bulk_sku_combobox.grid(row=0, column=1, padx=5, pady=5, sticky=tk.EW)
        
        # Botón de aplicar
        ttk.Button(filter_frame, text="Cargar Datos", 
                  command=self.load_bulk_data).grid(row=0, column=2, padx=5, pady=5)
        
        # Panel de opciones avanzadas
        options_frame = ttk.LabelFrame(config_frame, text="Opciones Avanzadas", padding="10")
        options_frame.pack(fill=tk.X, pady=5)
        
        # Excluir ubicaciones
        ttk.Label(options_frame, text="Excluir ubicaciones especiales:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        self.exclude_special_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(options_frame, variable=self.exclude_special_var).grid(row=0, column=1, padx=5, pady=5, sticky=tk.W)
        
        # Agrupar por ubicación
        ttk.Label(options_frame, text="Consolidar por ubicación:").grid(row=1, column=0, padx=5, pady=5, sticky=tk.W)
        self.group_by_loc_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(options_frame, variable=self.group_by_loc_var).grid(row=1, column=1, padx=5, pady=5, sticky=tk.W)
        
        # Resultados panel
        result_frame = ttk.LabelFrame(main_frame, text="Ubicaciones a Consolidar", padding="10")
        result_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        # Treeview para los resultados agrupados
        self.bulk_tree = ttk.Treeview(
            result_frame, 
            columns=('LOC', 'NUM_LOTES', 'TOTAL_QTY', 'STATUS'),
            show='headings'
        )

        # Configurar columnas
        self.bulk_tree.heading('LOC', text='Ubicación')
        self.bulk_tree.heading('NUM_LOTES', text='Lotes')
        self.bulk_tree.heading('TOTAL_QTY', text='Cantidad Total')
        self.bulk_tree.heading('STATUS', text='Estado')

        self.bulk_tree.column('LOC', width=150)
        self.bulk_tree.column('NUM_LOTES', width=80, anchor=tk.CENTER)
        self.bulk_tree.column('TOTAL_QTY', width=100, anchor=tk.CENTER)
        self.bulk_tree.column('STATUS', width=120)

        # Scrollbar
        y_scroll = ttk.Scrollbar(result_frame, orient=tk.VERTICAL, command=self.bulk_tree.yview)
        self.bulk_tree.configure(yscrollcommand=y_scroll.set)

        # Estilos para filas
        self.bulk_tree.tag_configure('pending', background='#ffffff')
        self.bulk_tree.tag_configure('in_progress', background='#fff9c4')
        self.bulk_tree.tag_configure('completed', background='#c8e6c9')
        self.bulk_tree.tag_configure('error', background='#ffcdd2')

        self.bulk_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        y_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        # Panel de detalle
        detail_frame = ttk.LabelFrame(main_frame, text="Detalle de Lotes", padding="10")
        detail_frame.pack(fill=tk.BOTH, expand=True, pady=5)

        # Treeview para los detalles de lotes
        self.detail_tree = ttk.Treeview(
            detail_frame,
            columns=('LPN', 'QTY', 'STATUS'),
            show='headings'
        )

        # Configurar columnas
        self.detail_tree.heading('LPN', text='LPN')
        self.detail_tree.heading('QTY', text='Cantidad')
        self.detail_tree.heading('STATUS', text='Estado')

        self.detail_tree.column('LPN', width=150)
        self.detail_tree.column('QTY', width=80, anchor=tk.CENTER)
        self.detail_tree.column('STATUS', width=120)

        # Scrollbar
        detail_scroll = ttk.Scrollbar(detail_frame, orient=tk.VERTICAL, command=self.detail_tree.yview)
        self.detail_tree.configure(yscrollcommand=detail_scroll.set)

        self.detail_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        detail_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        # Panel de acciones
        action_frame = ttk.Frame(main_frame)
        action_frame.pack(fill=tk.X, pady=10)

        # Botones de acción
        ttk.Button(action_frame, text="Iniciar Consolidación Masiva", 
                  command=self.start_bulk_consolidation).pack(side=tk.LEFT, padx=5)

        ttk.Button(action_frame, text="Detener Proceso", 
                  command=self.stop_bulk_consolidation).pack(side=tk.LEFT, padx=5)

        # Botón para ver los registros CSV
        ttk.Button(action_frame, text="Ver Registros CSV", 
                  command=self.open_csv_folder).pack(side=tk.LEFT, padx=20)

        # Variables de control para la consolidación masiva
        self.bulk_data = None
        self.bulk_groups = None
        self.bulk_running = False
        self.bulk_stop_requested = False

        # Evento para mostrar detalles al seleccionar una ubicación
        self.bulk_tree.bind('<<TreeviewSelect>>', self.on_bulk_location_selected)

    def load_bulk_data(self):
        """Carga datos para la consolidación masiva basada en SKU"""
        try:
            # Verificar que hay datos cargados
            if self.data_cache is None:
                self.log("No hay datos cargados. Carga datos primero.", "ERROR")
                messagebox.showerror("Error", "No hay datos cargados. Haga clic en 'Reload Data' primero.")
                return

            # Obtener SKU seleccionado
            sku = self.bulk_sku_combobox.get().strip()
            if not sku:
                self.log("Debe seleccionar un SKU", "ERROR")
                messagebox.showerror("Error", "Debe seleccionar un SKU para continuar")
                return

            # Filtrar datos por SKU
            data = self.data_cache[self.data_cache['SKU'] == sku].copy()
            if data.empty:
                self.log(f"No se encontraron datos para el SKU: {sku}", "ERROR")
                messagebox.showerror("Error", f"No se encontraron datos para el SKU: {sku}")
                return

            # Excluir ubicaciones especiales si es necesario
            if self.exclude_special_var.get():
                original_count = len(data)
                data = data[~data['LOC'].isin(self.UBICACIONES_ESPECIALES)]
                excluded_count = original_count - len(data)
                if excluded_count > 0:
                    self.log(f"Se excluyeron {excluded_count} registros de ubicaciones especiales", "INFO")

            # Verificar si quedan datos
            if data.empty:
                self.log("No hay datos válidos después de aplicar filtros", "ERROR")
                messagebox.showerror("Error", "No hay datos válidos después de aplicar filtros")
                return

            # MODIFICACIÓN: Agrupar por LOC y LOT (no solo por LOC)
            # Agrupar por ubicación y lote
            if self.group_by_loc_var.get():
                # Asegurarnos que LOT sea string para la agrupación
                data['LOT'] = data['LOT'].astype(str)

                # Agrupar por ubicación Y lote
                groups = data.groupby(['LOC', 'LOT']).agg({
                    'LPN': 'nunique',  # Contar LPNs únicos por grupo
                    'QTY': 'sum'      # Sumar cantidades
                }).reset_index()

                # Renombrar columnas para claridad
                groups.columns = ['LOC', 'LOT', 'NUM_LPNS', 'TOTAL_QTY']

                # Sólo incluir ubicaciones con más de 1 LPN para el mismo lote
                groups = groups[groups['NUM_LPNS'] > 1]

                if groups.empty:
                    self.log("No hay ubicaciones con múltiples LPNs para el mismo lote", "WARNING")
                    messagebox.showwarning("Advertencia", "No hay ubicaciones con múltiples LPNs para el mismo lote para consolidar")
                    return

                # Ordenar por número de LPNs (descendente)
                groups = groups.sort_values('NUM_LPNS', ascending=False)

                # Añadir estado
                groups['STATUS'] = 'Pendiente'

                # Guardar datos
                self.bulk_data = data
                self.bulk_groups = groups

                # Mostrar datos en treeview
                self.update_bulk_treeview()

                # Limpiar detalles
                self.detail_tree.delete(*self.detail_tree.get_children())

                self.log(f"Datos cargados para SKU {sku}: {len(groups)} grupos de LOC-LOT con múltiples LPNs", "SUCCESS")
            else:
                self.log("La agrupación por ubicación es obligatoria en esta versión", "ERROR")
                messagebox.showerror("Error", "La agrupación por ubicación es obligatoria en esta versión")

        except Exception as e:
            self.log(f"Error cargando datos para consolidación masiva: {str(e)}", "ERROR")
            messagebox.showerror("Error", f"Error cargando datos: {str(e)}")

    def update_bulk_treeview(self):
        """Actualiza el treeview de consolidación masiva"""
        # Limpiar treeview
        self.bulk_tree.delete(*self.bulk_tree.get_children())

        # Verificar que hay datos
        if self.bulk_groups is None:
            return

        # Modificar las columnas del treeview para incluir LOT
        if 'LOT' not in self.bulk_tree['columns']:
            self.bulk_tree['columns'] = ('LOC', 'LOT', 'NUM_LPNS', 'TOTAL_QTY', 'STATUS', 'DETALLES')

            # Configurar columnas
            self.bulk_tree.heading('LOC', text='Ubicación')
            self.bulk_tree.heading('LOT', text='Lote')
            self.bulk_tree.heading('NUM_LPNS', text='LPNs')
            self.bulk_tree.heading('TOTAL_QTY', text='Cantidad Total')
            self.bulk_tree.heading('STATUS', text='Estado')
            self.bulk_tree.heading('DETALLES', text='Detalles')

            self.bulk_tree.column('LOC', width=120)
            self.bulk_tree.column('LOT', width=100)
            self.bulk_tree.column('NUM_LPNS', width=60, anchor=tk.CENTER)
            self.bulk_tree.column('TOTAL_QTY', width=80, anchor=tk.CENTER)
            self.bulk_tree.column('STATUS', width=100, anchor=tk.CENTER)
            self.bulk_tree.column('DETALLES', width=150)

        # Counters for status summary
        status_count = {'Pendiente': 0, 'En Proceso': 0, 'Completado': 0, 'Parcial': 0, 'Error': 0}

        # Añadir filas
        for _, row in self.bulk_groups.iterrows():
            loc = row['LOC']
            lot = row['LOT']
            num_lpns = row['NUM_LPNS']
            total_qty = row['TOTAL_QTY']
            status = row['STATUS']

            # Count by status
            status_count[status] = status_count.get(status, 0) + 1

            # Generate details message
            details = ""
            if status == 'Pendiente':
                details = "Esperando procesamiento"
            elif status == 'En Proceso':
                details = "Procesando..."
            elif status == 'Completado':
                details = "Consolidación exitosa"
            elif status == 'Parcial':
                details = "Algunos LPNs no procesados"
            elif status == 'Error':
                details = "No se pudo consolidar"

            # Determinar tag para estilo
            tag = 'pending'
            if status == 'En Proceso':
                tag = 'in_progress'
            elif status == 'Completado':
                tag = 'completed'
            elif status == 'Parcial':
                tag = 'in_progress'  # Use in_progress for Parcial too
            elif status == 'Error':
                tag = 'error'

            # Insertar fila
            self.bulk_tree.insert('', 'end', values=(loc, lot, num_lpns, total_qty, status, details), tags=(tag,))

        # Configure better styles for tree tags
        self.bulk_tree.tag_configure('pending', background='#ffffff')
        self.bulk_tree.tag_configure('in_progress', background='#fff9c4')
        self.bulk_tree.tag_configure('completed', background='#c8e6c9', foreground='#2e7d32')
        self.bulk_tree.tag_configure('error', background='#ffcdd2', foreground='#c62828')

        # Log status summary
        if sum(status_count.values()) > 0:
            self.log("Resumen de estados de consolidación:", "INFO")
            for status, count in status_count.items():
                if count > 0:
                    level = "INFO"
                    if status == "Completado":
                        level = "SUCCESS"
                    elif status == "Error":
                        level = "ERROR"
                    elif status == "Parcial":
                        level = "WARNING"
                    self.log(f"  - {status}: {count}", level)

    def on_bulk_location_selected(self, event):
        """Muestra los detalles de los lotes cuando se selecciona una ubicación"""
        # Obtener selección
        selection = self.bulk_tree.selection()
        if not selection:
            return

        # Obtener ubicación y lote seleccionados
        item = self.bulk_tree.item(selection[0])
        values = item['values']

        # MODIFICACIÓN: Obtener tanto LOC como LOT
        if len(values) >= 2:
            loc = values[0]
            lot = values[1]
        else:
            return

        # Verificar que hay datos
        if self.bulk_data is None:
            return

        # Filtrar datos para la ubicación Y lote seleccionados
        loc_lot_data = self.bulk_data[
            (self.bulk_data['LOC'] == loc) & 
            (self.bulk_data['LOT'].astype(str) == str(lot))
        ].copy()

        # Limpiar treeview de detalles
        self.detail_tree.delete(*self.detail_tree.get_children())

        # Añadir filas con detalles
        for _, row in loc_lot_data.iterrows():
            lpn = row['LPN']
            qty = row['QTY']

            # Determinar estado
            status = "Pendiente"
            if row['INVENTORYID'] in self.processed_inventories:
                status = "Procesado"

            # Insertar fila - ya no necesitamos mostrar LOT aquí porque ya está en el encabezado
            self.detail_tree.insert('', 'end', values=(lpn, qty, status))

        self.log(f"Mostrando {len(loc_lot_data)} LPNs en ubicación {loc} con lote {lot}", "INFO")

    def start_bulk_consolidation(self):
        """Inicia el proceso de consolidación masiva"""
        # Verificar que hay datos
        if self.bulk_groups is None or self.bulk_data is None:
            self.log("No hay datos para consolidar", "ERROR")
            messagebox.showerror("Error", "No hay datos para consolidar. Cargue datos primero.")
            return

        # Verificar si ya está en ejecución
        if self.bulk_running:
            self.log("Ya hay un proceso de consolidación masiva en ejecución", "WARNING")
            messagebox.showwarning("Advertencia", "Ya hay un proceso en ejecución")
            return

        # Filtrar ubicaciones pendientes
        pending_rows = self.bulk_groups[self.bulk_groups['STATUS'] == 'Pendiente']

        if pending_rows.empty:
            self.log("No hay ubicaciones pendientes para consolidar", "WARNING")
            messagebox.showinfo("Información", "No hay ubicaciones pendientes para consolidar")
            return

        # Crear diálogo para seleccionar ubicaciones
        selection_dialog = tk.Toplevel(self.root)
        selection_dialog.title("Seleccionar Ubicaciones a Consolidar")
        selection_dialog.geometry("700x500")  # Aumentar tamaño
        selection_dialog.transient(self.root)
        selection_dialog.grab_set()
        
        ttk.Label(selection_dialog, text="Seleccione las ubicaciones a consolidar:", 
                 font=("Arial", 12)).pack(pady=10)
        
        # Frame para contener el Treeview y scrollbar
        tree_frame = ttk.Frame(selection_dialog)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        # Scrollbars
        y_scroll = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL)
        y_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Treeview con checkboxes para selección
        selection_tree = ttk.Treeview(
            tree_frame,
            columns=('SELECT', 'LOC', 'LOT', 'NUM_LPNS', 'TOTAL_QTY'),
            show='headings',
            selectmode='none',
            yscrollcommand=y_scroll.set
        )
        
        y_scroll.config(command=selection_tree.yview)
        
        # Configurar columnas
        selection_tree.heading('SELECT', text='Seleccionar')
        selection_tree.heading('LOC', text='Ubicación')
        selection_tree.heading('LOT', text='Lote')
        selection_tree.heading('NUM_LPNS', text='LPNs')
        selection_tree.heading('TOTAL_QTY', text='Cantidad Total')
        
        selection_tree.column('SELECT', width=80, anchor=tk.CENTER)
        selection_tree.column('LOC', width=120)
        selection_tree.column('LOT', width=100)
        selection_tree.column('NUM_LPNS', width=80, anchor=tk.CENTER)
        selection_tree.column('TOTAL_QTY', width=100, anchor=tk.CENTER)
        
        selection_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Diccionario para mantener el estado de selección
        selected_items = {}
        
        # Añadir filas al treeview con checkboxes
        for idx, row in pending_rows.iterrows():
            item_id = selection_tree.insert('', 'end', values=('☐', row['LOC'], row['LOT'], row['NUM_LPNS'], row['TOTAL_QTY']))
            selected_items[item_id] = False
        
        # Función para alternar la selección
        def toggle_selection(event):
            item_id = selection_tree.identify_row(event.y)
            if not item_id:
                return
                
            # Obtener valores actuales
            values = selection_tree.item(item_id, 'values')
            if not values:
                return
                
            # Cambiar estado de selección
            is_selected = selected_items[item_id]
            selected_items[item_id] = not is_selected
            
            # Actualizar checkbox en el treeview
            new_values = list(values)
            new_values[0] = '☑' if selected_items[item_id] else '☐'
            selection_tree.item(item_id, values=new_values)
        
        # Enlazar evento de clic
        selection_tree.bind('<ButtonRelease-1>', toggle_selection)
        
        # Botones de acción
        btn_frame = ttk.LabelFrame(selection_dialog, text="Acciones")
        btn_frame.pack(fill=tk.X, pady=10)
        
        # Función para seleccionar/deseleccionar todos
        def select_all():
            for item_id in selected_items:
                selected_items[item_id] = True
                values = list(selection_tree.item(item_id, 'values'))
                values[0] = '☑'
                selection_tree.item(item_id, values=values)
        
        def deselect_all():
            for item_id in selected_items:
                selected_items[item_id] = False
                values = list(selection_tree.item(item_id, 'values'))
                values[0] = '☐'
                selection_tree.item(item_id, values=values)
        
        ttk.Button(btn_frame, text="Seleccionar Todos", command=select_all).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Deseleccionar Todos", command=deselect_all).pack(side=tk.LEFT, padx=5)
        
        # Variables para retornar la selección
        dialog_result = {'confirmed': False, 'selected_locations': []}
        
        # Funciones para confirmar/cancelar
        def confirm_selection():
            # Obtener ubicaciones seleccionadas
            for item_id, is_selected in selected_items.items():
                if is_selected:
                    values = selection_tree.item(item_id, 'values')
                    dialog_result['selected_locations'].append((values[1], values[2]))  # (LOC, LOT)
            
            dialog_result['confirmed'] = True
            selection_dialog.destroy()
        
        def cancel_selection():
            selection_dialog.destroy()
        
        ttk.Button(btn_frame, text="Cancelar", command=cancel_selection).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Confirmar", command=confirm_selection).pack(side=tk.RIGHT, padx=5)
        
        # Esperar a que se cierre el diálogo
        self.root.wait_window(selection_dialog)
        
        # Verificar si se confirmó la selección
        if not dialog_result['confirmed'] or not dialog_result['selected_locations']:
            self.log("Proceso cancelado o no se seleccionaron ubicaciones", "INFO")
            return
        
        # Crear DataFrame con solo las ubicaciones seleccionadas
        selected_locs_lots = [(loc, lot) for loc, lot in dialog_result['selected_locations']]
        self.log(f"Se seleccionaron {len(selected_locs_lots)} ubicaciones para consolidar", "INFO")
        
        # Filtrar self.bulk_groups para mantener solo las seleccionadas
        mask = self.bulk_groups.apply(
            lambda row: (row['LOC'], row['LOT']) in selected_locs_lots, 
            axis=1
        )
        selected_bulk_groups = self.bulk_groups[mask].copy()
        
        # Confirmar operación final
        sku = self.bulk_data['SKU'].iloc[0]
        confirm_msg = (
            f"Se van a consolidar {len(selected_locs_lots)} ubicaciones con el SKU {sku}.\n\n"
            f"Esto moverá múltiples lotes a un único LPN de referencia para cada ubicación.\n\n"
            "¿Está seguro de querer continuar?"
        )
        
        if not messagebox.askyesno("Confirmar Consolidación Masiva", confirm_msg):
            self.log("Proceso cancelado por el usuario", "INFO")
            return
        
        # Guardar el DataFrame filtrado temporalmente
        self.selected_bulk_groups = selected_bulk_groups
        
        # Iniciar proceso en un hilo separado
        self.bulk_running = True
        self.bulk_stop_requested = False
        
        thread = threading.Thread(target=self.bulk_consolidation_thread, daemon=True)
        thread.start()
        self.threads.append(thread)
        self.running_threads += 1

    def bulk_consolidation_thread(self):
        """Hilo para la consolidación masiva por ubicación y lote"""
        try:
            # Guardar logs antes de iniciar un proceso crítico
            if self.log_auto_save:
                self.save_log(silent=True)

            total_processed = 0
            total_errors = 0
            total_locations = 0
            locations_with_errors = 0
            locations_success = 0
            locations_partial = 0
            generated_labels = []  # Lista para almacenar etiquetas generadas
            
            # Crear carpeta específica para este proceso de consolidación
            first_location = self.selected_bulk_groups['LOC'].iloc[0] if not self.selected_bulk_groups.empty else None
            filters_text = None
            
            # Obtener información de filtros aplicados
            if hasattr(self, 'bulk_filter_sku_var') and self.bulk_filter_sku_var.get():
                filters_text = f"SKU_{self.bulk_sku_combobox.get()}"
            elif hasattr(self, 'bulk_filter_status_var') and self.bulk_filter_status_var.get():
                filters_text = f"Status_{self.bulk_status_combobox.get()}"
            elif hasattr(self, 'bulk_filter_lot_var') and self.bulk_filter_lot_var.get():
                filters_text = f"Lot_{self.bulk_lot_combobox.get()}"
            
            # Crear carpeta para etiquetas de esta consolidación con manejo de errores
            consolidation_folder = None
            try:
                if first_location:
                    consolidation_folder = self.create_consolidation_folder(f"Bulk_{first_location}", filters_text)
                else:
                    consolidation_folder = self.create_consolidation_folder("Bulk_Unknown", filters_text)
            except Exception as e:
                self.log(f"Error creando carpeta de consolidación: {str(e)}", "ERROR")
                consolidation_folder = None

            # Registrar inicio del proceso con ID de usuario
            self.log(f"Iniciando consolidación masiva - Usuario ID: {self.user_id}", "INFO")
            if consolidation_folder:
                self.log(f"Etiquetas se guardarán en: {consolidation_folder}", "INFO")
            else:
                self.log("ADVERTENCIA: No se pudo crear carpeta para etiquetas", "WARNING")

            # Trabajo por lotes para cada ubicación
            for idx, row in self.selected_bulk_groups.iterrows():
                # Verificar si se solicitó detenero8,;kl
                if self.bulk_stop_requested:
                    self.log("Proceso detenido por el usuario", "WARNING")
                    break

                loc = row['LOC']
                lot = row['LOT']  # También obtener el lote del grupo actual

                # Si la ubicación no está pendiente, la saltamos
                if row['STATUS'] != 'Pendiente':
                    self.log(f"Saltando ubicación {loc}, lote {lot}: estado ya es {row['STATUS']}", "INFO")
                    continue

                total_locations += 1
                self.log(f"\n--- Procesando ubicación {loc} con lote {lot} ---", "INFO")

                # Actualizar estado a "En Proceso"
                self.selected_bulk_groups.at[idx, 'STATUS'] = 'En Proceso'
                self.root.after(0, self.update_bulk_treeview)

                # Extraer datos de esta ubicación Y lote
                loc_lot_data = self.bulk_data[
                    (self.bulk_data['LOC'] == loc) & 
                    (self.bulk_data['LOT'] == lot)
                ].copy()

                # Verificar datos
                if loc_lot_data.empty:
                    self.log(f"No hay datos para la ubicación {loc} con lote {lot}", "ERROR")
                    self.selected_bulk_groups.at[idx, 'STATUS'] = 'Error'
                    locations_with_errors += 1
                    self.root.after(0, self.update_bulk_treeview)
                    continue

                # Intentar consolidar esta ubicación
                success, errors, processed_inv_ids, label_filename = self.consolidate_location(loc_lot_data, consolidation_folder)

                # Almacenar etiqueta si se generó
                if label_filename:
                    generated_labels.append(label_filename)

                # Actualizar contadores
                total_processed += len(processed_inv_ids)
                total_errors += errors

                # Determinar el estado según los resultados
                if not success:
                    # No hubo éxito en ningún movimiento
                    status = 'Error'
                    locations_with_errors += 1
                    self.log(f"La ubicación {loc} con lote {lot} terminó con estado ERROR", "ERROR")
                elif errors > 0:
                    # Hubo éxito en algunos movimientos pero también errores
                    status = 'Parcial'
                    locations_partial += 1
                    self.log(f"La ubicación {loc} con lote {lot} terminó con estado PARCIAL: {len(processed_inv_ids)} éxitos, {errors} errores", "WARNING")
                else:
                    # Éxito completo sin errores
                    status = 'Completado'
                    locations_success += 1
                    self.log(f"La ubicación {loc} con lote {lot} terminó con estado COMPLETADO: {len(processed_inv_ids)} éxitos", "SUCCESS")

                self.selected_bulk_groups.at[idx, 'STATUS'] = status
                self.root.after(0, self.update_bulk_treeview)

                # Guardar registro de los LPNs procesados
                if len(processed_inv_ids) > 0:
                    self.log(f"Guardando registro de {len(processed_inv_ids)} movimientos en consolidación masiva...", "INFO")
                    self.log(f"Usuario ID: {self.user_id} - Ubicación: {loc}, Lote: {lot}", "INFO")
                    self.save_movement_record(processed_inv_ids)

                # Esperar brevemente para no saturar la API
                time.sleep(0.5)

            # Actualizar resumen de consolidación
            self.log(f"\n=== Resumen de Consolidación Masiva ===", "INFO")
            self.log(f"Total de ubicaciones procesadas: {total_locations}", "INFO")
            self.log(f"- Ubicaciones con estado COMPLETADO: {locations_success}", "SUCCESS")
            self.log(f"- Ubicaciones con estado PARCIAL: {locations_partial}", "WARNING")
            self.log(f"- Ubicaciones con estado ERROR: {locations_with_errors}", "ERROR")
            self.log(f"Total de LPNs procesados exitosamente: {total_processed}", "SUCCESS")
            if total_errors > 0:
                self.log(f"Total de errores encontrados: {total_errors}", "WARNING")

            # Mostrar resumen de etiquetas generadas
            if generated_labels:
                self.log(f"Se generaron {len(generated_labels)} etiquetas HTML", "SUCCESS")
                for i, label in enumerate(generated_labels[:3]):  # Mostrar solo las primeras 3
                    self.log(f"  {i+1}. {os.path.basename(label)}", "INFO")
                if len(generated_labels) > 3:
                    self.log(f"  ... y {len(generated_labels) - 3} etiquetas más.", "INFO")

            # Mostrar diálogo con resumen al finalizar, incluyendo la carpeta de consolidación
            self.root.after(0, lambda: self.show_bulk_completion_dialog(
                total_locations, locations_success, locations_partial, 
                locations_with_errors, total_processed, generated_labels,
                consolidation_folder
            ))

        except Exception as e:
            self.log(f"Error en proceso de consolidación masiva: {str(e)}", "ERROR")
            import traceback
            self.log(traceback.format_exc(), "ERROR")
        finally:
            # Asegurar que se restaura la interfaz
            self.bulk_stop_requested = False
            self.bulk_running = False
            self.root.after(0, self.update_bulk_treeview)
            self.root.after(0, lambda: self.log("Proceso de consolidación masiva finalizado", "INFO"))

    def show_bulk_completion_dialog(self, total_locations, locations_success, locations_partial,
                                     locations_with_errors, total_processed, generated_labels, consolidation_folder=None):
        """Muestra un diálogo con el resumen de la consolidación masiva"""
        # Crear ventana modal
        dialog = tk.Toplevel(self.root)
        dialog.title("Consolidación Masiva Completada")
        dialog.geometry("650x550")
        dialog.transient(self.root)
        dialog.grab_set()

        # Centrar en la pantalla
        dialog.update_idletasks()
        width = dialog.winfo_width()
        height = dialog.winfo_height()
        x = (dialog.winfo_screenwidth() // 2) - (width // 2)
        y = (dialog.winfo_screenheight() // 2) - (height // 2)
        dialog.geometry(f'+{x}+{y}')

        # Frame principal
        main_frame = ttk.Frame(dialog, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Título
        title_label = ttk.Label(
            main_frame,
            text="Proceso de Consolidación Masiva Completado",
            font=("Arial", 14, "bold")
        )
        title_label.pack(pady=(0, 20))

        # Frame para resumen
        summary_frame = ttk.LabelFrame(main_frame, text="Resumen del Proceso", padding="10")
        summary_frame.pack(fill=tk.X, pady=10)

        # Información del proceso
        ttk.Label(summary_frame, text=f"Total de ubicaciones procesadas: {total_locations}").pack(anchor=tk.W, pady=2)
        ttk.Label(summary_frame, text=f"Ubicaciones completadas exitosamente: {locations_success}",
                  foreground="green").pack(anchor=tk.W, pady=2)
        if locations_partial > 0:
            ttk.Label(summary_frame, text=f"Ubicaciones con consolidación parcial: {locations_partial}",
                      foreground="orange").pack(anchor=tk.W, pady=2)
        if locations_with_errors > 0:
            ttk.Label(summary_frame, text=f"Ubicaciones con errores: {locations_with_errors}",
                      foreground="red").pack(anchor=tk.W, pady=2)
        ttk.Label(summary_frame, text=f"Total de LPNs consolidados: {total_processed}").pack(anchor=tk.W, pady=2)
        
        # Información de la carpeta de consolidación
        if consolidation_folder and os.path.exists(consolidation_folder):
            folder_frame = ttk.LabelFrame(main_frame, text="Carpeta de Consolidación", padding="10")
            folder_frame.pack(fill=tk.X, pady=10)
            
            # Nombre de la carpeta
            folder_name = os.path.basename(consolidation_folder)
            folder_path = consolidation_folder
            
            folder_label = ttk.Label(folder_frame, 
                                      text=f"Nombre: {folder_name}", 
                                      foreground="blue")
            folder_label.pack(anchor=tk.W, pady=2)
            
            # Path completo (con posibilidad de seleccionarlo)
            path_entry = ttk.Entry(folder_frame, width=60)
            path_entry.insert(0, folder_path)
            path_entry.config(state="readonly")
            path_entry.pack(fill=tk.X, pady=2)
            
            # Botón para abrir la carpeta
            ttk.Button(folder_frame, text="Abrir Carpeta", 
                       command=lambda: os.startfile(folder_path)).pack(anchor=tk.W, pady=5)
        
        # Frame para etiquetas
        if generated_labels:
            labels_frame = ttk.LabelFrame(main_frame, text="Etiquetas Generadas", padding="10")
            labels_frame.pack(fill=tk.BOTH, expand=True, pady=10)
            
            # Lista de etiquetas generadas
            ttk.Label(labels_frame, text=f"Se generaron {len(generated_labels)} etiquetas HTML:").pack(anchor=tk.W, pady=5)
            
            # Crear lista para mostrar las etiquetas
            listbox = tk.Listbox(labels_frame, font=('Consolas', 10), height=10)
            listbox.pack(fill=tk.BOTH, expand=True, pady=5)
            
            # Agregar etiquetas a la lista
            for label in generated_labels:
                listbox.insert(tk.END, os.path.basename(label))
            
            # Agregar scroll si hay muchas etiquetas
            if len(generated_labels) > 10:
                scrollbar = ttk.Scrollbar(listbox, orient=tk.VERTICAL, command=listbox.yview)
                listbox.configure(yscrollcommand=scrollbar.set)
                scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            
            # Botones para abrir etiquetas
            buttons_frame = ttk.Frame(labels_frame)
            buttons_frame.pack(fill=tk.X, pady=5)
            
            # Función para abrir la etiqueta seleccionada
            def open_selected_label():
                selection = listbox.curselection()
                if selection:
                    selected_label = generated_labels[selection[0]]
                    self.open_html_in_browser(selected_label)
            
            ttk.Button(buttons_frame, text="Abrir Seleccionada", 
                       command=open_selected_label).pack(side=tk.LEFT, padx=5)
            
            # Modificar el botón "Abrir Carpeta" para usar la carpeta de consolidación cuando está disponible
            if consolidation_folder and os.path.exists(consolidation_folder):
                ttk.Button(buttons_frame, text="Abrir Carpeta", 
                           command=lambda: os.startfile(consolidation_folder)).pack(side=tk.LEFT, padx=5)
            else:
                ttk.Button(buttons_frame, text="Abrir Carpeta", 
                           command=lambda: os.startfile(self.PRINT_DIR)).pack(side=tk.LEFT, padx=5)
        else:
            ttk.Label(main_frame, text="No se generaron etiquetas durante el proceso.", 
                      foreground="orange").pack(anchor=tk.W, pady=10)
        
        # Botones de acción
        buttons_frame = ttk.Frame(main_frame)
        buttons_frame.pack(fill=tk.X, pady=10)
        
        # Botón para cerrar
        ttk.Button(buttons_frame, text="Cerrar", 
                   command=dialog.destroy).pack(side=tk.RIGHT, padx=5)
        
        # Botón de ayuda
        ttk.Label(main_frame, text="Nota: Puede acceder a las etiquetas posteriormente desde la pestaña 'Generación de Etiquetas'", 
                  foreground="gray", font=("Arial", 8)).pack(pady=(10, 0))
    
    def consolidate_location(self, location_data, custom_folder=None):
        """Consolida los LPNs de una ubicación y lote específicos"""
        self.log(f"Iniciando consolidación de ubicación con carpeta específica: {custom_folder}", "INFO")
        try:
            # MODIFICACIÓN: Verificar que los datos tengan el mismo LOC y LOT
            if len(location_data['LOC'].unique()) > 1 or len(location_data['LOT'].unique()) > 1:
                self.log("Los datos contienen diferentes ubicaciones o lotes", "ERROR")
                return False, 0, [], None
                
            # Verificar que hay suficientes datos
            if len(location_data) < 2:
                self.log("Se necesitan al menos 2 registros para consolidar", "ERROR")
                return False, 0, [], None
            
            # Verificar registros ya procesados
            already_processed = location_data[location_data['INVENTORYID'].isin(self.processed_inventories)]
            if not already_processed.empty:
                self.log(f"Se encontraron {len(already_processed)} registros ya procesados", "WARNING")
                self.log(f"Ejemplos de IDs ya procesados: {list(already_processed['INVENTORYID'])[:3]}", "WARNING")
            
            # IMPORTANTE: Mantener LOT y LPN como strings para preservar los ceros
            location_data['LOT'] = location_data['LOT'].astype(str)
            location_data['LPN'] = location_data['LPN'].astype(str)
            location_data['INVENTORYID'] = location_data['INVENTORYID'].astype(str)
            location_data['QTY'] = location_data['QTY'].astype(float)
            
            # Seleccionar LPN de referencia (valor numérico más pequeño)
            try:
                # Convertir LPNs a valores numéricos
                location_data['LPN_INT'] = location_data['LPN'].apply(
                    lambda x: int(x) if x.strip().isdigit() else float('inf')
                )
                
                # Seleccionar el de menor valor
                min_idx = location_data['LPN_INT'].idxmin()
                reference = location_data.loc[min_idx]
                
                self.log(f"LPN de referencia seleccionado: {reference['LPN']} (valor numérico: {reference['LPN_INT']})", "INFO")
            except Exception as e:
                # Fallback: usar el LPN más corto
                self.log(f"Error seleccionando LPN por valor numérico: {e}", "WARNING")
                min_idx = location_data['LPN'].str.len().idxmin()
                reference = location_data.loc[min_idx]
                self.log(f"Fallback: LPN de referencia seleccionado: {reference['LPN']} (más corto)", "WARNING")
            
            # Obtener valores de referencia
            reference_lpn = str(reference['LPN'])
            reference_loc = reference['LOC']
            reference_lot = reference['LOT']  # También usamos el lote de referencia
            reference_sku = reference['SKU']  # Almacenar el SKU para la etiqueta
            reference_inv_id = reference['Inventory_ID_HEX']  # Almacenar el ID para obtener datos actualizados
            
            # Guardar datos de referencia para el registro CSV
            self.last_reference_info = {
                'lpn': reference_lpn,
                'loc': reference_loc,
                'lot': reference_lot,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            self.log(f"Información de referencia guardada: LPN={reference_lpn}, LOC={reference_loc}, LOT={reference_lot}", "INFO")
            
            # Normalizar LPN de referencia
            normalized_lpn = self.normalize_id(reference_lpn)
            
            if normalized_lpn != reference_lpn:
                self.log(f"LPN de referencia normalizado: '{reference_lpn}' → '{normalized_lpn}'", "INFO")
            
            # Generar payloads para los movimientos
            payloads = []
            targets = location_data[location_data['LPN'] != reference_lpn]
            
            self.log(f"Preparando {len(targets)} movimientos hacia LPN {reference_lpn}", "INFO")
            
            # Contador de registros excluidos por ya estar procesados
            excluded_count = 0
            
            for idx, record in targets.iterrows():
                try:
                    # Verificar si ya fue procesado
                    if record['INVENTORYID'] in self.processed_inventories:
                        self.log(f"Saltando registro {idx+1}: Inventario {record['INVENTORYID']} ya procesado", "WARNING")
                        excluded_count += 1
                        continue
                    
                    # Conversión de cantidad a entero
                    qty = int(float(record['QTY']))
                    if qty <= 0:
                        self.log(f"Saltando registro {idx+1}: Cantidad inválida ({qty})", "WARNING")
                        excluded_count += 1
                        continue
                    
                    # Obtener y normalizar valores
                    lot_original = str(record['LOT'])
                    lpn_original = str(record['LPN'])
                    
                    lot_value = self.normalize_id(lot_original)
                    lpn_value = self.normalize_id(lpn_original)
                    
                    # Crear payload
                    payloads.append({
                        'ClientId': '1824',
                        'DocKey': str(record['INVENTORYID']),
                        'DocLineNo': '',
                        'DocRefKey': '',
                        'DocType': 'MANUAL',
                        'FromLoc': record['LOC'],
                        'FromLot': lot_value,
                        'FromLpn': lpn_value,
                        'Sku': record['SKU'],
                        'ToLoc': reference_loc,
                        'ToLpn': normalized_lpn,
                        'TransCode': 'MOVE',
                        'Uom': 'EA',
                        'UomQty': qty,
                        'inventoryID': int(record['INVENTORYID'])
                    })
                    
                except Exception as e:
                    self.log(f"Error preparando registro {record['INVENTORYID']}: {str(e)}", "ERROR")
                    excluded_count += 1
            
            if excluded_count > 0:
                self.log(f"Se excluyeron {excluded_count} registros de {len(targets)} totales", "WARNING")
            
            if not payloads:
                self.log("No hay movimientos válidos para procesar", "WARNING")
                return False, 0, [], None
            
            # Procesar los movimientos
            success_count = 0
            error_count = 0
            total = len(payloads)
            processed_ids = []
            
            self.log(f"Iniciando procesamiento de {total} movimientos...", "INFO")
            
            for idx, payload in enumerate(payloads):
                try:
                    self.log(f"Procesando movimiento {idx+1}/{total}: {payload['FromLpn']} → {payload['ToLpn']}", "INFO")
                    
                    # Llamada a la API
                    response = requests.post(
                        self.API_CONFIG['move_url'],
                        json=payload,
                        headers=self.API_CONFIG['headers']['move'],
                        timeout=self.API_CONFIG.get('timeout', 10)
                    )
                    
                    if response.status_code in [200, 201]:
                        success_count += 1
                        self.log(f"Movimiento {idx+1} exitoso", "SUCCESS")
                        processed_ids.append(str(payload['inventoryID']))
                    else:
                        error_count += 1
                        error_msg = response.text
                        self.log(f"Error {response.status_code} en movimiento {idx+1}: {error_msg}", "ERROR")
                    
                    # Pausa entre requests
                    time.sleep(self.API_CONFIG.get('delay_between_requests', 1.0))
                    
                except Exception as e:
                    error_count += 1
                    self.log(f"Error en movimiento {idx+1}: {str(e)}", "ERROR")
            
            # Resumen final de la consolidación
            self.log(f"\nResumen de consolidación para ubicación {reference_loc}, lote {reference_lot}:", "INFO")
            self.log(f"  - Total de movimientos planificados: {total}", "INFO")
            self.log(f"  - Movimientos exitosos: {success_count}", "SUCCESS" if success_count > 0 else "INFO")
            if error_count > 0:
                self.log(f"  - Movimientos con error: {error_count}", "ERROR")
            if excluded_count > 0:
                self.log(f"  - Registros excluidos: {excluded_count}", "WARNING")
            
            # Actualizar inventarios procesados
            with self.thread_lock:
                for inv_id in processed_ids:
                    self.processed_inventories.add(inv_id)
                
                if processed_ids:
                    # Guardar registro de movimientos
                    try:
                        # Convertir todos los IDs a string para consistencia
                        processed_ids_str = [str(pid) for pid in processed_ids]
                        self.log(f"IDs procesados para registro: {processed_ids_str[:5]}{'...' if len(processed_ids_str) > 5 else ''}", "INFO")
                        self.save_movement_record(processed_ids_str)
                    except Exception as reg_error:
                        self.log(f"Error guardando registro de movimientos: {str(reg_error)}", "ERROR")
            
            # Generar etiqueta HTML para el LPN consolidado si hubo éxito
            label_filename = None
            if success_count > 0:
                try:
                    # Obtener detalles actualizados del LPN consolidado desde la API
                    self.log(f"Generando etiqueta para LPN consolidado: {reference_lpn}", "INFO")
                    consolidated_data = self.get_inventory_details(reference_inv_id)
                    
                    if 'error' not in consolidated_data:
                        # Generar etiqueta HTML usando la carpeta personalizada si está disponible
                        self.log(f"Generando etiqueta para carpeta personalizada: {custom_folder}", "INFO")
                        label_filename = self.generate_label(consolidated_data, custom_folder)
                        self.log(f"Etiqueta generada con nombre: {label_filename}", "SUCCESS")
                        if label_filename:
                            self.log(f"Etiqueta HTML generada exitosamente: {label_filename}", "SUCCESS")
                            # Abrir la etiqueta en el navegador automáticamente
                            self.open_html_in_browser(label_filename)
                    else:
                        self.log(f"No se pudo obtener datos para generar etiqueta", "WARNING")
                except Exception as e:
                    self.log(f"Error generando etiqueta HTML: {str(e)}", "ERROR")
            
            # Verificar resultado final
            if success_count > 0:
                return True, error_count, processed_ids, label_filename
            return False, error_count, [], None
            
        except Exception as e:
            self.log(f"Error en consolidación de ubicación: {str(e)}", "ERROR")
            return False, 0, [], None
    
    def stop_bulk_consolidation(self):
        """Solicita detener el proceso de consolidación masiva"""
        if not self.bulk_running:
            self.log("No hay proceso de consolidación masiva en ejecución", "WARNING")
            return
        
        self.bulk_stop_requested = True
        self.log("Solicitando detener el proceso. Espere a que termine la operación actual...", "WARNING")

    def setup_html_tab(self):
        """Configura la pestaña de generación de etiquetas HTML"""
        # Frame principal
        main_frame = ttk.Frame(self.html_tab, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Panel superior con título
        top_frame = ttk.Frame(main_frame)
        top_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(top_frame, text="Generación de Etiquetas", 
                 font=("Arial", 14, "bold")).pack(side=tk.LEFT, padx=10)
        
        # Crear notebook para tener dos pestañas: Manual y Masiva
        html_notebook = ttk.Notebook(main_frame)
        html_notebook.pack(fill=tk.BOTH, expand=True, pady=5)
        
        # Pestaña 1: Generación Manual
        manual_tab = ttk.Frame(html_notebook)
        html_notebook.add(manual_tab, text="Generación Manual")
        
        # Panel de entrada de datos para generación manual
        input_frame = ttk.LabelFrame(manual_tab, text="Datos de Etiqueta", padding="10")
        input_frame.pack(fill=tk.X, pady=5)
        
        # Grid para organizar campos
        input_frame.columnconfigure(1, weight=1)
        input_frame.columnconfigure(3, weight=1)
        
        # Campo SKU
        ttk.Label(input_frame, text="SKU:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        self.html_sku_entry = ttk.Entry(input_frame, width=25)
        self.html_sku_entry.grid(row=0, column=1, padx=5, pady=5, sticky=tk.EW)
        
        # Campo LOC
        ttk.Label(input_frame, text="LOC:").grid(row=0, column=2, padx=5, pady=5, sticky=tk.W)
        self.html_loc_entry = ttk.Entry(input_frame, width=25)
        self.html_loc_entry.grid(row=0, column=3, padx=5, pady=5, sticky=tk.EW)
        
        # Campo LOT
        ttk.Label(input_frame, text="LOT:").grid(row=1, column=0, padx=5, pady=5, sticky=tk.W)
        self.html_lot_entry = ttk.Entry(input_frame, width=25)
        self.html_lot_entry.grid(row=1, column=1, padx=5, pady=5, sticky=tk.EW)
        
        # Campo LPN
        ttk.Label(input_frame, text="LPN:").grid(row=1, column=2, padx=5, pady=5, sticky=tk.W)
        self.html_lpn_entry = ttk.Entry(input_frame, width=25)
        self.html_lpn_entry.grid(row=1, column=3, padx=5, pady=5, sticky=tk.EW)
        
        # Panel de acciones
        button_frame = ttk.Frame(manual_tab)
        button_frame.pack(fill=tk.X, pady=10)
        
        ttk.Button(button_frame, text="Generar Etiqueta", 
                  command=self.generate_manual_label).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(button_frame, text="Limpiar Campos", 
                  command=self.clear_html_form).pack(side=tk.LEFT, padx=5)
        
        # Panel de vista previa
        preview_frame = ttk.LabelFrame(manual_tab, text="Etiquetas Generadas", padding="10")
        preview_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        # Lista de etiquetas generadas
        self.label_listbox = tk.Listbox(preview_frame, font=('Consolas', 10))
        self.label_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Scrollbar
        listbox_scroll = ttk.Scrollbar(preview_frame, orient=tk.VERTICAL, command=self.label_listbox.yview)
        self.label_listbox.configure(yscrollcommand=listbox_scroll.set)
        listbox_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Asociar doble clic para abrir la etiqueta
        self.label_listbox.bind('<Double-1>', self.open_selected_label)
        
        # Panel de botones para listbox
        listbox_buttons = ttk.Frame(preview_frame)
        listbox_buttons.pack(fill=tk.X, pady=5)
        
        ttk.Button(listbox_buttons, text="Abrir Seleccionada", 
                  command=lambda: self.open_selected_label(None)).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(listbox_buttons, text="Actualizar Lista", 
                  command=self.update_label_list).pack(side=tk.LEFT, padx=5)
        
        # Pestaña 2: Generación Masiva
        bulk_tab = ttk.Frame(html_notebook)
        html_notebook.add(bulk_tab, text="Generación Masiva")
        
        # Panel de filtros
        filter_frame = ttk.LabelFrame(bulk_tab, text="Filtros de Ubicación", padding="10")
        filter_frame.pack(fill=tk.X, pady=5)
        
        # Filtros para pasillo
        ttk.Label(filter_frame, text="Pasillo:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        self.aisle_entry = ttk.Entry(filter_frame, width=10)
        self.aisle_entry.grid(row=0, column=1, padx=5, pady=5, sticky=tk.W)
        
        # Filtros para rango de ubicaciones
        ttk.Label(filter_frame, text="Desde ubicación:").grid(row=0, column=2, padx=5, pady=5, sticky=tk.W)
        self.loc_from_entry = ttk.Entry(filter_frame, width=10)
        self.loc_from_entry.grid(row=0, column=3, padx=5, pady=5, sticky=tk.W)
        
        ttk.Label(filter_frame, text="Hasta ubicación:").grid(row=0, column=4, padx=5, pady=5, sticky=tk.W)
        self.loc_to_entry = ttk.Entry(filter_frame, width=10)
        self.loc_to_entry.grid(row=0, column=5, padx=5, pady=5, sticky=tk.W)
        
        # Filtros para niveles
        ttk.Label(filter_frame, text="Niveles (1-5):").grid(row=1, column=0, padx=5, pady=5, sticky=tk.W)
        self.level_from_entry = ttk.Entry(filter_frame, width=4)
        self.level_from_entry.grid(row=1, column=1, padx=5, pady=5, sticky=tk.W)
        self.level_from_entry.insert(0, "1")
        
        ttk.Label(filter_frame, text="hasta").grid(row=1, column=2, padx=5, pady=5, sticky=tk.W)
        self.level_to_entry = ttk.Entry(filter_frame, width=4)
        self.level_to_entry.grid(row=1, column=3, padx=5, pady=5, sticky=tk.W)
        self.level_to_entry.insert(0, "5")
        
        # Botón para generar múltiples etiquetas
        actions_frame = ttk.Frame(bulk_tab)
        actions_frame.pack(fill=tk.X, pady=10)
        
        ttk.Button(actions_frame, text="Cargar Datos Filtrados", 
                  command=self.load_filtered_labels).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(actions_frame, text="Generar Etiquetas", 
                  command=self.generate_bulk_labels).pack(side=tk.LEFT, padx=5)
        
        # Panel de resultados
        results_frame = ttk.LabelFrame(bulk_tab, text="Resultados", padding="10")
        results_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        # Tabla para mostrar datos filtrados
        columns = ('loc', 'lpn', 'sku', 'lot', 'qty')
        self.bulk_label_tree = ttk.Treeview(results_frame, columns=columns, show='headings')
        
        # Configurar encabezados
        self.bulk_label_tree.heading('loc', text='Ubicación')
        self.bulk_label_tree.heading('lpn', text='LPN')
        self.bulk_label_tree.heading('sku', text='SKU')
        self.bulk_label_tree.heading('lot', text='Lote')
        self.bulk_label_tree.heading('qty', text='Cantidad')
        
        # Configurar anchos de columna
        self.bulk_label_tree.column('loc', width=100)
        self.bulk_label_tree.column('lpn', width=100)
        self.bulk_label_tree.column('sku', width=150)
        self.bulk_label_tree.column('lot', width=100)
        self.bulk_label_tree.column('qty', width=80, anchor=tk.CENTER)
        
        # Scrollbars
        self.bulk_tree_yscroll = ttk.Scrollbar(results_frame, orient=tk.VERTICAL, command=self.bulk_label_tree.yview)
        self.bulk_label_tree.configure(yscrollcommand=self.bulk_tree_yscroll.set)
        
        # Colocar elementos
        self.bulk_label_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.bulk_tree_yscroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Panel de estadísticas 
        stats_frame = ttk.Frame(bulk_tab)
        stats_frame.pack(fill=tk.X, pady=5)
        
        self.bulk_stats_label = ttk.Label(stats_frame, text="Datos cargados: 0 registros")
        self.bulk_stats_label.pack(side=tk.LEFT, padx=5)
        
        # Actualizar la lista inicialmente
        self.update_label_list()
    
    def generate_manual_label(self):
        """Genera una etiqueta HTML con los datos ingresados manualmente"""
        try:
            # Obtener valores de los campos
            sku = self.html_sku_entry.get().strip()
            loc = self.html_loc_entry.get().strip()
            lot = self.html_lot_entry.get().strip()
            lpn = self.html_lpn_entry.get().strip()
            
            # Validar campos
            if not sku or not loc or not lot or not lpn:
                self.log("Todos los campos son obligatorios", "ERROR")
                messagebox.showerror("Error", "Todos los campos son obligatorios")
                return
            
            # Generar códigos de barras
            from barcode import Code128
            from barcode.writer import ImageWriter
            import base64
            import io
            
            def generate_barcode_base64(value):
                if not value:
                    return ""
                try:
                    value_str = str(value)
                    if not value_str.strip() or not all(32 <= ord(c) <= 127 for c in value_str):
                        return ""
                    barcode_buffer = io.BytesIO()
                    Code128(value_str, writer=ImageWriter()).write(
                        barcode_buffer,
                        options={'write_text': False, 'module_height': 12, 'module_width': 0.4}
                    )
                    barcode_buffer.seek(0)
                    return base64.b64encode(barcode_buffer.read()).decode('utf-8')
                except Exception as e:
                    self.log(f"Error generando código de barras para '{value}': {str(e)}", "ERROR")
                    return ""
            
            # Generar códigos de barras
            sku_barcode = generate_barcode_base64(sku)
            loc_barcode = generate_barcode_base64(loc)
            lot_barcode = generate_barcode_base64(lot)
            lpn_barcode = generate_barcode_base64(lpn)
            
            # Generar HTML
            html_content = self.create_label_html(sku, sku_barcode, loc, loc_barcode,
                                              lot, lot_barcode, lpn, lpn_barcode)
            
            # Usar la carpeta PRINT personalizada
            if not os.path.exists(self.PRINT_DIR):
                os.makedirs(self.PRINT_DIR)
                
            # Guardar archivo HTML
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = os.path.join(self.PRINT_DIR, f"{sku}_{lot}_{lpn}_label_{timestamp}.html")
            
            with open(filename, "w", encoding="utf-8") as f:
                f.write(html_content)
            
            self.log(f"Etiqueta HTML generada: {filename}", "SUCCESS")
            
            # Actualizar la lista de etiquetas
            self.update_label_list()
            
            # Preguntar si desea abrir la etiqueta
            if messagebox.askyesno("Etiqueta Generada", 
                                 f"Se ha generado la etiqueta: {filename}\n\n¿Desea abrirla en el navegador predeterminado?"):
                self.open_html_in_browser(filename)
            
            return filename
            
        except ImportError:
            self.log("Error: Módulo 'barcode' no instalado. Instale con 'pip install python-barcode pillow'", "ERROR")
            messagebox.showerror("Error", "Módulo 'barcode' no instalado. Instale con 'pip install python-barcode pillow'")
            return None
        except Exception as e:
            self.log(f"Error generando etiqueta: {str(e)}", "ERROR")
            messagebox.showerror("Error", f"Error generando etiqueta: {str(e)}")
            return None
    
    def clear_html_form(self):
        """Limpia los campos del formulario de etiquetas"""
        self.html_sku_entry.delete(0, tk.END)
        self.html_loc_entry.delete(0, tk.END)
        self.html_lot_entry.delete(0, tk.END)
        self.html_lpn_entry.delete(0, tk.END)
        self.html_sku_entry.focus()
    
    def update_label_list(self):
        """Actualiza la lista de etiquetas generadas"""
        try:
            # Limpiar lista
            self.label_listbox.delete(0, tk.END)
            
            # Verificar que existe el directorio
            if not os.path.exists(self.PRINT_DIR):
                os.makedirs(self.PRINT_DIR)
            
            # Obtener archivos HTML
            files = [f for f in os.listdir(self.PRINT_DIR) if f.endswith('.html')]
            
            if not files:
                self.label_listbox.insert(tk.END, "No hay etiquetas generadas")
                return
            
            # Ordenar por fecha (más reciente primero)
            files.sort(key=lambda x: os.path.getmtime(os.path.join(self.PRINT_DIR, x)), reverse=True)
            
            # Agregar a la lista
            for file in files:
                self.label_listbox.insert(tk.END, file)
            
            # Seleccionar el primero
            self.label_listbox.selection_set(0)
            
        except Exception as e:
            self.log(f"Error actualizando lista de etiquetas: {str(e)}", "ERROR")    
    def open_selected_label(self, event):
        """Abre la etiqueta seleccionada en el navegador"""
        # Obtener selección
        selection = self.label_listbox.curselection()
        if not selection:
            return
        
        # Obtener nombre de archivo
        filename = self.label_listbox.get(selection[0])
        if filename == "No hay etiquetas generadas":
            return
        
        # Construir ruta completa
        filepath = os.path.join(self.PRINT_DIR, filename)
        
        # Abrir en navegador
        if os.path.exists(filepath):
            self.open_html_in_browser(filepath)
            self.log(f"Etiqueta abierta: {filename}", "INFO")
        else:
            self.log(f"Archivo no encontrado: {filepath}", "ERROR")
            messagebox.showerror("Error", "Archivo no encontrado")
            
            # Actualizar lista
            self.update_label_list()

    def open_csv_folder(self):
        """Abre la carpeta de registros CSV"""
        try:
            if os.path.exists(self.REGISTRO_DIR):
                # Abrir la carpeta en el explorador de archivos
                os.startfile(self.REGISTRO_DIR)
                self.log(f"Abriendo carpeta de registros: {self.REGISTRO_DIR}", "INFO")
            else:
                self.log(f"La carpeta de registros no existe: {self.REGISTRO_DIR}", "WARNING")
                # Crear la carpeta si no existe
                os.makedirs(self.REGISTRO_DIR)
                os.startfile(self.REGISTRO_DIR)
                self.log(f"Carpeta creada y abierta: {self.REGISTRO_DIR}", "INFO")
        except Exception as e:
            self.log(f"Error al abrir carpeta de registros: {str(e)}", "ERROR")
            messagebox.showerror("Error", f"No se pudo abrir la carpeta de registros: {str(e)}")

    def get_user_id(self):
        """Solicita el ID de usuario al iniciar la aplicación"""
        dialog = LoginDialog(self.root, self.NAMES_CSV)
        
        # Guardar también el nombre y nombre corto
        self.user_name = getattr(dialog, 'user_name', "")
        self.user_short_name = getattr(dialog, 'user_short_name', "")
        
        return dialog.result

    def reset_thread_counter(self):
        with self.thread_lock:
            self.running_threads = 0
        self.log("Contador de hilos reiniciado manualmente", "WARNING")
        messagebox.showinfo("Reinicio de Hilos", "El contador de hilos ha sido reiniciado")

    def setup_global_tab(self):
        """Configura la pestaña de consolidación global por SKU, LOC y LOT"""
        # Frame principal
        main_frame = ttk.Frame(self.global_tab, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Panel superior
        top_frame = ttk.Frame(main_frame)
        top_frame.pack(fill=tk.X, pady=5)
        
        # Título
        ttk.Label(top_frame, text="Consolidación Global por SKU, LOC y LOT", 
                 font=("Arial", 14, "bold")).pack(side=tk.LEFT, padx=10)
        
        # Panel de configuración
        config_frame = ttk.LabelFrame(main_frame, text="Configuración", padding="10")
        config_frame.pack(fill=tk.X, pady=5)
        
        # Panel de filtros
        filter_frame = ttk.Frame(config_frame)
        filter_frame.pack(fill=tk.X, pady=5)
        
        # Botón de cargar
        ttk.Button(filter_frame, text="Cargar Datos Globales", 
                  command=self.load_global_data).grid(row=0, column=0, padx=5, pady=5)
        
        # Panel de opciones avanzadas
        options_frame = ttk.LabelFrame(config_frame, text="Opciones Avanzadas", padding="10")
        options_frame.pack(fill=tk.X, pady=5)
        
        # Excluir ubicaciones
        ttk.Label(options_frame, text="Excluir ubicaciones especiales:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        self.global_exclude_special_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(options_frame, variable=self.global_exclude_special_var).grid(row=0, column=1, padx=5, pady=5, sticky=tk.W)
        
        # Control de velocidad API
        ttk.Label(options_frame, text="Retraso entre grupos (s):").grid(row=0, column=2, padx=5, pady=5, sticky=tk.W)
        self.global_api_delay_var = tk.DoubleVar(value=2.0)
        delay_spinbox = ttk.Spinbox(options_frame, from_=0.5, to=10.0, increment=0.5, textvariable=self.global_api_delay_var, width=5)
        delay_spinbox.grid(row=0, column=3, padx=5, pady=5, sticky=tk.W)
        
        # Resultados panel
        result_frame = ttk.LabelFrame(main_frame, text="Grupos a Consolidar", padding="10")
        result_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        # Treeview para los resultados agrupados
        self.global_tree = ttk.Treeview(
            result_frame, 
            columns=('SKU', 'LOC', 'LOT', 'NUM_LPNS', 'TOTAL_QTY', 'STATUS', 'DETALLES'),
            show='headings'
        )
        
        # Configurar columnas
        self.global_tree.heading('SKU', text='SKU')
        self.global_tree.heading('LOC', text='Ubicación')
        self.global_tree.heading('LOT', text='Lote')
        self.global_tree.heading('NUM_LPNS', text='LPNs')
        self.global_tree.heading('TOTAL_QTY', text='Cantidad')
        self.global_tree.heading('STATUS', text='Estado')
        self.global_tree.heading('DETALLES', text='Detalles')
        
        self.global_tree.column('SKU', width=120)
        self.global_tree.column('LOC', width=100)
        self.global_tree.column('LOT', width=100)
        self.global_tree.column('NUM_LPNS', width=60, anchor=tk.CENTER)
        self.global_tree.column('TOTAL_QTY', width=80, anchor=tk.CENTER)
        self.global_tree.column('STATUS', width=100, anchor=tk.CENTER)
        self.global_tree.column('DETALLES', width=150)
        
        # Scrollbar
        y_scroll = ttk.Scrollbar(result_frame, orient=tk.VERTICAL, command=self.global_tree.yview)
        self.global_tree.configure(yscrollcommand=y_scroll.set)
        
        # Estilos para filas
        self.global_tree.tag_configure('pending', background='#ffffff')
        self.global_tree.tag_configure('in_progress', background='#fff9c4')
        self.global_tree.tag_configure('completed', background='#c8e6c9')
        self.global_tree.tag_configure('error', background='#ffcdd2')
        
        self.global_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        y_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Panel de detalle
        detail_frame = ttk.LabelFrame(main_frame, text="Detalle de LPNs", padding="10")
        detail_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        # Treeview para los detalles
        self.global_detail_tree = ttk.Treeview(
            detail_frame, 
            columns=('LPN', 'QTY', 'STATUS'),
            show='headings'
        )
        
        # Configurar columnas
        self.global_detail_tree.heading('LPN', text='LPN')
        self.global_detail_tree.heading('QTY', text='Cantidad')
        self.global_detail_tree.heading('STATUS', text='Estado')
        
        self.global_detail_tree.column('LPN', width=150)
        self.global_detail_tree.column('QTY', width=80, anchor=tk.CENTER)
        self.global_detail_tree.column('STATUS', width=120)
        
        # Scrollbar
        detail_scroll = ttk.Scrollbar(detail_frame, orient=tk.VERTICAL, command=self.global_detail_tree.yview)
        self.global_detail_tree.configure(yscrollcommand=detail_scroll.set)
        
        self.global_detail_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        detail_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Panel de acciones
        action_frame = ttk.Frame(main_frame)
        action_frame.pack(fill=tk.X, pady=10)
        
        # Botones de acción
        ttk.Button(action_frame, text="Iniciar Consolidación Global", 
                  command=self.start_global_consolidation).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(action_frame, text="Detener Proceso", 
                  command=self.stop_global_consolidation).pack(side=tk.LEFT, padx=5)
        
        # Variables de control para la consolidación global
        self.global_data = None
        self.global_groups = None
        self.global_running = False
        self.global_stop_requested = False
        
        # Evento para mostrar detalles al seleccionar un grupo
        self.global_tree.bind('<<TreeviewSelect>>', self.on_global_group_selected)
    
    def load_global_data(self):
        """Carga datos para la consolidación global basada en SKU, LOC y LOT"""
        try:
            # Verificar que hay datos cargados
            if self.data_cache is None:
                self.log("No hay datos cargados. Carga datos primero.", "ERROR")
                messagebox.showerror("Error", "No hay datos cargados. Haga clic en 'Reload Data' primero.")
                return
            
            # Usar todos los datos disponibles
            data = self.data_cache.copy()
            
            # Excluir ubicaciones especiales si es necesario
            if self.global_exclude_special_var.get():
                original_count = len(data)
                data = data[~data['LOC'].isin(self.UBICACIONES_ESPECIALES)]
                excluded_count = original_count - len(data)
                if excluded_count > 0:
                    self.log(f"Se excluyeron {excluded_count} registros de ubicaciones especiales", "INFO")
            
            # Verificar si quedan datos
            if data.empty:
                self.log("No hay datos válidos después de aplicar filtros", "ERROR")
                messagebox.showerror("Error", "No hay datos válidos después de aplicar filtros")
                return
            
            # Asegurarnos que LOT sea string para la agrupación
            data['LOT'] = data['LOT'].astype(str)
            
            # Agrupar por SKU, ubicación Y lote
            groups = data.groupby(['SKU', 'LOC', 'LOT']).agg({
                'LPN': 'nunique',  # Contar LPNs únicos por grupo
                'QTY': 'sum'      # Sumar cantidades
            }).reset_index()
            
            # Renombrar columnas para claridad
            groups.columns = ['SKU', 'LOC', 'LOT', 'NUM_LPNS', 'TOTAL_QTY']
            
            # Sólo incluir grupos con más de 1 LPN
            groups = groups[groups['NUM_LPNS'] > 1]
            
            if groups.empty:
                self.log("No hay grupos con múltiples LPNs para consolidar", "WARNING")
                messagebox.showwarning("Advertencia", "No hay grupos con múltiples LPNs para consolidar")
                return
            
            # Ordenar por número de LPNs (descendente)
            groups = groups.sort_values('NUM_LPNS', ascending=False)
            
            # Añadir estado
            groups['STATUS'] = 'Pendiente'
            groups['DETALLES'] = 'Esperando procesamiento'
            
            # Guardar datos
            self.global_data = data
            self.global_groups = groups
            
            # Mostrar datos en treeview
            self.update_global_treeview()
            
            # Limpiar detalles
            self.global_detail_tree.delete(*self.global_detail_tree.get_children())
            
            self.log(f"Datos cargados: {len(groups)} grupos con múltiples LPNs para consolidación global", "SUCCESS")
            
        except Exception as e:
            self.log(f"Error cargando datos para consolidación global: {str(e)}", "ERROR")
            messagebox.showerror("Error", f"Error cargando datos: {str(e)}")
    
    def update_global_treeview(self):
        """Actualiza el treeview de consolidación global"""
        # Limpiar treeview
        self.global_tree.delete(*self.global_tree.get_children())
        
        # Verificar que hay datos
        if self.global_groups is None:
            return
        
        # Counters for status summary
        status_count = {'Pendiente': 0, 'En Proceso': 0, 'Completado': 0, 'Parcial': 0, 'Error': 0}
        
        # Añadir filas
        for _, row in self.global_groups.iterrows():
            sku = row['SKU']
            loc = row['LOC']
            lot = row['LOT']
            num_lpns = row['NUM_LPNS']
            total_qty = row['TOTAL_QTY']
            status = row['STATUS']
            detalles = row['DETALLES']
            
            # Count by status
            status_count[status] = status_count.get(status, 0) + 1
            
            # Determinar tag para estilo
            tag = 'pending'
            if status == 'En Proceso':
                tag = 'in_progress'
            elif status == 'Completado':
                tag = 'completed'
            elif status == 'Parcial':
                tag = 'in_progress'
            elif status == 'Error':
                tag = 'error'
            
            # Insertar fila
            self.global_tree.insert('', 'end', values=(sku, loc, lot, num_lpns, total_qty, status, detalles), tags=(tag,))
        
        # Configure styles for tree tags
        self.global_tree.tag_configure('pending', background='#ffffff')
        self.global_tree.tag_configure('in_progress', background='#fff9c4')
        self.global_tree.tag_configure('completed', background='#c8e6c9', foreground='#2e7d32')
        self.global_tree.tag_configure('error', background='#ffcdd2', foreground='#c62828')
        
        # Log status summary
        if sum(status_count.values()) > 0:
            self.log("Resumen de estados de consolidación global:", "INFO")
            for status, count in status_count.items():
                if count > 0:
                    level = "INFO"
                    if status == "Completado":
                        level = "SUCCESS"
                    elif status == "Error":
                        level = "ERROR"
                    elif status == "Parcial":
                        level = "WARNING"
                    self.log(f"  - {status}: {count}", level)
    
    def on_global_group_selected(self, event):
        """Muestra los detalles de LPNs cuando se selecciona un grupo"""
        # Obtener selección
        selection = self.global_tree.selection()
        if not selection:
            return
        
        # Obtener SKU, ubicación y lote seleccionados
        item = self.global_tree.item(selection[0])
        values = item['values']
        
        if len(values) >= 3:
            sku = values[0]
            loc = values[1]
            lot = values[2]
        else:
            return
            
        # Verificar que hay datos
        if self.global_data is None:
            return
        
        # Filtrar datos para el SKU, ubicación Y lote seleccionados
        filtered_data = self.global_data[
            (self.global_data['SKU'] == sku) & 
            (self.global_data['LOC'] == loc) & 
            (self.global_data['LOT'].astype(str) == str(lot))
        ].copy()
        
        # Limpiar treeview de detalles
        self.global_detail_tree.delete(*self.global_detail_tree.get_children())
        
        # Añadir filas con detalles
        for _, row in filtered_data.iterrows():
            lpn = row['LPN']
            qty = row['QTY']
            
            # Determinar estado
            status = "Pendiente"
            if row['INVENTORYID'] in self.processed_inventories:
                status = "Procesado"
            
            # Insertar fila
            self.global_detail_tree.insert('', 'end', values=(lpn, qty, status))
        
        self.log(f"Mostrando {len(filtered_data)} LPNs para SKU={sku}, LOC={loc}, LOT={lot}", "INFO")
    
    def start_global_consolidation(self):
        """Inicia el proceso de consolidación global"""
        # Verificar que hay datos
        if self.global_groups is None or self.global_data is None:
            self.log("No hay datos para consolidar", "ERROR")
            messagebox.showerror("Error", "No hay datos para consolidar. Cargue datos primero.")
            return
        
        # Verificar si ya está en ejecución
        if self.global_running:
            self.log("Ya hay un proceso de consolidación global en ejecución", "WARNING")
            messagebox.showwarning("Advertencia", "Ya hay un proceso en ejecución")
            return
        
        # Confirmar operación
        total_groups = len(self.global_groups)
        pending_groups = len(self.global_groups[self.global_groups['STATUS'] == 'Pendiente'])
        
        if pending_groups == 0:
            self.log("No hay grupos pendientes para consolidar", "WARNING")
            messagebox.showinfo("Información", "No hay grupos pendientes para consolidar")
            return
        
        confirm_msg = (
            f"Se van a consolidar {pending_groups} grupos de SKU-LOC-LOT.\n\n"
            f"Esto moverá múltiples LPNs a un único LPN de referencia para cada grupo.\n\n"
            "¿Está seguro de querer continuar con la consolidación global?"
        )
        
        if not messagebox.askyesno("Confirmar Consolidación Global", confirm_msg):
            self.log("Proceso cancelado por el usuario", "INFO")
            return
        
        # Iniciar proceso en un hilo separado
        self.global_running = True
        self.global_stop_requested = False
        
        thread = threading.Thread(target=self.global_consolidation_thread, daemon=True)
        thread.start()
        self.threads.append(thread)
        self.running_threads += 1
    
    def global_consolidation_thread(self):
        """Hilo para la consolidación global por SKU, LOC y LOT"""
        try:
            # Guardar logs antes de iniciar un proceso crítico
            if self.log_auto_save:
                self.save_log(silent=True)
                
            total_processed = 0
            total_errors = 0
            total_groups = 0
            groups_with_errors = 0
            groups_success = 0
            groups_partial = 0
            generated_labels = []
            
            # Control de velocidad API
            api_delay = self.global_api_delay_var.get()
            last_api_call_time = 0
            api_call_count = 0
            max_calls_per_minute = 30  # Limitar a 30 llamadas por minuto (ajustable)
            
            # Crear carpeta específica para este proceso de consolidación
            first_group = self.global_groups['SKU'].iloc[0] if not self.global_groups.empty else None
            filters_text = None
            
            # Obtener información de filtros aplicados
            if hasattr(self, 'global_filter_sku_var') and self.global_filter_sku_var.get():
                filters_text = f"SKU_{self.global_sku_combobox.get()}"
            elif hasattr(self, 'global_filter_lot_var') and self.global_filter_lot_var.get():
                filters_text = f"Lot_{self.global_lot_combobox.get()}"
            
            # Crear carpeta para etiquetas de esta consolidación con manejo de errores
            consolidation_folder = None
            try:
                if first_group:
                    consolidation_folder = self.create_consolidation_folder(f"Global_{first_group}", filters_text)
                else:
                    consolidation_folder = self.create_consolidation_folder("Global_Unknown", filters_text)
            except Exception as e:
                self.log(f"Error creando carpeta de consolidación: {str(e)}", "ERROR")
                consolidation_folder = None
            
            # Registrar inicio del proceso con ID de usuario
            self.log(f"Iniciando consolidación global - Usuario ID: {self.user_id}", "INFO")
            self.log(f"Retraso configurado entre grupos: {api_delay} segundos", "INFO")
            if consolidation_folder:
                self.log(f"Etiquetas se guardarán en: {consolidation_folder}", "INFO")
            else:
                self.log("ADVERTENCIA: No se pudo crear carpeta para etiquetas", "WARNING")
            
            # Procesar cada grupo
            for idx, row in self.global_groups.iterrows():
                # Verificar si se solicitó detener
                if self.global_stop_requested:
                    self.log("Proceso detenido por el usuario", "WARNING")
                    break
                
                sku = row['SKU']
                loc = row['LOC']
                lot = row['LOT']
                
                # Si el grupo no está pendiente, lo saltamos
                if row['STATUS'] != 'Pendiente':
                    self.log(f"Saltando grupo SKU={sku}, LOC={loc}, LOT={lot}: estado ya es {row['STATUS']}", "INFO")
                    continue
                
                # Control de velocidad API - ajuste adaptativo entre grupos
                current_time = time.time()
                time_elapsed = current_time - last_api_call_time
                
                # Verificar si necesitamos esperar más para evitar saturación
                if api_call_count >= max_calls_per_minute:
                    required_wait = 4 - time_elapsed
                    if required_wait > 0:
                        self.log(f"Controlando velocidad de API - Esperando {required_wait:.1f}s para evitar saturación", "WARNING")
                        time.sleep(required_wait)
                        # Reiniciar contador después de esperar
                        api_call_count = 0
                        last_api_call_time = time.time()
                
                # Si ha pasado suficiente tiempo entre grupos, esperamos el retraso configurado
                if time_elapsed < api_delay:
                    wait_time = api_delay - time_elapsed
                    self.log(f"Esperando {wait_time:.1f}s entre grupos para evitar saturar la API...", "INFO")
                    time.sleep(wait_time)
                
                # Actualizar tiempo de última llamada
                last_api_call_time = time.time()
                
                total_groups += 1
                self.log(f"\n--- Procesando grupo SKU={sku}, LOC={loc}, LOT={lot} ---", "INFO")
                
                # Actualizar estado a "En Proceso"
                self.global_groups.at[idx, 'STATUS'] = 'En Proceso'
                self.global_groups.at[idx, 'DETALLES'] = 'Procesando...'
                self.root.after(0, self.update_global_treeview)
                
                # Extraer datos de este grupo
                group_data = self.global_data[
                    (self.global_data['SKU'] == sku) & 
                    (self.global_data['LOC'] == loc) & 
                    (self.global_data['LOT'] == lot)
                ].copy()
                
                # Verificar datos
                if group_data.empty:
                    self.log(f"No hay datos para el grupo SKU={sku}, LOC={loc}, LOT={lot}", "ERROR")
                    self.global_groups.at[idx, 'STATUS'] = 'Error'
                    self.global_groups.at[idx, 'DETALLES'] = 'No hay datos'
                    groups_with_errors += 1
                    self.root.after(0, self.update_global_treeview)
                    continue
                
                # Limitar el tamaño del grupo para evitar sobrecarga
                if len(group_data) > 20:
                    self.log(f"Grupo grande detectado ({len(group_data)} LPNs) - Procesando en lotes para evitar sobrecarga", "WARNING")
                    # Dividimos en lotes de máximo 20 LPNs
                    batch_size = 20
                    batches = [group_data.iloc[i:i+batch_size] for i in range(0, len(group_data), batch_size)]
                    
                    success_batch = 0
                    errors_batch = 0
                    processed_ids_batch = []
                    label_filename_batch = None
                    
                    for i, batch in enumerate(batches):
                        self.log(f"Procesando lote {i+1}/{len(batches)} ({len(batch)} LPNs)", "INFO")
                        
                        # Control de velocidad API entre lotes
                        if i > 0:
                            batch_delay = max(1.5, api_delay/2)  # Al menos 1.5 segundos entre lotes
                            self.log(f"Esperando {batch_delay:.1f}s entre lotes para evitar saturar la API...", "INFO")
                            time.sleep(batch_delay)
                        
                        success, errors, processed_ids, label_filename = self.consolidate_location(batch, consolidation_folder)
                        
                        # Acumular resultados
                        success_batch += len(processed_ids)
                        errors_batch += errors
                        processed_ids_batch.extend(processed_ids)
                        if label_filename:
                            label_filename_batch = label_filename
                        
                        # Incrementar contador de llamadas API
                        api_call_count += len(batch)
                    
                    # Usar los resultados acumulados
                    success = success_batch > 0
                    errors = errors_batch
                    processed_inv_ids = processed_ids_batch
                    label_filename = label_filename_batch
                else:
                    # Intentar consolidar este grupo (reutilizamos la función de consolidación por ubicación)
                    success, errors, processed_inv_ids, label_filename = self.consolidate_location(group_data, consolidation_folder)
                    
                    # Incrementar contador de llamadas API
                    api_call_count += len(group_data)
                
                # Almacenar etiqueta si se generó
                if label_filename:
                    generated_labels.append(label_filename)
                
                # Actualizar contadores
                total_processed += len(processed_inv_ids)
                total_errors += errors
                
                # Determinar el estado según los resultados
                if not success:
                    # No hubo éxito en ningún movimiento
                    status = 'Error'
                    detalles = 'No se pudo consolidar'
                    groups_with_errors += 1
                    self.log(f"El grupo SKU={sku}, LOC={loc}, LOT={lot} terminó con estado ERROR", "ERROR")
                elif errors > 0:
                    # Hubo éxito en algunos movimientos pero también errores
                    status = 'Parcial'
                    detalles = f'{len(processed_inv_ids)} OK, {errors} errores'
                    groups_partial += 1
                    self.log(f"El grupo SKU={sku}, LOC={loc}, LOT={lot} terminó con estado PARCIAL: {len(processed_inv_ids)} éxitos, {errors} errores", "WARNING")
                else:
                    # Éxito completo sin errores
                    status = 'Completado'
                    detalles = 'Consolidación exitosa'
                    groups_success += 1
                    self.log(f"El grupo SKU={sku}, LOC={loc}, LOT={lot} terminó con estado COMPLETADO: {len(processed_inv_ids)} éxitos", "SUCCESS")
                
                self.global_groups.at[idx, 'STATUS'] = status
                self.global_groups.at[idx, 'DETALLES'] = detalles
                self.root.after(0, self.update_global_treeview)
                
                # Guardar registro de los LPNs procesados
                if len(processed_inv_ids) > 0:
                    self.log(f"Guardando registro de {len(processed_inv_ids)} movimientos en consolidación global...", "INFO")
                    self.log(f"Usuario ID: {self.user_id} - SKU: {sku}, LOC: {loc}, LOT: {lot}", "INFO")
                    self.save_movement_record(processed_inv_ids)
            
            # Actualizar resumen de consolidación
            self.log(f"\n=== Resumen de Consolidación Global ===", "INFO")
            self.log(f"Total de grupos procesados: {total_groups}", "INFO")
            self.log(f"- Grupos con estado COMPLETADO: {groups_success}", "SUCCESS")
            self.log(f"- Grupos con estado PARCIAL: {groups_partial}", "WARNING")
            self.log(f"- Grupos con estado ERROR: {groups_with_errors}", "ERROR")
            self.log(f"Total de LPNs procesados exitosamente: {total_processed}", "SUCCESS")
            if total_errors > 0:
                self.log(f"Total de errores encontrados: {total_errors}", "WARNING")
            
            # Mostrar resumen de etiquetas generadas
            if generated_labels:
                self.log(f"Se generaron {len(generated_labels)} etiquetas HTML", "SUCCESS")
                for i, label in enumerate(generated_labels[:3]):
                    self.log(f"  {i+1}. {os.path.basename(label)}", "INFO")
                if len(generated_labels) > 3:
                    self.log(f"  ... y {len(generated_labels) - 3} etiquetas más.", "INFO")
            
            # Mostrar diálogo con resumen al finalizar, incluyendo la carpeta de consolidación
            self.root.after(0, lambda: self.show_global_completion_dialog(
                total_groups, groups_success, groups_partial, 
                groups_with_errors, total_processed, generated_labels,
                consolidation_folder
            ))
            
        except Exception as e:
            self.log(f"Error en proceso de consolidación global: {str(e)}", "ERROR")
            import traceback
            self.log(traceback.format_exc(), "ERROR")
        finally:
            # Asegurar que se restaura la interfaz
            self.global_stop_requested = False
            self.global_running = False
            self.root.after(0, self.update_global_treeview)
            self.root.after(0, lambda: self.log("Proceso de consolidación global finalizado", "INFO"))
    
    def show_global_completion_dialog(self, total_groups, groups_success, groups_partial, 
                                     groups_with_errors, total_processed, generated_labels, consolidation_folder=None):
        """Muestra un diálogo con el resumen de la consolidación global"""
        # Crear ventana modal
        dialog = tk.Toplevel(self.root)
        dialog.title("Consolidación Global Completada")
        dialog.geometry("650x550")
        dialog.transient(self.root)
        dialog.grab_set()
        
        # Centrar en la pantalla
        dialog.update_idletasks()
        width = dialog.winfo_width()
        height = dialog.winfo_height()
        x = (dialog.winfo_screenwidth() // 2) - (width // 2)
        y = (dialog.winfo_screenheight() // 2) - (height // 2)
        dialog.geometry(f'+{x}+{y}')
        
        # Frame principal
        main_frame = ttk.Frame(dialog, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Título
        title_label = ttk.Label(
            main_frame,
            text="Proceso de Consolidación Global Completado",
            font=("Arial", 14, "bold")
        )
        title_label.pack(pady=(0, 20))

        # Frame para resumen
        summary_frame = ttk.LabelFrame(main_frame, text="Resumen del Proceso", padding="10")
        summary_frame.pack(fill=tk.X, pady=10)

        # Información del proceso
        ttk.Label(summary_frame, text=f"Total de grupos procesados: {total_groups}").pack(anchor=tk.W, pady=2)
        ttk.Label(summary_frame, text=f"Grupos completados exitosamente: {groups_success}",
                foreground="green").pack(anchor=tk.W, pady=2)
        if groups_partial > 0:
            ttk.Label(summary_frame, text=f"Grupos con consolidación parcial: {groups_partial}",
                    foreground="orange").pack(anchor=tk.W, pady=2)
        if groups_with_errors > 0:
            ttk.Label(summary_frame, text=f"Grupos con errores: {groups_with_errors}",
                    foreground="red").pack(anchor=tk.W, pady=2)
        ttk.Label(summary_frame, text=f"Total de LPNs consolidados: {total_processed}").pack(anchor=tk.W, pady=2)
        
        # Frame para etiquetas
        if generated_labels:
            labels_frame = ttk.LabelFrame(main_frame, text="Etiquetas Generadas", padding="10")
            labels_frame.pack(fill=tk.BOTH, expand=True, pady=10)
            
            # Lista de etiquetas generadas
            ttk.Label(labels_frame, text=f"Se generaron {len(generated_labels)} etiquetas HTML:").pack(anchor=tk.W, pady=5)
            
            # Crear lista para mostrar las etiquetas
            listbox = tk.Listbox(labels_frame, font=('Consolas', 10), height=10)
            listbox.pack(fill=tk.BOTH, expand=True, pady=5)
            
            # Agregar etiquetas a la lista
            for label in generated_labels:
                listbox.insert(tk.END, os.path.basename(label))
            
            # Agregar scroll si hay muchas etiquetas
            if len(generated_labels) > 10:
                scrollbar = ttk.Scrollbar(listbox, orient=tk.VERTICAL, command=listbox.yview)
                listbox.configure(yscrollcommand=scrollbar.set)
                scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            
            # Botones para abrir etiquetas
            buttons_frame = ttk.Frame(labels_frame)
            buttons_frame.pack(fill=tk.X, pady=5)
            
            # Función para abrir la etiqueta seleccionada
            def open_selected_label():
                selection = listbox.curselection()
                if selection:
                    selected_label = generated_labels[selection[0]]
                    self.open_html_in_browser(selected_label)
            
            ttk.Button(buttons_frame, text="Abrir Seleccionada", 
                      command=open_selected_label).pack(side=tk.LEFT, padx=5)
            
            ttk.Button(buttons_frame, text="Abrir Carpeta", 
                      command=lambda: os.startfile(self.PRINT_DIR)).pack(side=tk.LEFT, padx=5)
        else:
            ttk.Label(main_frame, text="No se generaron etiquetas durante el proceso.", 
                    foreground="orange").pack(anchor=tk.W, pady=10)
        
        # Botones de acción
        buttons_frame = ttk.Frame(main_frame)
        buttons_frame.pack(fill=tk.X, pady=10)
        
        # Botón para cerrar
        ttk.Button(buttons_frame, text="Cerrar", 
                  command=dialog.destroy).pack(side=tk.RIGHT, padx=5)
        
        # Botón de ayuda
        ttk.Label(main_frame, text="Nota: Puede acceder a las etiquetas posteriormente desde la pestaña 'Generación de Etiquetas'", 
                foreground="gray", font=("Arial", 8)).pack(pady=(10, 0))
    
    def stop_global_consolidation(self):
        """Solicita detener el proceso de consolidación global"""
        if not self.global_running:
            self.log("No hay proceso de consolidación global en ejecución", "WARNING")
            return
        
        self.global_stop_requested = True
        self.log("Solicitando detener el proceso. Espere a que termine la operación actual...", "WARNING")

    def setup_rack_tab(self):
        """Configura la pestaña de consolidación por RACK"""
        # Frame principal
        main_frame = ttk.Frame(self.rack_tab, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Panel superior
        top_frame = ttk.Frame(main_frame)
        top_frame.pack(fill=tk.X, pady=5)
        
        # Título
        ttk.Label(top_frame, text="Consolidación por RACK", 
                 font=("Arial", 14, "bold")).pack(side=tk.LEFT, padx=10)
        
        # Panel de configuración
        config_frame = ttk.LabelFrame(main_frame, text="Configuración", padding="10")
        config_frame.pack(fill=tk.X, pady=5)
        
        # Panel de filtros
        filter_frame = ttk.Frame(config_frame)
        filter_frame.pack(fill=tk.X, pady=5)
        
        # RACK Filter
        ttk.Label(filter_frame, text="RACK:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        self.rack_input = ttk.Entry(filter_frame, width=15)
        self.rack_input.grid(row=0, column=1, padx=5, pady=5, sticky=tk.W)
        
        # Botón de aplicar
        ttk.Button(filter_frame, text="Cargar Datos", 
                  command=self.load_rack_data).grid(row=0, column=2, padx=5, pady=5)
        
        # Panel de opciones avanzadas
        options_frame = ttk.LabelFrame(config_frame, text="Opciones Avanzadas", padding="10")
        options_frame.pack(fill=tk.X, pady=5)
        
        # Excluir ubicaciones
        ttk.Label(options_frame, text="Excluir ubicaciones especiales:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        self.rack_exclude_special_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(options_frame, variable=self.rack_exclude_special_var).grid(row=0, column=1, padx=5, pady=5, sticky=tk.W)
        
        # Agrupar por ubicación
        ttk.Label(options_frame, text="Consolidar por ubicación:").grid(row=1, column=0, padx=5, pady=5, sticky=tk.W)
        self.rack_group_by_loc_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(options_frame, variable=self.rack_group_by_loc_var).grid(row=1, column=1, padx=5, pady=5, sticky=tk.W)
        
        # Resultados panel
        result_frame = ttk.LabelFrame(main_frame, text="Grupos a Consolidar", padding="10")
        result_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        # Treeview para los resultados agrupados
        self.rack_tree = ttk.Treeview(
            result_frame, 
            columns=('LOC', 'SKU', 'LOT', 'NUM_LPNS', 'TOTAL_QTY', 'STATUS'),
            show='headings'
        )

        # Configurar columnas
        self.rack_tree.heading('LOC', text='Ubicación')
        self.rack_tree.heading('SKU', text='SKU')
        self.rack_tree.heading('LOT', text='Lote')
        self.rack_tree.heading('NUM_LPNS', text='LPNs')
        self.rack_tree.heading('TOTAL_QTY', text='Cantidad Total')
        self.rack_tree.heading('STATUS', text='Estado')

        self.rack_tree.column('LOC', width=120)
        self.rack_tree.column('SKU', width=120)
        self.rack_tree.column('LOT', width=100)
        self.rack_tree.column('NUM_LPNS', width=60, anchor=tk.CENTER)
        self.rack_tree.column('TOTAL_QTY', width=80, anchor=tk.CENTER)
        self.rack_tree.column('STATUS', width=80, anchor=tk.CENTER)

        # Scrollbar
        y_scroll = ttk.Scrollbar(result_frame, orient=tk.VERTICAL, command=self.rack_tree.yview)
        self.rack_tree.configure(yscrollcommand=y_scroll.set)

        # Estilos para filas
        self.rack_tree.tag_configure('pending', background='#ffffff')
        self.rack_tree.tag_configure('in_progress', background='#fff9c4')
        self.rack_tree.tag_configure('completed', background='#c8e6c9')
        self.rack_tree.tag_configure('error', background='#ffcdd2')

        self.rack_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        y_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        # Panel de detalle
        detail_frame = ttk.LabelFrame(main_frame, text="Detalle de LPNs", padding="10")
        detail_frame.pack(fill=tk.BOTH, expand=True, pady=5)

        # Treeview para los detalles de lotes
        self.rack_detail_tree = ttk.Treeview(
            detail_frame,
            columns=('LPN', 'QTY', 'STATUS'),
            show='headings'
        )

        # Configurar columnas
        self.rack_detail_tree.heading('LPN', text='LPN')
        self.rack_detail_tree.heading('QTY', text='Cantidad')
        self.rack_detail_tree.heading('STATUS', text='Estado')

        self.rack_detail_tree.column('LPN', width=150)
        self.rack_detail_tree.column('QTY', width=80, anchor=tk.CENTER)
        self.rack_detail_tree.column('STATUS', width=120)

        # Scrollbar
        detail_scroll = ttk.Scrollbar(detail_frame, orient=tk.VERTICAL, command=self.rack_detail_tree.yview)
        self.rack_detail_tree.configure(yscrollcommand=detail_scroll.set)

        self.rack_detail_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        detail_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        # Panel de acciones
        action_frame = ttk.Frame(main_frame)
        action_frame.pack(fill=tk.X, pady=10)

        # Botones de acción
        ttk.Button(action_frame, text="Iniciar Consolidación por RACK", 
                  command=self.start_rack_consolidation).pack(side=tk.LEFT, padx=5)

        ttk.Button(action_frame, text="Detener Proceso", 
                  command=self.stop_rack_consolidation).pack(side=tk.LEFT, padx=5)

        # Botón para ver los registros CSV
        ttk.Button(action_frame, text="Ver Registros CSV", 
                  command=self.open_csv_folder).pack(side=tk.LEFT, padx=20)

        # Evento para mostrar detalles al seleccionar una ubicación
        self.rack_tree.bind('<<TreeviewSelect>>', self.on_rack_location_selected)

    def load_rack_data(self):
        """Carga datos para la consolidación por RACK"""
        try:
            # Verificar que hay datos cargados
            if self.data_cache is None:
                self.log("No hay datos cargados. Carga datos primero.", "ERROR")
                messagebox.showerror("Error", "No hay datos cargados. Haga clic en 'Reload Data' primero.")
                return

            # Obtener RACK seleccionado
            rack_prefix = self.rack_input.get().strip()
            if not rack_prefix:
                self.log("Debe ingresar un prefijo de RACK", "ERROR")
                messagebox.showerror("Error", "Debe ingresar un prefijo de RACK para continuar")
                return

            # Crear una copia de los datos y extraer el prefijo de RACK
            data = self.data_cache.copy()
            
            # Extraer prefijo de RACK de la columna LOC
            data['RACK_PREFIX'] = data['LOC'].apply(self.extract_rack_prefix)
            
            # Filtrar por el prefijo de RACK especificado
            data = data[data['RACK_PREFIX'] == rack_prefix]
            
            if data.empty:
                self.log(f"No se encontraron datos para el RACK: {rack_prefix}", "ERROR")
                messagebox.showerror("Error", f"No se encontraron datos para el RACK: {rack_prefix}")
                return

            # Excluir ubicaciones especiales si es necesario
            if self.rack_exclude_special_var.get():
                original_count = len(data)
                data = data[~data['LOC'].isin(self.UBICACIONES_ESPECIALES)]
                excluded_count = original_count - len(data)
                if excluded_count > 0:
                    self.log(f"Se excluyeron {excluded_count} registros de ubicaciones especiales", "INFO")

            # Verificar si quedan datos
            if data.empty:
                self.log("No hay datos válidos después de aplicar filtros", "ERROR")
                messagebox.showerror("Error", "No hay datos válidos después de aplicar filtros")
                return

            # Agrupar por ubicación, SKU y lote
            if self.rack_group_by_loc_var.get():
                # Asegurarnos que LOT sea string para la agrupación
                data['LOT'] = data['LOT'].astype(str)
                
                # Agrupar por ubicación, SKU y lote
                groups = data.groupby(['LOC', 'SKU', 'LOT']).agg({
                    'LPN': 'nunique',  # Contar LPNs únicos por grupo
                    'QTY': 'sum'      # Sumar cantidades
                }).reset_index()

                # Renombrar columnas para claridad
                groups.columns = ['LOC', 'SKU', 'LOT', 'NUM_LPNS', 'TOTAL_QTY']

                # Filtrar para mostrar solo ubicaciones con múltiples LPNs
                groups = groups[groups['NUM_LPNS'] > 1]

                if groups.empty:
                    self.log(f"No hay grupos con múltiples LPNs en el RACK {rack_prefix}", "WARNING")
                    messagebox.showwarning("Advertencia", f"No hay grupos con múltiples LPNs en el RACK {rack_prefix}")
                    return

                # Ordenar por número de LPNs (descendente)
                groups = groups.sort_values('NUM_LPNS', ascending=False)

                # Añadir estado
                groups['STATUS'] = 'Pendiente'

                # Guardar datos
                self.rack_data = data
                self.rack_groups = groups

                # Mostrar datos en treeview
                self.update_rack_treeview()

                # Limpiar detalles
                self.rack_detail_tree.delete(*self.rack_detail_tree.get_children())

                self.log(f"Datos cargados para RACK {rack_prefix}: {len(groups)} grupos con múltiples LPNs", "SUCCESS")
            else:
                self.log("La agrupación por ubicación es obligatoria en esta versión", "ERROR")
                messagebox.showerror("Error", "La agrupación por ubicación es obligatoria en esta versión")

        except Exception as e:
            self.log(f"Error cargando datos para consolidación por RACK: {str(e)}", "ERROR")
            messagebox.showerror("Error", f"Error cargando datos: {str(e)}")
            
    def extract_rack_prefix(self, loc):
        """Extrae el prefijo de RACK de una ubicación"""
        if not loc or not isinstance(loc, str):
            return ""
        
        # Extrae el prefijo hasta el primer guión
        # Ejemplos: "203-12-2" -> "203", "234P-23-2" -> "234P"
        parts = loc.split('-')
        if parts and parts[0]:
            return parts[0]
        return ""

    def update_rack_treeview(self):
        """Actualiza el treeview de consolidación por RACK"""
        # Limpiar treeview
        self.rack_tree.delete(*self.rack_tree.get_children())

        # Verificar que hay datos
        if self.rack_groups is None:
            return

        # Configurar columnas si no están configuradas
        if 'SKU' not in self.rack_tree['columns']:
            self.rack_tree['columns'] = ('LOC', 'SKU', 'LOT', 'NUM_LPNS', 'TOTAL_QTY', 'STATUS', 'DETALLES')

            # Configurar columnas
            self.rack_tree.heading('LOC', text='Ubicación')
            self.rack_tree.heading('SKU', text='SKU')
            self.rack_tree.heading('LOT', text='Lote')
            self.rack_tree.heading('NUM_LPNS', text='LPNs')
            self.rack_tree.heading('TOTAL_QTY', text='Cantidad Total')
            self.rack_tree.heading('STATUS', text='Estado')
            self.rack_tree.heading('DETALLES', text='Detalles')

            self.rack_tree.column('LOC', width=120)
            self.rack_tree.column('SKU', width=120)
            self.rack_tree.column('LOT', width=100)
            self.rack_tree.column('NUM_LPNS', width=60, anchor=tk.CENTER)
            self.rack_tree.column('TOTAL_QTY', width=80, anchor=tk.CENTER)
            self.rack_tree.column('STATUS', width=80, anchor=tk.CENTER)
            self.rack_tree.column('DETALLES', width=120)

        # Contador para resumen de estados
        status_count = {'Pendiente': 0, 'En Proceso': 0, 'Completado': 0, 'Parcial': 0, 'Error': 0}

        # Añadir filas
        for _, row in self.rack_groups.iterrows():
            loc = row['LOC']
            sku = row['SKU']
            lot = row['LOT']
            num_lpns = row['NUM_LPNS']
            total_qty = row['TOTAL_QTY']
            status = row['STATUS']

            # Contar por estado
            status_count[status] = status_count.get(status, 0) + 1

            # Generar mensaje de detalles
            details = ""
            if status == 'Pendiente':
                details = "Esperando procesamiento"
            elif status == 'En Proceso':
                details = "Procesando..."
            elif status == 'Completado':
                details = "Consolidación exitosa"
            elif status == 'Parcial':
                details = "Algunos lotes no procesados"
            elif status == 'Error':
                details = "Error en la consolidación"

            # Determinar etiqueta según estado
            tag = 'pending'
            if status == 'En Proceso':
                tag = 'in_progress'
            elif status == 'Completado':
                tag = 'completed'
            elif status in ['Parcial', 'Error']:
                tag = 'error'

            # Insertar fila
            self.rack_tree.insert('', 'end', values=(loc, sku, lot, num_lpns, total_qty, status, details), tags=(tag,))

        # Log resumen de estados
        if sum(status_count.values()) > 0:
            self.log("Resumen de estados de consolidación por RACK:", "INFO")
            for status, count in status_count.items():
                if count > 0:
                    level = "INFO"
                    if status == "Completado":
                        level = "SUCCESS"
                    elif status == "Error":
                        level = "ERROR"
                    elif status == "Parcial":
                        level = "WARNING"
                    self.log(f"  - {status}: {count}", level)

    def on_rack_location_selected(self, event):
        """Muestra los detalles de los lotes cuando se selecciona una ubicación"""
        # Obtener selección
        selection = self.rack_tree.selection()
        if not selection:
            return

        # Obtener ubicación seleccionada
        item = self.rack_tree.item(selection[0])
        values = item['values']

        # Obtener ubicación, SKU y lote
        if len(values) >= 3:
            loc = values[0]
            sku = values[1]
            lot = values[2]
        else:
            return

        # Verificar que hay datos
        if self.rack_data is None:
            return

        # Filtrar datos para la ubicación, SKU y lote seleccionados
        loc_data = self.rack_data[
            (self.rack_data['LOC'] == loc) & 
            (self.rack_data['SKU'] == sku) & 
            (self.rack_data['LOT'].astype(str) == str(lot))
        ].copy()

        # Limpiar treeview de detalles
        self.rack_detail_tree.delete(*self.rack_detail_tree.get_children())

        # Añadir filas con detalles
        for _, row in loc_data.iterrows():
            lpn = row['LPN']
            qty = row['QTY']

            # Determinar estado
            status = "Pendiente"
            if row['INVENTORYID'] in self.processed_inventories:
                status = "Procesado"

            # Insertar fila (ya no es necesario mostrar el lote porque ya está en el encabezado)
            self.rack_detail_tree.insert('', 'end', values=(lpn, qty, status))

        self.log(f"Mostrando {len(loc_data)} LPNs en ubicación {loc}, SKU {sku}, lote {lot}", "INFO")

    def start_rack_consolidation(self):
        """Inicia el proceso de consolidación por RACK"""
        # Verificar que hay datos
        if self.rack_groups is None or self.rack_data is None:
            self.log("No hay datos para consolidar", "ERROR")
            messagebox.showerror("Error", "No hay datos para consolidar. Cargue datos primero.")
            return

        # Verificar si ya está en ejecución
        if self.rack_running:
            self.log("Ya hay un proceso de consolidación por RACK en ejecución", "WARNING")
            messagebox.showwarning("Advertencia", "Ya hay un proceso en ejecución")
            return

        # Filtrar ubicaciones pendientes
        pending_rows = self.rack_groups[self.rack_groups['STATUS'] == 'Pendiente']

        if pending_rows.empty:
            self.log("No hay ubicaciones pendientes para consolidar", "WARNING")
            messagebox.showinfo("Información", "No hay ubicaciones pendientes para consolidar")
            return

        # Crear diálogo para seleccionar ubicaciones
        selection_dialog = tk.Toplevel(self.root)
        selection_dialog.title("Seleccionar Ubicaciones a Consolidar")
        selection_dialog.geometry("700x500")  # Aumentar tamaño
        selection_dialog.transient(self.root)
        selection_dialog.grab_set()
        
        ttk.Label(selection_dialog, text="Seleccione las ubicaciones que desea consolidar:", 
                 font=("Arial", 12)).pack(pady=10)
        
        # Frame para contener el Treeview y scrollbar
        tree_frame = ttk.Frame(selection_dialog)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        # Scrollbars
        y_scroll = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL)
        y_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Treeview con checkboxes para selección
        selection_tree = ttk.Treeview(
            tree_frame,
            columns=('SELECT', 'LOC', 'NUM_LPNS', 'TOTAL_QTY'),
            show='headings',
            selectmode='none',
            yscrollcommand=y_scroll.set
        )
        
        y_scroll.config(command=selection_tree.yview)
        
        # Configurar columnas
        selection_tree.heading('SELECT', text='Seleccionar')
        selection_tree.heading('LOC', text='Ubicación')
        selection_tree.heading('NUM_LPNS', text='Lotes')
        selection_tree.heading('TOTAL_QTY', text='Cantidad Total')
        
        selection_tree.column('SELECT', width=80, anchor=tk.CENTER)
        selection_tree.column('LOC', width=120)
        selection_tree.column('NUM_LPNS', width=80, anchor=tk.CENTER)
        selection_tree.column('TOTAL_QTY', width=100, anchor=tk.CENTER)
        
        selection_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Diccionario para rastrear elementos seleccionados
        selected_items = {}
        
        # Resultado del diálogo
        dialog_result = {
            'confirmed': False,
            'selected_locations': []
        }
        
        # Añadir filas del DataFrame pendiente
        for idx, row in pending_rows.iterrows():
            item_id = selection_tree.insert(
                '', 'end', 
                values=("□", row['LOC'], row['NUM_LPNS'], row['TOTAL_QTY'])
            )
            selected_items[item_id] = False
        
        # Función para alternar selección al hacer clic
        def toggle_selection(event):
            region = selection_tree.identify_region(event.x, event.y)
            if region == "cell":
                item_id = selection_tree.identify_row(event.y)
                if item_id:
                    # Alternar estado de selección
                    selected_items[item_id] = not selected_items[item_id]
                    
                    # Actualizar el texto del checkbox visual
                    values = selection_tree.item(item_id, 'values')
                    check_symbol = "■" if selected_items[item_id] else "□"
                    selection_tree.item(item_id, values=(check_symbol,) + values[1:])
        
        # Función para seleccionar todos
        def select_all():
            for item_id in selected_items:
                selected_items[item_id] = True
                values = selection_tree.item(item_id, 'values')
                selection_tree.item(item_id, values=("■",) + values[1:])
        
        # Función para deseleccionar todos
        def deselect_all():
            for item_id in selected_items:
                selected_items[item_id] = False
                values = selection_tree.item(item_id, 'values')
                selection_tree.item(item_id, values=("□",) + values[1:])
        
        # Vincular evento de clic
        selection_tree.bind('<ButtonRelease-1>', toggle_selection)
        
        # Panel de botones de selección
        select_frame = ttk.Frame(selection_dialog)
        select_frame.pack(fill=tk.X, pady=5)
        
        ttk.Button(select_frame, text="Seleccionar Todos", command=select_all).pack(side=tk.LEFT, padx=5)
        ttk.Button(select_frame, text="Deseleccionar Todos", command=deselect_all).pack(side=tk.LEFT, padx=5)
        
        # Panel de botones de acción
        btn_frame = ttk.Frame(selection_dialog)
        btn_frame.pack(fill=tk.X, pady=10)
        
        # Funciones para confirmar/cancelar
        def confirm_selection():
            # Obtener ubicaciones seleccionadas
            for item_id, is_selected in selected_items.items():
                if is_selected:
                    values = selection_tree.item(item_id, 'values')
                    dialog_result['selected_locations'].append(values[1])  # LOC
            
            dialog_result['confirmed'] = True
            selection_dialog.destroy()
        
        def cancel_selection():
            selection_dialog.destroy()
        
        # Botones más visibles
        ttk.Button(btn_frame, text="Cancelar", command=cancel_selection).pack(side=tk.LEFT, padx=20, pady=10)
        ttk.Button(btn_frame, text="Confirmar", command=confirm_selection).pack(side=tk.RIGHT, padx=20, pady=10)
        
        # Esperar a que se cierre el diálogo
        self.root.wait_window(selection_dialog)
        
        # Verificar si se confirmó la selección
        if not dialog_result['confirmed'] or not dialog_result['selected_locations']:
            self.log("Proceso cancelado o no se seleccionaron ubicaciones", "INFO")
            return
        
        # Crear DataFrame con solo las ubicaciones seleccionadas
        selected_locs = dialog_result['selected_locations']
        self.log(f"Se seleccionaron {len(selected_locs)} ubicaciones para consolidar", "INFO")
        
        # Filtrar self.rack_groups para mantener solo las seleccionadas
        mask = self.rack_groups['LOC'].isin(selected_locs)
        selected_rack_groups = self.rack_groups[mask].copy()
        
        # Confirmar operación final
        rack_prefix = self.rack_data['RACK_PREFIX'].iloc[0]
        confirm_msg = (
            f"Se van a consolidar {len(selected_locs)} ubicaciones en el RACK {rack_prefix}.\n\n"
            f"Esto agrupará todos los lotes en cada ubicación a un único LPN de referencia.\n\n"
            "¿Está seguro de querer continuar?"
        )
        
        if not messagebox.askyesno("Confirmar Consolidación por RACK", confirm_msg):
            self.log("Proceso cancelado por el usuario", "INFO")
            return
        
        # Guardar el DataFrame filtrado temporalmente
        self.selected_rack_groups = selected_rack_groups
        
        # Iniciar proceso en un hilo separado
        self.rack_running = True
        self.rack_stop_requested = False
        
        thread = threading.Thread(target=self.rack_consolidation_thread, daemon=True)
        thread.start()  
        self.threads.append(thread)
        self.running_threads += 1

    def rack_consolidation_thread(self):
        """Hilo para la consolidación por RACK"""
        try:
            # Guardar logs antes de iniciar un proceso crítico
            if self.log_auto_save:
                self.save_log(silent=True)

            total_processed = 0
            total_errors = 0
            total_groups = 0
            groups_with_errors = 0
            groups_success = 0
            groups_partial = 0
            generated_labels = []  # Lista para almacenar etiquetas generadas
            
            # Crear carpeta específica para este proceso de consolidación
            first_rack = None
            if not self.selected_rack_groups.empty:
                first_loc = self.selected_rack_groups['LOC'].iloc[0]
                first_rack = self.extract_rack_prefix(first_loc)
            
            filters_text = None
            
            # Obtener información de filtros aplicados
            if hasattr(self, 'rack_filter_sku_var') and self.rack_filter_sku_var.get():
                filters_text = f"SKU_{self.rack_sku_combobox.get()}"
            elif hasattr(self, 'rack_filter_lot_var') and self.rack_filter_lot_var.get():
                filters_text = f"Lot_{self.rack_lot_combobox.get()}"
            
            # Crear carpeta para etiquetas de esta consolidación
            consolidation_folder = None
            try:
                if first_rack:
                    consolidation_folder = self.create_consolidation_folder(f"Rack_{first_rack}", filters_text)
                else:
                    consolidation_folder = self.create_consolidation_folder("Rack_Unknown", filters_text)
            except Exception as e:
                self.log(f"Error creando carpeta de consolidación: {str(e)}", "ERROR")
                consolidation_folder = None

            # Registrar inicio del proceso con ID de usuario
            self.log(f"Iniciando consolidación por RACK - Usuario ID: {self.user_id}", "INFO")
            if consolidation_folder:
                self.log(f"Etiquetas se guardarán en: {consolidation_folder}", "INFO")
            else:
                self.log("ADVERTENCIA: No se pudo crear carpeta para etiquetas", "WARNING")

            # Trabajo por lotes para cada grupo (LOC-SKU-LOT)
            for idx, row in self.selected_rack_groups.iterrows():
                # Verificar si se solicitó detener
                if self.rack_stop_requested:
                    self.log("Proceso detenido por el usuario", "WARNING")
                    break

                loc = row['LOC']
                sku = row['SKU']
                lot = row['LOT']

                # Si el grupo no está pendiente, lo saltamos
                if row['STATUS'] != 'Pendiente':
                    self.log(f"Saltando grupo {loc}, SKU {sku}, lote {lot}: estado ya es {row['STATUS']}", "INFO")
                    continue

                total_groups += 1
                self.log(f"\n--- Procesando grupo: {loc}, SKU {sku}, lote {lot} ---", "INFO")

                # Actualizar estado a "En Proceso"
                self.selected_rack_groups.at[idx, 'STATUS'] = 'En Proceso'
                self.root.after(0, self.update_rack_treeview)

                # Extraer datos de este grupo
                group_data = self.rack_data[
                    (self.rack_data['LOC'] == loc) & 
                    (self.rack_data['SKU'] == sku) & 
                    (self.rack_data['LOT'] == lot)
                ].copy()

                # Verificar datos
                if group_data.empty:
                    self.log(f"No hay datos para el grupo: {loc}, SKU {sku}, lote {lot}", "ERROR")
                    self.selected_rack_groups.at[idx, 'STATUS'] = 'Error'
                    groups_with_errors += 1
                    self.root.after(0, self.update_rack_treeview)
                    continue

                # Intentar consolidar este grupo
                success, errors, processed_inv_ids, label_filename = self.consolidate_location(group_data, consolidation_folder)

                # Almacenar etiqueta si se generó
                if label_filename:
                    generated_labels.append(label_filename)

                # Actualizar contadores
                total_processed += len(processed_inv_ids)
                total_errors += errors

                # Determinar el estado según los resultados
                if not success:
                    # No hubo éxito en ningún movimiento
                    status = 'Error'
                    groups_with_errors += 1
                    self.log(f"El grupo {loc}, SKU {sku}, lote {lot} terminó con estado ERROR", "ERROR")
                elif errors > 0:
                    # Hubo éxito en algunos movimientos pero también errores
                    status = 'Parcial'
                    groups_partial += 1
                    self.log(f"El grupo {loc}, SKU {sku}, lote {lot} terminó con estado PARCIAL: {len(processed_inv_ids)} éxitos, {errors} errores", "WARNING")
                else:
                    # Éxito completo sin errores
                    status = 'Completado'
                    groups_success += 1
                    self.log(f"El grupo {loc}, SKU {sku}, lote {lot} terminó con estado COMPLETADO: {len(processed_inv_ids)} éxitos", "SUCCESS")

                self.selected_rack_groups.at[idx, 'STATUS'] = status
                
                # También actualizar el status en el DataFrame original
                idx_original = self.rack_groups[
                    (self.rack_groups['LOC'] == loc) & 
                    (self.rack_groups['SKU'] == sku) & 
                    (self.rack_groups['LOT'] == lot)
                ].index
                
                if not idx_original.empty:
                    self.rack_groups.at[idx_original[0], 'STATUS'] = status
                
            # Mostrar diálogo con resumen al finalizar, incluyendo la carpeta de consolidación
            self.root.after(0, lambda: self.show_rack_completion_dialog(
                total_groups, groups_success, groups_partial, 
                groups_with_errors, total_processed, generated_labels,
                consolidation_folder
            ))
            
        except Exception as e:
            self.log(f"Error en proceso de consolidación por RACK: {str(e)}", "ERROR")
            import traceback
            self.log(traceback.format_exc(), "ERROR")
        finally:
            # Asegurar que se restaura la interfaz
            self.rack_stop_requested = False
            self.rack_running = False
            self.root.after(0, self.update_rack_treeview)
            self.root.after(0, lambda: self.log("Proceso de consolidación por RACK finalizado", "INFO"))
    
    def show_rack_completion_dialog(self, total_groups, groups_success, groups_partial,
                                   groups_with_errors, total_processed, generated_labels, consolidation_folder=None):
        """Muestra un diálogo con el resumen de la consolidación por RACK"""
        # Crear ventana modal
        dialog = tk.Toplevel(self.root)
        dialog.title("Consolidación por RACK Completada")
        dialog.geometry("650x550")
        dialog.transient(self.root)
        dialog.grab_set()
        
        # Centrar en la pantalla
        dialog.update_idletasks()
        width = dialog.winfo_width()
        height = dialog.winfo_height()
        x = (dialog.winfo_screenwidth() // 2) - (width // 2)
        y = (dialog.winfo_screenheight() // 2) - (height // 2)
        dialog.geometry(f'+{x}+{y}')
        
        # Frame principal
        main_frame = ttk.Frame(dialog, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Título
        title_label = ttk.Label(
            main_frame, 
            text="Proceso de Consolidación por RACK Completado", 
            font=("Arial", 14, "bold")
        )
        title_label.pack(pady=(0, 20))
        
        # Frame para resumen
        summary_frame = ttk.LabelFrame(main_frame, text="Resumen del Proceso", padding="10")
        summary_frame.pack(fill=tk.X, pady=10)
        
        # Información del proceso
        ttk.Label(summary_frame, text=f"Total de grupos procesados: {total_groups}").pack(anchor=tk.W, pady=2)
        ttk.Label(summary_frame, text=f"Grupos completados exitosamente: {groups_success}",
                 foreground="green").pack(anchor=tk.W, pady=2)
        if groups_partial > 0:
            ttk.Label(summary_frame, text=f"Grupos con consolidación parcial: {groups_partial}",
                    foreground="orange").pack(anchor=tk.W, pady=2)
        if groups_with_errors > 0:
            ttk.Label(summary_frame, text=f"Grupos con errores: {groups_with_errors}",
                    foreground="red").pack(anchor=tk.W, pady=2)
        ttk.Label(summary_frame, text=f"Total de LPNs consolidados: {total_processed}").pack(anchor=tk.W, pady=2)
        
        # Frame para etiquetas
        if generated_labels:
            labels_frame = ttk.LabelFrame(main_frame, text="Etiquetas Generadas", padding="10")
            labels_frame.pack(fill=tk.BOTH, expand=True, pady=10)
            
            # Lista de etiquetas generadas
            ttk.Label(labels_frame, text=f"Se generaron {len(generated_labels)} etiquetas HTML:").pack(anchor=tk.W, pady=5)
            
            # Crear lista para mostrar las etiquetas
            listbox = tk.Listbox(labels_frame, font=('Consolas', 10), height=10)
            listbox.pack(fill=tk.BOTH, expand=True, pady=5)
            
            # Agregar etiquetas a la lista
            for label in generated_labels:
                listbox.insert(tk.END, os.path.basename(label))
            
            # Agregar scroll si hay muchas etiquetas
            if len(generated_labels) > 10:
                scrollbar = ttk.Scrollbar(listbox, orient=tk.VERTICAL, command=listbox.yview)
                listbox.configure(yscrollcommand=scrollbar.set)
                scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            
            # Botones para abrir etiquetas
            buttons_frame = ttk.Frame(labels_frame)
            buttons_frame.pack(fill=tk.X, pady=5)
            
            # Función para abrir la etiqueta seleccionada
            def open_selected_label():
                selection = listbox.curselection()
                if selection:
                    selected_label = generated_labels[selection[0]]
                    self.open_html_in_browser(selected_label)
            
            ttk.Button(buttons_frame, text="Abrir Seleccionada", 
                      command=open_selected_label).pack(side=tk.LEFT, padx=5)
            
            ttk.Button(buttons_frame, text="Abrir Carpeta", 
                      command=lambda: os.startfile(self.PRINT_DIR)).pack(side=tk.LEFT, padx=5)
        else:
            ttk.Label(main_frame, text="No se generaron etiquetas durante el proceso.", 
                    foreground="orange").pack(anchor=tk.W, pady=10)
        
        # Botones de acción
        buttons_frame = ttk.Frame(main_frame)
        buttons_frame.pack(fill=tk.X, pady=10)
        
        # Botón para cerrar
        ttk.Button(buttons_frame, text="Cerrar", 
                  command=dialog.destroy).pack(side=tk.RIGHT, padx=5)
    
    def stop_rack_consolidation(self):
        """Solicita detener el proceso de consolidación por RACK"""
        if not self.rack_running:
            self.log("No hay proceso de consolidación por RACK en ejecución", "WARNING")
            return
        
        self.rack_stop_requested = True
        self.log("Solicitando detener el proceso. Espere a que termine la operación actual...", "WARNING")

    def load_filtered_labels(self):
        """Carga datos filtrados por ubicación para generación masiva de etiquetas"""
        try:
            # Verificar que hay datos cargados
            if self.data_cache is None:
                self.log("No hay datos cargados. Carga datos primero.", "ERROR")
                messagebox.showerror("Error", "No hay datos cargados. Haga clic en 'Reload Data' primero.")
                return
            
            # Obtener valores de los filtros
            pasillo = self.aisle_entry.get().strip()
            loc_desde = self.loc_from_entry.get().strip()
            loc_hasta = self.loc_to_entry.get().strip()
            nivel_desde = self.level_from_entry.get().strip()
            nivel_hasta = self.level_to_entry.get().strip()
            
            # Validar pasillo
            if not pasillo:
                self.log("Debe especificar un pasillo", "ERROR")
                messagebox.showerror("Error", "Debe especificar un pasillo")
                return
            
            # Validar rangos
            try:
                if loc_desde and loc_hasta:
                    loc_desde = int(loc_desde)
                    loc_hasta = int(loc_hasta)
                    if loc_desde > loc_hasta:
                        self.log("El rango de ubicaciones es inválido", "ERROR")
                        messagebox.showerror("Error", "El rango de ubicaciones es inválido")
                        return
                
                if nivel_desde and nivel_hasta:
                    nivel_desde = int(nivel_desde)
                    nivel_hasta = int(nivel_hasta)
                    if nivel_desde > nivel_hasta or nivel_desde < 1 or nivel_hasta > 5:
                        self.log("El rango de niveles debe estar entre 1 y 5", "ERROR")
                        messagebox.showerror("Error", "El rango de niveles debe estar entre 1 y 5")
                        return
            except ValueError:
                self.log("Los valores deben ser numéricos", "ERROR")
                messagebox.showerror("Error", "Los valores de rangos deben ser numéricos")
                return
            
            # Crear una copia de los datos
            data = self.data_cache.copy()
            
            # Filtrar solo por el pasillo exacto, sin incluir versiones con "P"
            data = data[data['LOC'].str.startswith(pasillo + '-', na=False)]
            
            if data.empty:
                self.log(f"No se encontraron datos para el pasillo: {pasillo}", "ERROR")
                messagebox.showerror("Error", f"No se encontraron datos para el pasillo: {pasillo}")
                return
            
            # Extraer número de ubicación y nivel usando regex
            # Formato esperado: "XXX-YY-Z" donde XXX es pasillo, YY es ubicación, Z es nivel
            data['UBICACION'] = data['LOC'].str.extract(r'-(\d+)-', expand=False).astype(int, errors='ignore')
            data['NIVEL'] = data['LOC'].str.extract(r'-\d+-(\d+)', expand=False).astype(int, errors='ignore')
            
            # Filtrar por rango de ubicaciones
            if loc_desde and loc_hasta:
                data = data[(data['UBICACION'] >= loc_desde) & (data['UBICACION'] <= loc_hasta)]
            
            # Filtrar por rango de niveles
            if nivel_desde and nivel_hasta:
                data = data[(data['NIVEL'] >= nivel_desde) & (data['NIVEL'] <= nivel_hasta)]
            
            if data.empty:
                self.log("No se encontraron datos con los filtros aplicados", "ERROR")
                messagebox.showerror("Error", "No se encontraron datos con los filtros aplicados")
                return
            
            # Guardar datos filtrados
            self.filtered_label_data = data
            
            # Actualizar treeview
            self.bulk_label_tree.delete(*self.bulk_label_tree.get_children())
            
            # Limitar a 1000 registros para prevenir problemas de rendimiento
            display_data = data.head(1000) if len(data) > 1000 else data
            
            for _, row in display_data.iterrows():
                values = (
                    row['LOC'],
                    row['LPN'],
                    row['SKU'],
                    row['LOT'],
                    row['QTY']
                )
                self.bulk_label_tree.insert('', 'end', values=values)
            
            # Actualizar estadísticas
            self.bulk_stats_label.config(text=f"Datos cargados: {len(data)} registros" + 
                                       (" (mostrando 1000)" if len(data) > 1000 else ""))
            
            self.log(f"Se cargaron {len(data)} registros para generación de etiquetas", "SUCCESS")
            
        except Exception as e:
            self.log(f"Error cargando datos filtrados: {str(e)}", "ERROR")
            messagebox.showerror("Error", f"Error cargando datos: {str(e)}")
    
    def generate_bulk_labels(self):
        """Genera etiquetas para todos los LPNs filtrados"""
        try:
            # Verificar que hay datos filtrados
            if not hasattr(self, 'filtered_label_data') or self.filtered_label_data is None or self.filtered_label_data.empty:
                self.log("No hay datos filtrados para generar etiquetas", "ERROR")
                messagebox.showerror("Error", "Primero debe cargar datos filtrados")
                return
            
            # Preguntar confirmación al usuario
            if not messagebox.askyesno("Confirmar generación", 
                                     f"¿Está seguro de generar {len(self.filtered_label_data)} etiquetas?\n\n" + 
                                     "Este proceso puede tomar tiempo."):
                return
            
            # Crear directorio si no existe
            if not os.path.exists(self.PRINT_DIR):
                os.makedirs(self.PRINT_DIR)
            
            # Contador de etiquetas generadas
            etiquetas_generadas = 0
            etiquetas_fallidas = 0
            
            # Importar módulos necesarios
            from barcode import Code128
            from barcode.writer import ImageWriter
            import base64
            import io
            
            def generate_barcode_base64(value):
                if not value:
                    return ""
                try:
                    # Convertir a mayúsculas para resolver el problema de minúsculas en el escaneo
                    value_str = str(value).upper()
                    if not value_str.strip() or not all(32 <= ord(c) <= 127 for c in value_str):
                        return ""
                    barcode_buffer = io.BytesIO()
                    Code128(value_str, writer=ImageWriter()).write(
                        barcode_buffer,
                        options={'write_text': False, 'module_height': 12, 'module_width': 0.4}
                    )
                    barcode_buffer.seek(0)
                    return base64.b64encode(barcode_buffer.read()).decode('utf-8')
                except Exception as e:
                    self.log(f"Error generando código de barras para '{value}': {str(e)}", "ERROR")
                    return ""
            
            # Crear una ventana de progreso
            progress_window = tk.Toplevel(self.root)
            progress_window.title("Generando Etiquetas")
            progress_window.geometry("400x150")
            progress_window.transient(self.root)
            progress_window.grab_set()
            
            # Centrar en pantalla
            progress_window.geometry("+%d+%d" % (
                self.root.winfo_rootx() + (self.root.winfo_width() / 2) - 200,
                self.root.winfo_rooty() + (self.root.winfo_height() / 2) - 75
            ))
            
            # Etiqueta para mostrar progreso
            progress_label = ttk.Label(progress_window, text="Generando etiquetas...")
            progress_label.pack(pady=10)
            
            # Barra de progreso
            progress_bar = ttk.Progressbar(progress_window, orient=tk.HORIZONTAL, length=300, mode='determinate')
            progress_bar.pack(pady=10, padx=20)
            
            # Etiqueta para contador
            counter_label = ttk.Label(progress_window, text="0 / " + str(len(self.filtered_label_data)))
            counter_label.pack(pady=5)
            
            # Configurar máximo de la barra de progreso
            progress_bar['maximum'] = len(self.filtered_label_data)
            
            # Lista de etiquetas generadas
            etiquetas_paths = []
            
            # Procesar cada fila
            for i, (_, row) in enumerate(self.filtered_label_data.iterrows()):
                try:
                    # Actualizar UI
                    progress_bar['value'] = i + 1
                    counter_label.config(text=f"{i + 1} / {len(self.filtered_label_data)}")
                    progress_window.update()
                    
                    # Obtener valores
                    sku = row['SKU']
                    loc = row['LOC']
                    lot = self.normalize_id(str(row['LOT']), 10)  # Normalizar LOT a 10 dígitos
                    lpn = self.normalize_id(str(row['LPN']), 10)  # Normalizar LPN a 10 dígitos
                    
                    # Generar códigos de barras
                    sku_barcode = generate_barcode_base64(sku)
                    loc_barcode = generate_barcode_base64(loc)
                    lot_barcode = generate_barcode_base64(lot)
                    lpn_barcode = generate_barcode_base64(lpn)
                    
                    if not all([sku_barcode, loc_barcode, lot_barcode, lpn_barcode]):
                        self.log(f"Error generando códigos de barras para LPN {lpn}", "ERROR")
                        etiquetas_fallidas += 1
                        continue
                    
                    # Crear valores de visualización
                    lot_display = str(int(lot)) if lot.strip().isdigit() else lot
                    lpn_display = str(int(lpn)) if lpn.strip().isdigit() else lpn
                    
                    # Generar HTML
                    html_content = self.create_label_html(sku, sku_barcode, loc, loc_barcode,
                                                      lot, lot_barcode, lpn, lpn_barcode)
                    
                    # Guardar archivo HTML
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    safe_loc = loc.replace("-", "_").replace("/", "_")
                    filename = os.path.join(self.PRINT_DIR, f"{sku}_{lot}_{lpn}_{safe_loc}_{timestamp}.html")
                    
                    with open(filename, "w", encoding="utf-8") as f:
                        f.write(html_content)
                    
                    etiquetas_paths.append(filename)
                    etiquetas_generadas += 1
                    
                except Exception as e:
                    self.log(f"Error generando etiqueta para LPN {row['LPN']}: {str(e)}", "ERROR")
                    etiquetas_fallidas += 1
            
            # Cerrar ventana de progreso
            progress_window.destroy()
            
            # Actualizar la lista de etiquetas
            self.update_label_list()
            
            # Mostrar resultado
            resultado = (
                f"Proceso completado:\n\n"
                f"- Etiquetas generadas: {etiquetas_generadas}\n"
                f"- Etiquetas fallidas: {etiquetas_fallidas}\n\n"
            )
            
            if etiquetas_generadas > 0:
                if messagebox.askyesno("Proceso Completado", 
                                     resultado + "¿Desea abrir la carpeta de etiquetas?"):
                    self.open_csv_folder()
            else:
                messagebox.showinfo("Proceso Completado", resultado)
            
            self.log(f"Generación masiva completada. Generadas: {etiquetas_generadas}, Fallidas: {etiquetas_fallidas}", "SUCCESS")
            
            # FLUJO COMPLETO - CONVERSIÓN E IMPRESIÓN
            if etiquetas_generadas > 0:
                try:
                    import conversion_utils
                    # Activar multithreading completo con 12 workers para máximo rendimiento
                    conversion_utils.flujo_completo_etiquetas(
                        self.root, etiquetas_paths, self.PRINT_DIR, self.log, max_workers=12
                    )
                except Exception as e:
                    self.log(f'Error en flujo completo: {str(e)}', 'ERROR')
            
            
        except ImportError:
            self.log("Error: Módulo 'barcode' no instalado. Instale con 'pip install python-barcode pillow'", "ERROR")
            messagebox.showerror("Error", "Módulo 'barcode' no instalado. Instale con 'pip install python-barcode pillow'")
        except Exception as e:
            self.log(f"Error generando etiquetas masivas: {str(e)}", "ERROR")
            messagebox.showerror("Error", f"Error generando etiquetas masivas: {str(e)}")

    def eliminar_html_duplicados(self):
        """Elimina archivos HTML duplicados de la carpeta de impresión"""
        try:
            # Verificar que la carpeta de impresión existe
            if not os.path.exists(self.PRINT_DIR):
                self.log("Carpeta de impresión no encontrada", "ERROR")
                return
                
            # Obtener lista de archivos HTML
            archivos_html = [f for f in os.listdir(self.PRINT_DIR) if f.endswith('.html')]
            
            if not archivos_html:
                self.log("No se encontraron archivos HTML", "WARNING")
                return
            
            self.log(f"Analizando {len(archivos_html)} archivos HTML...", "INFO")
            
            # Mapear archivos por su contenido para detectar duplicados
            contenido_a_archivos = {}
            eliminados = 0
            
            for archivo in archivos_html:
                ruta_completa = os.path.join(self.PRINT_DIR, archivo)
                with open(ruta_completa, 'r', encoding='utf-8') as f:
                    contenido = f.read()
                
                # Si este contenido ya existe, es un duplicado
                if contenido in contenido_a_archivos:
                    try:
                        os.remove(ruta_completa)
                        eliminados += 1
                        self.log(f"Eliminado duplicado: {archivo}", "SUCCESS")
                    except Exception as e:
                        self.log(f"Error eliminando {archivo}: {str(e)}", "ERROR")
                else:
                    # Si es un archivo nuevo, agregar al diccionario
                    contenido_a_archivos[contenido] = archivo
            
            # Mostrar resumen
            if eliminados > 0:
                self.log(f"Proceso completado: {eliminados} archivos duplicados eliminados", "SUCCESS")
            else:
                self.log("No se encontraron archivos duplicados", "INFO")
                
        except Exception as e:
            self.log(f"Error al procesar archivos HTML: {str(e)}", "ERROR")

    def create_consolidation_folder(self, first_location, filters=None):
        """Crea una carpeta específica para el proceso de consolidación masiva actual"""
        try:
            # Obtener el pasillo (primera parte de la ubicación)
            if first_location:
                location_parts = first_location.split('-')
                aisle = location_parts[0] if len(location_parts) > 0 else "Unknown"
            else:
                aisle = "Unknown"
            
            # Crear nombre de carpeta con timestamp para evitar sobrescritura
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Incluir información del usuario si está disponible
            user_info = ""
            if hasattr(self, 'user_id') and self.user_id:
                user_info = f"_{self.user_id}"
            
            folder_name = f"Consolidacion_{aisle}{user_info}_{timestamp}"
            
            # Añadir filtros al nombre de la carpeta si existen
            if filters:
                # Limpiar caracteres no válidos para nombres de carpeta
                cleaned_filters = re.sub(r'[<>:"/\\|?*]', '_', filters)
                folder_name += f"_{cleaned_filters}"
            
            # Usar rutas absolutas para evitar problemas
            # Intentar dos rutas alternativas para mayor compatibilidad
            base_path_1 = r"C:\Users\bryan.marcial\OneDrive - GXO\Documents\PYTHON\Code\Local\Teo PutAway\Juntar LPN\print"
            base_path_2 = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Juntar LPN", "print")
            
            # Verificar cuál existe y usarla
            if os.path.exists(base_path_1):
                base_path = base_path_1
            else:
                base_path = base_path_2
                
            folder_path = os.path.join(base_path, folder_name)
            
            # Imprimir información de depuración
            self.log(f"Intentando crear carpeta: {folder_path}", "INFO")
            self.log(f"Directorio base: {base_path}", "INFO")
            self.log(f"¿Directorio base existe? {os.path.exists(base_path)}", "INFO")
            
            # Asegurar que el directorio base existe
            if not os.path.exists(base_path):
                self.log(f"Creando directorio base: {base_path}", "WARNING")
                os.makedirs(base_path, exist_ok=True)
            
            # Crear la carpeta usando try/except explícito
            try:
                if not os.path.exists(folder_path):
                    os.makedirs(folder_path, exist_ok=True)
                    self.log(f"Carpeta para consolidación creada: {folder_path}", "SUCCESS")
                
                # Verificar que la carpeta se creó correctamente
                if os.path.exists(folder_path):
                    self.log(f"Verificación: carpeta existe correctamente", "SUCCESS")
                else:
                    self.log(f"ERROR: La carpeta no se creó correctamente", "ERROR")
            except Exception as folder_error:
                self.log(f"Error específico creando carpeta: {str(folder_error)}", "ERROR")
                import traceback
                self.log(traceback.format_exc(), "ERROR")
                folder_path = base_path  # Usar la carpeta base como fallback
                
            return folder_path
        except Exception as e:
            self.log(f"Error general creando carpeta para consolidación: {str(e)}", "ERROR")
            import traceback
            self.log(traceback.format_exc(), "ERROR")
            return self.PRINT_DIR  # En caso de error, usar la carpeta predeterminada

# Definición de la clase LoginDialog
class LoginDialog:
    def __init__(self, parent, names_csv_path):
        self.result = None
        self.user_name = ""
        self.user_short_name = ""
        
        # Crear ventana de diálogo
        self.dialog = tk.Toplevel(parent)
        self.dialog.title("Iniciar Sesión")
        self.dialog.geometry("400x300")
        self.dialog.transient(parent)
        self.dialog.grab_set()
        
        # Centrar en la pantalla
        self.dialog.update_idletasks()
        width = self.dialog.winfo_width()
        height = self.dialog.winfo_height()
        x = (self.dialog.winfo_screenwidth() // 2) - (width // 2)
        y = (self.dialog.winfo_screenheight() // 2) - (height // 2)
        self.dialog.geometry(f'+{x}+{y}')
        
        # Frame principal
        main_frame = ttk.Frame(self.dialog, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Título
        title_label = ttk.Label(
            main_frame, 
            text="Iniciar Sesión", 
            font=("Arial", 16, "bold")
        )
        title_label.pack(pady=(0, 20))
        
        # Cargar datos de usuarios
        self.users_data = self.load_users_data(names_csv_path)
        
        # Campo de ID de usuario
        user_frame = ttk.Frame(main_frame)
        user_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(user_frame, text="ID Usuario:").pack(side=tk.LEFT, padx=5)
        self.user_id_var = tk.StringVar()
        self.user_combo = ttk.Combobox(
            user_frame, 
            textvariable=self.user_id_var,
            values=list(self.users_data.keys())
        )
        self.user_combo.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
        self.user_combo.bind("<<ComboboxSelected>>", self.on_user_selected)
        
        # Campo de nombre (solo lectura)
        name_frame = ttk.Frame(main_frame)
        name_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(name_frame, text="Nombre:").pack(side=tk.LEFT, padx=5)
        self.name_var = tk.StringVar()
        name_entry = ttk.Entry(name_frame, textvariable=self.name_var, state="readonly")
        name_entry.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
        
        # Botones
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=20)
        
        ttk.Button(
            button_frame, 
            text="Cancelar", 
            command=self.cancel
        ).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(
            button_frame, 
            text="Confirmar", 
            command=self.confirm
        ).pack(side=tk.RIGHT, padx=5)
        
        # Hacer que el diálogo se cierre al presionar Escape
        self.dialog.bind("<Escape>", lambda e: self.cancel())
        
        # Esperar hasta que el diálogo se cierre
        parent.wait_window(self.dialog)
    
    def load_users_data(self, names_csv_path):
        """Carga los datos de usuarios desde el archivo CSV"""
        users = {}
        
        try:
            if os.path.exists(names_csv_path):
                with open(names_csv_path, "r", encoding="utf-8-sig") as file:
                    for line in file:
                        parts = line.strip().split(',')
                        if len(parts) >= 3:
                            user_id = parts[0].strip()
                            full_name = parts[1].strip()
                            short_name = parts[2].strip() if len(parts) > 2 else ""
                            users[user_id] = {"name": full_name, "short_name": short_name}
            else:
                messagebox.showwarning(
                    "Advertencia", 
                    f"No se encontró el archivo de usuarios: {names_csv_path}\n"
                    "Se utilizará un modo sin autenticación."
                )
        except Exception as e:
            messagebox.showerror(
                "Error", 
                f"Error al cargar datos de usuarios: {str(e)}"
            )
        
        # Si no hay usuarios, agregar uno por defecto
        if not users:
            users["ADMIN"] = {"name": "Administrador", "short_name": "Admin"}
            
        return users
    
    def on_user_selected(self, event=None):
        """Actualiza el campo de nombre al seleccionar un usuario"""
        user_id = self.user_id_var.get()
        if user_id in self.users_data:
            self.name_var.set(self.users_data[user_id]["name"])
        else:
            self.name_var.set("")
    
    def confirm(self):
        """Confirma la selección de usuario"""
        user_id = self.user_id_var.get()
        
        if not user_id:
            messagebox.showerror("Error", "Por favor seleccione un usuario")
            return
        
        if user_id not in self.users_data:
            messagebox.showerror("Error", "Usuario no válido")
            return
        
        self.result = user_id
        self.user_name = self.users_data[user_id]["name"]
        self.user_short_name = self.users_data[user_id]["short_name"]
        self.dialog.destroy()
    
    def cancel(self):
        """Cancela la selección de usuario"""
        self.result = None
        self.dialog.destroy()

# Si se ejecuta directamente este archivo
if __name__ == "__main__":
    root = tk.Tk()
    app = OptimizedInventoryApp(root)
    root.mainloop()

