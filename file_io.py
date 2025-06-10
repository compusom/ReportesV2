# report_generator_project/file_io.py
import pandas as pd
import os
import re
import csv
import sys
from datetime import date # Solo para el rango razonable de fechas
import traceback # Para debugging

from utils import normalize, create_flexible_regex_pattern # De utils.py local

# Intenta importar date_parse de dateutil, pero no falles si no está
try:
    from dateutil.parser import parse as date_parse
except ImportError:
    date_parse = None

# ============================================================
# FUNCIONES DE LECTURA Y DETECCIÓN EN ARCHIVOS
# ============================================================
def find_date_column_name(file_path):
    try:
        df_peek=None; ext=os.path.splitext(file_path)[1].lower()
        if ext in ['.xlsx','.xls']:
            engine = None
            try:
                if 'openpyxl' in sys.modules:
                    engine = 'openpyxl'
                    pd.read_excel(file_path, engine=engine, nrows=0)
            except Exception: engine = None
            try:
                df_peek=pd.read_excel(file_path,engine=engine,dtype=str,nrows=0)
            except Exception as e_peek_excel:
                print(f"  Adv: Peek Excel header falló ({e_peek_excel}), reintentando con skiprows=[1]")
                try:
                     df_peek=pd.read_excel(file_path,engine=engine,dtype=str,nrows=0, skiprows=[1])
                except Exception as e_peek_excel_skip:
                     print(f"  Adv: Peek Excel header con skiprows=[1] también falló ({e_peek_excel_skip})")
                     return None
        elif ext=='.csv':
            sep_f=None; enc_f=None; encs=['utf-8-sig','latin-1','cp1252']; lines_f=None
            for enc in encs:
                try:
                    with open(file_path,'r',encoding=enc, errors='ignore') as f: lines_f=[f.readline() for _ in range(5)]
                    if lines_f and any(ln.strip() for ln in lines_f): enc_f=enc; break
                except Exception: continue
            if not lines_f or not any(ln.strip() for ln in lines_f): return None
            sniffer=csv.Sniffer(); hdr=next((ln for ln in lines_f if ln.strip()),None)
            if not hdr: return None
            try: dialect=sniffer.sniff(hdr, delimiters=',;\t|'); sep_f=dialect.delimiter
            except csv.Error: common=[',',';','\t','|']; sep_f=',' # Default to comma if sniffer fails
            counts = {s: hdr.count(s) for s in common}
            if any(c > 0 for c in counts.values()): # If any common delimiter is found, use the most frequent
                sep_f = max(counts, key=counts.get)

            try:
                df_peek=pd.read_csv(file_path,dtype=str,sep=sep_f,engine='python',encoding=enc_f,nrows=0)
            except pd.errors.ParserError:
                 print(f"  Adv: Peek CSV header falló, reintentando con skiprows=[1]")
                 try:
                     df_peek=pd.read_csv(file_path,dtype=str,sep=sep_f,engine='python',encoding=enc_f,nrows=0,skiprows=[1])
                 except Exception as e_peek_csv_skip:
                      print(f"  Adv: Peek CSV header con skiprows=[1] también falló ({e_peek_csv_skip})")
                      return None
            except Exception as e_peek_csv:
                 print(f"  Adv: Error inesperado al leer header CSV: {e_peek_csv}")
                 return None

        if df_peek is None: return None
        norm_cols={c:normalize(c) for c in df_peek.columns}; orig_map={v:k for k,v in norm_cols.items()}
        keys=[normalize('Día'),normalize('Date'),normalize('Fecha')]
        for k in keys:
            orig=orig_map.get(k);
            if orig and orig in df_peek.columns: return orig
        for k in keys:
            pat=create_flexible_regex_pattern(k)
            for orig,norm_val in norm_cols.items():
                if re.fullmatch(pat,norm_val,re.IGNORECASE) and orig in df_peek.columns: return orig
        return None
    except Exception as e: print(f"Adv: Error general detectando col fecha ({os.path.basename(file_path)}): {e}"); return None

def get_dates_from_file(file_path, date_column_name):
    try:
        dates_series = None
        ext = os.path.splitext(file_path)[1].lower()
        skip_rows = [1] 

        if ext in ['.xlsx', '.xls']:
            engine = None
            try:
                if 'openpyxl' in sys.modules: engine = 'openpyxl'
            except Exception: engine = None
            try:
                dates_series = pd.read_excel(file_path, engine=engine, usecols=[date_column_name], skiprows=skip_rows, dtype=str)[date_column_name]
            except (KeyError, IndexError, ValueError, pd.errors.EmptyDataError) as e:
                 print(f"Adv: Leyendo fechas Excel con skiprows={skip_rows} falló ({e}) en {os.path.basename(file_path)}. Reintentando sin skiprows...");
                 try:
                     dates_series = pd.read_excel(file_path, engine=engine, usecols=[date_column_name], dtype=str)[date_column_name]
                 except Exception as e_noskip:
                      print(f"Adv: Leer fechas Excel sin skiprows también falló ({e_noskip}).")
                      return pd.Series(dtype='datetime64[ns]')

        elif ext == '.csv':
            sep = None; enc = None; encodings = ['utf-8-sig', 'latin-1', 'cp1252']
            lines = None
            for e_enc in encodings:
                try:
                    with open(file_path, 'r', encoding=e_enc, errors='ignore') as f:
                        lines = [f.readline() for _ in range(10)]
                    if lines and any(ln.strip() for ln in lines):
                        enc = e_enc
                        break
                except Exception: continue

            if not lines or not any(ln.strip() for ln in lines):
                 print(f"Adv: CSV vacío o ilegible para detectar separador/encoding: {os.path.basename(file_path)}")
                 return pd.Series(dtype='datetime64[ns]')

            sniffer = csv.Sniffer()
            content_line_for_sniffer = next((ln for ln in lines if ln.strip()), None)

            if not content_line_for_sniffer:
                 print(f"Adv: No se encontró línea con contenido en CSV para sniffer: {os.path.basename(file_path)}")
                 return pd.Series(dtype='datetime64[ns]')
            
            try:
                dialect = sniffer.sniff(content_line_for_sniffer, delimiters=',;\t|')
                sep = dialect.delimiter
            except csv.Error:
                common_delimiters = [',', ';', '\t', '|']
                counts = {s: content_line_for_sniffer.count(s) for s in common_delimiters}
                sep = max(counts, key=counts.get) if any(c > 0 for c in counts.values()) else ','
                print(f"Adv: Sniffer CSV falló en {os.path.basename(file_path)}, usando separador estimado: '{sep}'")

            try:
                dates_series = pd.read_csv(file_path, usecols=[date_column_name], sep=sep, engine='python', encoding=enc, on_bad_lines='skip', dtype=str, skiprows=skip_rows)[date_column_name]
            except (KeyError, IndexError, ValueError, pd.errors.EmptyDataError, pd.errors.ParserError) as e:
                 print(f"Adv: Leyendo fechas CSV con skiprows={skip_rows} falló ({e}) en {os.path.basename(file_path)}. Reintentando sin skiprows...");
                 try:
                     dates_series = pd.read_csv(file_path, usecols=[date_column_name], sep=sep, engine='python', encoding=enc, on_bad_lines='skip', dtype=str)[date_column_name]
                 except Exception as e_noskip_csv:
                     print(f"Adv: Leer fechas CSV sin skiprows también falló ({e_noskip_csv}).")
                     return pd.Series(dtype='datetime64[ns]')

        if dates_series is not None:
            dates_series = dates_series[~dates_series.astype(str).str.lower().isin(['día', 'date', 'fecha', date_column_name.lower()])]
            
            parsed_dates = pd.to_datetime(dates_series, format="%Y-%m-%d", errors='coerce')
            
            if parsed_dates.isnull().all() and not dates_series.isnull().all():
                parsed_dates = pd.to_datetime(dates_series, dayfirst=True, errors='coerce')

            if parsed_dates.isnull().all() and not dates_series.isnull().all():
                 parsed_dates = pd.to_datetime(dates_series, dayfirst=False, errors='coerce')

            if parsed_dates.isnull().all() and not dates_series.isnull().all():
                common_formats = ["%m/%d/%Y", "%d-%m-%Y", "%Y/%m/%d", "%d %b %Y", "%d-%b-%Y"]
                for fmt_date in common_formats:
                    parsed_dates_fmt = pd.to_datetime(dates_series, format=fmt_date, errors='coerce')
                    if not parsed_dates_fmt.isnull().all():
                        parsed_dates = parsed_dates_fmt
                        print(f"  Info: Fechas parseadas con formato '{fmt_date}' para {os.path.basename(file_path)}")
                        break
            
            if parsed_dates.isnull().all() and not dates_series.isnull().all() and date_parse is not None:
                 try:
                      parsed_dates_list = []
                      for date_str in dates_series:
                          if pd.isna(date_str):
                              parsed_dates_list.append(pd.NaT)
                              continue
                          try:
                              parsed_dates_list.append(date_parse(str(date_str), dayfirst=True))
                          except (ValueError, TypeError):
                              try:
                                  parsed_dates_list.append(date_parse(str(date_str), dayfirst=False))
                              except (ValueError, TypeError):
                                  parsed_dates_list.append(pd.NaT)
                      
                      temp_parsed_dates = pd.Series(parsed_dates_list, dtype='datetime64[ns]')
                      if not temp_parsed_dates.isnull().all():
                          parsed_dates = temp_parsed_dates
                          print(f"  Info: Fechas parseadas con dateutil.parser para {os.path.basename(file_path)}")
                 except Exception as e_infer:
                      print(f"Adv: dateutil.parser falló ({e_infer}) para {os.path.basename(file_path)}")
            
            if parsed_dates.notnull().any():
                min_year = date.today().year - 10
                max_year = date.today().year + 2
                original_parsed_count = parsed_dates.notnull().sum()
                parsed_dates = parsed_dates.apply(
                    lambda x: x if pd.NaT or (min_year <= x.year <= max_year) else pd.NaT
                )
                filtered_count = parsed_dates.notnull().sum()
                if original_parsed_count > 0 and filtered_count < original_parsed_count:
                    print(f"  Adv: {original_parsed_count - filtered_count} fechas filtradas por estar fuera del rango ({min_year}-{max_year}) en {os.path.basename(file_path)}.")
                if parsed_dates.isnull().all() and not dates_series.isnull().all() and original_parsed_count > 0:
                    print(f"  Adv: Todas las fechas parseadas para {os.path.basename(file_path)} están fuera del rango esperado ({min_year}-{max_year}).")

            return parsed_dates
        return pd.Series(dtype='datetime64[ns]')
    except Exception as e:
        print(f"Adv: Error general leyendo fechas {os.path.basename(file_path)} col '{date_column_name}': {e}")
        traceback.print_exc()
        return pd.Series(dtype='datetime64[ns]')