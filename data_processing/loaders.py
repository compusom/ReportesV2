# report_generator_project/data_processing/loaders.py
import pandas as pd
import os
import re
import csv
import sys
import traceback
import numpy as np
from config import norm_map, numeric_internal_cols, CURRENCY_SYMBOLS, DEFAULT_CURRENCY_SYMBOL # Importar de config
from utils import normalize, create_flexible_regex_pattern, robust_numeric_conversion
from file_io import find_date_column_name, get_dates_from_file

# ============================================================
# CARGA Y PREPARACIÓN DE DATOS
# ============================================================
def _cargar_y_preparar_datos(input_files, status_queue, selected_campaign):
    log_and_update = lambda msg: status_queue.put(msg)
    detected_currency_symbol = DEFAULT_CURRENCY_SYMBOL 
    first_currency_detected = False
    all_dataframes = []
    all_campaign_adsets = set()
    log_and_update("\n--- Fase 1: Leyendo y Estandarizando Archivos ---")

    for file_path in input_files:
        base_filename = os.path.basename(file_path)
        log_and_update(f"\n=== Procesando archivo: {base_filename} ===")
        try:
            file_extension = os.path.splitext(file_path)[1].lower()
            df_raw = None
            read_successful = False
            skip_rows_excel = [1]
            skip_rows_csv = [1]

            if file_extension in ['.xls', '.xlsx']:
                engine = None
                try:
                    if 'openpyxl' in sys.modules: engine = 'openpyxl'
                except Exception: engine = None 
                try:
                    df_raw = pd.read_excel(file_path, engine=engine, dtype=str, skiprows=skip_rows_excel)
                    if df_raw.empty or df_raw.shape[1] < 3 or len(df_raw) < 1: raise ValueError("Potentially incorrect read with skiprows")
                    read_successful = True
                except Exception as read_error:
                    log_and_update(f"   ! Leer Excel (con skiprows={skip_rows_excel}) falló: {read_error}. Reintentando sin skiprows...");
                    try:
                        df_raw = pd.read_excel(file_path, engine=engine, dtype=str)
                        if df_raw.empty: log_and_update("   !!! Error leer Excel (sin skiprows): DataFrame vacío. Saltando archivo."); continue
                        read_successful = True
                    except Exception as read_error_no_skip:
                        log_and_update(f"   !!! Error leer Excel (sin skiprows): {read_error_no_skip}. Saltando archivo."); continue
            elif file_extension == '.csv':
                sep_f=None; final_encoding_csv=None; encodings_to_try=['utf-8-sig','latin-1','cp1252']; first_lines_csv=None
                for enc in encodings_to_try:
                    try:
                        with open(file_path,'r',encoding=enc, errors='ignore') as f_csv: first_lines_csv=[f_csv.readline() for _ in range(10)]
                        if first_lines_csv and any(ln.strip() for ln in first_lines_csv): final_encoding_csv=enc; break
                    except Exception: continue
                if not first_lines_csv or not any(ln.strip() for ln in first_lines_csv): log_and_update("   !!! Error: CSV vacío/ilegible. Saltando."); continue
                sniffer=csv.Sniffer(); header_line_csv=next((ln for ln in first_lines_csv if ln.strip()), None)
                if not header_line_csv: log_and_update("   !!! Error: No se encontró header en CSV. Saltando."); continue
                try:
                    dialect=sniffer.sniff(header_line_csv, delimiters=',;\t|'); sep_f=dialect.delimiter
                except csv.Error:
                    common=[',',';','\t','|']; counts = {s: header_line_csv.count(s) for s in common}; sep_f = max(counts, key=counts.get) if any(c>0 for c in counts.values()) else ','
                try:
                    df_raw=pd.read_csv(file_path,dtype=str,sep=sep_f,engine='python',encoding=final_encoding_csv,on_bad_lines='warn',skiprows=skip_rows_csv)
                    if df_raw.empty or df_raw.shape[1] < 3 or len(df_raw) < 1: raise ValueError("Potentially incorrect read with skiprows")
                    read_successful=True
                except Exception as read_error:
                    log_and_update(f"   ! Leer CSV (con skiprows={skip_rows_csv}) falló: {read_error}. Reintentando sin skiprows...")
                    try:
                        df_raw=pd.read_csv(file_path,dtype=str,sep=sep_f,engine='python',encoding=final_encoding_csv,on_bad_lines='warn')
                        if df_raw.empty: log_and_update("   !!! Error leer CSV (sin skiprows): DataFrame vacío. Saltando archivo."); continue
                        read_successful=True
                    except Exception as read_error_no_skip:
                        log_and_update(f"   !!! Error leer CSV (sin skiprows): {read_error_no_skip}. Saltando archivo."); continue
            else: log_and_update(f"   Formato no soportado: {file_extension}. Saltando."); continue

            if not read_successful or df_raw is None or df_raw.empty: log_and_update(f"   No se leyeron datos válidos. Saltando."); continue

            if not df_raw.empty: 
                original_row_count = len(df_raw)
                nan_threshold = int(df_raw.shape[1] * 0.8) 
                df_raw = df_raw.loc[df_raw.isnull().sum(axis=1) < nan_threshold].copy()
                if len(df_raw) < original_row_count:
                    log_and_update(f"   Filas eliminadas por exceso de NaNs (posible resumen): {original_row_count - len(df_raw)}")
            if df_raw.empty: log_and_update(f"   No quedaron datos válidos después de quitar filas vacías/resumen. Saltando."); continue

            log_and_update(f"   Lectura OK ({len(df_raw)} filas). Mapeando...")
            original_columns=df_raw.columns.tolist(); file_cols_normalized={c:normalize(c) for c in df_raw.columns}; normalized_to_original={v:k for k,v in file_cols_normalized.items()}
            rename_mapping={}; found_internal_names=set()
            date_col_original_name = find_date_column_name(file_path)

            if date_col_original_name:
                rename_mapping[date_col_original_name]='date'; found_internal_names.add('date'); log_and_update(f"     -> Col Fecha: '{date_col_original_name}'")
            else: log_and_update(f"   !!! Error Crítico: Col fecha no encontrada en '{base_filename}'. Saltando archivo."); continue

            for internal_name,normalized_keys_list in norm_map.items():
                if internal_name in found_internal_names: continue
                found_original_col_name=None
                for norm_key in normalized_keys_list: 
                    original_col=normalized_to_original.get(norm_key)
                    if original_col and original_col in df_raw.columns and original_col not in rename_mapping:
                        found_original_col_name=original_col; break
                if not found_original_col_name: 
                    for norm_key in normalized_keys_list:
                        pattern=create_flexible_regex_pattern(norm_key)
                        for original_col_val,normalized_col_val in file_cols_normalized.items():
                            if re.fullmatch(pattern,normalized_col_val,re.IGNORECASE) and original_col_val in df_raw.columns and original_col_val not in rename_mapping:
                                found_original_col_name=original_col_val; break
                        if found_original_col_name: break
                
                if found_original_col_name:
                    rename_mapping[found_original_col_name]=internal_name; found_internal_names.add(internal_name)
                    if internal_name == 'spend': 
                        match_curr = re.search(r'\((.*?)\)', found_original_col_name) 
                        if match_curr:
                            code_curr = match_curr.group(1).strip().upper()
                            symbol_from_code = CURRENCY_SYMBOLS.get(code_curr)
                            
                            if symbol_from_code: 
                                current_file_symbol = symbol_from_code
                            elif code_curr in CURRENCY_SYMBOLS.values(): 
                                current_file_symbol = code_curr
                            elif len(code_curr) == 1 and not code_curr.isalnum(): 
                                current_file_symbol = code_curr
                            else:
                                current_file_symbol = None

                            if current_file_symbol:
                                if not first_currency_detected:
                                    detected_currency_symbol = current_file_symbol
                                    first_currency_detected = True
                                    log_and_update(f"     Moneda global establecida a '{detected_currency_symbol}' (desde '{code_curr}' en '{found_original_col_name}')")
                                elif detected_currency_symbol != current_file_symbol:
                                    log_and_update(f"     !!! ADVERTENCIA Moneda: '{current_file_symbol}' (desde '{code_curr}' en '{found_original_col_name}') diferente a la global '{detected_currency_symbol}'. Se usará la global '{detected_currency_symbol}'.")

            if 'entrega' not in found_internal_names: 
                delivery_fallbacks=['ad_delivery_status','adset_delivery_status','campaign_delivery_status']
                for fb_col in delivery_fallbacks:
                    if fb_col in found_internal_names:
                         original_fb_col_name = next((k for k,v in rename_mapping.items() if v == fb_col), None)
                         if original_fb_col_name:
                             log_and_update(f"       -> Usando '{original_fb_col_name}' (mapeado como '{fb_col}') como fallback para 'entrega'")
                             rename_mapping[original_fb_col_name] = 'entrega'
                             found_internal_names.discard(fb_col) 
                             found_internal_names.add('entrega')
                             break
            
            cols_to_select=list(rename_mapping.keys())
            if not cols_to_select or 'date' not in rename_mapping.values():
                 log_and_update("   !!! Error: No se mapearon columnas suficientes (¿falta fecha?). Saltando archivo."); continue
            df_renamed=df_raw[cols_to_select].copy(); df_renamed.rename(columns=rename_mapping,inplace=True);

            df_renamed['date'] = get_dates_from_file(file_path, date_col_original_name)
            orig_rows_date_processing = len(df_renamed)
            df_renamed.dropna(subset=['date'], inplace=True)
            if len(df_renamed) < orig_rows_date_processing:
                log_and_update(f"     Filas eliminadas por fecha inválida/no parseable: {orig_rows_date_processing - len(df_renamed)}")
            if df_renamed.empty: log_and_update("   !!! No quedaron filas válidas después de procesar fechas. Saltando archivo."); continue

            log_and_update("     Limpiando numéricos...")
            for col_num in numeric_internal_cols:
                if col_num in df_renamed.columns:
                    try: df_renamed[col_num] = df_renamed[col_num].apply(robust_numeric_conversion)
                    except Exception as e_num_conv:
                        log_and_update(f"     !!! Error robust_numeric_conversion en '{col_num}': {e_num_conv}. Usando pd.to_numeric.");
                        df_renamed[col_num] = pd.to_numeric(df_renamed[col_num], errors='coerce')
            log_and_update("     Limpieza numérica OK.")
            
            def extract_ad_name_safe(txt):
                if pd.isna(txt): return ""
                try: s=str(txt); return normalize(s.split('🆔')[-1].strip() if '🆔' in s and len(s.split('🆔'))>1 and s.split('🆔')[-1].strip() else s)
                except: return ""

            df_renamed['Campaign']=df_renamed.get('campaign', pd.Series(dtype=str)).fillna('(No Campaign)').astype(str).apply(normalize)
            df_renamed['AdSet']=df_renamed.get('adset', pd.Series(dtype=str)).fillna('(No AdSet)').astype(str).apply(normalize)
            
            if 'ad' in df_renamed.columns:
                df_renamed['Anuncio']=df_renamed['ad'].apply(extract_ad_name_safe)
            else:
                df_renamed['Anuncio']=pd.Series('(No Ad)', index=df_renamed.index, dtype=str)

            # MODIFICACIÓN para 'Públicos In' y 'Públicos Ex'
            if 'aud_in' in df_renamed.columns:
                df_renamed['Públicos In'] = df_renamed['aud_in'].fillna('').astype(str).apply(normalize)
            else:
                df_renamed['Públicos In'] = pd.Series('', index=df_renamed.index, dtype=str).apply(normalize)

            if 'aud_ex' in df_renamed.columns:
                df_renamed['Públicos Ex'] = df_renamed['aud_ex'].fillna('').astype(str).apply(normalize)
            else:
                df_renamed['Públicos Ex'] = pd.Series('', index=df_renamed.index, dtype=str).apply(normalize)


            delivery_status_map={'active':'Activo','inactive':'Apagado','not_delivering':'Sin entrega','rejected':'Rechazado','pending_review':'Pendiente', 'archived': 'Archivado', 'completed': 'Completado', 'limited': 'Limitado', 'not approved': 'No Aprobado'}
            if 'entrega' in df_renamed.columns:
                df_renamed['Entrega']=df_renamed['entrega'].fillna('Desconocido').astype(str).str.lower().str.replace('_',' ').str.strip().map(delivery_status_map).fillna('Otro')
            else: df_renamed['Entrega']='Desconocido'

            all_dataframes.append(df_renamed)

            if 'Campaign' in df_renamed.columns and 'AdSet' in df_renamed.columns:
                 current_file_pairs = df_renamed[['Campaign', 'AdSet']].astype(str).drop_duplicates()
                 current_file_pairs = current_file_pairs[
                     (~current_file_pairs['Campaign'].str.lower().str.startswith('(no')) &
                     (~current_file_pairs['AdSet'].str.lower().str.startswith('(no'))
                 ]
                 for _, row_pair in current_file_pairs.iterrows():
                     all_campaign_adsets.add((row_pair['Campaign'], row_pair['AdSet']))

        except Exception as e_fase1:
            log_and_update(f"!!! Error Procesando Archivo '{base_filename}': {e_fase1} !!!\n{traceback.format_exc()}"); continue

    if not all_dataframes:
        log_and_update("\n!!! No se procesó ningún archivo con éxito. Abortando. !!!"); return None, None, None
    try:
        df_combined = pd.concat(all_dataframes, ignore_index=True)
        log_and_update(f"\n--- Datos combinados ({len(df_combined)} filas). Verificando tipos y filtrando campaña... ---")
        
        for col_num_final in numeric_internal_cols:
            if col_num_final in df_combined.columns:
                df_combined[col_num_final] = pd.to_numeric(df_combined[col_num_final], errors='coerce')
        
        for col_cat_final in ['Campaign','AdSet','Anuncio','Entrega','Públicos In','Públicos Ex']:
             default_val = '(No Data)' if col_cat_final != 'Entrega' else 'Desconocido'
             if col_cat_final not in df_combined.columns:
                 df_combined[col_cat_final] = default_val # Si la columna no existe en absoluto, la crea con el default
             df_combined[col_cat_final] = df_combined[col_cat_final].astype(str).fillna(default_val)


        if selected_campaign not in ['__ALL__', '__DO_NOT_FILTER__']:
            if 'Campaign' in df_combined.columns:
                log_and_update(f"Filtrando para la campaña: '{selected_campaign}'...")
                original_count_filter = len(df_combined)
                selected_campaign_normalized = normalize(selected_campaign)
                df_combined = df_combined[df_combined['Campaign'].str.strip().str.lower() == selected_campaign_normalized].copy()
                log_and_update(f"Filtrado OK. Quedan {len(df_combined)} de {original_count_filter} filas para la campaña '{selected_campaign}'.")
                if df_combined.empty:
                    log_and_update(f"ADVERTENCIA: No quedaron datos después de filtrar por la campaña '{selected_campaign}'.")
            else:
                log_and_update("ADVERTENCIA: Columna 'Campaign' no encontrada en datos combinados. No se pudo filtrar por campaña.")
        
        del all_dataframes 

        campaign_adset_list_for_gui = sorted(list(all_campaign_adsets))

        log_and_update("--- Fase 1 Finalizada ---");
        return df_combined, detected_currency_symbol, campaign_adset_list_for_gui

    except Exception as e_combine:
        log_and_update(f"!!! Error Crítico al combinar DataFrames: {e_combine} !!!\n{traceback.format_exc()}"); return None, None, None