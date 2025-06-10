# report_generator_project/data_processing/orchestrators.py
import os
import pandas as pd
import numpy as np
import traceback
import re # Para el resumen de log
from datetime import datetime, date, timedelta # Para fechas default
import locale # Para el locale en bitácora

# Importaciones de dateutil (intentar, pero no hacer que falle el módulo entero si no están)
try:
    from dateutil.relativedelta import relativedelta
    from dateutil.parser import parse as date_parse
except ImportError:
    relativedelta = None
    date_parse = None
    print("ADVERTENCIA (orchestrators.py): python-dateutil no encontrado. Funcionalidad de Bitácora Mensual y algunas comparaciones de fechas podrían fallar.")

# Importaciones relativas para módulos dentro del mismo paquete 'data_processing'
from .loaders import _cargar_y_preparar_datos
from .aggregators import _agregar_datos_diarios
from .metric_calculators import _calcular_dias_activos_totales
from .report_sections import (
    _generar_tabla_vertical_global, _generar_tabla_vertical_entidad,
    _generar_tabla_embudo_rendimiento, _generar_tabla_embudo_bitacora,
    _generar_analisis_ads, _generar_tabla_top_ads_historico,
    _generar_tabla_bitacora_entidad
)

# Importaciones de módulos en la raíz del proyecto
from config import numeric_internal_cols

# Variable global para mensajes de resumen específicos de este módulo
log_summary_messages_orchestrator = []

def procesar_reporte_rendimiento(input_files, output_dir, output_filename, status_queue, selected_campaign, selected_adsets):
    log_file_handler = None
    global log_summary_messages_orchestrator
    log_summary_messages_orchestrator = []

    def log_with_summary(line='', importante=False):
        processed_line = str(line)
        if log_file_handler and not log_file_handler.closed:
            try:
                log_file_handler.write(processed_line + '\n')
            except Exception as e_write:
                status_queue.put(f"Error escribiendo log a archivo: {e_write}") 
        status_queue.put(processed_line)
        if importante:
            log_summary_messages_orchestrator.append(processed_line)
    log = log_with_summary

    try:
        log("--- Iniciando Reporte Rendimiento ---", importante=True)
        log("--- Fase 1: Carga y Preparación ---", importante=True)
        df_combined, detected_currency, _ = _cargar_y_preparar_datos(input_files, status_queue, selected_campaign)
        if df_combined is None or df_combined.empty:
            log("Fallo al cargar/filtrar datos. Abortando.", importante=True)
            status_queue.put("---ERROR---")
            return

        output_path = os.path.join(output_dir, output_filename)
        with open(output_path, 'w', encoding='utf-8') as f_out:
            log_file_handler = f_out
            log(f"Reporte Rendimiento {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            log(f"Moneda Detectada: {detected_currency}")
            campaign_display='Todas' if selected_campaign=='__ALL__' else selected_campaign
            log(f"Campaña Filtrada: {campaign_display}")
            adset_list_for_log = selected_adsets if isinstance(selected_adsets, list) else [selected_adsets]
            adset_display = 'Todos' if adset_list_for_log is None or adset_list_for_log == ['__ALL__'] else ", ".join(adset_list_for_log)
            log(f"AdSets Filtrados: {adset_display}")

            log("\n--- Análisis de Rendimiento ---")
            log("\n--- Iniciando Agregación Diaria ---", importante=True)
            df_daily_agg = _agregar_datos_diarios(df_combined, status_queue, selected_adsets)

            if df_daily_agg is None or df_daily_agg.empty or 'date' not in df_daily_agg.columns or df_daily_agg['date'].dropna().empty:
                 log("!!! Falló agregación diaria o resultado inválido. Abortando. !!!",importante=True)
                 status_queue.put("---ERROR---")
                 return
            log("Agregación diaria OK.")
            
            log("\n--- Calculando Días Activos Totales ---", importante=True)
            active_days_results = _calcular_dias_activos_totales(df_combined)
            active_days_campaign = active_days_results.get('Campaign',pd.DataFrame())
            active_days_adset = active_days_results.get('AdSet',pd.DataFrame())
            active_days_ad = active_days_results.get('Anuncio',pd.DataFrame())
            
            last_day_status_lookup=pd.DataFrame(); max_date_global=None
            if not df_combined.empty and 'date' in df_combined.columns and pd.api.types.is_datetime64_any_dtype(df_combined['date']) and not df_combined['date'].dropna().empty:
                max_date_global = df_combined['date'].max()
                if pd.notna(max_date_global):
                    status_cols=['ad_delivery_status','adset_delivery_status','campaign_delivery_status', 'entrega']
                    group_cols=['Campaign','AdSet','Anuncio']
                    existing_status_cols = [c for c in status_cols if c in df_combined.columns]
                    existing_group_cols = [c for c in group_cols if c in df_combined.columns]
                    
                    cols_to_select_for_lookup = ['date'] + existing_group_cols + existing_status_cols
                    cols_to_select_for_lookup = list(set(cols_to_select_for_lookup)) 

                    if 'date' in cols_to_select_for_lookup:
                        last_day_data=df_combined[df_combined['date']==max_date_global][cols_to_select_for_lookup].copy()
                        group_cols_dedup=[c for c in existing_group_cols if c in last_day_data.columns]
                        if group_cols_dedup:
                            last_day_status_lookup=last_day_data.drop_duplicates(subset=group_cols_dedup,keep='last')
                            log("Lookup estado último día creado.")
                        else: log("Adv: No cols. agrupación encontradas para lookup estado.")
                    else: log("Adv: Faltan columnas ('date'?) para lookup estado.")
                else: log("Adv: Fecha máxima no válida para lookup estado.")
            else: log("Adv: No datos combinados o fechas para lookup estado.")


            periods_for_entity_tables=[3,7,14,30] 
            log("--- Iniciando Sección 1: Global ---",importante=True);
            try: _generar_tabla_vertical_global(df_daily_agg,detected_currency,log) 
            except Exception as e_s1: log(f"\n!!! Error Sección 1 (Global): {e_s1} !!!\n{traceback.format_exc()}",importante=True)
            
            log("--- Iniciando Sección 4: Embudo ---",importante=True);
            try: _generar_tabla_embudo_rendimiento(df_daily_agg,periods_for_entity_tables,log,detected_currency) 
            except Exception as e_s4: log(f"\n!!! Error Sección 4 (Embudo): {e_s4} !!!\n{traceback.format_exc()}",importante=True)

            log("\n--- Filtrando Campañas y AdSets con Impresiones > 0 ---",importante=True); 
            valid_campaigns=pd.Index([]); valid_adsets_filter=pd.Index([])
            if 'impr' in df_daily_agg.columns and not df_daily_agg.empty and 'Campaign' in df_daily_agg.columns and 'AdSet' in df_daily_agg.columns:
                 entity_impressions=df_daily_agg.groupby(['Campaign','AdSet'],observed=False)['impr'].sum().fillna(0) 
                 if not entity_impressions.empty:
                     valid_entities_index = entity_impressions[entity_impressions>0].index 
                     if isinstance(valid_entities_index, pd.MultiIndex) and not valid_entities_index.empty: 
                        if 'Campaign' in valid_entities_index.names:
                             valid_campaigns=valid_entities_index.get_level_values('Campaign').unique() 
                        if 'AdSet' in valid_entities_index.names:
                             valid_adsets_filter=valid_entities_index.get_level_values('AdSet').unique() 
                 log(f"Campañas con Impr > 0: {len(valid_campaigns)}",importante=True); log(f"AdSets con Impr > 0: {len(valid_adsets_filter)}",importante=True)
            else: 
                 log("Advertencia: Col 'impr', 'Campaign' o 'AdSet' no encontrada o datos agregados vacíos para filtrar entidades.",importante=True)
                 if 'Campaign' in df_daily_agg: valid_campaigns=df_daily_agg['Campaign'].unique() 
                 if 'AdSet' in df_daily_agg: valid_adsets_filter=df_daily_agg['AdSet'].unique()

            log("--- Iniciando Sección 2: Campañas (Filtradas) ---",importante=True); log("\n\n============================================================"); log("===== 2. Métricas y Estabilidad por Campaña (Filtrado: Impr > 0) ====="); log("============================================================")
            if 'Campaign' in df_daily_agg.columns and not df_daily_agg.empty and max_date_global is not None and pd.notna(max_date_global): 
                 campaign_list_filtered=sorted([c for c in df_daily_agg['Campaign'].unique() if c and c in valid_campaigns]) 
                 if not campaign_list_filtered: log("No se encontraron campañas válidas con impresiones > 0 para analizar.")
                 else:
                     log(f"--- Analizando {len(campaign_list_filtered)} Campaña(s) Válida(s) ---")
                     for cn in campaign_list_filtered: 
                          df_subset_camp=df_daily_agg[df_daily_agg['Campaign'].eq(cn)].copy(); 
                          if df_subset_camp.empty : continue
                          dias_row=active_days_campaign[active_days_campaign['Campaign'].eq(cn)]; dias=dias_row['Días_Activo_Total'].iloc[0] if not dias_row.empty else 0; 
                          adsets_in_camp_all = df_subset_camp['AdSet'].unique() if 'AdSet' in df_subset_camp else [] 
                          valid_adsets_for_camp = [adset for adset in adsets_in_camp_all if adset in valid_adsets_filter] 
                          adset_count = len(valid_adsets_for_camp) 
                          min_dt_camp=df_subset_camp['date'].min(); max_dt_camp=df_subset_camp['date'].max() 
                          try: _generar_tabla_vertical_entidad('Campaña',cn,dias,df_subset_camp,min_dt_camp,max_dt_camp,adset_count,periods_for_entity_tables,detected_currency,log, period_type="Days") 
                          except Exception as e_s2: log(f"\n!!! Error generando tabla campaña '{cn}': {e_s2} !!!\n{traceback.format_exc()}",importante=True)
            else: log("Datos insuficientes ('Campaign', fechas) para análisis por Campaña.")

            log("--- Iniciando Sección 3: AdSets (Filtrados) ---",importante=True); log("\n\n============================================================"); log("===== 3. Métricas y Estabilidad por AdSet (Filtrado: Impr > 0) ====="); log("============================================================")
            if 'AdSet' in df_daily_agg.columns and not df_daily_agg.empty and max_date_global is not None and pd.notna(max_date_global): 
                adset_list_filtered=sorted([a for a in df_daily_agg['AdSet'].unique() if a and a in valid_adsets_filter]) 
                if not adset_list_filtered: log("No se encontraron AdSets válidos con impresiones > 0 para analizar.")
                else:
                    log(f"\n--- Analizando {len(adset_list_filtered)} AdSet(s) Válido(s) ---")
                    for adset_name in adset_list_filtered: 
                        df_subset_adset=df_daily_agg[df_daily_agg['AdSet'].eq(adset_name)].copy(); 
                        if df_subset_adset.empty: continue
                        dias_total_adset = 0 
                        if not active_days_adset.empty and 'AdSet' in active_days_adset.columns: 
                            dias_rows_adset = active_days_adset[active_days_adset['AdSet'] == adset_name]
                            if not dias_rows_adset.empty:
                                dias_total_adset = dias_rows_adset['Días_Activo_Total'].sum() 
                        min_dt_adset=df_subset_adset['date'].min(); max_dt_adset=df_subset_adset['date'].max() 
                        try: _generar_tabla_vertical_entidad('AdSet',adset_name,dias_total_adset,df_subset_adset,min_dt_adset,max_dt_adset,None,periods_for_entity_tables,detected_currency,log, period_type="Days") 
                        except Exception as e_s3: log(f"\n!!! Error generando tabla AdSet '{adset_name}': {e_s3} !!!\n{traceback.format_exc()}",importante=True)
            else: log("Datos insuficientes ('AdSet', fechas) para análisis por AdSet.")

            log("--- Iniciando Sección 5: Ads (Consolidado) ---",importante=True);
            try: _generar_analisis_ads(df_combined,df_daily_agg,active_days_ad,log,detected_currency,last_day_status_lookup) 
            except Exception as e_s5: log(f"\n!!! Error Sección 5 (Análisis Ads): {e_s5} !!!\n{traceback.format_exc()}",importante=True)
            
            log("--- Iniciando Sección 6: Top Ads Histórico ---",importante=True);
            try: _generar_tabla_top_ads_historico(df_daily_agg,active_days_ad,log,detected_currency) 
            except Exception as e_s6: log(f"\n!!! Error Sección 6 (Top Ads): {e_s6} !!!\n{traceback.format_exc()}",importante=True)

            log("\n\n============================================================");log("===== Resumen del Proceso =====");log("============================================================")
            if log_summary_messages_orchestrator: [log(f"  - {re.sub(r'^\s*\[\d{2}:\d{2}:\d{2}\]\s*','',msg).strip().replace('---','-')}") for msg in log_summary_messages_orchestrator if re.sub(r'^\s*\[\d{2}:\d{2}:\d{2}\]\s*','',msg).strip()]
            else: log("  No se generaron mensajes de resumen.")
            log("============================================================")
            log("\n\n--- FIN DEL REPORTE RENDIMIENTO ---",importante=True); status_queue.put("---DONE---")
    except Exception as e_main:
        error_details=traceback.format_exc(); log_msg=f"!!! Error Fatal General Reporte Rendimiento: {e_main} !!!\n{error_details}";
        log(log_msg,importante=True)
        status_queue.put(log_msg)
        status_queue.put("---ERROR---")
    finally:
        if log_file_handler and not log_file_handler.closed:
            try: log_file_handler.close()
            except: pass


def procesar_reporte_bitacora(input_files, output_dir, output_filename, status_queue, 
                               selected_campaign, selected_adsets, 
                               current_week_start_input_str, current_week_end_input_str, 
                               bitacora_comparison_type):
    log_file_handler = None
    global log_summary_messages_orchestrator
    log_summary_messages_orchestrator = []
    def log_with_summary(line='', importante=False):
        processed_line = str(line)
        if log_file_handler and not log_file_handler.closed:
            try: log_file_handler.write(processed_line + '\n')
            except Exception as e_write: status_queue.put(f"Error escribiendo log a archivo: {e_write}")
        status_queue.put(processed_line)
        if importante: log_summary_messages_orchestrator.append(processed_line)
    log = log_with_summary
    
    original_locale_setting = locale.getlocale(locale.LC_TIME)
    try:
        locale_candidates = ['es_ES.UTF-8', 'es_ES', 'Spanish_Spain', 'Spanish']
        locale_set = False
        for loc in locale_candidates:
            try:
                locale.setlocale(locale.LC_TIME, loc)
                log(f"Locale para fechas configurado a: {loc}")
                locale_set = True
                break
            except locale.Error:
                continue
        if not locale_set:
            log("Adv: No se pudo configurar el locale a español para nombres de meses. Se usarán nombres en inglés por defecto.")

        log(f"--- Iniciando Reporte Bitácora ({bitacora_comparison_type}) ---", importante=True)
        if bitacora_comparison_type == "Weekly":
             log(f"--- Rango semana de referencia (desde GUI): '{current_week_start_input_str or 'Auto'}' a '{current_week_end_input_str or 'Auto'}' ---")

        log("--- Fase 1: Carga y Preparación (Bitácora) ---", importante=True)
        df_combined, detected_currency, _ = _cargar_y_preparar_datos(input_files, status_queue, selected_campaign)
        if df_combined is None or df_combined.empty:
            log("Fallo al cargar/filtrar datos para Bitácora. Abortando.", importante=True)
            status_queue.put("---ERROR---"); return

        output_path = os.path.join(output_dir, output_filename)
        with open(output_path, 'w', encoding='utf-8') as f_out:
            log_file_handler = f_out
            log(f"Reporte Bitácora ({bitacora_comparison_type}) {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            log(f"Moneda Detectada: {detected_currency}")
            campaign_display = 'Todas' if selected_campaign == '__ALL__' else selected_campaign
            log(f"Campaña Filtrada: {campaign_display}")
            
            adset_list_for_log_b = selected_adsets if isinstance(selected_adsets, list) else [selected_adsets]
            adset_display_b = 'Todos' if adset_list_for_log_b is None or adset_list_for_log_b == ['__ALL__'] else ", ".join(adset_list_for_log_b)
            log(f"AdSets Filtrados: {adset_display_b}");

            log("\n--- Análisis de Bitácora ---")
            log("\n--- Iniciando Agregación Diaria (Bitácora) ---", importante=True)
            df_daily_agg_full = _agregar_datos_diarios(df_combined, status_queue, selected_adsets) 

            if df_daily_agg_full is None or df_daily_agg_full.empty or 'date' not in df_daily_agg_full.columns or df_daily_agg_full['date'].dropna().empty:
                log("!!! Falló agregación diaria o no hay fechas válidas. Abortando Bitácora. !!!", importante=True)
                status_queue.put("---ERROR---"); return
            log("Agregación diaria OK.")

            min_date_overall = df_daily_agg_full['date'].min().date()
            max_date_overall = df_daily_agg_full['date'].max().date()
            log(f"  Rango total de datos agregados (considerando filtros): {min_date_overall.strftime('%d/%m/%Y')} a {max_date_overall.strftime('%d/%m/%Y')}")

            bitacora_periods_list = [] 

            if bitacora_comparison_type == "Weekly":
                current_week_start_obj = None
                current_week_end_obj = None

                if current_week_start_input_str and current_week_end_input_str and date_parse:
                    try:
                        parsed_start = date_parse(current_week_start_input_str, dayfirst=True).date()
                        parsed_end = date_parse(current_week_end_input_str, dayfirst=True).date()
                        
                        # Validar que es un período de 7 días (Lunes a Domingo o similar)
                        if (parsed_end - parsed_start).days == 6:
                            current_week_start_obj = parsed_start
                            current_week_end_obj = parsed_end
                            log(f"  Bitácora (Semanal): Rango de semana de referencia válido recibido de GUI: {current_week_start_obj.strftime('%d/%m/%Y')} a {current_week_end_obj.strftime('%d/%m/%Y')}")
                        else:
                             log(f"  Advertencia: Rango de GUI ({current_week_start_input_str} - {current_week_end_input_str}) no es de 7 días. Usando fallback.")
                             current_week_start_obj = None # Forzar fallback
                             current_week_end_obj = None
                    except Exception as e_ref_parse:
                        log(f"  Advertencia: Fechas de referencia de GUI inválidas ({e_ref_parse}). Usando fallback.")
                        current_week_start_obj = None
                        current_week_end_obj = None
                
                if not current_week_start_obj or not current_week_end_obj: 
                    log("  Bitácora (Semanal): Buscando semana de referencia automáticamente (última semana completa con datos).")
                    unique_dates_in_data = sorted(df_daily_agg_full['date'].dt.date.unique())
                    if unique_dates_in_data:
                        max_date_in_filtered_data = max(unique_dates_in_data)
                        
                        # Iniciar con la semana que contiene la fecha máxima de datos
                        temp_monday_candidate = max_date_in_filtered_data - timedelta(days=max_date_in_filtered_data.weekday())
                        
                        found_suitable_week = False
                        # Iterar hacia atrás desde la semana de la fecha máxima
                        for _ in range(min(52, (temp_monday_candidate - min_date_overall).days // 7 + 2 if temp_monday_candidate >= min_date_overall else 1)):
                            temp_sunday_candidate = temp_monday_candidate + timedelta(days=6)

                            # Ajustar el inicio y fin de la semana candidata al rango real de datos
                            week_actual_start_for_check = max(temp_monday_candidate, min_date_overall)
                            week_actual_end_for_check = min(temp_sunday_candidate, max_date_overall)

                            if week_actual_start_for_check > week_actual_end_for_check: # Esta semana está completamente fuera del rango de datos
                                temp_monday_candidate -= timedelta(weeks=1)
                                continue
                            
                            # Verificar si hay *algún* dato en este segmento de semana ajustado
                            has_data_in_this_actual_week_segment = False
                            current_check_day = week_actual_start_for_check
                            while current_check_day <= week_actual_end_for_check:
                                if current_check_day in unique_dates_in_data:
                                    has_data_in_this_actual_week_segment = True
                                    break
                                current_check_day += timedelta(days=1)
                            
                            if has_data_in_this_actual_week_segment:
                                current_week_start_obj = temp_monday_candidate # Usar el Lunes teórico de la semana
                                current_week_end_obj = temp_sunday_candidate   # Usar el Domingo teórico de la semana
                                log(f"    -> Semana de referencia automática (basado en datos más recientes): {current_week_start_obj.strftime('%d/%m/%Y')} a {current_week_end_obj.strftime('%d/%m/%Y')}")
                                found_suitable_week = True
                                break
                            
                            temp_monday_candidate -= timedelta(weeks=1) # Probar la semana anterior
                        
                        if not found_suitable_week:
                            log("    -> No se encontraron semanas con datos en el fallback. Usando Lunes de la semana de la fecha mínima de datos.")
                            current_week_start_obj = min_date_overall - timedelta(days=min_date_overall.weekday())
                            current_week_end_obj = current_week_start_obj + timedelta(days=6)
                    
                    if not current_week_start_obj: 
                        log("!!! Error: No hay fechas únicas en los datos agregados. No se puede determinar semana de referencia. !!!", importante=True)
                        status_queue.put("---ERROR---"); return
                
                # Ajustes finales para asegurar que la semana "actual" no esté completamente fuera del rango de datos
                if current_week_end_obj < min_date_overall:
                    log(f"  Ajuste Fuerte: Semana de referencia {current_week_start_obj.strftime('%d/%m/%Y')} - {current_week_end_obj.strftime('%d/%m/%Y')} es demasiado temprana. Ajustando a primera semana con datos.")
                    current_week_start_obj = min_date_overall - timedelta(days=min_date_overall.weekday())
                    current_week_end_obj = current_week_start_obj + timedelta(days=6)
                
                if current_week_start_obj > max_date_overall:
                    log(f"  Ajuste Fuerte: Semana de referencia {current_week_start_obj.strftime('%d/%m/%Y')} - {current_week_end_obj.strftime('%d/%m/%Y')} es demasiado tarde. Ajustando a última semana con datos.")
                    current_week_start_obj = max_date_overall - timedelta(days=max_date_overall.weekday()) # Lunes de la semana de la fecha máxima
                    current_week_end_obj = current_week_start_obj + timedelta(days=6)


                log(f"  Bitácora (Semanal): Semana efectiva para 'Semana actual' (después de todos los ajustes): {current_week_start_obj.strftime('%d/%m/%Y')} a {current_week_end_obj.strftime('%d/%m/%Y')}")

                for i in range(4): # 4 semanas: actual y 3 anteriores
                    p_start_dt_calc = current_week_start_obj - timedelta(weeks=i)
                    p_end_dt_calc = current_week_end_obj - timedelta(weeks=i)

                    # Ajustar el inicio y fin del período al rango real de los datos
                    actual_p_start_for_data = max(p_start_dt_calc, min_date_overall)
                    actual_p_end_for_data = min(p_end_dt_calc, max_date_overall)

                    if actual_p_start_for_data <= actual_p_end_for_data: # Si hay superposición con los datos
                        if i == 0: label_base = "Semana actual"
                        else: label_base = f"{i}ª semana anterior"
                        
                        # La etiqueta mostrará el rango teórico de la semana (Lunes-Domingo)
                        # pero los datos se tomarán del rango real (actual_p_start_for_data a actual_p_end_for_data)
                        date_range_str_label = f"({p_start_dt_calc.strftime('%d %b').lower()} – {p_end_dt_calc.strftime('%d %b %Y').lower()})"
                        label = f"{label_base} {date_range_str_label}"
                        
                        bitacora_periods_list.append(
                            (datetime.combine(actual_p_start_for_data, datetime.min.time()),
                             datetime.combine(actual_p_end_for_data, datetime.max.time()),
                             label) 
                        )
                    else:
                        label_base_log = f"Semana {i}" if i > 0 else "Semana actual"
                        log(f"    -> Período semanal ({label_base_log} base {p_start_dt_calc.strftime('%d/%m/%y')}) omitido (completamente fuera del rango de datos).")
                
            elif bitacora_comparison_type == "Monthly":
                if not relativedelta or not date_parse:
                    log("!!! Error: 'python-dateutil' no disponible. No se puede generar Bitácora Mensual. !!!", importante=True)
                    status_queue.put("---ERROR---"); return

                latest_date_for_monthly = df_daily_agg_full['date'].max()
                found_months_data = []
                log(f"  Bitácora (Mensual): Buscando los últimos meses calendario completos...")
                month_candidate_start = latest_date_for_monthly.replace(day=1)

                for k_month_offset in range(12): 
                    m_start = month_candidate_start - relativedelta(months=k_month_offset)
                    m_end = m_start + relativedelta(months=1) - timedelta(days=1)

                    if m_start.date() < min_date_overall:
                        if m_end.date() < min_date_overall:
                             log(f"    -> Mes {m_start.strftime('%Y-%m')} completamente anterior a los datos. Deteniendo búsqueda.")
                             break
                    
                    actual_m_start_date = max(m_start.date(), min_date_overall)
                    actual_m_end_date = min(m_end.date(), max_date_overall)

                    if actual_m_start_date > actual_m_end_date: continue
                    
                    if m_start.date() >= min_date_overall and m_end.date() <= max_date_overall:
                        df_month_subset = df_daily_agg_full[
                            (df_daily_agg_full['date'].dt.date >= m_start.date()) &
                            (df_daily_agg_full['date'].dt.date <= m_end.date())
                        ].copy()
                        
                        expected_days_in_calendar_month = (m_end.date() - m_start.date()).days + 1
                        unique_days_in_month_data = df_month_subset['date'].nunique() if not df_month_subset.empty else 0

                        if unique_days_in_month_data == expected_days_in_calendar_month:
                            log(f"      -> Mes calendario completo encontrado: {m_start.strftime('%Y-%m')}")
                            if len(found_months_data) == 0: label_base_monthly = "Mes actual"
                            else: label_base_monthly = f"{len(found_months_data)}º mes anterior" 
                            
                            date_range_str_label_monthly = f"({m_start.strftime('%d %b').lower()} – {m_end.strftime('%d %b %Y').lower()})"
                            label_monthly = f"{label_base_monthly} {date_range_str_label_monthly}"

                            found_months_data.append(
                                (datetime.combine(m_start.date(), datetime.min.time()),
                                 datetime.combine(m_end.date(), datetime.max.time()),
                                 label_monthly)
                            )
                            if len(found_months_data) >= 2: break 
                        else:
                             log(f"      -> Mes {m_start.strftime('%Y-%m')} incompleto en datos ({unique_days_in_month_data}/{expected_days_in_calendar_month} días).")
                    else:
                        log(f"      -> Mes calendario {m_start.strftime('%Y-%m')} no está completamente dentro del rango de datos ({min_date_overall.strftime('%Y-%m')} - {max_date_overall.strftime('%Y-%m')}). No se considera para 'mes completo'.")

                if len(found_months_data) >= 1:
                    bitacora_periods_list.extend(found_months_data) 
                    log(f"  Mes Actual para Bitácora Mensual: {found_months_data[0][2]}")
                    if len(found_months_data) >= 2:
                        log(f"  Mes Anterior 1 para Bitácora Mensual: {found_months_data[1][2]}")
                    else:
                        log("  No se encontró un segundo mes calendario completo para comparación mensual.")
                else:
                     log("\nNo se encontró ningún mes calendario completo en los datos para generar la Bitácora Mensual.")

            if not bitacora_periods_list:
                log(f"No se pudieron definir períodos para la Bitácora ({bitacora_comparison_type}). Verifique el rango de fechas de los datos.", importante=True)
                status_queue.put("---ERROR---"); return

            if bitacora_comparison_type == "Weekly":
                 log(f"  Períodos de Bitácora Semanal definidos ({len(bitacora_periods_list)} semanas):")
            else: 
                 log(f"  Períodos de Bitácora Mensual definidos ({len(bitacora_periods_list)} meses):")

            for _, _, p_label_log in bitacora_periods_list: log(f"    - {p_label_log}")

            existing_numeric_cols_in_agg = [col for col in numeric_internal_cols if col in df_daily_agg_full.columns]
            if not existing_numeric_cols_in_agg:
                log("!!! No se encontraron columnas numéricas para agregar en la bitácora. Abortando. !!!", importante=True)
                status_queue.put("---ERROR---"); return

            df_daily_total_for_bitacora = df_daily_agg_full.groupby('date', as_index=False, observed=True)[existing_numeric_cols_in_agg].sum()

            from formatting_utils import safe_division, safe_division_pct 
            s_tot=df_daily_total_for_bitacora.get('spend',pd.Series(np.nan,index=df_daily_total_for_bitacora.index));v_tot=df_daily_total_for_bitacora.get('value',pd.Series(np.nan,index=df_daily_total_for_bitacora.index));p_tot=df_daily_total_for_bitacora.get('purchases',pd.Series(np.nan,index=df_daily_total_for_bitacora.index));c_tot=df_daily_total_for_bitacora.get('clicks',pd.Series(np.nan,index=df_daily_total_for_bitacora.index));i_tot=df_daily_total_for_bitacora.get('impr',pd.Series(np.nan,index=df_daily_total_for_bitacora.index));r_tot=df_daily_total_for_bitacora.get('reach',pd.Series(np.nan,index=df_daily_total_for_bitacora.index));vi_tot=df_daily_total_for_bitacora.get('visits',pd.Series(np.nan,index=df_daily_total_for_bitacora.index));co_tot=df_daily_total_for_bitacora.get('clicks_out',pd.Series(np.nan,index=df_daily_total_for_bitacora.index));rv3_tot=df_daily_total_for_bitacora.get('rv3',pd.Series(np.nan,index=df_daily_total_for_bitacora.index));rv25_tot=df_daily_total_for_bitacora.get('rv25',pd.Series(np.nan,index=df_daily_total_for_bitacora.index));rv75_tot=df_daily_total_for_bitacora.get('rv75',pd.Series(np.nan,index=df_daily_total_for_bitacora.index));rv100_tot=df_daily_total_for_bitacora.get('rv100',pd.Series(np.nan,index=df_daily_total_for_bitacora.index))
            df_daily_total_for_bitacora['roas']=safe_division(v_tot,s_tot)
            df_daily_total_for_bitacora['cpa']=safe_division(s_tot,p_tot)
            df_daily_total_for_bitacora['ctr']=safe_division_pct(c_tot,i_tot)
            df_daily_total_for_bitacora['cpm']=safe_division(s_tot,i_tot)*1000
            df_daily_total_for_bitacora['frequency']=safe_division(i_tot,r_tot)
            df_daily_total_for_bitacora['lpv_rate']=safe_division_pct(vi_tot,c_tot); df_daily_total_for_bitacora['purchase_rate']=safe_division_pct(p_tot,vi_tot)
            df_daily_total_for_bitacora['ctr_out'] = safe_division_pct(co_tot, i_tot)
            base_rv_tot=np.where(pd.Series(rv3_tot>0).fillna(False),rv3_tot,i_tot); df_daily_total_for_bitacora['rv25_pct']=safe_division_pct(rv25_tot,base_rv_tot); df_daily_total_for_bitacora['rv75_pct']=safe_division_pct(rv75_tot,base_rv_tot); df_daily_total_for_bitacora['rv100_pct']=safe_division_pct(rv100_tot,base_rv_tot)

            _generar_tabla_bitacora_entidad('Cuenta Completa', 'Agregado Total', df_daily_total_for_bitacora,
                                            bitacora_periods_list, detected_currency, log, period_type=bitacora_comparison_type)

            _generar_tabla_embudo_bitacora(df_daily_total_for_bitacora, bitacora_periods_list, log, detected_currency, period_type=bitacora_comparison_type)

            log("\n\n============================================================");log(f"===== Resumen del Proceso (Bitácora {bitacora_comparison_type}) =====");log("============================================================")
            if log_summary_messages_orchestrator: [log(f"  - {re.sub(r'^\s*\[\d{2}:\d{2}:\d{2}\]\s*','',msg).strip().replace('---','-')}") for msg in log_summary_messages_orchestrator if re.sub(r'^\s*\[\d{2}:\d{2}:\d{2}\]\s*','',msg).strip()]
            else: log("  No se generaron mensajes de resumen.")
            log("============================================================")
            log(f"\n\n--- FIN DEL REPORTE BITÁCORA ({bitacora_comparison_type}) ---", importante=True); status_queue.put("---DONE---")

    except Exception as e_main_bitacora:
        error_details = traceback.format_exc()
        log_msg = f"!!! Error Fatal General Reporte Bitácora: {bitacora_comparison_type}: {e_main_bitacora} !!!\n{error_details}"
        log(log_msg, importante=True)
        status_queue.put(log_msg)
        status_queue.put("---ERROR---")
    finally:
        if log_file_handler and not log_file_handler.closed:
            try: log_file_handler.close()
            except: pass
        try: locale.setlocale(locale.LC_TIME, original_locale_setting)
        except: pass