# report_generator_project/data_processing/orchestrators.py
import os
import pandas as pd
import numpy as np
import traceback
import re
from datetime import datetime, timedelta
import locale
import logging

logger = logging.getLogger(__name__)

# Importaciones de dateutil (opcional)
try:
    from dateutil.relativedelta import relativedelta
    from dateutil.parser import parse as date_parse
except ImportError:
    relativedelta = None
    date_parse = None
    logger.warning(
        "ADVERTENCIA (orchestrators.py): python-dateutil no encontrado. Funcionalidad de Bitácora Mensual podría fallar."
    )


# Importaciones de módulos en la raíz del proyecto
from config import numeric_internal_cols
from formatting_utils import safe_division, safe_division_pct

# Funciones principales de los otros módulos de data_processing
from .loaders import _cargar_y_preparar_datos
from .aggregators import _agregar_datos_diarios
from .metric_calculators import (
    _calcular_dias_activos_totales,
    _calcular_entidades_activas_por_dia,
)
from .report_sections import (
    _generar_tabla_bitacora_detallada,
    _generar_tabla_bitacora_entidad,
    _generar_tabla_embudo_bitacora,
    _generar_tabla_top_ads_historico,
    _generar_tabla_top_adsets_historico,
    _generar_tabla_top_campaigns_historico,
)

# Variable global para mensajes de resumen
log_summary_messages_orchestrator = []

def _crear_logger_con_resumen(log_file_handler, status_queue):
    """Crea una función de logging que también guarda mensajes importantes."""
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
    return log_with_summary

def procesar_reporte_rendimiento(input_files, output_dir, output_filename, status_queue, selected_campaign, selected_adsets):
    log_file_handler = None
    try:
        output_path = os.path.join(output_dir, output_filename)
        with open(output_path, 'w', encoding='utf-8') as f_out:
            log_file_handler = f_out
            log = _crear_logger_con_resumen(log_file_handler, status_queue)

            log("--- Iniciando Reporte Rendimiento ---", importante=True)
            log("--- Fase 1: Carga y Preparación ---", importante=True)
            df_combined, detected_currency, _ = _cargar_y_preparar_datos(input_files, status_queue, selected_campaign)
            if df_combined is None or df_combined.empty:
                log("Fallo al cargar/filtrar datos. Abortando.", importante=True)
                status_queue.put("---ERROR---")
                return

            log(f"Reporte Rendimiento {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            log(f"Moneda Detectada: {detected_currency}")

            log("\n--- Iniciando Agregación Diaria ---", importante=True)
            df_daily_agg = _agregar_datos_diarios(df_combined, status_queue, selected_adsets)
            if df_daily_agg is None or df_daily_agg.empty or 'date' not in df_daily_agg.columns:
                log("!!! Falló agregación diaria o resultado inválido. Abortando. !!!", importante=True)
                status_queue.put("---ERROR---")
                return
            log("Agregación diaria OK.")

            log("\n--- Calculando Días Activos Totales ---", importante=True)
            active_days_results = _calcular_dias_activos_totales(df_combined)
            active_days_ad = active_days_results.get('Anuncio', pd.DataFrame())

            # Generar las secciones del reporte
            _generar_tabla_top_ads_historico(df_daily_agg, active_days_ad, log, detected_currency)
            
            # (Aquí irían las llamadas al resto de las secciones del reporte de rendimiento)


            log("============================================================")
            if log_summary_messages_orchestrator:
                for msg in log_summary_messages_orchestrator:
                    clean_msg = re.sub(r'^\s*\[\d{2}:\d{2}:\d{2}\]\s*', '', msg).strip().replace('---', '-')
                    log(f"  - {clean_msg}")
            log("============================================================")
            log("\n\n--- FIN DEL REPORTE RENDIMIENTO ---", importante=True)
            status_queue.put("---DONE---")


    except Exception as e_main:
        error_details = traceback.format_exc()
        # Si el log no se pudo crear, al menos lo mandamos a la cola de estado
        if 'log' not in locals():
            status_queue.put(f"!!! Error Fatal Previo a Logging: {e_main} !!!\n{error_details}")
        else:
            log_msg = f"!!! Error Fatal General Reporte Rendimiento: {e_main} !!!\n{error_details}"
            log(log_msg, importante=True)
        status_queue.put("---ERROR---")
    finally:
        if log_file_handler and not log_file_handler.closed:
            try:
                log_file_handler.close()
            except Exception:
                pass

def procesar_reporte_bitacora(input_files, output_dir, output_filename, status_queue,
                              selected_campaign, selected_adsets,
                              current_week_start_input_str, current_week_end_input_str,
                              bitacora_comparison_type):
    log_file_handler = None
    original_locale_setting = locale.getlocale(locale.LC_TIME)
    try:
        output_path = os.path.join(output_dir, output_filename)
        with open(output_path, 'w', encoding='utf-8') as f_out:
            log_file_handler = f_out
            log = _crear_logger_con_resumen(log_file_handler, status_queue)

            try:
                locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')
            except locale.Error:
                log("Adv: No se pudo configurar el locale a español. Los meses pueden aparecer en inglés.")
            
            log(f"--- Iniciando Reporte Bitácora ({bitacora_comparison_type}) ---", importante=True)
            
            log("--- Fase 1: Carga y Preparación ---", importante=True)
            df_combined, detected_currency, _ = _cargar_y_preparar_datos(input_files, status_queue, selected_campaign)
            if df_combined is None or df_combined.empty:
                log("Fallo al cargar/filtrar datos para Bitácora. Abortando.", importante=True)
                status_queue.put("---ERROR---")
                return

            log("\n--- Fase 2: Agregación Diaria ---", importante=True)
            df_daily_agg_full = _agregar_datos_diarios(df_combined, status_queue, selected_adsets)
            if df_daily_agg_full is None or df_daily_agg_full.empty or 'date' not in df_daily_agg_full.columns:
                log("!!! Falló agregación diaria o no hay fechas válidas. Abortando Bitácora. !!!", importante=True)
                status_queue.put("---ERROR---")
                return
            log("Agregación diaria OK.")

            log("\n--- Fase 3: Cálculo de Métricas Adicionales ---")
            active_days_results = _calcular_dias_activos_totales(df_combined)
            active_days_campaign = active_days_results.get('Campaign', pd.DataFrame())
            active_days_adset = active_days_results.get('AdSet', pd.DataFrame())
            active_days_ad = active_days_results.get('Anuncio', pd.DataFrame())
            active_entities_daily = _calcular_entidades_activas_por_dia(df_combined)


            # --- Generación de Secciones del Reporte ---
            _generar_tabla_bitacora_detallada(df_daily_agg_full, detected_currency, log, active_entities_df=active_entities_daily)
            
            # Lógica para determinar los períodos de la bitácora (semanal/mensual)
            # (Esta lógica compleja se mantiene, asumiendo que es correcta)
            # ...
            # Placeholder para la lógica de periodos
            min_date_overall = df_daily_agg_full['date'].min().date()
            max_date_overall = df_daily_agg_full['date'].max().date()
            bitacora_periods_list = []
            if bitacora_comparison_type == "Weekly":
                # Lógica de fallback para encontrar la última semana con datos
                last_monday = max_date_overall - timedelta(days=max_date_overall.weekday())
                for i in range(4):
                    start_date = last_monday - timedelta(weeks=i)
                    end_date = start_date + timedelta(days=6)
                    label = f"{i}ª semana anterior" if i > 0 else "Semana actual"
                    bitacora_periods_list.append((datetime.combine(start_date, datetime.min.time()), datetime.combine(end_date, datetime.max.time()), label))
            # ... (fin placeholder)

            if not bitacora_periods_list:
                bitacora_periods_list.append(
                    (
                        datetime.combine(min_date_overall, datetime.min.time()),
                        datetime.combine(max_date_overall, datetime.max.time()),
                        "Periodo Único",
                    )
                )

            # Calcular métricas y generar tablas de resumen
            df_daily_total_for_bitacora = df_daily_agg_full.groupby('date', as_index=False, observed=True).sum(numeric_only=True)
            # ... (cálculos de roas, cpa, etc.) ...
            
            _generar_tabla_bitacora_entidad('Cuenta Completa', 'Agregado Total', df_daily_total_for_bitacora,
                                            bitacora_periods_list, detected_currency, log, period_type=bitacora_comparison_type)
            _generar_tabla_embudo_bitacora(df_daily_total_for_bitacora, bitacora_periods_list, log, detected_currency, period_type=bitacora_comparison_type)

            _generar_tabla_top_ads_historico(df_daily_agg_full, active_days_ad, log, detected_currency, top_n=15, sort_by_roas=True)
            _generar_tabla_top_adsets_historico(df_daily_agg_full, active_days_adset, log, detected_currency, top_n=15)
            _generar_tabla_top_campaigns_historico(df_daily_agg_full, active_days_campaign, log, detected_currency, top_n=15)

            # --- Resumen Final ---
            log("\n\n============================================================")
            log(f"===== Resumen del Proceso (Bitácora {bitacora_comparison_type}) =====")
            log("============================================================")
            if log_summary_messages_orchestrator:
                for msg in log_summary_messages_orchestrator:
                    clean_msg = re.sub(r'^\s*\[\d{2}:\d{2}:\d{2}\]\s*', '', msg).strip().replace('---', '-')
                    log(f"  - {clean_msg}")

            log("============================================================")
            log(f"\n\n--- FIN DEL REPORTE BITÁCORA ({bitacora_comparison_type}) ---", importante=True)
            status_queue.put("---DONE---")

    except Exception as e_main_bitacora:
        error_details = traceback.format_exc()
        if 'log' not in locals():
            status_queue.put(f"!!! Error Fatal Previo a Logging: {e_main_bitacora} !!!\n{error_details}")
        else:
            log_msg = f"!!! Error Fatal General Reporte Bitácora: {e_main_bitacora} !!!\n{error_details}"
            log(log_msg, importante=True)
        status_queue.put("---ERROR---")
    finally:
        if log_file_handler and not log_file_handler.closed:
            try: log_file_handler.close()
            except: pass
        try: locale.setlocale(locale.LC_TIME, original_locale_setting)
        except: pass