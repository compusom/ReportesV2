# report_generator_project/data_processing/report_sections.py
import pandas as pd
import numpy as np
import re # Para búsqueda de U{dias}
import locale # Para nombres de meses
from datetime import datetime, date, timedelta

# Intentar importar dateutil, pero es opcional para algunas funciones si no está
try:
    from dateutil.relativedelta import relativedelta
    from dateutil.parser import parse as date_parse
except ImportError:
    relativedelta = None
    date_parse = None

from formatting_utils import (
    fmt_int, fmt_float, fmt_pct, fmt_stability, variation, format_step_pct,
    safe_division, safe_division_pct, _format_dataframe_to_markdown
)
from .metric_calculators import _calcular_metricas_agregadas_y_estabilidad, _calculate_stability_pct # Nótese el .
from config import numeric_internal_cols # Importar desde la raíz del proyecto
from utils import aggregate_strings # <--- AÑADIR ESTA LÍNEA DE IMPORTACIÓN

# ============================================================
# GENERACIÓN DE SECCIONES DEL REPORTE
# ============================================================

def _generar_tabla_vertical_global(df_daily_agg, detected_currency, log_func):
    log_func("\n\n============================================================"); log_func("===== 1. Métricas Globales y Comparativa Mensual ====="); log_func("============================================================")
    if df_daily_agg is None or df_daily_agg.empty or 'date' not in df_daily_agg.columns or df_daily_agg['date'].dropna().empty:
        log_func("\nNo hay datos agregados diarios o fechas válidas."); return

    global_metrics_current= _calcular_metricas_agregadas_y_estabilidad(df_daily_agg,'Global', log_func)
    global_days_count=df_daily_agg['date'].nunique() if not df_daily_agg.empty else 0

    previous_month_metrics=None; previous_month_label="-"; prev_month_date_range=""
    if relativedelta and date_parse and global_days_count>=28 and not df_daily_agg.empty:
        latest_date=df_daily_agg['date'].max(); earliest_date_val=df_daily_agg['date'].min();
        found_current_complete_month=False
        current_check_month_start_dt=(latest_date.replace(day=1)-relativedelta(months=1))
        log_func(f"  Buscando mes completo para comparar (datos: {earliest_date_val.strftime('%d/%m/%y')} - {latest_date.strftime('%d/%m/%y')})...")
        while current_check_month_start_dt.date()>=earliest_date_val.date().replace(day=1):
            current_month_end_dt=current_check_month_start_dt+relativedelta(months=1)-timedelta(days=1)
            log_func(f"    Probando mes: {current_check_month_start_dt.strftime('%Y-%m')}")
            if current_month_end_dt.date()<=latest_date.date() and current_check_month_start_dt.date() >= earliest_date_val.date():
                df_month_subset=df_daily_agg[(df_daily_agg['date'].dt.date>=current_check_month_start_dt.date())&(df_daily_agg['date'].dt.date<=current_month_end_dt.date())].copy();
                actual_days_in_calendar_month=(current_month_end_dt.date()-current_check_month_start_dt.date()).days+1;
                unique_days_in_month_data=df_month_subset['date'].nunique() if not df_month_subset.empty else 0
                if unique_days_in_month_data==actual_days_in_calendar_month:
                    log_func(f"      -> Mes completo encontrado: {current_check_month_start_dt.strftime('%Y-%m')} (será 'Mes Anterior')");
                    previous_month_metrics=_calcular_metricas_agregadas_y_estabilidad(df_month_subset,(current_check_month_start_dt.date(),current_month_end_dt.date()), log_func)
                    previous_month_label=f"Mes Ant. ({current_check_month_start_dt.strftime('%Y-%m')})";
                    prev_month_date_range=previous_month_metrics.get('date_range','')
                    found_current_complete_month = True
                    break
                else:
                    log_func(f"      -> Mes {current_check_month_start_dt.strftime('%Y-%m')} incompleto ({unique_days_in_month_data}/{actual_days_in_calendar_month} días).")
            else:
                log_func(f"      -> Mes {current_check_month_start_dt.strftime('%Y-%m')} fuera del rango de datos o futuro.")
            current_check_month_start_dt-=relativedelta(months=1)
        if not found_current_complete_month: log_func("\nNo se encontró un mes calendario completo anterior en los datos para comparación.")
    elif not (relativedelta and date_parse):
         log_func("\n'python-dateutil' no disponible, no se puede realizar comparación mensual.")
    else:
         log_func(f"\nDatos insuficientes ({global_days_count} días únicos < 28) para comparación mensual significativa.")

    stability_keys=['ROAS_Stability_%','CPA_Stability_%','CPM_Stability_%','CTR_Stability_%','IMPR_Stability_%','CTR_DIV_FREQ_RATIO_Stability_%']
    metric_map={'Inversion Total':{'key':'Inversion','formatter':lambda x: f"{detected_currency}{fmt_float(x,2)}"},'Ventas Totales':{'key':'Ventas_Totales','formatter':lambda x: f"{detected_currency}{fmt_float(x,2)}"},'ROAS Promedio':{'key':'ROAS','formatter':lambda x: f"{fmt_float(x,2)}x"},'Compras Total':{'key':'Compras','formatter':fmt_int},'CPA Promedio':{'key':'CPA','formatter':lambda x: f"{detected_currency}{fmt_float(x,2)}"},'Ticket Promedio':{'key':'Ticket_Promedio','formatter':lambda x: f"{detected_currency}{fmt_float(x,2)}"},'Impresiones Total':{'key':'Impresiones','formatter':fmt_int},'Alcance Total':{'key':'Alcance','formatter':fmt_int},'Frecuencia Promedio':{'key':'Frecuencia','formatter':lambda x: fmt_float(x,2)},'CPM Promedio':{'key':'CPM','formatter':lambda x: f"{detected_currency}{fmt_float(x,2)}"},'Clics Total':{'key':'Clics','formatter':fmt_int},'CTR Promedio (%)':{'key':'CTR','formatter':lambda x: fmt_pct(x,2)},'Visitas Total':{'key':'Visitas','formatter':fmt_int},'Tasa Visita LP (%)':{'key':'LVP_Rate_%','formatter':lambda x: fmt_pct(x,1)},'Tasa Compra (%)':{'key':'Conv_Rate_%','formatter':lambda x: fmt_pct(x,1)},'Estabilidad ROAS (%)':{'key':'ROAS_Stability_%','formatter':fmt_stability},'Estabilidad CPA (%)':{'key':'CPA_Stability_%','formatter':fmt_stability},'Estabilidad CPM (%)':{'key':'CPM_Stability_%','formatter':fmt_stability},'Estabilidad CTR (%)':{'key':'CTR_Stability_%','formatter':fmt_stability},'Estabilidad Impr (%)':{'key':'IMPR_Stability_%','formatter':fmt_stability},'Estabilidad CTR/Freq (%)':{'key':'CTR_DIV_FREQ_RATIO_Stability_%','formatter':fmt_stability}}
    order=['Inversion Total','Ventas Totales','ROAS Promedio','Compras Total','CPA Promedio','Ticket Promedio','Impresiones Total','Alcance Total','Frecuencia Promedio','CPM Promedio','Clics Total','CTR Promedio (%)','Visitas Total','Tasa Visita LP (%)','Tasa Compra (%)','Estabilidad ROAS (%)','Estabilidad CPA (%)','Estabilidad CPM (%)','Estabilidad CTR (%)','Estabilidad Impr (%)','Estabilidad CTR/Freq (%)']
    headers=["Metrica",f"Global ({global_days_count} Dias)"];
    if previous_month_metrics: headers.append(previous_month_label)
    rows=[]
    for disp_name in order:
        info=metric_map.get(disp_name);
        if not info: continue
        key_m=info['key']; fmt=info['formatter']; row=[disp_name]; is_stab='Stability' in key_m
        curr_raw=global_metrics_current.get(key_m,np.nan); disp_curr='-' if (is_stab and not global_metrics_current.get('is_complete',False)) else (fmt(curr_raw) if pd.notna(curr_raw) else '-')
        row.append(disp_curr)
        if previous_month_metrics:
            prev_raw=previous_month_metrics.get(key_m,np.nan); disp_prev='-' if (is_stab and not previous_month_metrics.get('is_complete',False)) else (fmt(prev_raw) if pd.notna(prev_raw) else '-')
            if not is_stab:
                 var_val=variation(curr_raw,prev_raw);
                 row[1]=f"{disp_curr} ({var_val})" if var_val!='-' else disp_curr
            row.append(disp_prev)
        rows.append(row)
    df_disp=pd.DataFrame(rows,columns=headers)
    _format_dataframe_to_markdown(df_disp,"",log_func,currency_cols=detected_currency, stability_cols=stability_keys,numeric_cols_for_alignment=[h for h in headers if h!="Metrica"])
    log_func("\n  **Detalle de Métricas (Global):**");
    log_func(f"  * **Global ({global_days_count} Dias):** Métricas totales (sumas) o promedios/tasas de toda la cuenta para el período completo de datos ({global_metrics_current.get('date_range','desc.')}).")
    if previous_month_metrics:
        log_func(f"  * **{previous_month_label}:** Métricas del mes calendario completo anterior encontrado en los datos ({prev_month_date_range}).")
        log_func(f"  * **Valor en paréntesis (...) en Global:** Variación porcentual de las Métricas Globales comparadas con el Mes Anterior Completo. Una flecha 🔺 indica mejora, 🔻 indica empeoramiento respecto al mes anterior.")
    else: log_func("  * **Mes Ant.:** No se encontró un mes anterior completo en los datos para comparación.")
    log_func("  * **Estabilidad (%):** Mide la consistencia de la métrica diaria dentro del período total. Un % alto indica que la métrica fue estable día a día. Calculada si el período tiene al menos 7 días con datos y cumple umbrales mínimos. Iconos: ✅ >= 50%, 🏆 >= 70%. '-' si no aplica o datos insuficientes.");
    log_func("  ---")


def _generar_tabla_vertical_entidad(entity_level, entity_name, dias_activos_total, df_daily_entity,
                                    min_entity_dt, max_entity_dt, adset_count,
                                    periods, detected_currency, log_func, period_type="Days"):
    header_label=entity_level.capitalize(); date_range_str=f"({min_entity_dt.strftime('%d/%m/%y')} - {max_entity_dt.strftime('%d/%m/%y')})" if min_entity_dt and max_entity_dt else ""; adset_count_str=f"(AdSets: {adset_count}) " if entity_level.lower()=='campaña' and adset_count is not None else ""
    log_func(f"\n\n--------------------------------------------------------------------------------"); log_func(f" {header_label}: {entity_name} {adset_count_str}{date_range_str} (Días Activo Total: {fmt_int(dias_activos_total)})"); log_func(f"--------------------------------------------------------------------------------")
    if df_daily_entity is None or df_daily_entity.empty: log_func("   No hay datos diarios."); return

    global_metrics_raw=_calcular_metricas_agregadas_y_estabilidad(df_daily_entity,'Global', log_func);
    global_days_count=df_daily_entity['date'].nunique() if not df_daily_entity.empty else 0

    results_by_period={}
    period_labels = [] 
    period_details_local = [] 

    if period_type == "Days":
         log_func("  Calculando métricas para períodos U... Dias...");
         periods_numeric = periods 
         for p_days in periods_numeric:
             p_label=f"U{p_days} Dias"; 
             start_date_p=max(max_entity_dt-timedelta(days=p_days-1),min_entity_dt)
             df_period_subset=df_daily_entity[(df_daily_entity['date']>=start_date_p)&(df_daily_entity['date']<=max_entity_dt)].copy();
             results_by_period[p_label]=_calcular_metricas_agregadas_y_estabilidad(df_period_subset,p_days, log_func)
             period_labels.append(p_label)
             period_details_local.append((start_date_p, max_entity_dt, p_label)) 
    elif period_type == "Weeks" or period_type == "Months":
         log_func(f"  Calculando métricas para períodos {period_type}...");
         period_details_local = periods 
         for start_dt, end_dt, label_from_orchestrator in period_details_local:
              df_period_subset = df_daily_entity[
                  (df_daily_entity['date'] >= start_dt) &
                  (df_daily_entity['date'] <= end_dt)
              ].copy();
              period_identifier_tuple = (start_dt.date(), end_dt.date()) 
              results_by_period[label_from_orchestrator] = _calcular_metricas_agregadas_y_estabilidad(df_period_subset, period_identifier_tuple, log_func)
              period_labels.append(label_from_orchestrator) 
              
    global_daily_avg={}; global_rates={}
    if period_type == "Days" and global_metrics_raw and global_days_count>0:
        sum_keys=['Alcance','Impresiones','Inversion','Ventas_Totales','Compras','Clics','Visitas']; rate_keys=['Frecuencia','CPM','CPA','CTR','ROAS','Ticket_Promedio','LVP_Rate_%','Conv_Rate_%']
        for key_s in sum_keys: global_daily_avg[key_s]=safe_division(global_metrics_raw.get(key_s,np.nan),global_days_count)
        for key_r in rate_keys: global_rates[key_r]=global_metrics_raw.get(key_r,np.nan)

    u7_inversion_adset = np.nan; u7_roas_adset = np.nan; prev_7_day_inversion_adset = np.nan; prev_7_day_roas_adset = np.nan; estado_inversion_roas_str = "-"
    results_prev_30 = None 

    if entity_level.lower() == 'adset' and period_type == "Days" and 7 in periods: 
         u7_label = "U7 Dias" 
         if u7_label in results_by_period:
             u7_results = results_by_period[u7_label]
             u7_inversion_adset = u7_results.get('Inversion', np.nan)
             u7_roas_adset = u7_results.get('ROAS', np.nan)

             u7_period_dates_tuple = next(((s,e,l) for s,e,l in period_details_local if l == u7_label), None)
             if u7_period_dates_tuple:
                 u7_start_dt_obj, u7_end_dt_obj, _ = u7_period_dates_tuple 
                 end_prev_7d_obj = u7_start_dt_obj - timedelta(days=1) 
                 start_prev_7d_obj = max(end_prev_7d_obj - timedelta(days=6), min_entity_dt) 

                 if start_prev_7d_obj <= end_prev_7d_obj: 
                     df_prev_7d_subset = df_daily_entity[
                         (df_daily_entity['date'] >= start_prev_7d_obj) &
                         (df_daily_entity['date'] <= end_prev_7d_obj)
                     ].copy();
                     prev_7_days_count = df_prev_7d_subset['date'].nunique()
                     is_prev_7_complete = (prev_7_days_count == 7) 

                     if not df_prev_7d_subset.empty and is_prev_7_complete:
                         metrics_prev_7d = _calcular_metricas_agregadas_y_estabilidad(df_prev_7d_subset, 7, log_func)
                         prev_7_day_inversion_adset = metrics_prev_7d.get('Inversion', np.nan)
                         prev_7_day_roas_adset = metrics_prev_7d.get('ROAS', np.nan)
                         log_func(f"    -> Métricas Prev 7 Días ({start_prev_7d_obj.strftime('%d/%m')}-{end_prev_7d_obj.strftime('%d/%m')}) calculadas (Completas: {is_prev_7_complete}).")
                     else:
                          log_func(f"    -> Período Prev 7 Días ({start_prev_7d_obj.strftime('%d/%m')}-{end_prev_7d_obj.strftime('%d/%m')}) incompleto ({prev_7_days_count} días) o sin datos. No se usará para comparación directa de estado.")

         if pd.isna(prev_7_day_inversion_adset) and global_metrics_raw and global_days_count > 0:
            if pd.notna(u7_inversion_adset) and u7_inversion_adset > 0 or \
               (u7_label in results_by_period and results_by_period[u7_label].get('Impresiones', 0) > 0):
                 log_func("    -> Usando Promedio Diario Global (de esta entidad) x 7 como referencia para Prev 7 Días (para estado).")
                 prev_7_day_inversion_adset = global_daily_avg.get('Inversion', np.nan) * 7 if pd.notna(global_daily_avg.get('Inversion')) else np.nan
                 prev_7_day_roas_adset = global_rates.get('ROAS', np.nan) 

         if pd.notna(u7_inversion_adset) or (u7_label in results_by_period and pd.notna(results_by_period[u7_label].get('Impresiones'))): 
            if pd.notna(u7_inversion_adset) and pd.notna(prev_7_day_inversion_adset) and pd.notna(u7_roas_adset) and pd.notna(prev_7_day_roas_adset):
                inversion_change_pct = 0
                if abs(prev_7_day_inversion_adset) > 1e-9: 
                    inversion_change_pct = ((u7_inversion_adset - prev_7_day_inversion_adset) / prev_7_day_inversion_adset) * 100

                roas_change_pct = 0
                if abs(prev_7_day_roas_adset) > 1e-9: 
                    roas_change_pct = ((u7_roas_adset - prev_7_day_roas_adset) / prev_7_day_roas_adset) * 100
                
                INVERSION_CHANGE_THRESHOLD_PCT = 10.0 
                ROAS_DROP_TOLERANCE_RATIO = 0.5 
                ROAS_CHANGE_THRESHOLD_PCT = 5.0 

                if inversion_change_pct > INVERSION_CHANGE_THRESHOLD_PCT: 
                    if roas_change_pct >= -ROAS_CHANGE_THRESHOLD_PCT: estado_inversion_roas_str = "Positivo 🟢 (↑Inv, >=ROAS)"
                    elif abs(roas_change_pct) <= abs(inversion_change_pct * ROAS_DROP_TOLERANCE_RATIO): estado_inversion_roas_str = "Estable 🟡 (↑Inv, ~ROAS)"
                    else: estado_inversion_roas_str = "Negativo 🔴 (↑Inv, ↓↓ROAS)"
                elif inversion_change_pct < -INVERSION_CHANGE_THRESHOLD_PCT: 
                    if roas_change_pct >= -ROAS_CHANGE_THRESHOLD_PCT: estado_inversion_roas_str = "Estable 🟡 (↓Inv, >=ROAS)"
                    else: estado_inversion_roas_str = "Precaución 🟠 (↓Inv, ↓ROAS)"
                else: 
                    if roas_change_pct > ROAS_CHANGE_THRESHOLD_PCT: estado_inversion_roas_str = "Positivo 🟢 (~Inv, ↑ROAS)"
                    elif roas_change_pct < -ROAS_CHANGE_THRESHOLD_PCT: estado_inversion_roas_str = "Precaución 🟠 (~Inv, ↓ROAS)"
                    else: estado_inversion_roas_str = "Estable 🟡 (~Inv, ~ROAS)"
            else:
                 estado_inversion_roas_str = "Datos Insuficientes U7/Prev7"
    
    if period_type == "Days" and global_days_count >= 60 and relativedelta is not None and 30 in periods and 'U30 Dias' in results_by_period:
        u30_res = results_by_period['U30 Dias']
        if u30_res and u30_res.get('is_complete'): 
            u30_dates_tuple = next(((s,e,l) for s,e,l in period_details_local if l == 'U30 Dias'), None)
            if u30_dates_tuple:
                u30_start_dt_obj, _, _ = u30_dates_tuple 
                prev_30_end_dt_obj = u30_start_dt_obj - timedelta(days=1)
                prev_30_start_dt_obj = max(prev_30_end_dt_obj - timedelta(days=29), min_entity_dt)
                if prev_30_start_dt_obj <= prev_30_end_dt_obj: 
                    df_prev_30_subset = df_daily_entity[
                        (df_daily_entity['date'] >= prev_30_start_dt_obj) &
                        (df_daily_entity['date'] <= prev_30_end_dt_obj)
                    ].copy()
                    results_prev_30 = _calcular_metricas_agregadas_y_estabilidad(df_prev_30_subset, (prev_30_start_dt_obj.date(), prev_30_end_dt_obj.date()), log_func)
                    if results_prev_30 and results_prev_30.get('is_complete'):
                        log_func(f"    -> Métricas Prev 30 Días ({results_prev_30.get('date_range')}) calculadas para comparación U30.")

    metric_map_base_rendimiento={
        'Inversion Total':{'key':'Inversion','formatter':lambda x: f"{detected_currency}{fmt_float(x,2)}",'type':'sum', 'display':'Inversion Total'},
        'Ventas Totales':{'key':'Ventas_Totales','formatter':lambda x: f"{detected_currency}{fmt_float(x,2)}",'type':'sum', 'display':'Ventas Totales'},
        'ROAS Promedio':{'key':'ROAS','formatter':lambda x: f"{fmt_float(x,2)}x",'type':'rate', 'display':'ROAS Promedio'},
        'Compras Total':{'key':'Compras','formatter':fmt_int,'type':'sum', 'display':'Compras Total'},
        'CPA Promedio':{'key':'CPA','formatter':lambda x: f"{detected_currency}{fmt_float(x,2)}",'type':'rate', 'display':'CPA Promedio'},
        'Ticket Promedio':{'key':'Ticket_Promedio','formatter':lambda x: f"{detected_currency}{fmt_float(x,2)}",'type':'rate', 'display':'Ticket Promedio'},
        'Impresiones Total':{'key':'Impresiones','formatter':fmt_int,'type':'sum', 'display':'Impresiones Total'},
        'Alcance Total':{'key':'Alcance','formatter':fmt_int,'type':'sum', 'display':'Alcance Total'},
        'Frecuencia Promedio':{'key':'Frecuencia','formatter':lambda x: fmt_float(x,2),'type':'rate', 'display':'Frecuencia Promedio'},
        'CPM Promedio':{'key':'CPM','formatter':lambda x: f"{detected_currency}{fmt_float(x,2)}",'type':'rate', 'display':'CPM Promedio'},
        'Clics Total':{'key':'Clics','formatter':fmt_int,'type':'sum', 'display':'Clics Total'},
        'CTR Promedio (%)':{'key':'CTR','formatter':lambda x: fmt_pct(x,2),'type':'rate', 'display':'CTR Promedio (%)'},
        'Visitas Total':{'key':'Visitas','formatter':fmt_int,'type':'sum', 'display':'Visitas Total'},
        'Tasa Visita LP (%)':{'key':'LVP_Rate_%','formatter':lambda x: fmt_pct(x,1),'type':'rate', 'display':'Tasa Visita LP (%)'},
        'Tasa Compra (%)':{'key':'Conv_Rate_%','formatter':lambda x: fmt_pct(x,1),'type':'rate', 'display':'Tasa Compra (%)'}
    }
    
    metric_map_video_rendimiento = {} 
    if not (entity_level.lower() in ['campaña', 'adset'] and period_type == "Days"):
        metric_map_video_rendimiento = {
            'Tiempo Promedio RV (s)':{'key':'Tiempo_Promedio','formatter':lambda x: fmt_float(x,1),'type':'rate', 'display':'Tiempo Promedio RV (s)'},
            'RV25 (%)':{'key':'RV25_%','formatter':lambda x: fmt_pct(x,1),'type':'rate', 'display':'RV25 (%)'},
            'RV75 (%)':{'key':'RV75_%','formatter':lambda x: fmt_pct(x,1),'type':'rate', 'display':'RV75 (%)'},
            'RV100 (%)':{'key':'RV100_%','formatter':lambda x: fmt_pct(x,1),'type':'rate', 'display':'RV100 (%)'}
        }
    
    current_metric_map_base = {**metric_map_base_rendimiento, **metric_map_video_rendimiento}

    metric_map_stability = {'Estabilidad ROAS (%)':{'key':'ROAS_Stability_%','formatter':fmt_stability,'type':'stability', 'display':'Estabilidad ROAS (%)'},'Estabilidad CPA (%)':{'key':'CPA_Stability_%','formatter':fmt_stability,'type':'stability', 'display':'Estabilidad CPA (%)'},'Estabilidad CPM (%)':{'key':'CPM_Stability_%','formatter':fmt_stability,'type':'stability', 'display':'Estabilidad CPM (%)'},'Estabilidad CTR (%)':{'key':'CTR_Stability_%','formatter':fmt_stability,'type':'stability', 'display':'Estabilidad CTR (%)'},'Estabilidad Impr (%)':{'key':'IMPR_Stability_%','formatter':fmt_stability,'type':'stability', 'display':'Estabilidad Impr (%)'},'Estabilidad CTR/Freq (%)':{'key':'CTR_DIV_FREQ_RATIO_Stability_%','formatter':fmt_stability,'type':'stability', 'display':'Estabilidad CTR/Freq (%)'}}
    
    metric_map = { v['key']: v for v in list(current_metric_map_base.values()) + list(metric_map_stability.values())} 
    
    order_display_names_base=[ 
        'Inversion Total', 'CPM Promedio', 'Impresiones Total', 'Alcance Total', 'Frecuencia Promedio',
        'Clics Total', 'CTR Promedio (%)', 'Visitas Total', 'Tasa Visita LP (%)',
        'Compras Total', 'Tasa Compra (%)', 'Ventas Totales', 'CPA Promedio', 'Ticket Promedio', 'ROAS Promedio'
    ]
    order_display_names_video = []
    if not (entity_level.lower() in ['campaña', 'adset'] and period_type == "Days"):
        order_display_names_video = ['Tiempo Promedio RV (s)', 'RV25 (%)', 'RV75 (%)', 'RV100 (%)']

    order_display_names_stability = [ 
        'Estabilidad ROAS (%)', 'Estabilidad CPA (%)', 'Estabilidad CPM (%)', 'Estabilidad CTR (%)', 'Estabilidad Impr (%)', 'Estabilidad CTR/Freq (%)'
    ]
    order_display_names = order_display_names_base + order_display_names_video + order_display_names_stability

    headers=["Metrica"] + period_labels 
    can_compare_30d_vertical = False 
    if period_type == "Days" and global_days_count>=60 and relativedelta is not None and 30 in periods: 
        if results_prev_30 and results_prev_30.get('is_complete'): 
            can_compare_30d_vertical=True
            if "Prev 30 Dias" not in headers: headers.append("Prev 30 Dias") 

    rows=[]
    for disp_name_order in order_display_names: 
        internal_key = None
        for k_map, v_map in metric_map.items(): 
            if v_map.get('display') == disp_name_order:
                internal_key = k_map 
                break
        
        if not internal_key: 
            continue

        info = metric_map.get(internal_key) 
        if not info: 
            print(f"Advertencia: No se encontró información para la métrica clave '{internal_key}' en metric_map.")
            continue

        key_m=info['key']; fmt=info['formatter']; mtype=info['type']; row=[disp_name_order]; glob_comp=np.nan

        if period_type == "Days" and global_metrics_raw:
            if mtype=='sum': glob_comp=global_daily_avg.get(key_m,np.nan)
            elif mtype=='rate': glob_comp=global_rates.get(key_m,np.nan)

        for p_label in period_labels: 
             p_res=results_by_period.get(p_label) 
             curr_raw=p_res.get(key_m,np.nan) if p_res is not None else np.nan 
             fmt_val=fmt(curr_raw) if pd.notna(curr_raw) else '-' 
             var_fmt='-' 

             if period_type == "Days" and mtype!='stability' and pd.notna(glob_comp) and pd.notna(curr_raw):
                 p_days_val_search = re.search(r'U(\d+) Dias', p_label)
                 if p_days_val_search:
                     p_days_val = int(p_days_val_search.group(1))
                     p_dates_tuple = next(((s,e,l) for s,e,l in period_details_local if l == p_label), None)
                     act_days = 0
                     if p_dates_tuple:
                        p_start_dt_obj, p_end_dt_obj, _ = p_dates_tuple
                        p_df_temp=df_daily_entity[(df_daily_entity['date']>=p_start_dt_obj)&(df_daily_entity['date']<=p_end_dt_obj)].copy()
                        act_days=p_df_temp['date'].nunique() if not p_df_temp.empty else 0

                     comp_val=np.nan 
                     if mtype=='sum': comp_val=glob_comp*act_days if act_days>0 else np.nan 
                     elif mtype=='rate': comp_val=glob_comp 
                     if pd.notna(comp_val): var_fmt=variation(curr_raw,comp_val) 
             
             disp_val=f"{fmt_val} ({var_fmt})" if period_type == "Days" and mtype!='stability' and var_fmt!='-' and fmt_val!='-' else fmt_val
             if mtype=='stability' and (p_res is None or not p_res.get('is_complete',False)): disp_val='-' 
             row.append(disp_val)

        if period_type == "Days" and can_compare_30d_vertical: 
            prev30_raw=results_prev_30.get(key_m,np.nan) if results_prev_30 is not None else np.nan
            fmt_p30=fmt(prev30_raw) if pd.notna(prev30_raw) else '-'

            if "U30 Dias" in period_labels: 
                u30_index_in_row = period_labels.index("U30 Dias") + 1 
                if u30_index_in_row < len(row): 
                    u30_raw=results_by_period.get("U30 Dias",{}).get(key_m,np.nan)
                    var_vs_l30=variation(u30_raw,prev30_raw) 
                    u30_fmt_val = fmt(u30_raw) if pd.notna(u30_raw) else '-'
                    u30_comp_str = f" ({var_vs_l30})" if var_vs_l30 != '-' else ""
                    u30_disp_val = f"{u30_fmt_val}{u30_comp_str}" if mtype!='stability' and u30_fmt_val != '-' else u30_fmt_val
                    
                    if mtype=='stability' and (results_by_period.get("U30 Dias") is None or not results_by_period.get("U30 Dias",{}).get('is_complete',False)): u30_disp_val='-'
                    row[u30_index_in_row] = u30_disp_val 
            
            disp_p30=fmt_p30 
            if mtype=='stability' and (results_prev_30 is None or not results_prev_30.get('is_complete',False)): disp_p30='-'
            row.append(disp_p30) 
        rows.append(row)

    if entity_level.lower() == 'adset' and period_type == "Days" and estado_inversion_roas_str != "-":
        row_estado_display = ["Estado Inversión/ROAS (U7D)"]
        u7_col_index = next((i for i, label in enumerate(period_labels) if label == "U7 Dias"), None) 
        if u7_col_index is not None:
             placeholders_needed = len(headers) - 1 
             full_row_estado = ["-"] * placeholders_needed 
             full_row_estado[u7_col_index] = estado_inversion_roas_str 
             row_estado_display.extend(full_row_estado)
        
        if len(row_estado_display) == len(headers): 
             rows.append(row_estado_display)
        elif estado_inversion_roas_str != "-": 
             log_func("Adv: No se pudo alinear la fila 'Estado Inversión/ROAS (U7D)' con las columnas de período. Omitiendo.")

    df_disp = pd.DataFrame(rows, columns=headers)
    numeric_cols_for_alignment = [h for h in headers if h != "Metrica" and "Estado Inversión/ROAS (U7D)" not in h] 
    stability_cols_display = [v['display'] for k,v in metric_map.items() if 'Stability' in k] 
    _format_dataframe_to_markdown(df_disp, "", log_func, currency_cols=detected_currency,
                                  stability_cols=stability_cols_display, 
                                  numeric_cols_for_alignment=numeric_cols_for_alignment)

    log_func("\n  **Detalle de Métricas de Rendimiento por Entidad:**");
    if period_type == "Days":
         log_func("  * **UX Dias:** Métricas acumuladas (sumas) o promedios/tasas para los últimos X días indicados, específicas para esta entidad.")
         log_func("  * **Valor en paréntesis (...) en UX Dias:** Compara el valor del período UX Días contra el promedio diario histórico de *esta entidad* (proyectado para X días si es una suma, o directo si es una tasa/promedio). Una flecha 🔺 indica mejora, 🔻 indica empeoramiento respecto al histórico de la entidad. Ayuda a ver si el rendimiento reciente es mejor o peor que su histórico.")
         if can_compare_30d_vertical: log_func("  * **Col 'Prev 30 Dias':** Valor para los 30 días anteriores al período 'U30 Dias'. **(...)** en 'U30 Dias' es la variación de U30 vs Prev 30 Dias para esta entidad.")
    elif period_type == "Weeks":
         log_func("  * **Columnas (Semana actual, Xª semana anterior):** Muestran las métricas acumuladas (sumas) o promedios/tasas para cada semana definida (Actual a más antigua, de izq. a der.), específicas para esta entidad.")
         log_func("  * **Valor en paréntesis (...) en columnas Semanales:** Compara el valor de esa semana con la semana *inmediatamente anterior* mostrada (la columna a su derecha). Muestra la variación porcentual semana a semana (WoW). Una flecha 🔺 indica mejora, 🔻 indica empeoramiento respecto a la semana anterior.")
    elif period_type == "Months":
         log_func("  * **Columnas (Mes Actual, Mes Ant. 1):** Muestran las métricas acumuladas (sumas) o promedios/tasas para los 2 últimos meses calendario completos detectados, específicas para esta entidad.")
         log_func("  * **Valor en paréntesis (...) en 'Mes Actual':** Compara el valor del Mes Actual con el Mes Anterior mostrado. Muestra la variación porcentual mes a mes (MoM). Una flecha 🔺 indica mejora, 🔻 indica empeoramiento respecto al mes anterior.")

    log_func("  * **Estabilidad (%):** Mide la consistencia diaria *dentro* del período indicado. Un % alto indica que la métrica fue estable día a día. Se calcula si el período tiene todos sus días con datos y cumple umbrales mínimos. Iconos: ✅ >= 50%, 🏆 >= 70%.")

    if entity_level.lower() == 'adset' and period_type == "Days":
        log_func("  * **Estado Inversión/ROAS (U7D):** (Solo para AdSets) Evalúa cambio en inversión y ROAS en U7D vs los 7 días anteriores (o vs promedio global si no hay datos previos suficientes):")
        log_func("    - **Positivo 🟢:** ↑Inversión con ROAS igual/mejor O ≈Inversión con ↑ROAS.")
        log_func("    - **Estable 🟡:** ↑Inversión con leve caída ROAS O ↓Inversión con ROAS igual/mejor O ≈Inversión y ≈ROAS.")
        log_func("    - **Precaución 🟠:** ↓Inversión con ↓ROAS O ≈Inversión con ↓ROAS.")
        log_func("    - **Negativo 🔴:** ↑Inversión con caída significativa de ROAS.")
    log_func("  ---")

def _generar_tabla_embudo_rendimiento(df_daily_agg, periods_numeric, log_func, detected_currency):
    log_func("\n\n============================================================");log_func("===== 4. Análisis de Embudo por Período (vs Promedio U30 Proyectado) =====");log_func("============================================================")
    funnel_steps_config=[ 
        ('Impresiones','impr'),
        ('Clics (Enlace)','clicks'),
        ('Clics Salientes','clicks_out'),
        ('Visitas LP','visits'),
        ('Atención (Hook)','attention'), 
        ('Interés (Body)','interest'),
        ('Deseo (CTA)','deseo'),
        ('Añadido al Carrito','addcart'),
        ('Inicio de Pago','checkout'),
        ('Compras','purchases')
    ]
    if df_daily_agg is None or df_daily_agg.empty or 'date' not in df_daily_agg.columns or df_daily_agg['date'].dropna().empty:
        log_func(f"\nNo hay datos o fechas válidas para embudo."); return

    available_internal_cols=[s[1] for s in funnel_steps_config if s[1] in df_daily_agg.columns] 
    funnel_steps_config_available=[s for s in funnel_steps_config if s[1] in available_internal_cols] 
    if not funnel_steps_config_available: log_func(f"\nNo hay columnas del embudo disponibles en los datos agregados."); return
    log_func(f"Columnas embudo disponibles: {[s[0] for s in funnel_steps_config_available]}")

    max_date_val=df_daily_agg['date'].max();min_data_date_val=df_daily_agg['date'].min()
    u30_start_val=max(max_date_val-timedelta(days=30-1),min_data_date_val) 
    df_u30_subset=df_daily_agg[(df_daily_agg['date']>=u30_start_val)&(df_daily_agg['date']<=max_date_val)].copy();
    u30_sums=df_u30_subset[available_internal_cols].sum(skipna=True);u30_actual_days=df_u30_subset['date'].nunique()
    u30_daily_avg=safe_division(u30_sums,u30_actual_days) if u30_actual_days>0 else pd.Series(0.0,index=u30_sums.index) 
    log_func(f"Promedio diario U30 para proyección basado en {u30_actual_days} días reales.")

    period_sums={}; period_days={}
    for p_days in periods_numeric: 
        p_label=f"U{p_days}"; p_start_date=max(max_date_val-timedelta(days=p_days-1),min_data_date_val)
        if p_start_date <= max_date_val: 
             df_p_subset=df_daily_agg[(df_daily_agg['date']>=p_start_date)&(df_daily_agg['date']<=max_date_val)].copy();
             period_sums[p_label]=df_p_subset[available_internal_cols].sum(skipna=True) 
             period_days[p_label]=df_p_subset['date'].nunique() 
        else: 
             period_sums[p_label] = pd.Series(0.0, index=available_internal_cols)
             period_days[p_label] = 0

    headers=["Paso del Embudo"]; [headers.extend([f"U{p} (Proj)",f"% U{p}"]) for p in periods_numeric] 
    formatted_rows_data=[];last_step_total=pd.Series(np.nan,index=[f"U{p}" for p in periods_numeric]) 

    for idx, (disp_name,int_col) in enumerate(funnel_steps_config_available): 
        row_vals=[disp_name];u30_avg_val=u30_daily_avg.get(int_col,np.nan);curr_step_total=pd.Series(np.nan,index=[f"U{p}" for p in periods_numeric])
        for p_days in periods_numeric: 
            p_label=f"U{p_days}";actual_total=period_sums.get(p_label,{}).get(int_col,np.nan);actual_days=period_days.get(p_label,0)
            proj_val=u30_avg_val*actual_days if pd.notna(u30_avg_val) and actual_days>0 else np.nan 
            fmt_act=fmt_int(actual_total);fmt_proj=fmt_int(proj_val); proj_check='' 
            if pd.notna(actual_total) and pd.notna(proj_val) and abs(proj_val) > 1e-9 :
                diff_pct=abs(actual_total-proj_val)/abs(proj_val)*100;
                if diff_pct<=20.0: proj_check=' ✅' 
            pct_vs_prev=np.nan 
            if idx > 0: 
                last_total_val=last_step_total.get(p_label,np.nan)
                if pd.notna(last_total_val) and last_total_val>0 and pd.notna(actual_total):
                     pct_vs_prev=safe_division(actual_total,last_total_val)*100 
            pct_prev_fmt=format_step_pct(pct_vs_prev) if idx > 0 else '-' 
            row_vals.append(f"{fmt_act} ({fmt_proj}){proj_check}"); row_vals.append(pct_prev_fmt); 
            curr_step_total[p_label]=actual_total if pd.notna(actual_total) else 0 
        formatted_rows_data.append(row_vals);last_step_total=curr_step_total.copy()

    df_temp_display=pd.DataFrame(formatted_rows_data,columns=headers) 
    _format_dataframe_to_markdown(df_temp_display,"",log_func,numeric_cols_for_alignment=[h for h in headers if h!="Paso del Embudo"]) 
    log_func("\n  **Detalle de Métricas (Embudo de Rendimiento):**");
    log_func("  * **Paso del Embudo:** Etapa del proceso de conversión, desde la exposición inicial (Impresiones) hasta la acción final (Compras). Los datos aquí son el total de la cuenta para cada paso.")
    log_func("  * **UX (Proj):** Muestra el valor *Real* acumulado para esa etapa en los últimos X días. El valor entre paréntesis **(...)** es la *Proyección* de lo que se esperaría para ese paso, basada en el rendimiento promedio diario de los últimos 30 días (U30) de toda la cuenta, ajustado a la cantidad de días del período UX.")
    log_func("  * **✅ (Checkmark):** Indica que el valor Real del paso está dentro de un +/-20% de su Proyección, sugiriendo un rendimiento acorde a lo esperado recientemente para toda la cuenta.")
    log_func("  * **% UX:** Es la tasa de conversión o 'tasa de paso' de esta etapa con respecto a la etapa *anterior* en el embudo (ej. Clics/Impresiones). Muestra qué porcentaje de usuarios avanzó de un paso al siguiente. La **Flecha (🔺/🔻)** indica si este porcentaje de paso es mayor o menor que el 100% (tasas menores son normales). '-' para el primer paso.")
    log_func("  * _Nota:_ La disponibilidad de pasos como Clics Salientes, Atención, Interés y Deseo depende de si estos datos están presentes en los archivos de origen."); log_func("  ---")


def _generar_tabla_embudo_bitacora(df_daily_agg, bitacora_periods_list, log_func, detected_currency, period_type="Weeks"):
    original_locale = locale.getlocale(locale.LC_TIME)
    try:
        locale_candidates = ['es_ES.UTF-8', 'es_ES', 'Spanish_Spain', 'Spanish']
        locale_set = False
        for loc_candidate in locale_candidates:
            try:
                locale.setlocale(locale.LC_TIME, loc_candidate)
                locale_set = True
                break
            except locale.Error:
                continue
        if not locale_set:
            log_func("Adv: No se pudo configurar el locale a español para nombres de meses en embudo bitácora. Se usarán nombres en inglés por defecto.")
    except Exception as e_locale_set_embudo:
         log_func(f"Adv: Error al intentar configurar locale en embudo bitácora: {e_locale_set_embudo}")


    log_func("\n\n============================================================")
    title_comp = "Semanal" if period_type == "Weeks" else "Mensual"
    log_func(f"===== Análisis de Embudo - Comparativa {title_comp} =====")
    log_func("============================================================")

    funnel_steps_config=[
        ('Impresiones','impr'), ('Clics (Enlace)','clicks'), ('Clics Salientes','clicks_out'),
        ('Visitas LP','visits'), ('Atención (Hook)','attention'), ('Interés (Body)','interest'),
        ('Deseo (CTA)','deseo'), ('Añadido al Carrito','addcart'), ('Inicio de Pago','checkout'),
        ('Compras','purchases')
    ]
    if df_daily_agg is None or df_daily_agg.empty or 'date' not in df_daily_agg.columns or df_daily_agg['date'].dropna().empty:
        log_func(f"\nNo hay datos o fechas válidas para embudo de bitácora."); return

    available_internal_cols=[s[1] for s in funnel_steps_config if s[1] in df_daily_agg.columns]
    funnel_steps_config_available=[s for s in funnel_steps_config if s[1] in available_internal_cols]
    if not funnel_steps_config_available: 
        log_func(f"\nNo hay columnas del embudo disponibles para bitácora.") 
        df_temp_display = pd.DataFrame(columns=["Paso del Embudo"] + [f"Período {i+1}" for i in range(len(bitacora_periods_list))])
        _format_dataframe_to_markdown(df_temp_display,"",log_func,numeric_cols_for_alignment=[])
        try: locale.setlocale(locale.LC_TIME, original_locale) 
        except: pass
        return
    log_func(f"Columnas embudo disponibles: {[s[0] for s in funnel_steps_config_available]}")
    
    existing_numeric_cols_in_agg = [col for col in numeric_internal_cols if col in df_daily_agg.columns]
    if not existing_numeric_cols_in_agg:
         log_func("Adv: No se encontraron columnas numéricas para agregar en la bitácora del embudo.");
         df_temp_display = pd.DataFrame(columns=["Paso del Embudo"] + [f"Período {i+1}" for i in range(len(bitacora_periods_list))])
         _format_dataframe_to_markdown(df_temp_display,"",log_func,numeric_cols_for_alignment=[])
         try: locale.setlocale(locale.LC_TIME, original_locale)
         except: pass
         return

    df_daily_total_for_bitacora = df_daily_agg.groupby('date', as_index=False, observed=True)[existing_numeric_cols_in_agg].sum()
    s_tot=df_daily_total_for_bitacora.get('spend',pd.Series(np.nan,index=df_daily_total_for_bitacora.index));
    i_tot=df_daily_total_for_bitacora.get('impr',pd.Series(np.nan,index=df_daily_total_for_bitacora.index))
    c_tot=df_daily_total_for_bitacora.get('clicks',pd.Series(np.nan,index=df_daily_total_for_bitacora.index))
    co_tot=df_daily_total_for_bitacora.get('clicks_out',pd.Series(np.nan,index=df_daily_total_for_bitacora.index))
    vi_tot=df_daily_total_for_bitacora.get('visits',pd.Series(np.nan,index=df_daily_total_for_bitacora.index))
    p_tot=df_daily_total_for_bitacora.get('purchases',pd.Series(np.nan,index=df_daily_total_for_bitacora.index))
    rv3_tot=df_daily_total_for_bitacora.get('rv3',pd.Series(np.nan,index=df_daily_total_for_bitacora.index))
    rv25_tot=df_daily_total_for_bitacora.get('rv25',pd.Series(np.nan,index=df_daily_total_for_bitacora.index))
    rv75_tot=df_daily_total_for_bitacora.get('rv75',pd.Series(np.nan,index=df_daily_total_for_bitacora.index))
    rv100_tot=df_daily_total_for_bitacora.get('rv100',pd.Series(np.nan,index=df_daily_total_for_bitacora.index))


    df_daily_total_for_bitacora['ctr']=safe_division_pct(c_tot,i_tot)
    df_daily_total_for_bitacora['ctr_out'] = safe_division_pct(co_tot, i_tot)
    df_daily_total_for_bitacora['lpv_rate']=safe_division_pct(vi_tot,c_tot)
    df_daily_total_for_bitacora['purchase_rate']=safe_division_pct(p_tot,vi_tot)
    
    base_rv_tot = np.where(pd.Series(rv3_tot > 0).fillna(False), rv3_tot, i_tot)
    df_daily_total_for_bitacora['rv25_pct_daily'] = safe_division_pct(rv25_tot, base_rv_tot)
    df_daily_total_for_bitacora['rv75_pct_daily'] = safe_division_pct(rv75_tot, base_rv_tot)
    df_daily_total_for_bitacora['rv100_pct_daily'] = safe_division_pct(rv100_tot, base_rv_tot)

    period_sums = {}
    cols_to_sum_in_period = available_internal_cols + [c for c in ['ctr', 'ctr_out', 'lpv_rate', 'purchase_rate', 'rv25_pct_daily', 'rv75_pct_daily', 'rv100_pct_daily'] if c in df_daily_total_for_bitacora.columns]

    for start_dt, end_dt, original_label_from_list in bitacora_periods_list:
        df_p_subset = df_daily_total_for_bitacora[(df_daily_total_for_bitacora['date'] >= start_dt) & (df_daily_total_for_bitacora['date'] <= end_dt)].copy()
        period_sums[original_label_from_list] = df_p_subset[cols_to_sum_in_period].sum(skipna=True)

    headers_with_pct = ["Paso del Embudo"]
    for i, (start_dt_period, end_dt_period, period_label_formatted) in enumerate(bitacora_periods_list):
        headers_with_pct.append(period_label_formatted) 
        header_base_name = period_label_formatted.split(" (")[0] if " (" in period_label_formatted else period_label_formatted
        headers_with_pct.append(f"% Paso ({header_base_name})")


    formatted_rows_data=[]
    previous_step_totals = {p[2]: np.nan for p in bitacora_periods_list} 

    funnel_internal_cols_ordered = [s[1] for s in funnel_steps_config_available]
    funnel_display_names_ordered = [s[0] for s in funnel_steps_config_available]

    for idx_embudo, int_col_embudo in enumerate(funnel_internal_cols_ordered):
        disp_name_embudo = funnel_display_names_ordered[idx_embudo]
        row_vals = [disp_name_embudo]
        current_step_totals_for_this_embudo_step = {}

        for i_period, (_, _, original_period_label) in enumerate(bitacora_periods_list):
            actual_total_paso_embudo = period_sums.get(original_period_label, {}).get(int_col_embudo, np.nan)
            current_step_totals_for_this_embudo_step[original_period_label] = actual_total_paso_embudo if pd.notna(actual_total_paso_embudo) else 0
            
            fmt_act_total = fmt_int(actual_total_paso_embudo)
            row_vals.append(fmt_act_total) 

            pct_vs_prev_embudo_step = np.nan
            if idx_embudo > 0 and original_period_label in previous_step_totals:
                total_paso_anterior_embudo = previous_step_totals[original_period_label]
                if pd.notna(actual_total_paso_embudo) and pd.notna(total_paso_anterior_embudo) and abs(total_paso_anterior_embudo) > 1e-9:
                     pct_vs_prev_embudo_step = safe_division_pct(actual_total_paso_embudo, total_paso_anterior_embudo)
            
            pct_prev_fmt = str(format_step_pct(pct_vs_prev_embudo_step)) if pd.notna(pct_vs_prev_embudo_step) else '-'
            row_vals.append(pct_prev_fmt)
        
        formatted_rows_data.append(row_vals)
        previous_step_totals = current_step_totals_for_this_embudo_step

    if not formatted_rows_data:
        log_func("No se generaron datos para la tabla de embudo de bitácora.")
    else:
        df_temp_display=pd.DataFrame(formatted_rows_data,columns=headers_with_pct)
        _format_dataframe_to_markdown(df_temp_display,"",log_func,numeric_cols_for_alignment=[h for h in headers_with_pct if h!="Paso del Embudo"])
    
    log_func("\n  **Detalle de Métricas (Embudo de Bitácora):**");
    log_func(f"  * **Paso del Embudo:** Etapa del proceso de conversión (datos agregados de cuenta completa).")
    log_func(f"  * **Columnas ({'Semana actual, Xª semana anterior' if period_type == 'Weeks' else 'Mes actual, Xº mes anterior'}):** Muestran el valor *Real* acumulado para esa etapa en el período indicado.")
    log_func(f"  * **% Paso ({'Semana/Mes'}):** Es la tasa de conversión de esta etapa con respecto a la etapa *anterior en el embudo* (ej. Clics/Impresiones) DENTRO DEL MISMO PERÍODO. La Flecha (🔺/🔻) indica si este porcentaje de paso es mayor o menor que el 100%. '-' para el primer paso.");
    log_func("  ---")
    try: locale.setlocale(locale.LC_TIME, original_locale) 
    except: pass

def _generar_analisis_ads(df_combined, df_daily_agg, active_days_total_ad_df, log_func, detected_currency, last_day_status_lookup=None):
    log_func("\n\n============================================================");log_func("===== 5. Análisis Consolidado de ADS =====");log_func("=====     (Filtro: Ads con Gasto > 0, Impresiones > 0 Y Días Activos > 0) =====");log_func("============================================================")
    essential_cols=['Campaign','AdSet','Anuncio','date','spend','impr']; 
    if df_daily_agg is None or df_daily_agg.empty or not all(c_col in df_daily_agg.columns for c_col in essential_cols) or df_daily_agg['date'].dropna().empty:
        log_func("\nNo hay datos/columnas esenciales/fechas válidas para análisis ADS."); return
    group_cols_ad=['Campaign','AdSet','Anuncio']; active_days_cols=group_cols_ad+['Días_Activo_Total'] 
    if active_days_total_ad_df is None or active_days_total_ad_df.empty or not all(c_col in active_days_total_ad_df.columns for c_col in active_days_cols):
        log_func("Adv: Datos Días Activos no disponibles para Ads. Se asumirá 0.");
        active_days_total_ad_df=pd.DataFrame(columns=active_days_cols) 

    df_daily_agg_copy = df_daily_agg.copy() 
    for col in group_cols_ad:
        if col in df_daily_agg_copy.columns:
             df_daily_agg_copy[col] = df_daily_agg_copy[col].astype(str)

    max_date_val=df_daily_agg_copy['date'].max(); min_data_date_val=df_daily_agg_copy['date'].min() 
    log_func("\nAgregando métricas globales por Anuncio...", importante=True);
    agg_dict_base={
        'spend':'sum','value':'sum','purchases':'sum','clicks':'sum','clicks_out':'sum',
        'impr':'sum','reach':'sum','visits':'sum','rv3':'sum','rv25':'sum','rv75':'sum','rv100':'sum',
        'rtime':'mean','frequency':'mean','cpm':'mean','ctr':'mean','ctr_out':'mean',
        'roas':'mean','cpa':'mean',
        'rv25_pct':'mean','rv75_pct':'mean','rv100_pct':'mean', 
        'Públicos In':lambda x:aggregate_strings(x,separator=' | ',max_len=None), 
        'Públicos Ex':lambda x:aggregate_strings(x,separator=' | ',max_len=None)
    }
    agg_dict_ad_global_available={k:v for k,v in agg_dict_base.items() if k in df_daily_agg_copy.columns} 
    if not agg_dict_ad_global_available: log_func("Adv: No hay columnas para agregación global Ads."); return

    ad_global_metrics_raw=df_daily_agg_copy.groupby(group_cols_ad,as_index=False,observed=False).agg(agg_dict_ad_global_available) 

    if all(c_col in ad_global_metrics_raw.columns for c_col in ['value','spend']): ad_global_metrics_raw['roas']=safe_division(ad_global_metrics_raw['value'],ad_global_metrics_raw['spend'])
    if all(c_col in ad_global_metrics_raw.columns for c_col in ['spend','purchases']): ad_global_metrics_raw['cpa']=safe_division(ad_global_metrics_raw['spend'],ad_global_metrics_raw['purchases'])
    if all(c_col in ad_global_metrics_raw.columns for c_col in ['clicks','impr']): ad_global_metrics_raw['ctr']=safe_division_pct(ad_global_metrics_raw['clicks'],ad_global_metrics_raw['impr'])
    if all(c_col in ad_global_metrics_raw.columns for c_col in ['clicks_out','impr']): ad_global_metrics_raw['ctr_out']=safe_division_pct(ad_global_metrics_raw['clicks_out'],ad_global_metrics_raw['impr'])
    if all(c_col in ad_global_metrics_raw.columns for c_col in ['spend','impr']): ad_global_metrics_raw['cpm']=safe_division(ad_global_metrics_raw['spend'],ad_global_metrics_raw['impr'])*1000
    if all(c_col in ad_global_metrics_raw.columns for c_col in ['impr','reach']): ad_global_metrics_raw['frequency']=safe_division(ad_global_metrics_raw['impr'],ad_global_metrics_raw['reach'])
    base_rv_col_g = 'rv3' if 'rv3' in ad_global_metrics_raw.columns and ad_global_metrics_raw['rv3'].sum() > 0 else 'impr'
    if base_rv_col_g in ad_global_metrics_raw.columns:
        base_sum_g = ad_global_metrics_raw[base_rv_col_g]
        if 'rv25' in ad_global_metrics_raw.columns: ad_global_metrics_raw['rv25_pct'] = safe_division_pct(ad_global_metrics_raw['rv25'], base_sum_g)
        if 'rv75' in ad_global_metrics_raw.columns: ad_global_metrics_raw['rv75_pct'] = safe_division_pct(ad_global_metrics_raw['rv75'], base_sum_g)
        if 'rv100' in ad_global_metrics_raw.columns: ad_global_metrics_raw['rv100_pct'] = safe_division_pct(ad_global_metrics_raw['rv100'], base_sum_g)

    log_func(f"Agregación global OK ({len(ad_global_metrics_raw)} filas).")

    ad_period_metrics_raw={}; periods_to_calc=[3,7] 
    for p_days in periods_to_calc:
        log_func(f"Calculando métricas U{p_days}...");
        period_start_date=max(max_date_val-timedelta(days=p_days-1),min_data_date_val) 
        if period_start_date <= max_date_val: 
            df_daily_period=df_daily_agg_copy[(df_daily_agg_copy['date']>=period_start_date)&(df_daily_agg_copy['date']<=max_date_val)].copy();
        else: 
            df_daily_period = pd.DataFrame(columns=df_daily_agg_copy.columns)

        if df_daily_period.empty: ad_period_metrics_raw[p_days]=pd.DataFrame(columns=group_cols_ad + list(agg_dict_base.keys())) 
        else: 
            agg_dict_period={k:v for k,v in agg_dict_base.items() if k in df_daily_period.columns} 
            if not agg_dict_period: ad_period_metrics_raw[p_days]=pd.DataFrame(columns=group_cols_ad + list(agg_dict_base.keys())) 
            else: 
                 df_p=df_daily_period.groupby(group_cols_ad,as_index=False,observed=False).agg(agg_dict_period)
                 if all(c_col in df_p.columns for c_col in ['value','spend']): df_p['roas']=safe_division(df_p['value'],df_p['spend'])
                 if all(c_col in df_p.columns for c_col in ['spend','purchases']): df_p['cpa']=safe_division(df_p['spend'],df_p['purchases'])
                 if all(c_col in df_p.columns for c_col in ['clicks','impr']): df_p['ctr']=safe_division_pct(df_p['clicks'],df_p['impr'])
                 if all(c_col in df_p.columns for c_col in ['clicks_out','impr']): df_p['ctr_out']=safe_division_pct(df_p['clicks_out'],df_p['impr'])
                 if all(c_col in df_p.columns for c_col in ['spend','impr']): df_p['cpm']=safe_division(df_p['spend'],df_p['impr'])*1000
                 if all(c_col in df_p.columns for c_col in ['impr','reach']): df_p['frequency']=safe_division(df_p['impr'],df_p['reach'])
                 base_rv_col_p = 'rv3' if 'rv3' in df_p.columns and df_p['rv3'].sum() > 0 else 'impr'
                 if base_rv_col_p in df_p.columns:
                     base_sum_p = df_p[base_rv_col_p]
                     if 'rv25' in df_p.columns: df_p['rv25_pct'] = safe_division_pct(df_p['rv25'], base_sum_p)
                     if 'rv75' in df_p.columns: df_p['rv75_pct'] = safe_division_pct(df_p['rv75'], base_sum_p)
                     if 'rv100' in df_p.columns: df_p['rv100_pct'] = safe_division_pct(df_p['rv100'], base_sum_p)
                 ad_period_metrics_raw[p_days]=df_p 
        log_func(f"Agregación U{p_days} OK ({len(ad_period_metrics_raw[p_days]) if p_days in ad_period_metrics_raw else 0} filas).")

    log_func("Fusionando métricas..."); ad_metrics=ad_global_metrics_raw.copy() 
    ad_metrics.columns=[f"{c_col}_global" if c_col not in group_cols_ad else c_col for c_col in ad_metrics.columns] 
    for p_days in periods_to_calc: 
        if p_days in ad_period_metrics_raw:
            df_p=ad_period_metrics_raw[p_days];
            if not df_p.empty:
                 merge_cols=[c_col for c_col in group_cols_ad if c_col in ad_metrics.columns and c_col in df_p.columns] 
                 if merge_cols:
                     for col in merge_cols:
                         ad_metrics[col] = ad_metrics[col].astype(str)
                         df_p[col] = df_p[col].astype(str)
                     df_p_renamed=df_p.rename(columns={c_col:f"{c_col}_u{p_days}" for c_col in df_p.columns if c_col not in group_cols_ad}) 
                     ad_metrics=pd.merge(ad_metrics,df_p_renamed,on=merge_cols,how='left') 
                 else: log_func(f"WARN: No se encontraron columnas comunes para fusionar métricas U{p_days}.")
            else: 
                 log_func(f"WARN: Datos U{p_days} vacíos. Rellenando columnas con NaN.")
                 for c_base in agg_dict_base.keys(): 
                     c_p_name=f"{c_base}_u{p_days}";
                     if c_p_name not in ad_metrics.columns: ad_metrics[c_p_name]=np.nan 
        else: 
            log_func(f"WARN: No hay datos para U{p_days}. Rellenando columnas con NaN.")
            for c_base in agg_dict_base.keys():
                 c_p_name=f"{c_base}_u{p_days}";
                 if c_p_name not in ad_metrics.columns: ad_metrics[c_p_name]=np.nan
    log_func(f"Fusión periodos OK.")

    log_func("Fusionando Días Activos..."); 
    if not active_days_total_ad_df.empty and 'Días_Activo_Total' in active_days_total_ad_df.columns:
        merge_cols=[c_col for c_col in group_cols_ad if c_col in active_days_total_ad_df.columns] 
        if merge_cols:
             for col in merge_cols:
                 ad_metrics[col] = ad_metrics[col].astype(str)
                 active_days_total_ad_df[col] = active_days_total_ad_df[col].astype(str)
             ad_metrics=pd.merge(ad_metrics,active_days_total_ad_df[merge_cols+['Días_Activo_Total']],on=merge_cols,how='left');
        else: log_func("Adv: No cols comunes para fusionar Días Activos.")
    if 'Días_Activo_Total' not in ad_metrics.columns: ad_metrics['Días_Activo_Total'] = 0 
    ad_metrics['Días_Activo_Total']=ad_metrics['Días_Activo_Total'].fillna(0).astype(int); 
    log_func("Fusión Días Activos OK.")

    log_func("Fusionando Estado Último Día..."); 
    if last_day_status_lookup is not None and not last_day_status_lookup.empty:
         s_cols_prio=['ad_delivery_status','adset_delivery_status','campaign_delivery_status', 'entrega']; 
         s_col_use=next((c_col for c_col in s_cols_prio if c_col in last_day_status_lookup.columns),None) 
         if s_col_use:
              merge_cols_s=[c_col for c_col in group_cols_ad if c_col in last_day_status_lookup.columns] 
              if merge_cols_s:
                 for col in merge_cols_s:
                      ad_metrics[col] = ad_metrics[col].astype(str)
                      last_day_status_lookup[col] = last_day_status_lookup[col].astype(str)
                 last_day_df=last_day_status_lookup[merge_cols_s+[s_col_use]].rename(columns={s_col_use:'Estado_Raw'}).drop_duplicates(subset=merge_cols_s,keep='last') 
                 ad_metrics=pd.merge(ad_metrics,last_day_df,on=merge_cols_s,how='left') 
                 map_last={'active':'Activo ✅','inactive':'Inactivo ❌','not_delivering':'Sin Entrega ⚠️','rejected':'Rechazado ⛔','pending_review':'Pendiente ⏳', 'archived':'Archivado 📦', 'completed': 'Completado🏁', 'limited':'Limitado🤏', 'not approved':'No Aprob.🚫'} 
                 ad_metrics['Estado_Ult_Dia']=ad_metrics['Estado_Raw'].fillna('Desc.').astype(str).str.lower().str.replace('_',' ').str.strip().map(map_last).fillna('Otro ?') 
                 log_func(f"Fusión Estado OK (usando '{s_col_use}').")
              else: log_func("Adv: No cols comunes para fusionar estado.")
         else: log_func("Adv: No se encontró columna de estado adecuada en lookup.")
    if 'Estado_Ult_Dia' not in ad_metrics.columns: ad_metrics['Estado_Ult_Dia']='Desc. ?' 

    log_func("Calculando U7 Estabilidad CPM..."); ad_metrics['cpm_stability_u7']=np.nan 
    u7_start_val=max(max_date_val-timedelta(days=6),min_data_date_val); 
    if u7_start_val <= max_date_val: 
        df_u7_stab=df_daily_agg_copy[(df_daily_agg_copy['date']>=u7_start_val)&(df_daily_agg_copy['date']<=max_date_val)&(df_daily_agg_copy['cpm'].notna())&(np.isfinite(df_daily_agg_copy['cpm']))].copy() 
        if not df_u7_stab.empty and 'cpm' in df_u7_stab.columns:
            try:
                grouped=df_u7_stab.groupby(group_cols_ad,observed=False)['cpm']; 
                stab_res=grouped.apply(lambda s_series:_calculate_stability_pct(s_series) if s_series.count()>=2 else np.nan).rename('cpm_stability_u7_calc') 
                merge_cols_st=[c_col for c_col in group_cols_ad if c_col in ad_metrics.columns and c_col in stab_res.index.names] 
                if merge_cols_st and not stab_res.empty:
                    ad_metrics=pd.merge(ad_metrics,stab_res.reset_index(),on=merge_cols_st,how='left') 
                    ad_metrics['cpm_stability_u7']=ad_metrics['cpm_stability_u7_calc'] 
                    if 'cpm_stability_u7_calc' in ad_metrics.columns: 
                        ad_metrics=ad_metrics.drop(columns=['cpm_stability_u7_calc'],errors='ignore')
                    processed=ad_metrics['cpm_stability_u7'].notna().sum(); log_func(f"Estabilidad CPM U7 OK ({processed} ads calculados).")
                else: log_func("Adv: No se pudo fusionar estabilidad CPM U7 (¿no hay grupos con >=2 días o no hay columnas comunes?).")
            except Exception as e_stab: log_func(f"Error calculando estabilidad CPM U7: {e_stab}")
        else: log_func("Adv: No hay datos U7 válidos para calcular estabilidad CPM.")
    else: log_func("Adv: Rango U7 inválido (start > end). No se calcula estabilidad CPM.")
    if 'cpm_stability_u7' not in ad_metrics.columns: ad_metrics['cpm_stability_u7']=np.nan 

    log_func("Filtrando Anuncios...", importante=True); spend_g='spend_global'; impr_g='impr_global'; dias='Días_Activo_Total'
    filtered_ads=ad_metrics.copy(); initial_ad_count=len(filtered_ads); cond=pd.Series(True,index=filtered_ads.index) 
    if spend_g in filtered_ads.columns: cond&=(pd.to_numeric(filtered_ads[spend_g],errors='coerce').fillna(0)>0) 
    else: log_func(f"Adv: Falta columna '{spend_g}' para filtrar Ads. No se filtrará por gasto.");
    if impr_g in filtered_ads.columns: cond&=(pd.to_numeric(filtered_ads[impr_g],errors='coerce').fillna(0)>0) 
    else: log_func(f"Adv: Falta columna '{impr_g}' para filtrar Ads. No se filtrará por impresiones.");
    if dias in filtered_ads.columns: cond&=(pd.to_numeric(filtered_ads[dias],errors='coerce').fillna(0)>0) 
    else: log_func(f"Adv: Falta columna '{dias}' para filtrar Ads. No se filtrará por días activos.");

    filtered_ads=filtered_ads[cond].copy() 
    log_func(f"Ads iniciales: {initial_ad_count}. Cumplen filtros (Gasto>0, Impr>0, Días>0): {len(filtered_ads)}", importante=True)
    if filtered_ads.empty: log_func("\n** No se encontraron Anuncios que cumplan los filtros para analizar. **", importante=True); return

    log_func("\nGenerando tablas consolidadas de Ads...");
    log_func("  Preparando Tabla Rendimiento Consolidada...");
    sort_col_spend = spend_g if spend_g in filtered_ads.columns else None 
    if sort_col_spend: 
        df_ads_sorted_spend = filtered_ads.sort_values(sort_col_spend, ascending=False, na_position='last').copy()
    else: 
         log_func("Adv: No se pudo ordenar por gasto (columna ausente).")
         df_ads_sorted_spend = filtered_ads.copy()

    t1_headers=['Campaña','AdSet','Nombre ADs','dias','Estado','Alcance','ROAS','Compras','CPM','CTR','CTR Saliente','Var U7 CTR','Var U7 ROAS','Var U7 Freq','Var U7 CPM','Var U7 Compras']
    t1_data=[]
    for _,r_row in df_ads_sorted_spend.iterrows(): t1_data.append({
        'Campaña':r_row.get('Campaign','-'),
        'AdSet':r_row.get('AdSet','-'),
        'Nombre ADs':r_row.get('Anuncio','-'),
        'dias':fmt_int(r_row.get('Días_Activo_Total', 0)),
        'Estado':r_row.get('Estado_Ult_Dia','-'),
        'Alcance':fmt_int(r_row.get('reach_global')),
        'ROAS':f"{fmt_float(r_row.get('roas_global'),2)}x",
        'Compras':fmt_int(r_row.get('purchases_global')),
        'CPM':f"{detected_currency}{fmt_float(r_row.get('cpm_global'),2)}",
        'CTR':fmt_pct(r_row.get('ctr_global'),2),
        'CTR Saliente':fmt_pct(r_row.get('ctr_out_global'),2),
        'Var U7 CTR':variation(r_row.get('ctr_u7'),r_row.get('ctr_global')), 
        'Var U7 ROAS':variation(r_row.get('roas_u7'),r_row.get('roas_global')),
        'Var U7 Freq':variation(r_row.get('frequency_u7'),r_row.get('frequency_global')),
        'Var U7 CPM':variation(r_row.get('cpm_u7'),r_row.get('cpm_global')),
        'Var U7 Compras':variation(r_row.get('purchases_u7'),r_row.get('purchases_global'))
        })
    if t1_data: 
        df_t1=pd.DataFrame(t1_data)
        df_t1 = df_t1[[h for h in t1_headers if h in df_t1.columns]] 
        num_cols_t1=[h for h in df_t1.columns if h not in ['Campaña','AdSet','Nombre ADs','Estado']] 
        _format_dataframe_to_markdown(df_t1,f"** Tabla Ads: Rendimiento y Variación (Orden: Gasto Desc) **",log_func,currency_cols=detected_currency, numeric_cols_for_alignment=num_cols_t1, max_col_width=45)
        log_func("\n  **Detalle Tabla Ads: Rendimiento y Variación:**");
        log_func("  * **Columnas principales (Alcance, ROAS, etc.):** Muestran el valor *Global Acumulado* para cada Ad durante todo el período de datos analizado.")
        log_func("  * **Columnas 'Var UX ...':** Muestran la variación porcentual del rendimiento en los *Últimos 7 Días* (U7) en comparación con el rendimiento *Global Acumulado* de ese mismo Ad. Una flecha 🔺 indica mejora, 🔻 indica empeoramiento respecto al global del Ad. Ayuda a identificar tendencias recientes.");
        log_func("  ---")
    else: log_func("  No hay datos para Tabla Rendimiento.")

    log_func("\n  Preparando Tabla Creatividad Consolidada...");
    roas_g='roas_global'; reach_g='reach_global'; dias_col='Días_Activo_Total' 
    sort_cols_roas=[c_col for c_col in [roas_g,reach_g,dias_col] if c_col in filtered_ads.columns]; 
    ascend_roas=[False, False, False] 

    df_ads_sorted_roas=filtered_ads.copy()
    if sort_cols_roas: 
         for scol,asc_val in zip(sort_cols_roas,ascend_roas):
              if pd.api.types.is_numeric_dtype(df_ads_sorted_roas[scol]):
                  fill_value = -np.inf if not asc_val else np.inf 
                  df_ads_sorted_roas[scol]=df_ads_sorted_roas[scol].fillna(fill_value)
         df_ads_sorted_roas=df_ads_sorted_roas.sort_values(by=sort_cols_roas,ascending=ascend_roas)
    else: 
         log_func("Adv: No se pudo ordenar por ROAS/Reach/Días (columnas ausentes).")

    t2_headers=['Campaña','AdSet','Nombre Ads','dias','Estado','CTR Glob (%)','Tiempo RV (s)','% RV 25','% RV 75','% RV 100','CPM Stab U7 (%)','Públicos Incluidos','Públicos Excluidos']
    t2_data=[]
    for _,r_row in df_ads_sorted_roas.iterrows(): t2_data.append({
        'Campaña':r_row.get('Campaign','-'),
        'AdSet':r_row.get('AdSet','-'),
        'Nombre Ads':r_row.get('Anuncio','-'),
        'dias':fmt_int(r_row.get('Días_Activo_Total', 0)),
        'Estado':r_row.get('Estado_Ult_Dia','-'),
        'CTR Glob (%)':fmt_pct(r_row.get('ctr_global'),2),
        'Tiempo RV (s)':f"{fmt_float(r_row.get('rtime_global'),1)}s",
        '% RV 25':fmt_pct(r_row.get('rv25_pct_global'),1),
        '% RV 75':fmt_pct(r_row.get('rv75_pct_global'),1),
        '% RV 100':fmt_pct(r_row.get('rv100_pct_global'),1),
        'CPM Stab U7 (%)':fmt_stability(r_row.get('cpm_stability_u7')),
        'Públicos Incluidos':str(r_row.get('Públicos In_global','-')), 
        'Públicos Excluidos':str(r_row.get('Públicos Ex_global','-')) 
        })
    if t2_data: 
        df_t2=pd.DataFrame(t2_data)
        df_t2 = df_t2[[h for h in t2_headers if h in df_t2.columns]] 
        num_cols_t2=[h for h in df_t2.columns if h not in ['Campaña','AdSet','Nombre Ads','Estado','Públicos Incluidos','Públicos Excluidos']] 
        stab_cols_t2=[h for h in df_t2.columns if 'Stab' in h] 
        _format_dataframe_to_markdown(df_t2,f"** Tabla Ads: Creatividad y Audiencia (Orden: ROAS Desc > Alcance Desc > Días Act Desc) **",log_func,currency_cols=detected_currency, stability_cols=stab_cols_t2,numeric_cols_for_alignment=num_cols_t2,max_col_width=45)
        log_func("\n  **Detalle Tabla Ads: Creatividad y Audiencia:**");
        log_func("  * **CTR Glob (%):** Porcentaje global de clics en el enlace sobre impresiones para el Ad.")
        log_func("  * **Tiempo RV (s):** Tiempo promedio global de reproducción del video (si aplica).")
        log_func("  * **% RV X%:** Porcentaje global de reproducciones de video que alcanzaron X% de su duración. Base: Impresiones (o Repr. 3s si > 0).")
        log_func("  * **CPM Stab U7 (%):** Estabilidad del Costo Por Mil Impresiones en los últimos 7 días para este Ad.")
        log_func("  * **Públicos:** Públicos personalizados usados por el Ad (agregados si varían).");
        log_func("  ---")
    else: log_func("  No hay datos para Tabla Creatividad.")
    log_func("\n--- Fin Análisis Consolidado de Ads ---")

def _generar_tabla_top_ads_historico(df_daily_agg, active_days_total_ad_df, log_func, detected_currency, top_n=10):
    log_func("\n\n============================================================");log_func(f"===== 6. Top {top_n} Ads Histórico (Orden: Gasto Desc > ROAS Desc) =====");log_func("============================================================")
    group_cols_ad=['Campaign','AdSet','Anuncio'] 
    essential_cols = group_cols_ad + ['spend','impr'] 
    if df_daily_agg is None or df_daily_agg.empty or not all(c_col in df_daily_agg.columns for c_col in essential_cols):
        log_func("   Faltan columnas esenciales (Campaign, AdSet, Anuncio, spend, impr) para Top Ads."); return

    df_daily_agg_copy = df_daily_agg.copy() 
    for col in group_cols_ad:
        if col in df_daily_agg_copy.columns:
            df_daily_agg_copy[col] = df_daily_agg_copy[col].astype(str)

    agg_dict={'spend':'sum','value':'sum','purchases':'sum','clicks':'sum','impr':'sum','reach':'sum','rtime':'mean', 'rv3':'sum'}
    agg_dict_available={k:v for k,v in agg_dict.items() if k in df_daily_agg_copy.columns} 
    if not agg_dict_available or 'spend' not in agg_dict_available or 'impr' not in agg_dict_available: 
        log_func("   No hay métricas suficientes (falta spend o impr) para agregar para Top Ads."); return

    ads_global=df_daily_agg_copy.groupby(group_cols_ad,observed=False,as_index=False).agg(agg_dict_available) 

    if all(c_col in ads_global for c_col in ['value','spend']): ads_global['roas']=safe_division(ads_global['value'],ads_global['spend'])
    if all(c_col in ads_global for c_col in ['clicks','impr']): ads_global['ctr']=safe_division_pct(ads_global['clicks'],ads_global['impr'])
    if all(c_col in ads_global for c_col in ['impr','reach']): ads_global['frequency']=safe_division(ads_global['impr'],ads_global['reach'])
    if active_days_total_ad_df is not None and not active_days_total_ad_df.empty and 'Días_Activo_Total' in active_days_total_ad_df.columns:
        merge_cols=[c_col for c_col in group_cols_ad if c_col in active_days_total_ad_df.columns] 
        if merge_cols:
             for col in merge_cols:
                 ads_global[col] = ads_global[col].astype(str)
                 active_days_total_ad_df[col] = active_days_total_ad_df[col].astype(str)
             ads_global=pd.merge(ads_global,active_days_total_ad_df[merge_cols+['Días_Activo_Total']],on=merge_cols,how='left');
             ads_global['Días_Activo_Total']=ads_global['Días_Activo_Total'].fillna(0).astype(int) 
    if 'Días_Activo_Total' not in ads_global: ads_global['Días_Activo_Total']=0 

    ads_global=ads_global[(ads_global['impr'].fillna(0)>0)&(ads_global['spend'].fillna(0)>0)].copy() 
    if ads_global.empty: log_func("   No hay Ads con impresiones y gasto positivos."); return

    sort_cols_top=[]; ascend_top=[]
    if 'spend' in ads_global: sort_cols_top.append('spend'); ascend_top.append(False) 
    if 'roas' in ads_global: sort_cols_top.append('roas'); ascend_top.append(False)  

    df_top=ads_global.copy()
    if sort_cols_top: 
         for scol,asc_val in zip(sort_cols_top,ascend_top):
              if pd.api.types.is_numeric_dtype(df_top[scol]):
                  fill_value = -np.inf if not asc_val else np.inf
                  df_top[scol]=df_top[scol].fillna(fill_value)
         df_top=df_top.sort_values(by=sort_cols_top,ascending=ascend_top).head(top_n) 
    else: 
        log_func("   No se pudo ordenar Top Ads (faltan columnas spend/roas). Mostrando los primeros {top_n}.")
        df_top=df_top.head(top_n)

    table_headers=['Campaña','AdSet','Anuncio','Días Act','Gasto','ROAS','Compras','CTR (%)','Frecuencia','Tiempo RV (s)']
    table_data=[]
    for _,row_val in df_top.iterrows(): table_data.append({
        'Campaña':row_val.get('Campaign','-'),
        'AdSet':row_val.get('AdSet','-'),
        'Anuncio':row_val.get('Anuncio','-'),
        'Días Act':fmt_int(row_val.get('Días_Activo_Total', 0)),
        'Gasto':f"{detected_currency}{fmt_float(row_val.get('spend'),0)}",
        'ROAS':f"{fmt_float(row_val.get('roas'),2)}x",
        'Compras':fmt_int(row_val.get('purchases')),
        'CTR (%)':fmt_pct(row_val.get('ctr'),2),
        'Frecuencia':fmt_float(row_val.get('frequency'),2),
        'Tiempo RV (s)':f"{fmt_float(row_val.get('rtime'),1)}s",
        })
    if table_data: 
        df_display=pd.DataFrame(table_data)
        df_display = df_display[[h for h in table_headers if h in df_display.columns]] 
        num_cols=[h for h in df_display.columns if h not in ['Campaña','AdSet','Anuncio']] 
        _format_dataframe_to_markdown(df_display,f"** Top {top_n} Ads por Gasto > ROAS (Global Acumulado) **",log_func,currency_cols=detected_currency, stability_cols=[], numeric_cols_for_alignment=num_cols)
    else: log_func(f"   No hay datos para mostrar en Top {top_n} Ads.");
    log_func("\n  **Detalle Top Ads Histórico:** Muestra los anuncios con mejor rendimiento histórico, ordenados primero por mayor gasto total y luego por ROAS más alto. Todas las métricas son acumuladas globales.");
    log_func("  ---")

def _generar_tabla_bitacora_entidad(entity_level, entity_name, df_daily_entity,
                                   bitacora_periods_list, detected_currency, log_func, period_type="Weeks"):
    original_locale = locale.getlocale(locale.LC_TIME) 
    try:
        locale_candidates = ['es_ES.UTF-8', 'es_ES', 'Spanish_Spain', 'Spanish']
        locale_set = False
        for loc_candidate in locale_candidates:
            try:
                locale.setlocale(locale.LC_TIME, loc_candidate)
                # log_func(f"Locale para fechas (tabla entidad) configurado a: {loc_candidate}") # Comentado para reducir logs
                locale_set = True
                break
            except locale.Error:
                continue
        if not locale_set:
            log_func("Adv: No se pudo configurar el locale a español para nombres de meses en tabla entidad. Se usarán nombres en inglés por defecto.")
    except Exception as e_locale_set:
        log_func(f"Adv: Error al intentar configurar locale en tabla entidad: {e_locale_set}")

    
    header_label = entity_level.capitalize()
    log_func(f"\n\n--------------------------------------------------------------------------------")
    log_func(f" {header_label}: {entity_name} - Comparativa {'Semanal' if period_type == 'Weeks' else 'Mensual'}")
    log_func(f"--------------------------------------------------------------------------------")

    if df_daily_entity is None or df_daily_entity.empty or 'date' not in df_daily_entity.columns:
        log_func("   No hay datos diarios para generar la tabla de bitácora.")
        try: locale.setlocale(locale.LC_TIME, original_locale) 
        except: pass
        return
    if not bitacora_periods_list: 
        log_func("   No se proporcionaron períodos para la bitácora.")
        try: locale.setlocale(locale.LC_TIME, original_locale) 
        except: pass
        return

    results_by_period = {} 
    period_labels_for_table = []     

    log_func(f"  Calculando métricas para períodos ({'Semanas' if period_type == 'Weeks' else 'Meses'})...")
    for i, (start_dt, end_dt, original_label_from_list) in enumerate(bitacora_periods_list): 
        current_period_table_label = original_label_from_list 
        period_labels_for_table.append(current_period_table_label)
        
        df_period_subset = df_daily_entity[ 
            (df_daily_entity['date'] >= start_dt) & 
            (df_daily_entity['date'] <= end_dt)
        ].copy()
        period_identifier_tuple = (start_dt.date(), end_dt.date()) 
        results_by_period[original_label_from_list] = _calcular_metricas_agregadas_y_estabilidad(df_period_subset, period_identifier_tuple, log_func) 
        # log_func(f"    -> Métricas para '{original_label_from_list}' calculadas ({results_by_period[original_label_from_list].get('date_range', 'N/A')}).") # Comentado para reducir logs

    metric_map = {
        'Inversion': {'display':'Inversion', 'formatter': lambda x: f"{detected_currency}{fmt_float(x, 2)}"},
        'Ventas_Totales': {'display':'Ventas', 'formatter': lambda x: f"{detected_currency}{fmt_float(x, 2)}"},
        'ROAS': {'display':'ROAS', 'formatter': lambda x: f"{fmt_float(x, 2)}x"},
        'Compras': {'display':'Compras', 'formatter': fmt_int},
        'CPA': {'display':'CPA', 'formatter': lambda x: f"{detected_currency}{fmt_float(x, 2)}"},
        'Ticket_Promedio': {'display':'Ticket Prom.', 'formatter': lambda x: f"{detected_currency}{fmt_float(x, 2)}"},
        'Impresiones': {'display':'Impresiones', 'formatter': fmt_int},
        'Alcance': {'display':'Alcance', 'formatter': fmt_int},
        'Frecuencia': {'display':'Frecuencia', 'formatter': lambda x: fmt_float(x, 2)},
        'CPM': {'display':'CPM', 'formatter': lambda x: f"{detected_currency}{fmt_float(x, 2)}"},
        'Clics': {'display':'Clics (Link)', 'formatter': fmt_int},
        'CTR': {'display':'CTR (Link) %', 'formatter': lambda x: fmt_pct(x, 2)},
        'Clics Salientes': {'display':'Clics (Out)', 'formatter': fmt_int}, 
        'CTR Saliente': {'display':'CTR (Out) %', 'formatter': lambda x: fmt_pct(x, 2)}, 
        'Visitas': {'display':'Visitas LP', 'formatter': fmt_int},
        'LVP_Rate_%': {'display':'Tasa Visita LP %', 'formatter': lambda x: fmt_pct(x, 1)},
        'Conv_Rate_%': {'display':'Tasa Compra %', 'formatter': lambda x: fmt_pct(x, 1)},
        'Tiempo_Promedio': {'display':'Tiempo RV (s)', 'formatter': lambda x: fmt_float(x,1)}, 
        'RV25_%': {'display': 'RV 25%','formatter': lambda x: fmt_pct(x, 1)},
        'RV75_%': {'display': 'RV 75%','formatter': lambda x: fmt_pct(x, 1)},
        'RV100_%': {'display': 'RV 100%','formatter': lambda x: fmt_pct(x, 1)},
        'ROAS_Stability_%': {'display':'Est. ROAS %', 'formatter':fmt_stability}, 
        'CPA_Stability_%': {'display':'Est. CPA %', 'formatter':fmt_stability},
        'CPM_Stability_%': {'display':'Est. CPM %', 'formatter':fmt_stability},
        'CTR_Stability_%': {'display':'Est. CTR %', 'formatter':fmt_stability},
        'IMPR_Stability_%': {'display':'Est. Impr %', 'formatter':fmt_stability},
        'CTR_DIV_FREQ_RATIO_Stability_%': {'display':'Est. CTR/Freq %', 'formatter':fmt_stability}
    }
    order = [ 
        'Inversion', 'Ventas_Totales', 'ROAS', 'Compras', 'CPA', 'Ticket_Promedio',
        'Impresiones', 'Alcance', 'Frecuencia', 'CPM',
        'Clics', 'CTR', 'Clics Salientes', 'CTR Saliente', 'Visitas', 'LVP_Rate_%', 'Conv_Rate_%', 
        'Tiempo_Promedio', 'RV25_%', 'RV75_%', 'RV100_%', 
        'ROAS_Stability_%', 'CPA_Stability_%', 'CPM_Stability_%', 'CTR_Stability_%', 'IMPR_Stability_%', 'CTR_DIV_FREQ_RATIO_Stability_%' 
    ]
    headers = ["Métrica"] + period_labels_for_table 
    rows = []
    stability_keys_map = {v['display']: k for k,v in metric_map.items() if 'Stability' in k} 

    for internal_key in order: 
        info = metric_map.get(internal_key)
        if not info: continue 
        display_name = info['display'] 
        fmt = info['formatter']        
        row_vals = [display_name]      
        is_stab_metric = 'Stability' in internal_key 

        for i, (_, _, original_period_label) in enumerate(bitacora_periods_list): 
            period_results = results_by_period.get(original_period_label) 
            current_raw = period_results.get(internal_key, np.nan) if period_results else np.nan 

            if is_stab_metric and (period_results is None or not period_results.get('is_complete', False)):
                 formatted_val = '-'
            else:
                 formatted_val = fmt(current_raw) if pd.notna(current_raw) else '-'

            var_vs_prev_fmt = '-' 
            if not is_stab_metric and i < len(bitacora_periods_list) - 1: 
                prev_original_label_for_comparison = bitacora_periods_list[i+1][2] 
                prev_results_for_comparison = results_by_period.get(prev_original_label_for_comparison)
                if prev_results_for_comparison:
                    prev_raw_for_comparison = prev_results_for_comparison.get(internal_key, np.nan)
                    var_vs_prev_fmt = variation(current_raw, prev_raw_for_comparison) 
            
            display_cell = f"{formatted_val}" 
            if not is_stab_metric and var_vs_prev_fmt != '-': 
                 display_cell += f" ({var_vs_prev_fmt})"

            row_vals.append(display_cell) 

        rows.append(row_vals) 

    df_disp = pd.DataFrame(rows, columns=headers) 
    numeric_cols_for_alignment = [h for h in headers if h != "Métrica"] 
    stability_cols_display = list(stability_keys_map.keys()) 
    _format_dataframe_to_markdown(df_disp, "", log_func, currency_cols=detected_currency,
                                  stability_cols=stability_cols_display, 
                                  numeric_cols_for_alignment=numeric_cols_for_alignment)

    log_func("\n  **Nota aclaratoria:**")
    log_func("  * **Semana actual / Mes actual:** Corresponde al periodo más reciente analizado (semana 0 o mes 0).")
    log_func("  * **Xª semana anterior / Xº mes anterior:** Es la semana/mes inmediatamente previa (semana/mes –X).")
    log_func("  * El análisis comparativo (valores en paréntesis con 🔺/🔻) se realiza siempre contra el período inmediatamente anterior mostrado en la tabla (columna a la derecha).")
    log_func("\n  **Detalle de Cálculo de Métricas Clave (Bitácora):**")
    log_func("  * **Inversión:** Suma del `Importe gastado` para el período.")
    log_func("  * **Ventas Totales:** Suma del `Valor de conversión de compras` para el período.")
    log_func("  * **ROAS (Retorno de la Inversión Publicitaria):** `Ventas Totales / Inversión`. Mide la rentabilidad de la publicidad.")
    log_func("  * **Compras:** Suma de `Compras` para el período.")
    log_func("  * **CPA (Costo por Adquisición/Compra):** `Inversión / Compras`. Costo promedio para generar una compra.")
    log_func("  * **Ticket Promedio:** `Ventas Totales / Compras`. Valor promedio de cada compra.")
    log_func("  * **Impresiones:** Suma de `Impresiones` para el período.")
    log_func("  * **Alcance:** Suma del `Alcance` para el período (Nota: El alcance agregado puede no ser único si se suman datos de diferentes niveles sin dedup. Aquí se suma el alcance diario).")
    log_func("  * **Frecuencia:** `Impresiones / Alcance`. Número promedio de veces que cada persona vio el anuncio.")
    log_func("  * **CPM (Costo por Mil Impresiones):** `(Inversión / Impresiones) * 1000`. Costo de mostrar el anuncio mil veces.")
    log_func("  * **Clics (Link):** Suma de `Clics en el enlace` para el período.")
    log_func("  * **CTR (Link) % (Tasa de Clics en el Enlace) %:** `(Clics en el Enlace / Impresiones) * 100`. Porcentaje de impresiones que resultaron en un clic al enlace.")
    log_func("  * **Clics (Out):** Suma de `Clics salientes` para el período.")
    log_func("  * **CTR (Out) % (Tasa de Clics Salientes) %:** `(Clics Salientes / Impresiones) * 100`. Porcentaje de impresiones que resultaron en un clic que lleva fuera de la plataforma.")
    log_func("  * **Visitas LP:** Suma de `Visitas a la página de destino` para el período.")
    log_func("  * **Tasa Visita LP %:** `(Visitas a la Página de Destino / Clics en el Enlace) * 100`. Porcentaje de clics que resultaron en una carga de la página de destino.")
    log_func("  * **Tasa Compra % (Tasa de Conversión de Compra):** `(Compras / Visitas a la Página de Destino) * 100`. Porcentaje de visitas a la LP que resultaron en una compra.")
    log_func("  * **Tiempo RV (s) (Tiempo Promedio de Reproducción de Video):** Promedio del `Tiempo promedio de reproducción del video` diario.")
    log_func("  * **% RV X% (Porcentaje de Reproducción de Video):** `(Reproducciones hasta X% / Base de Video) * 100`. La base es `Reproducciones de 3 segundos` si es > 0, sino `Impresiones`.")
    log_func("  * **Estabilidad (%):** Mide la consistencia diaria de la métrica *dentro* del período de la columna. Un % alto indica estabilidad. Se calcula si el período tiene todos sus días con datos y cumple umbrales mínimos. Iconos: ✅ >= 50%, 🏆 >= 70%.")
    log_func("  ---")
    try: locale.setlocale(locale.LC_TIME, original_locale) 
    except: pass