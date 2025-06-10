# report_generator_project/data_processing/metric_calculators.py
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta # Para _calcular_metricas_agregadas_y_estabilidad
from formatting_utils import safe_division, safe_division_pct # De formatting_utils.py en raíz

# ============================================================
# CÁLCULO DE MÉTRICAS ESPECÍFICAS Y ESTABILIDAD
# ============================================================
def _calcular_dias_activos_totales(df_combined):
    results={'Campaign':pd.DataFrame(columns=['Campaign','Días_Activo_Total']),'AdSet':pd.DataFrame(columns=['Campaign','AdSet','Días_Activo_Total']),'Anuncio':pd.DataFrame(columns=['Campaign','AdSet','Anuncio','Días_Activo_Total'])}
    # ... (copiar el cuerpo completo de la función _calcular_dias_activos_totales aquí)
    # [PEGAR AQUÍ EL CÓDIGO COMPLETO DE LA FUNCIÓN _calcular_dias_activos_totales DESDE EL SCRIPT ORIGINAL]
    if df_combined is None or df_combined.empty: print("Adv: DF vacío (días activos)."); return results
    if 'Entrega' not in df_combined.columns: print("Adv: Col 'Entrega' no encontrada (días activos)."); return results
    active_df=df_combined[df_combined['Entrega'].eq('Activo')].copy()
    if active_df.empty: print("Adv: No hay filas con estado 'Activo'."); return results
    if 'date' not in active_df.columns or not pd.api.types.is_datetime64_any_dtype(active_df['date']): print("Adv: Col 'date' inválida."); return results

    if 'Campaign' in active_df.columns: active_df['Campaign'] = active_df['Campaign'].astype(str)
    if 'AdSet' in active_df.columns: active_df['AdSet'] = active_df['AdSet'].astype(str)
    if 'Anuncio' in active_df.columns: active_df['Anuncio'] = active_df['Anuncio'].astype(str)

    if 'Campaign' in active_df.columns:
        try: days_camp=active_df.groupby('Campaign',observed=False)['date'].nunique().reset_index(name='Días_Activo_Total'); results['Campaign']=days_camp; print(f"Días activos calc. {len(days_camp)} campañas.")
        except Exception as e_camp: print(f"Error días campaña: {e_camp}"); results['Campaign'] = pd.DataFrame(columns=['Campaign','Días_Activo_Total'])
    if 'AdSet' in active_df.columns and 'Campaign' in active_df.columns :
        try:
            days_adset=active_df.groupby(['Campaign','AdSet'],observed=False)['date'].nunique().reset_index(name='Días_Activo_Total')
            results['AdSet']=days_adset
            print(f"Días activos calc. {len(results['AdSet'])} AdSets (Campaign/AdSet).")
        except Exception as e_adset: print(f"Error días AdSet: {e_adset}"); results['AdSet'] = pd.DataFrame(columns=['Campaign','AdSet','Días_Activo_Total'])
    if 'Anuncio' in active_df.columns and 'Campaign' in active_df.columns and 'AdSet' in active_df.columns:
       try:
           days_ad=active_df.groupby(['Campaign','AdSet','Anuncio'],observed=False)['date'].nunique().reset_index(name='Días_Activo_Total'); results['Anuncio']=days_ad; print(f"Días activos calc. {len(days_ad)} Anuncios.")
       except Exception as e_ad: print(f"Error días Anuncio: {e_ad}"); results['Anuncio'] = pd.DataFrame(columns=['Campaign','AdSet','Anuncio','Días_Activo_Total'])
    return results

def _calculate_stability_pct(series):
    series_num=pd.to_numeric(series,errors='coerce').dropna(); series_num=series_num[np.isfinite(series_num)]
    # ... (copiar el cuerpo completo de la función _calculate_stability_pct aquí)
    # [PEGAR AQUÍ EL CÓDIGO COMPLETO DE LA FUNCIÓN _calculate_stability_pct DESDE EL SCRIPT ORIGINAL]
    if len(series_num)<2: return 100.0 if len(series_num)==1 else np.nan
    mean_val=series_num.mean(); std_val=series_num.std()
    if pd.isna(mean_val) or pd.isna(std_val): return np.nan
    eps=1e-9
    if abs(mean_val)<eps: return 100.0 if abs(std_val)<eps else 0.0
    else: cv=abs(std_val/(mean_val+eps)); raw=(1-cv)*100; stab=max(0,min(100,raw)); return stab

def _calcular_metricas_agregadas_y_estabilidad(df_period, period_identifier, log_func):
    results={'is_complete':False};keys=['Alcance','Impresiones','Frecuencia','Inversion','CPM','Compras','Clics','Visitas','CPA','CTR','Ventas_Totales','ROAS','Ticket_Promedio','Tiempo_Promedio','LVP_Rate_%','Conv_Rate_%','RV25_%','RV75_%','RV100_%','ROAS_Stability_%','CPA_Stability_%','CPM_Stability_%','CTR_Stability_%','IMPR_Stability_%','CTR_DIV_FREQ_RATIO_Stability_%','Clics Salientes','CTR Saliente']
    # ... (copiar el cuerpo completo de la función _calcular_metricas_agregadas_y_estabilidad aquí)
    # [PEGAR AQUÍ EL CÓDIGO COMPLETO DE LA FUNCIÓN _calcular_metricas_agregadas_y_estabilidad DESDE EL SCRIPT ORIGINAL]
    for k in keys: results[k]=np.nan
    if df_period is None or df_period.empty or 'date' not in df_period.columns or df_period['date'].dropna().empty:
        results['date_range'] = 'Datos insuficientes'
        return results

    n_days=df_period['date'].nunique()
    min_date_period = df_period['date'].min() if n_days > 0 else None
    max_date_period = df_period['date'].max() if n_days > 0 else None
    results['date_range']=f"{min_date_period.strftime('%d/%m/%y')}-{max_date_period.strftime('%d/%m/%y')}" if min_date_period and max_date_period else "N/A"

    expected_days_in_period = None
    results['is_complete'] = False
    if n_days > 0:
        if period_identifier == 'Global':
            results['is_complete'] = (n_days >= 7)
        elif isinstance(period_identifier, int):
            expected_days_in_period = period_identifier
            potential_days_in_range = (max_date_period.date() - min_date_period.date()).days + 1 if max_date_period and min_date_period else 0
            results['is_complete'] = (n_days == min(expected_days_in_period, potential_days_in_range)) if potential_days_in_range > 0 else (n_days >= expected_days_in_period)
        elif isinstance(period_identifier, tuple) and len(period_identifier) == 2:
            start_id, end_id = period_identifier
            if all(isinstance(d, (date, datetime)) for d in [start_id, end_id]):
                start_date_obj = start_id.date() if isinstance(start_id, datetime) else start_id
                end_date_obj = end_id.date() if isinstance(end_id, datetime) else end_id
                expected_days_in_period = (end_date_obj - start_date_obj).days + 1
                results['is_complete'] = (n_days == expected_days_in_period)
            else:
                results['is_complete'] = (n_days >= 7) # Fallback if period_identifier tuple is not dates

    cols_sum=['spend','value','purchases','clicks','impr','reach','visits','rv25','rv75','rv100','clicks_out', 'attention','interest','deseo','addcart','checkout'];agg={}
    for c_col in cols_sum: agg[c_col]=df_period[c_col].sum(skipna=True) if c_col in df_period.columns else 0
    agg['frequency']=safe_division(agg.get('impr',0),agg.get('reach',0));
    agg['cpm']=safe_division(agg.get('spend',0),agg.get('impr',0))*1000;
    agg['cpa']=safe_division(agg.get('spend',0),agg.get('purchases',0));
    agg['ctr']=safe_division_pct(agg.get('clicks',0),agg.get('impr',0));
    agg['ctr_out']=safe_division_pct(agg.get('clicks_out',0),agg.get('impr',0));
    agg['roas']=safe_division(agg.get('value',0),agg.get('spend',0));
    agg['ticket_promedio']=safe_division(agg.get('value',0),agg.get('purchases',0));
    agg['lpv_rate']=safe_division_pct(agg.get('visits',0),agg.get('clicks',0));
    agg['purchase_rate']=safe_division_pct(agg.get('purchases',0),agg.get('visits',0))
    base_rv_col_name = 'rv3' if 'rv3' in df_period.columns and df_period['rv3'].sum(skipna=True) > 0 else 'impr'
    base_rv_sum = agg.get(base_rv_col_name, 0)
    agg['rv25_pct']=safe_division_pct(agg.get('rv25',0),base_rv_sum)
    agg['rv75_pct']=safe_division_pct(agg.get('rv75',0),base_rv_sum)
    agg['rv100_pct']=safe_division_pct(agg.get('rv100',0),base_rv_sum)
    agg['rtime']=df_period['rtime'].mean(skipna=True) if 'rtime' in df_period.columns else np.nan

    map_agg={'reach':'Alcance','impr':'Impresiones','frequency':'Frecuencia','spend':'Inversion','cpm':'CPM','purchases':'Compras','clicks':'Clics','clicks_out':'Clics Salientes','visits':'Visitas','cpa':'CPA','ctr':'CTR','ctr_out':'CTR Saliente','value':'Ventas_Totales','roas':'ROAS','ticket_promedio':'Ticket_Promedio','rtime':'Tiempo_Promedio','lpv_rate':'LVP_Rate_%','purchase_rate':'Conv_Rate_%','rv25_pct':'RV25_%','rv75_pct':'RV75_%','rv100_pct':'RV100_%'}
    for ik,ok in map_agg.items():
        if ik in agg: results[ok]=agg[ik]

    if results.get('is_complete', False):
        thr={'imp':500,'sale':5,'spend':20,'click':10,'reach':200};cols_stab=['roas','cpa','cpm','ctr','impr','ctr_div_freq_ratio'];daily_s={}
        for c_stab in cols_stab:
            if c_stab in df_period.columns: daily_s[c_stab]=pd.to_numeric(df_period[c_stab],errors='coerce')[lambda s_series:np.isfinite(s_series)].dropna()
        if results.get('Ventas_Totales',0)>=thr['sale'] and results.get('Inversion',0)>thr['spend'] and 'roas' in daily_s: results['ROAS_Stability_%']=_calculate_stability_pct(daily_s['roas'])
        if results.get('Compras',0)>=thr['sale'] and results.get('Inversion',0)>thr['spend'] and 'cpa' in daily_s: results['CPA_Stability_%']=_calculate_stability_pct(daily_s['cpa'])
        if results.get('Impresiones',0)>thr['imp'] and 'cpm' in daily_s: results['CPM_Stability_%']=_calculate_stability_pct(daily_s['cpm'])
        if results.get('Impresiones',0)>thr['imp'] and results.get('Clics',0)>=thr['click'] and 'ctr' in daily_s: results['CTR_Stability_%']=_calculate_stability_pct(daily_s['ctr'])
        if results.get('Impresiones',0)>thr['imp'] and 'impr' in daily_s: results['IMPR_Stability_%']=_calculate_stability_pct(daily_s['impr'])
        if results.get('Impresiones',0)>thr['imp'] and results.get('Alcance',0)>thr['reach'] and results.get('Clics',0)>=thr['click'] and 'ctr_div_freq_ratio' in daily_s: results['CTR_DIV_FREQ_RATIO_Stability_%']=_calculate_stability_pct(daily_s['ctr_div_freq_ratio'])
    elif n_days > 0 and log_func is not None: # Check for log_func
         period_id_str = str(period_identifier)
         if isinstance(period_identifier, tuple) and len(period_identifier) == 2 and all(isinstance(d, (date,datetime)) for d in period_identifier): # Ensure it's a date tuple
             start_date_p = period_identifier[0].strftime('%d/%m/%y') if isinstance(period_identifier[0], (date, datetime)) else str(period_identifier[0])
             end_date_p = period_identifier[1].strftime('%d/%m/%y') if isinstance(period_identifier[1], (date, datetime)) else str(period_identifier[1])
             period_id_str = f"{start_date_p}-{end_date_p}"

         log_func(f"      (Estabilidad no calculada para período '{period_id_str}' - incompleto: {n_days} días encontrados vs {expected_days_in_period or 'N/A'} esperados)")

    for k_res in results:
        if isinstance(results[k_res],(int,float)) and not np.isfinite(results[k_res]): results[k_res]=np.nan
    return results