# report_generator_project/data_processing/aggregators.py
import pandas as pd
import numpy as np
import traceback
from utils import normalize, aggregate_strings # De utils.py en raíz
from formatting_utils import safe_division, safe_division_pct # De formatting_utils.py en raíz

# ============================================================
# AGREGACIÓN DE DATOS
# ============================================================
def _agregar_datos_diarios(df_combined, status_queue, selected_adsets=None):
    log_and_update = lambda msg: status_queue.put(f"  [Diario] {msg}")
    # ... (copiar el cuerpo completo de la función _agregar_datos_diarios aquí)
    # [PEGAR AQUÍ EL CÓDIGO COMPLETO DE LA FUNCIÓN _agregar_datos_diarios DESDE EL SCRIPT ORIGINAL]
    log_and_update("Iniciando agregación diaria por Entidad...")
    if df_combined is None or df_combined.empty: log_and_update("DataFrame combinado vacío."); return pd.DataFrame()

    df_filtered = df_combined.copy()
    if selected_adsets and isinstance(selected_adsets, list) and len(selected_adsets) > 0 and selected_adsets[0] != '__ALL__':
        if 'AdSet' in df_filtered.columns:
            log_and_update(f"Filtrando AdSets: {selected_adsets}")
            original_row_count = len(df_filtered)
            adsets_to_filter_normalized = [normalize(a) for a in selected_adsets]
            df_filtered = df_filtered[df_filtered['AdSet'].str.strip().str.lower().isin(adsets_to_filter_normalized)].copy()
            log_and_update(f"Filtrado OK. Quedan {len(df_filtered)} de {original_row_count} filas para los AdSets seleccionados.")
            if df_filtered.empty: log_and_update(f"WARN: No quedaron datos después de filtrar por los AdSets seleccionados.")
        else:
            log_and_update("WARN: Columna 'AdSet' no encontrada. No se pudo filtrar por AdSet.")

    if df_filtered.empty: return pd.DataFrame()


    try:
        grouping_cols=['date','Campaign','AdSet','Anuncio']; actual_grouping_cols=[col for col in grouping_cols if col in df_filtered.columns]
        if 'date' not in actual_grouping_cols: raise ValueError("Falta columna 'date' esencial para agregación diaria.")
        if len(actual_grouping_cols)<2:
             log_and_update(f"WARN: Faltan columnas de agrupación (Campaign/AdSet/Anuncio). Agregando solo por fecha.")
             actual_grouping_cols=['date']

        log_and_update(f"Agrupando por: {actual_grouping_cols}")
        agg_dict_base={
            'spend':'sum','value':'sum','purchases':'sum','clicks':'sum','impr':'sum','reach':'sum',
            'visits':'sum','clicks_out':'sum', 'rv3':'sum','rv25':'sum','rv75':'sum','rv100':'sum',
            'attention':'sum','interest':'sum','deseo':'sum','addcart':'sum','checkout':'sum',
            'rtime':'mean','freq':'mean','roas':'mean','cpa':'mean',
            'Públicos In':lambda x:aggregate_strings(x,separator=' | ',max_len=None),
            'Públicos Ex':lambda x:aggregate_strings(x,separator=' | ',max_len=None),
            'Entrega':lambda x:aggregate_strings(x,separator='|',max_len=50)
        }
        actual_agg_dict={k:v for k,v in agg_dict_base.items() if k in df_filtered.columns}
        if not actual_agg_dict: raise ValueError("No hay columnas numéricas o de texto válidas para agregar.")

        log_and_update(f"Agregando con: {list(actual_agg_dict.keys())}")
        group_cols_exist=[col for col in actual_grouping_cols if col in df_filtered.columns]
        if not group_cols_exist: raise ValueError(f"Columnas de agrupación especificadas ({actual_grouping_cols}) no encontradas en el DataFrame.")

        for col in group_cols_exist:
             if col != 'date':
                  df_filtered[col] = df_filtered[col].astype(str)

        df_daily=df_filtered.groupby(group_cols_exist,as_index=False,observed=False).agg(actual_agg_dict)
        log_and_update(f"Groupby().agg() OK. {len(df_daily)} filas agregadas.")
        if df_daily.empty: return df_daily

        log_and_update("Calculando métricas derivadas diarias...");
        s_spend=df_daily.get('spend',pd.Series(np.nan,index=df_daily.index));v_value=df_daily.get('value',pd.Series(np.nan,index=df_daily.index));p_purch=df_daily.get('purchases',pd.Series(np.nan,index=df_daily.index));c_clicks=df_daily.get('clicks',pd.Series(np.nan,index=df_daily.index));i_impr=df_daily.get('impr',pd.Series(np.nan,index=df_daily.index));r_reach=df_daily.get('reach',pd.Series(np.nan,index=df_daily.index));vi_visits=df_daily.get('visits',pd.Series(np.nan,index=df_daily.index));co_clicks_out=df_daily.get('clicks_out',pd.Series(np.nan,index=df_daily.index));rv3_s=df_daily.get('rv3',pd.Series(np.nan,index=df_daily.index));rv25_s=df_daily.get('rv25',pd.Series(np.nan,index=df_daily.index));rv75_s=df_daily.get('rv75',pd.Series(np.nan,index=df_daily.index));rv100_s=df_daily.get('rv100',pd.Series(np.nan,index=df_daily.index));
        ro_roas=df_daily.get('roas',pd.Series(np.nan,index=df_daily.index))
        cp_cpa=df_daily.get('cpa',pd.Series(np.nan,index=df_daily.index))
        fr_freq=df_daily.get('freq',pd.Series(np.nan,index=df_daily.index))

        df_daily['roas_calc']=safe_division(v_value,s_spend); df_daily['roas']=np.where(ro_roas.notna() & np.isfinite(ro_roas),ro_roas,df_daily['roas_calc'])
        df_daily['cpa_calc']=safe_division(s_spend,p_purch); df_daily['cpa']=np.where(cp_cpa.notna() & np.isfinite(cp_cpa),cp_cpa,df_daily['cpa_calc'])
        df_daily['ctr']=safe_division_pct(c_clicks,i_impr); df_daily['cpm']=safe_division(s_spend,i_impr)*1000
        df_daily['frequency_calc']=safe_division(i_impr,r_reach); fr_num=pd.to_numeric(fr_freq,errors='coerce'); df_daily['frequency']=np.where(pd.notna(fr_num)&np.isfinite(fr_num)&(fr_num>0),fr_num,df_daily['frequency_calc'])
        ctr_dec=safe_division(c_clicks,i_impr); df_daily['ctr_div_freq_ratio']=safe_division(ctr_dec,df_daily['frequency'])
        df_daily['conversion_per_click']=safe_division_pct(p_purch,c_clicks)
        base_rv_val=np.where(pd.Series(rv3_s>0).fillna(False),rv3_s,i_impr);
        df_daily['rv25_pct']=safe_division_pct(rv25_s,base_rv_val); df_daily['rv75_pct']=safe_division_pct(rv75_s,base_rv_val); df_daily['rv100_pct']=safe_division_pct(rv100_s,base_rv_val)
        df_daily['lpv_rate']=safe_division_pct(vi_visits,c_clicks); df_daily['purchase_rate']=safe_division_pct(p_purch,vi_visits)
        df_daily['ctr_out'] = safe_division_pct(co_clicks_out, i_impr)

        log_and_update("Métricas derivadas diarias calculadas.")
        log_and_update("Agregación diaria finalizada."); return df_daily
    except Exception as e: log_and_update(f"!!! ERROR en agregación diaria: {e} !!!\n{traceback.format_exc()}"); return pd.DataFrame()