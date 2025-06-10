# report_generator_project/formatting_utils.py
import pandas as pd
import numpy as np
import re
import locale 
from datetime import datetime, date, timedelta
from utils import robust_numeric_conversion # De utils.py local


# ============================================================
# FUNCIONES DE FORMATO PARA EL REPORTE
# ============================================================
fmt_int = lambda x: f"{int(round(x)):,}".replace(',', '.') if pd.notna(x) and np.isfinite(x) and pd.api.types.is_number(x) else '-'

def fmt_float(x, d=2):
    try: val = float(x)
    except: return '-'
    if pd.isna(val) or not np.isfinite(val): return '-'
    if abs(val) < 1e-9: return f"0,{ '0' * d }"
    s = f"{val:,.{d}f}"; return s.replace(',', 'X').replace('.', ',').replace('X', '.')

def fmt_pct(x, d=2):
    try: val = float(x)
    except: return '-'
    if pd.isna(val) or not np.isfinite(val): return '-'
    if abs(val) < 1e-9 : return f"0,{ '0' * d }%"
    s_num = f"{abs(val):,.{d}f}"; s_fmt = s_num.replace(',', 'X').replace('.', ',').replace('X', '.')
    return f"-{s_fmt}%" if val < 0 else f"{s_fmt}%"

def fmt_stability(x):
    if pd.isna(x) or not np.isfinite(x): return '-'
    val = float(x); pct_fmt = fmt_pct(val, 0)
    if val >= 70: return f"{pct_fmt} 🏆"
    elif val >= 50: return f"{pct_fmt} ✅"
    else: return pct_fmt

def variation(current, previous):
    c = pd.to_numeric(current, errors='coerce'); p = pd.to_numeric(previous, errors='coerce')
    if pd.isna(c) or pd.isna(p) or not np.isfinite(c) or not np.isfinite(p): return '-'
    if abs(p) < 1e-9:
        if abs(c) < 1e-9: return '-'
        return 'Inf%🔺' if c > 1e-9 else ('-Inf%🔻' if c < -1e-9 else '-')
    var_pct = (c - p) / abs(p) * 100; arrow = '🔺' if c > p else ('🔻' if c < p else '')
    if abs(var_pct) < 0.5: arrow = ''
    return fmt_pct(var_pct, 1) + arrow

def format_step_pct(value):
    if pd.isna(value) or not np.isfinite(value): return '-'
    pct_val = float(value); arrow = '🔺' if pct_val > 100.01 else ('🔻' if pct_val < 99.99 else '')
    if abs(pct_val) < 0.01 or abs(pct_val - 100) < 0.01 : arrow = ''
    return fmt_pct(pct_val, 1) + arrow

# --- ESTAS SON LAS FUNCIONES IMPORTANTES QUE FALTAN O TIENEN ERROR DE NOMBRE ---
def safe_division(n_input, d_input):
    n = pd.to_numeric(n_input, errors='coerce')
    d = pd.to_numeric(d_input, errors='coerce')
    return_scalar = not isinstance(n_input, (pd.Series, np.ndarray)) and \
                    not isinstance(d_input, (pd.Series, np.ndarray))
    mask = pd.notna(n) & pd.notna(d) & np.isfinite(n) & np.isfinite(d) & (abs(d) > 1e-9)
    result_values = np.where(mask, n / d, np.nan)
    if return_scalar:
        try: return result_values.item()
        except IndexError: return np.nan
        except ValueError: return result_values[0].item() if result_values.size > 0 else np.nan
    else:
        index = n_input.index if isinstance(n_input, pd.Series) else \
                (d_input.index if isinstance(d_input, pd.Series) else None)
        name = (n_input.name + "_div" if isinstance(n_input, pd.Series) and n_input.name else
                (d_input.name + "_denom_div" if isinstance(d_input, pd.Series) and d_input.name else None))
        return pd.Series(result_values.flatten(), index=index, name=name)

def safe_division_pct(n_input, d_input):
    n = pd.to_numeric(n_input, errors='coerce')
    d = pd.to_numeric(d_input, errors='coerce')
    return_scalar = not isinstance(n_input, (pd.Series, np.ndarray)) and \
                    not isinstance(d_input, (pd.Series, np.ndarray))
    mask = pd.notna(n) & pd.notna(d) & np.isfinite(n) & np.isfinite(d) & (abs(d) > 1e-9)
    result_values = np.where(mask, (n / d) * 100, np.nan)
    if return_scalar:
        try: return result_values.item()
        except IndexError: return np.nan
        except ValueError: return result_values[0].item() if result_values.size > 0 else np.nan
    else:
        index = n_input.index if isinstance(n_input, pd.Series) else \
                (d_input.index if isinstance(d_input, pd.Series) else None)
        name = (n_input.name + "_pct" if isinstance(n_input, pd.Series) and n_input.name else
                (d_input.name + "_denom_pct" if isinstance(d_input, pd.Series) and d_input.name else None))
        return pd.Series(result_values.flatten(), index=index, name=name)
# --- FIN DE FUNCIONES IMPORTANTES ---

def _format_dataframe_to_markdown(df, title, log_func, float_cols_fmt={}, int_cols=[], pct_cols_fmt={}, currency_cols={}, stability_cols=[], default_prec=2, max_col_width=None, numeric_cols_for_alignment=[]):
    if df is None or df.empty: log_func(f"\n** {title} **"); log_func("   No hay datos disponibles."); return
    log_func(f"\n** {title} **"); df_formatted=df.copy();

    headers=df_formatted.columns.tolist()
    clean_headers=[]; header_map_reverse = {}
    for h in headers:
        h_str=str(h);clean_h=h_str
        if h_str.startswith("('") and h_str.endswith("')"): clean_h=h_str[2:-2].replace("', '"," / ")
        elif h_str.startswith("(") and h_str.endswith(")"):
             try: eval_h=eval(h_str); clean_h=" / ".join(map(str,eval_h)) if isinstance(eval_h,tuple) else h_str
             except Exception: pass
        clean_headers.append(clean_h)
        header_map_reverse[clean_h]=h

    numeric_cols_for_alignment_str = [str(c) for c in numeric_cols_for_alignment]
    id_cols_base=['Campaign','AdSet','Anuncio','Ad (🆔)','Level','Metric','Date','PeriodStart','PeriodEnd','Metrica','Nombre ADs','Paso del Embudo','Públicos Incluidos','Públicos Excluidos','Públicos In','Públicos Ex','Entrega','Estado_Ult_Dia','Estado_Ult_Dia','Estado', 'Entidad', 'Nombre Ads']
    id_cols = [str(c) for c in id_cols_base if str(c) in df_formatted.columns]

    cols_fmt={};
    cols_fmt.update({str(c):('float',p) for c,p in float_cols_fmt.items() if str(c) in df_formatted.columns})
    cols_fmt.update({str(c):('int',None) for c in int_cols if str(c) in df_formatted.columns})
    cols_fmt.update({str(c):('pct',p) for c,p in pct_cols_fmt.items() if str(c) in df_formatted.columns})
    cols_fmt.update({str(c):('stability',None) for c in stability_cols if str(c) in df_formatted.columns})

    curr_symbol = "$";
    if isinstance(currency_cols,dict):
        curr_dict = {str(c):('currency',s) for c,s in currency_cols.items() if str(c) in df_formatted.columns}
        cols_fmt.update(curr_dict)
        if currency_cols: curr_symbol = next(iter(curr_dict.values()), ('$',))[1]
    elif isinstance(currency_cols,str):
         curr_symbol = currency_cols
         standard_curr_cols = ['Inversion', 'Ventas_Totales', 'CPA', 'Ticket_Promedio', 'CPM', 'spend', 'value', 'cpa_global', 'cpm_global']
         for sc in standard_curr_cols:
             if str(sc) in df_formatted.columns: cols_fmt.update({str(sc):('currency',curr_symbol)})
    elif isinstance(currency_cols,list):
         curr_list_tuples = [(str(i[0]),('currency',i[1])) for i in currency_cols if isinstance(i,tuple) and len(i)==2 and str(i[0]) in df_formatted.columns]
         [cols_fmt.update({c:t}) for c,t in curr_list_tuples]
         if currency_cols and currency_cols[0] and isinstance(currency_cols[0], tuple) and len(currency_cols[0]) > 1:
             curr_symbol = currency_cols[0][1] if currency_cols[0][1] else "$"

    for col in headers:
        if col in cols_fmt:
             fmt, param = cols_fmt[col]
             try:
                 is_numeric_format = fmt in ['float', 'int', 'pct', 'stability', 'currency']
                 is_numeric_dtype = pd.api.types.is_numeric_dtype(df_formatted[col])
                 if is_numeric_format and not is_numeric_dtype:
                     # Aquí robust_numeric_conversion se importa desde utils, pero debe estar disponible
                     converted_col = df_formatted[col].apply(robust_numeric_conversion)
                 else:
                      converted_col = df_formatted[col].copy()
                 if fmt=='float': df_formatted[col]=converted_col.apply(lambda x:fmt_float(x,param))
                 elif fmt=='int': df_formatted[col]=converted_col.apply(fmt_int)
                 elif fmt=='pct': df_formatted[col]=converted_col.apply(lambda x:fmt_pct(x,param))
                 elif fmt=='stability': df_formatted[col]=converted_col.apply(fmt_stability)
                 elif fmt=='currency': df_formatted[col]=converted_col.apply(lambda x:f"{param}{fmt_float(x,default_prec)}" if pd.notna(x) and np.isfinite(x) else '-')
             except Exception as e_apply_fmt:
                 print(f"Adv: Error applying format '{fmt}' to column '{col}': {e_apply_fmt}. Column might have unexpected data types.")

    for col in headers:
        try:
            df_formatted[col] = df_formatted[col].apply(lambda x: str(x) if pd.notna(x) else '-')
        except Exception as e_final_str:
            print(f"FATAL Adv: Error in final str conversion for column '{col}': {e_final_str}. Attempting list comprehension fallback.")
            try:
                 df_formatted[col] = [str(item) if pd.notna(item) else '-' for item in df_formatted[col].values]
            except Exception as e_list_comp:
                 print(f"FATAL Adv: List comprehension str conversion failed for '{col}': {e_list_comp}. Column might be corrupted.")
                 df_formatted[col] = [f"!!!Error in {col}!!!" for _ in range(len(df_formatted))]

    col_widths={};
    for clean_h in clean_headers:
        orig_h=header_map_reverse[clean_h];
        if orig_h in df_formatted.columns:
            try:
                max_val = df_formatted[orig_h].str.len().max()
                max_val = 0 if pd.isna(max_val) else int(max_val)
                max_hdr = len(clean_h)
                width = max(max_val, max_hdr)
                if max_col_width is not None:
                    width = min(width, max_col_width)
                col_widths[orig_h]=width
            except Exception as e_width:
                 print(f"Adv: Error calculating string width for column '{orig_h}': {type(df_formatted[orig_h]).__name__} object has no attribute 'str'. Using header length.")
                 col_widths[orig_h] = len(clean_h)

    h_parts=[];s_parts=[]
    for clean_h in clean_headers:
        orig_h=header_map_reverse[clean_h];
        w=col_widths.get(orig_h, len(clean_h));
        h_parts.append(f"{clean_h:<{w}}")
        s_parts.append('-'*w)

    h_line="| "+ " | ".join(h_parts)+" |"; s_line="|-" + "-|-".join(s_parts) + "-|"
    log_func(h_line);log_func(s_line)

    for _,row in df_formatted.iterrows():
        vals=[]
        for orig_h in headers:
            clean_h = next((ch for ch, oh in header_map_reverse.items() if oh == orig_h), str(orig_h))
            val_full=str(row[orig_h]);
            w=col_widths.get(orig_h,len(val_full));
            val = val_full
            if len(val_full) > w:
                val = val_full[:w-1] + '…'
            is_id = orig_h in id_cols
            is_hinted_num = orig_h in numeric_cols_for_alignment_str
            is_formatted_numeric = orig_h in cols_fmt
            is_stab = orig_h in [str(c) for c in stability_cols]
            is_pct_col = clean_h.startswith('% Paso (')
            align = ">" if (is_formatted_numeric or is_hinted_num or is_stab or is_pct_col) and not is_id else "<"
            vals.append(f"{val:{align}{w}}")
        log_func("| "+" | ".join(vals)+" |")