import unicodedata
import re
import pandas as pd

__all__ = [
    'normalize',
    'create_flexible_regex_pattern',
    'aggregate_strings',
    'robust_numeric_conversion'
]

def normalize(text: str) -> str:
    """Simplified normalizer used across the project."""
    if text is None:
        return ''
    text = str(text)
    text = unicodedata.normalize('NFKD', text)
    text = ''.join(c for c in text if not unicodedata.combining(c))
    text = text.lower().strip()
    return text

def create_flexible_regex_pattern(text: str) -> str:
    """Return a regex pattern that loosely matches the normalized text."""
    norm = normalize(text)
    # Escape regex special characters
    escaped = re.escape(norm)
    # Allow arbitrary whitespace/underscore sequences where spaces appear
    pattern = re.sub(r'\s+', r'\\s*', escaped)
    pattern = pattern.replace('_', r'[\\s_]*')
    return pattern

def aggregate_strings(series, separator=' | ', max_len=None):
    if series is None:
        return ''
    vals = [normalize(v) for v in series if pd.notna(v) and str(v).strip()]
    uniq = []
    for v in vals:
        if v not in uniq:
            uniq.append(v)
    result = separator.join(uniq)
    if max_len is not None and len(result) > max_len:
        result = result[:max_len-1] + '…'
    return result

def robust_numeric_conversion(value):
    if pd.isna(value):
        return pd.NA
    if isinstance(value, (int, float)):
        return value
    text = str(value).strip()
    # Remove currency symbols and spaces
    text = re.sub(r'[^0-9,.-]', '', text)
    # Replace comma as decimal if appropriate
    if text.count(',') > 0 and text.count('.') == 0:
        text = text.replace('.', '')
        text = text.replace(',', '.')
    elif text.count(',') > 1 and text.count('.') > 0:
        text = text.replace('.', '')
        text = text.replace(',', '.')
    try:
        return float(text)
    except ValueError:
        return pd.NA
