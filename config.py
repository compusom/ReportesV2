# report_generator_project/config.py
from utils import normalize # Suponiendo que normalize está en utils.py

# ============================================================
# CONFIGURACIÓN ESPECÍFICA DEL REPORTE
# ============================================================
norm_map = {
    'campaign': [normalize('Nombre de la campaña'), normalize('Campaign name')],
    'adset': [normalize('Nombre del conjunto de anuncios'), normalize('Ad set name')],
    'ad': [normalize('Nombre del anuncio'), normalize('Ad name')],
    'entrega':  [normalize('Entrega del anuncio'), normalize('Ad delivery')],
    'adset_delivery_status':  [normalize('Estado entrega AdSet'), normalize('Ad Set Delivery Status'), normalize('Entrega del conjunto de anuncios')],
    'campaign_delivery_status':[normalize('Estado entrega campaña'), normalize('Campaign Delivery Status'), normalize('Entrega de la campaña')],
    'ad_delivery_status':     [normalize('Estado entrega anuncio'), normalize('Ad Delivery Status')],
    'aud_in': [normalize('Públicos personalizados incluidos'), normalize('Custom audiences included')],
    'aud_ex': [normalize('Públicos personalizados excluidos'), normalize('Custom audiences excluded')],
    'spend': [normalize('Importe gastado'), normalize('Amount spent')], # Gasto
    'reach': [normalize('Alcance'), normalize('Reach')], # Alcance
    'impr': [normalize('Impresiones'), normalize('Impressions')], # Impresiones
    'freq': [normalize('Frecuencia'), normalize('Frequency')], # Frecuencia
    
    # CTR: Usaremos 'ctr_unico_todos' como el nombre interno principal para CTR
    # Mapear todas las variantes de CTR (enlace, salientes, todos) a este si se decide usar solo uno.
    # O mantenerlos separados si se necesita el desglose y luego elegir cuál mostrar.
    # Por ahora, mapearemos el "CTR único (todos)" a un nombre interno específico.
    'ctr_unico_todos': [normalize('Clics únicos en el enlace (todos)'), normalize('Unique link clicks (all)'), 
                        normalize('CTR (todos)'), normalize('CTR (todo)'), normalize('CTR único (todos)')], # CTR (para usar como principal)

    # Mantenemos los otros mapeos de clics por si son necesarios para otros cálculos o por si se decide mostrarlos
    'clicks': [normalize('Clics en el enlace'), normalize('Link clicks')], # Clics en el enlace (tradicional)
    'clicks_out': [normalize('Clics salientes'), normalize('Outbound clicks')], # Clics salientes

    'visits': [normalize('Visitas a la página de destino'), normalize('Landing page views')], # Visitas a la página de destino
    
    # Nuevas métricas de embudo
    'attention': [normalize('Atención'), normalize('Atencion')], # Atención (Hook)
    'interest': [normalize('Interés'), normalize('Interes')], # Interés (Body)
    'deseo': [normalize('Deseo')], # Deseo (CTA)

    'addcart': [normalize('Artículos agregados al carrito'), normalize('Website adds to cart'), normalize('Adds to cart')], # Agregados al carrito
    
    # Checkout / Información de pagp
    'checkout': [normalize('Pagos iniciados en el sitio web'), normalize('Website checkouts initiated'), 
                 normalize('Pagos iniciados'), normalize('Checkouts initiated'),
                 normalize('Información de pago agregada en el sitio web'), normalize('Website payment info added'), # Nuevo mapeo
                 normalize('Información de pago agregada')],                                                            # Nuevo mapeo

    'purchases': [normalize('Compras'), normalize('Website purchases')], # Compras (que ahora se llamará "Ventas" en el reporte)
    'value': [normalize('Valor de conversión de compras'), normalize('Website purchase conversion value')], # Valor (que se llamará "Total Ventas")
    
    'value_avg': [normalize('Valor de conversión de compras promedio'), normalize('Average purchase conversion value')],
    'roas': [normalize('ROAS (retorno de la inversión en publicidad) de compras en el sitio web'), normalize('ROAS (retorno de la inversión en publicidad) de compras'), normalize('Website purchase ROAS (return on ad spend)'), normalize('Purchase ROAS (return on ad spend)')],
    'cpa': [normalize('Costo por compra'), normalize('Cost per website purchase'), normalize('Cost per Purchase')],
    
    # Métricas de video (se mapean pero no se mostrarán en bitácora)
    'rv3': [normalize('Reproducciones de video de 3 segundos'), normalize('Video plays at 3s'), normalize('3-Second Video Plays')],
    'rv25': [normalize('Reproducciones de video hasta el 25%'), normalize('Video plays at 25%'), normalize('Video Plays at 25%')],
    'rv75': [normalize('Reproducciones de video hasta el 75%'), normalize('Video plays at 75%'), normalize('Video Plays at 75%')],
    'rv100': [normalize('Reproducciones de video hasta el 100%'), normalize('Video plays at 100%'), normalize('Video Plays at 100%')],
    'rtime': [normalize('Tiempo promedio de reproducción del video'), normalize('Avg. video watch time'), normalize('Average video play time')],
}

numeric_internal_cols = [
    'spend', 'reach', 'impr', 'freq', 
    'ctr_unico_todos', # Usar este como el CTR principal
    'clicks', 'clicks_out', # Mantener por si son necesarios para calcular ctr_unico_todos si no viene directo
    'visits',
    'attention', 'interest', 'deseo', # Nuevas de embudo
    'addcart', 'checkout', 'purchases', 
    'value', 'value_avg', 'roas', 'cpa',
    'rv3', 'rv25', 'rv75', 'rv100', 'rtime' # Video
]

# Símbolos de moneda preferidos (puedes expandir esto)
CURRENCY_SYMBOLS = {
    'USD': '$',
    'EUR': '€',
    'GBP': '£',
    'ARS': '$', # Peso argentino
    'COP': '$', # Peso colombiano
    # Añade más códigos ISO y sus símbolos
}
DEFAULT_CURRENCY_SYMBOL = '$' # Símbolo por defecto si no se encuentra