import pandas as pd 

import numpy as np 

  

#Cargar los datos 

rfm_data = pd.read_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-09/resultados_cluster_rfm_mensual_09_24.csv')  
data =pd.read_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-09/data_filtrada_hasta_mes_ant_09_24.csv')
clientes_features_relevantes = pd.read_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-09/clientes_features_mensual_09_24.csv')
  

# Separar los clusters para obtener estadísticas 

cluster_0 = rfm_data[rfm_data['rfm'] == 0]  

cluster_1 = rfm_data[rfm_data['rfm'] == 1]  

cluster_2 = rfm_data[rfm_data['rfm'] == 2]  

  

# Valor monetario promedio de cada uno  

valor_monetario_0 = cluster_0['total_actualizado'].mean()  

valor_monetario_1 = cluster_1['total_actualizado'].mean()  

valor_monetario_2 = cluster_2['total_actualizado'].mean()  

  

# Recencia de cada uno  

recencia_0 = cluster_0['Recencia'].mean()  

recencia_1 = cluster_1['Recencia'].mean()  

recencia_2 = cluster_2['Recencia'].mean()  

  

# Frecuencia de cada uno  

frecuencia_0 = cluster_0['Frecuencia'].mean()  

frecuencia_1 = cluster_1['Frecuencia'].mean()  

frecuencia_2 = cluster_2['Frecuencia'].mean()  

  

# Diccionario vacío para llenar con nombre de cada cluster  

cluster_traducciones = {}  

  

# Determinar el nombre de cada cluster 

if valor_monetario_0 > valor_monetario_1 and valor_monetario_0 > valor_monetario_2:  

    cluster_traducciones[0] = 'fidelizado'  

    if recencia_1 > recencia_2:  

        cluster_traducciones[1] = 'perdido'  

        cluster_traducciones[2] = 'en peligro'  

    else:  

        cluster_traducciones[1] = 'en peligro'  

        cluster_traducciones[2] = 'perdido'  

elif valor_monetario_1 > valor_monetario_0 and valor_monetario_1 > valor_monetario_2:  

    cluster_traducciones[1] = 'fidelizado'  

    if recencia_0 > recencia_2:  

        cluster_traducciones[0] = 'perdido'  

        cluster_traducciones[2] = 'en peligro'   

    else:  

        cluster_traducciones[0] = 'en peligro'  

        cluster_traducciones[2] = 'perdido'  

else:   

    cluster_traducciones[2] = 'fidelizado'  

    if recencia_0 > recencia_1:  

        cluster_traducciones[0] = 'perdido'  

        cluster_traducciones[1] = 'en peligro'   

    else:  

        cluster_traducciones[0] = 'en peligro'  

        cluster_traducciones[1] = 'perdido'  

  

# Aplicar el mapeo de nombres a la columna 'rfm' 

rfm_data['rfm'] = rfm_data['rfm'].map(cluster_traducciones)  

  

# Juntar y poner fecha 

segmentacion_clientes = pd.merge(clientes_features_relevantes, rfm_data)  

segmentacion_clientes['fecha_segmentacion'] = '2024-09'  

# Marca que consumen los monomarca  

# Obtengo los mails  

clientes_monomarca = segmentacion_clientes[segmentacion_clientes.Multimarca == 0][['mail']] 

  

# Asegúrate de que la columna 'producto_que_consume' esté en formato 'object' o 'string' 

clientes_monomarca['producto_que_consume'] = ''  # Inicializa como cadena vacía 

  

for i in clientes_monomarca.index:  

    # Filtro por mail  

    marca_que_consume = data[data.mail == clientes_monomarca.mail[i]]  

    # Solo cigarrillos  

    marca_que_consume = marca_que_consume[marca_que_consume.tipo_prd_id == 1]  

    marca_que_consume = str(marca_que_consume.titulo.unique())  

    clientes_monomarca.loc[i, 'producto_que_consume'] = marca_que_consume  

  

# Anexo  

segmentacion_clientes = segmentacion_clientes.merge(clientes_monomarca, how='left') 

# Fecha última compra  

# Obtengo última fecha  

ultima_fecha_por_cliente = data[['mail','fecha']].groupby('mail').max().reset_index()  

# Renombro  

ultima_fecha_por_cliente.rename(columns={'fecha':'ultima_compra'}, inplace=True)  

# Anexo  

segmentacion_clientes = segmentacion_clientes.merge(ultima_fecha_por_cliente, how='left')  

  

# Contenido última compra  

contenido_ultima_compra_por_cliente = data.groupby(['mail','Periodo'])['titulo'].apply(', '.join).reset_index().drop_duplicates(subset=['mail'], keep='last')  

# Descarto periodo  

contenido_ultima_compra_por_cliente.drop(columns=['Periodo'], inplace=True)  

# Renombro  

contenido_ultima_compra_por_cliente.rename(columns={'titulo':'Contenido Ultima Compra'}, inplace=True)  

# Merge con segmentacion clientes  

segmentacion_clientes = segmentacion_clientes.merge(contenido_ultima_compra_por_cliente, how='left')  

  

# Contenido anteúltima compra  

contenido_compras_por_cliente = data.groupby(['mail','Periodo'])['titulo'].apply(', '.join).reset_index()  

  

# Asegúrate de que la columna 'Contenido Ante Ultima Compra' esté en formato 'object' 

segmentacion_clientes['Contenido Ante Ultima Compra'] = ''  # Inicializa como cadena vacía 

  

for i, cliente in enumerate(segmentacion_clientes.mail):  

    try:  

        segmentacion_clientes.loc[i, 'Contenido Ante Ultima Compra'] = contenido_compras_por_cliente[contenido_compras_por_cliente.mail == cliente]['titulo'].values[-2]  

    except: 

        segmentacion_clientes.loc[i, 'Contenido Ante Ultima Compra'] = np.nan 

  

# Importo segmentación vieja  

segmentacio_vieja = pd.read_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-08/segmentacion_18_meses_hasta_AGO_2024.csv', index_col=0)  

segmentacion_clientes = pd.concat([segmentacio_vieja, segmentacion_clientes], ignore_index=True)  

  

# Cambio estrella por fidelizados  

segmentacion_clientes.replace({'estrella':'fidelizado'}, inplace=True)  

  

# Guardo CSV  
segmentacion_clientes.to_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-09/segmentacion_18_meses_hasta_SEP_2024.txt', sep='\t',index=False)  

segmentacion_clientes.to_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-09/segmentacion_18_meses_hasta_SEP_2024.csv', index=False)  

segmentacion_clientes.to_excel(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-09/segmentacion_18_meses_hasta_SEP_2024.xlsx', index=False)  
  

# Guardar el DataFrame actualizado de rfm_data en un nuevo CSV

rfm_data.to_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-09/resultados_cluster_rfm_etiquetado_mensual_09_24.csv', index=False) 

  

# Mostrar las primeras filas de segmentacion_clientes 

print(segmentacion_clientes.head())  # Muestra las primeras filas del DataFram

print(f'Fechas de segmentacion: {segmentacion_clientes.fecha_segmentacion.unique()}')
print ('Se ha creado el CSV "resultados_cluster_rfm_etiquetado_mensual" y "segmantacion_clientes_mensual"')
print (' El proceso se ha terminado con exito ')