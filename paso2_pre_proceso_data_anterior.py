import pandas as pd 

import datetime 

  

# Cargar datasets necesarios 

precios = pd.read_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-09/precios_mensual_09_24.csv') 

data = pd.read_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-09/resultados_formateados_mensual_09_24.csv') 

data_anterior = pd.read_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-08/data_18_meses_hasta_AGO_2024.csv') 

  

# Calcular el índice de actualización con los 4 productos más vendidos del mes 

productos_mas_vendidos = data.groupby('product_id').sum(numeric_only=True)['quantity'].reset_index().sort_values('quantity', ascending=False).head(4)['product_id'].values 

  

# Precio de la canasta de esos productos (nueva) 

canasta_nueva = precios[precios.product_id.isin(productos_mas_vendidos)]['precio'].sum() 

  

# Precio viejo de esos productos 

productos_viejos = data_anterior[data_anterior.product_id.isin(productos_mas_vendidos)] 

  

# Precio individual de productos viejos 

productos_viejos['precio'] = productos_viejos['total_actualizado'] / productos_viejos['quantity'] 

  

# Descartar columnas innecesarias 

productos_viejos = productos_viejos[['product_id', 'precio']] 

  

# Precio de la canasta vieja 

canasta_vieja = productos_viejos.drop_duplicates(subset=['product_id'])['precio'].sum() 

  

# Índice de actualización 

indice_actualizacion = pd.to_numeric(canasta_nueva) / canasta_vieja 

  

# Calcular precio viejo para data_anterior 

data_anterior['precio_viejo'] = data_anterior['total_actualizado'] / data_anterior['quantity'] 

  

# Agregar precios actuales a data_anterior 

data_anterior = data_anterior.merge(precios, how='left') 

  

# Actualizar precios si son NaN usando el índice de actualización 

for i in data_anterior.index: 

    if pd.isna(data_anterior.loc[i, 'precio']): 

        data_anterior.loc[i, 'precio'] = data_anterior.loc[i, 'precio_viejo'] * indice_actualizacion 

  

# Actualizar el total actualizado 

data_anterior['total_actualizado'] = pd.to_numeric(data_anterior.precio) * data_anterior.quantity 

  

# Descartar la columna de precio viejo 

data_anterior.drop(columns=['precio_viejo'], inplace=True) 

  

# Combinar datos antiguos con los nuevos 

data_hasta_mes_ant = pd.concat([data_anterior, data], ignore_index=True) 

  

# Guardar el CSV actualizado al mes actual

data_hasta_mes_ant.to_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-09/data_18_meses_hasta_SEP_2024.csv') 

  

# Recargar el CSV para procesamiento adicional 

data_hasta_mes_ant = pd.read_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-09/data_18_meses_hasta_SEP_2024.csv', index_col=0) 

  

# Filtrar clientes que no compraron en el último año 

data = data_hasta_mes_ant.copy() 

  

# Convertir formatos de fecha 

data["fecha"] = pd.to_datetime(data["fecha"]) 

  

# Ordenar el dataframe por fecha de compra en orden ascendente 

data = data.sort_values("fecha", ascending=True) 

  

# Agrupar el dataframe por cliente y obtener la última fecha de compra 

ultima_compra_por_cliente = data.groupby("mail").fecha.last().reset_index() 

  

# Fecha actual 

today = datetime.date.today() 

  

# Un año atrás 

one_year_ago = today - pd.Timedelta(days=365) 

  

# Filtrar clientes que compraron en el último año 

ultima_compra_por_cliente = ultima_compra_por_cliente[ultima_compra_por_cliente['fecha'] >= pd.to_datetime(one_year_ago)] 

  

# Obtener la lista de mails de clientes que compraron en el último año 

clientes_que_compraron_ultimo_anio = ultima_compra_por_cliente.mail.to_list() 

  

# Filtrar el dataframe principal con los clientes que compraron en el último año 

data = data[data.mail.isin(clientes_que_compraron_ultimo_anio)] 

  

# Guardar el CSV final con los datos filtrados 

data.to_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-09/data_filtrada_hasta_mes_ant_09_24.csv', index=False) 

  

# PATRONES DE CONSUMO

clientes = data['mail'].unique().tolist() 

clientes_features = pd.DataFrame(clientes, columns=['mail']) 

  
#cigarrillos u otros prod
df_otros_productos = data[data['tipo_prd_id'] == 2] 

df_cigarrillos = data[data['tipo_prd_id'] == 1] 

  
#clientes de otros productos
clientes_clientes_otros_prod = df_otros_productos['mail'].unique().tolist() 

clientes_features['otros_productos'] = 0 

  
#consumen otros prod?
for i in clientes_features['mail']: 

    if i in clientes_clientes_otros_prod: 
        #si esta en otros prod. cambia valor a 1
        clientes_features.loc[clientes_features['mail'] == i, 'otros_productos'] = 1 

  

# Multimarca / MONOMARCA
#si el cliente consume mas de una marca, se agrega
clientes_multimarcas = [] 

  

for i in clientes: 

    if len(df_cigarrillos[df_cigarrillos['mail'] == i].MARCA.unique()) != 1: 

        clientes_multimarcas.append(i) 
#creo la columna con valor 0, si el cliente esta en la lista, se cambia a 1
  

clientes_features['Multimarca'] = 0 

for i in clientes_features['mail']: 

    if i in clientes_multimarcas: 

        clientes_features.loc[clientes_features['mail'] == i, 'Multimarca'] = 1 

  

# Combos 

#columna para cambiar si consume combo
clientes_features['consume_combo'] = 0
#lista de clientes que consumen combo

clientes_combo = data[data['Combo'] == 1]['mail'].unique().tolist() 

  
#si esta en lista de combos, cambia valor a 1
for i in clientes_features['mail']: 

    if i in clientes_combo: 

        clientes_features.loc[clientes_features['mail'] == i, 'consume_combo'] = 1 

  

# Tipo de presentación 
#genero las columnas para despues cambiar el valor si es necesario
clientes_features['box'] = 0 

clientes_features['soft_pack'] = 0 
#clientes que consumen Box
clientes_box = df_cigarrillos[df_cigarrillos.Presentacion == 'Box']['mail'].unique().tolist() 
#clientes que consumen Soft_pack
clientes_soft_pack = df_cigarrillos[df_cigarrillos.Presentacion == 'Soft_pack']['mail'].unique().tolist() 

  

for i in clientes_features['mail']: 

    if i in clientes_box: 

        clientes_features.loc[clientes_features['mail'] == i, 'box'] = 1 

    if i in clientes_soft_pack: 

        clientes_features.loc[clientes_features['mail'] == i, 'soft_pack'] = 1 

  

# Mezcla marcas en la misma compra 
#agrupo por numero de comprobante y cuento cantidades de marcas, si son mayores a uno, guardo las facturas en una lista
mask = df_cigarrillos.groupby('bill_id')['MARCA'].nunique() > 1 

facturas_donde_se_mezclan_marcas = df_cigarrillos.groupby('bill_id')['MARCA'].nunique()[mask].index.tolist() 
#de la lista de ventas de cigarrillos, filtro por facturas donde se mezclan marcas
ventas_mezcla_marcas = df_cigarrillos[df_cigarrillos['bill_id'].isin(facturas_donde_se_mezclan_marcas)] 

#armo lista con clientes que estan en estas ventas
clientes_que_mezclan_en_misma_compra = ventas_mezcla_marcas['mail'].unique().tolist() 

clientes_features['mezcla_en_misma_compra'] = 0 
#si esta en lista de combos, cambia valor a 1
for i in clientes_features['mail']: 

    if i in clientes_que_mezclan_en_misma_compra: 

        clientes_features.loc[clientes_features['mail'] == i, 'mezcla_en_misma_compra'] = 1 

  

# Full Flavors (FF) 

clientes_no_fff = data[data.CATEGORÍA != 'FF'].mail.unique().tolist() 

clientes_features['FF'] = 1 

#si esta en lista de no FF, cambia valor a 0
for i in clientes_features['mail']: 

    if i in clientes_no_fff: 

        clientes_features.loc[clientes_features['mail'] == i, 'FF'] = 0 

  

# Clusters 
#selecciono columnas relevantes
clientes_features_relevantes = clientes_features[['mail', 'Multimarca', 'mezcla_en_misma_compra', 'consume_combo', 'FF']]   
#creo mascaras
monomarca_FF_mask = (clientes_features_relevantes.Multimarca == 0) & (clientes_features_relevantes.FF == 1) 

multimarca_FF_mask = (clientes_features_relevantes.Multimarca == 1) & (clientes_features_relevantes.FF == 1) 

monomarca_no_FF_mask = (clientes_features_relevantes.Multimarca == 0) & (clientes_features_relevantes.FF == 0) 

multimarca_no_FF_mask = (clientes_features_relevantes.Multimarca == 1) & (clientes_features_relevantes.FF == 0) 

  

clientes_features_relevantes.loc[monomarca_FF_mask, "cluster_patrones_consumo"] = "FF - Monomarca" 

clientes_features_relevantes.loc[multimarca_FF_mask, "cluster_patrones_consumo"] = "FF - Multimarca" 

clientes_features_relevantes.loc[monomarca_no_FF_mask, "cluster_patrones_consumo"] = "No FF - Monomarca" 

clientes_features_relevantes.loc[multimarca_no_FF_mask, "cluster_patrones_consumo"] = "No FF - Multimarca" 

  

# Guardar CSV final 

clientes_features_relevantes.to_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-09/clientes_features_mensual_09_24.csv', index=False) 

print('CSV "clientes_features_relevantes_mensual" creado con éxito') 