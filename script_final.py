import mysql.connector 
from mysql.connector import Error 
import datetime 
import pandas as pd 
import numpy as np 
import matplotlib.pyplot as plt 
import seaborn as sns 
from sklearn.preprocessing import StandardScaler 
from sklearn.cluster import KMeans 


# Obtener la fecha actual 

today = datetime.date.today() 

# crear conexión 

try: 

    cnx = mysql.connector.connect(user='dbreader', password='ZPO5ZMI8q9$6ncRv', 
                                  host='ecomarprddb-20240412104121819200000001.cvpvuylh12yi.eu-west-1.rds.amazonaws.com', 
                                  database='gharg-wp-prd', 
                                  port=3306) 

    # crear cursor 

    cursor = cnx.cursor() 

  

    # ejecutar una prueba 

    cursor.execute('SELECT 1') 
    result = cursor.fetchone() 
    print(f'Conexión exitosa!!!') 

except Error as e: 

    print(f'Error de conexión {e}') 

  

# consulta todos los ids del ultimo mes

query = (f"""SELECT ID FROM pr_2_posts 
                WHERE post_type = 'shop_order' and post_status = 'wc-bill' 
                and YEAR(post_date) = 2024
                and MONTH(post_date) = 11
            ORDER BY post_date DESC;""")




cursor.execute(query) 

# crear lista con ventas 

post_id_ventas_hasta_mes_actual = [] 
for i in cursor: 
    post_id_ventas_hasta_mes_actual.append(i[0]) 

  

# crear DataFrame 

ventas_hasta_mes_actual = pd.DataFrame() 
ventas_hasta_mes_actual['post_id'] = post_id_ventas_hasta_mes_actual 
ventas_hasta_mes_actual['post_id'] = ventas_hasta_mes_actual['post_id'].astype(int)
#ventas_hasta_mes_actual['bill_id'] = ventas_hasta_mes_actual['bill_id'].astype(int)

# mail 

def buscar_cuil(df): 

    """ 

    Recibe un df con post_id de ventas y devuelve otro df con el 
    cuil correspondiente a cada venta 

    """ 

    # Convertir explícitamente los valores a Python 'int'  

    post_id_values = tuple(int(x) for x in df['post_id'].values) 

    query = (f"""SELECT post_id, meta_value FROM pr_2_postmeta  

                 WHERE post_id in {post_id_values} 
                 and meta_key = '_billing_email';""") 

    cursor.execute(query) 

    resultado = cursor.fetchall() 
    cuil = pd.DataFrame(resultado, columns=['post_id', 'mail']) 

    return df.merge(cuil) 
  

ventas_hasta_mes_actual = buscar_cuil(ventas_hasta_mes_actual) 

print(f'CSV ventas_hasta_mes_actual {ventas_hasta_mes_actual.info()}')
  

# comprobantes 

# hacer la consulta 

# Convertir explícitamente los valores de post_id a int (si no son nativos de Python) 

order_id_values = tuple(int(x) for x in ventas_hasta_mes_actual['post_id'].values) 
query = (f"""SELECT order_id, id, type, creation_date 
         FROM pr_2_pmi_bills 
         WHERE order_id in {order_id_values};""") 
cursor.execute(query) 
resultado = cursor.fetchall() 

bills = pd.DataFrame(resultado, columns=['post_id', 'bill_id', 'type', 'fecha']) 

# añadir el mail 

ventas_hasta_mes_actual = bills.merge(ventas_hasta_mes_actual, how='left') 

  

# prod por bill id 

# hacer la consulta 

bill_id_values = tuple(int(x) for x in ventas_hasta_mes_actual['bill_id'].values) 

query = (f"""SELECT bill_id, product_id, quantity 
         FROM pr_2_pmi_bill_items 
         WHERE bill_id in {bill_id_values};""") 

cursor.execute(query) 


# guardar el resultado 

productos_vendidos_hasta_mes_actual = cursor.fetchall() 

# transformar a df 

productos_vendidos_hasta_mes_actual = pd.DataFrame(productos_vendidos_hasta_mes_actual, columns=['bill_id', 'product_id', 'quantity']) 
productos_vendidos_hasta_mes_actual['bill_id'] = productos_vendidos_hasta_mes_actual['bill_id'].astype(int)
productos_vendidos_hasta_mes_actual['product_id'] = productos_vendidos_hasta_mes_actual['product_id'].astype(int)
productos_vendidos_hasta_mes_actual['quantity'] = productos_vendidos_hasta_mes_actual['quantity'].astype(int)

print(f'CSV productos_vendidos_hasta_mes_actual {productos_vendidos_hasta_mes_actual.info()}')

# descripcion
#consulto a post

product_id_values = tuple(int(x) for x in productos_vendidos_hasta_mes_actual['product_id'].values) 

query = (f"""SELECT id, post_title, post_content 
         FROM pr_2_posts 
         WHERE id  in {product_id_values};""")
cursor.execute(query)
#guardo el resultado
prod_descripcion = cursor.fetchall()
#paso a df
prod_descripcion = pd.DataFrame(prod_descripcion, columns =['product_id', 'titulo', 'descripcion'])

# cigarrillos u otros prod
#consulot a postmeta si son cigarrillos u otros prod
product_id_values = tuple(int(x) for x in productos_vendidos_hasta_mes_actual['product_id'].values) 

query = (f"""SELECT post_id, meta_value 
         FROM pr_2_postmeta 
         WHERE post_id  in {product_id_values} and meta_key = 'tipo_prd_id';""")
cursor.execute(query)
#guardo el resultado
tipo_prd_id = cursor.fetchall()

#paso a df
tipo_prd_id = pd.DataFrame(tipo_prd_id, columns =['product_id', 'tipo_prd_id'])

#junto
prod_descripcion = prod_descripcion.merge(tipo_prd_id)
productos_vendidos_hasta_mes_actual = productos_vendidos_hasta_mes_actual.merge(prod_descripcion,how='left')

print(f'CSV productos_vendidos_hasta_mes_actual (completo) {productos_vendidos_hasta_mes_actual.info()}')


# replico datos de facturante 
# juntar las dos tablas que venía manejando 

df = productos_vendidos_hasta_mes_actual.merge(ventas_hasta_mes_actual, how='left') 

# convertir a numérico 

df['bill_id'] = df['bill_id'].astype(int)

df['product_id'] = df['product_id'].astype(int)

df['quantity'] = df['quantity'].astype(int)

df['tipo_prd_id'] = df['tipo_prd_id'].astype(int)

df['post_id'] = df['post_id'].astype(int) 

data = df.copy() 

  

# título descripción 

data['titulo_descripcion'] = data.titulo + ' ' + data.descripcion 

  

# notas de crédito 

# ordenar 

data.sort_values(['post_id', 'titulo_descripcion', 'fecha'], inplace=True) 

data.reset_index(drop=True, inplace=True) 

  

# si hay nota de crédito y coinciden el product id, post id, 
# y la fecha es anterior, resta la cantidad 

for i in data.index: 

    if (data.type[i] == 'pmi-credit-note') and (data.product_id[i] == data.product_id[i-1]) and (data.post_id[i] == data.post_id[i-1]) and (data.fecha[i] > data.fecha[i-1]): 

        data.loc[i-1, 'quantity'] = data['quantity'][i-1] - data['quantity'][i] 

  

# eliminar las nc 

data = data[data.type != 'pmi-credit-note'] 

  
# eliminar los comprobantes sin unidad 

data = data[data.quantity != 0] 

  

# campo unidad 

# máscaras 

uni10_mask = data.titulo_descripcion.str.contains("10") 
uni20_mask = data.titulo_descripcion.str.contains("20") 
uni12_mask = data.titulo_descripcion.str.contains("12") 
otros_mask = data.tipo_prd_id == 2 

  

# aplicar 

data.loc[uni10_mask == True, "Unidad"] = 10 
data.loc[uni20_mask == True, "Unidad"] = 20 
data.loc[uni12_mask == True, "Unidad"] = 12 
data.loc[otros_mask == True, "Unidad"] = 1 

  

# presentación 

# máscaras de PRESENTACIÓN 

Box_mask = data.titulo_descripcion.str.contains("Box|BOX|box") 
soft_mask = data.titulo_descripcion.str.contains("Box|BOX|box") == False 
otros_mask = data.tipo_prd_id == 2 

  

# aplicar 

data.Presentacion = "sin datos" 
data.loc[Box_mask == True, "Presentacion"] = "Box" 
data.loc[soft_mask == True, "Presentacion"] = "Soft_pack" 
data.loc[otros_mask == True, "Presentacion"] = "Otros Productos" 

  

# combos 

# de las cantidades posibles, cuáles indican combos 

cantidades_que_indican_combo = [i for i in data.quantity.unique() if (i < 10 or i % 2 != 0) and i != 15 and i != 45] 

  

# crear la columna combo 

data['Combo'] = 0 

  

# para cada elemento de la tabla, si la columna cantidad indica combo, 
# cambia el valor a 1. 

for i in data.index: 

    if data.loc[i]['quantity'] in cantidades_que_indican_combo: 

        data.loc[i, 'Combo'] = 1 

  

data['Combo'] = pd.to_numeric(data['Combo']) 

  

# cartones por mes 

# transformar columna fecha a formato fecha 

data['fecha'] = pd.to_datetime(data.fecha) 

data["Periodo"] = data["fecha"].apply(lambda x: x.strftime('%Y-%m')) 

  

# agregar el campo Carton_unidad para indicar cuánto trae cada cartón 

# todos los cartones traen 10 unidades excepto los Parliament Super Slims Box 20 que traen 15 

data["Carton_unidad"] = 0 

data.loc[data["tipo_prd_id"] == 1, "Carton_unidad"] = 10 

data.loc[data["titulo_descripcion"] == "Parliament Super Slims Box 20", "Carton_unidad"] = 15 

  

# agregar el campo Carton_cantidad para indicar cuántos cartones compró 

data["Carton_cantidad"] = data["quantity"] / data["Carton_unidad"] 

  

# marca y categoría 

catalogacion = pd.read_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-06/Catalogación - Sheet1.csv').drop(columns=['Unnamed: 0']) 

catalogacion.rename(columns={'PRODUCTO': 'titulo'}, inplace=True) 

  

# acomodar algunos títulos 

data['titulo'] = data.titulo.str.replace('amp;', '') 

data['titulo'] = data.titulo.str.replace('\xa0', ' ') 

data = data.merge(catalogacion, how='left') 

data['MARCA'] = data['MARCA'].fillna('otros') 

  

# precios 

# precios de los productos vendidos 

# hacer la consulta 

product_id_values = tuple(int(x) for x in data['product_id'].values) 

query = (f"""SELECT product_id, creation_date, neto 

            FROM pr_2_pmi_prices_log 

            WHERE product_id in {product_id_values};""") 

cursor.execute(query) 

  

resultado = cursor.fetchall() 

precios_mensual = pd.DataFrame(resultado, columns=['product_id', 'fecha', 'precio']) 

  

# ordenar por product id y fecha 

precios_mensual.sort_values(['product_id', 'fecha'], inplace=True) 

# último precio de cada producto 

precios_mensual.drop_duplicates(subset=['product_id'], keep='last', inplace=True) 

# descartar fecha 

precios_mensual.drop(columns=['fecha'], inplace=True) 
data_mensual = data.merge(precios_mensual, how='left')
data_mensual['total_actualizado'] = data_mensual.precio * data_mensual.quantity 

  

# Guardar el DataFrame como un archivo CSV 
#precios_mensual.to_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-09/precios_mensual_09_24.csv', index=False)
#data_mensual.to_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-09/resultados_formateados_mensual_09_24.csv', index=False)  
print(f'Fecha inicial {data['fecha'].min()} / Fecha final {data['fecha'].max()}')
#print("Datos guardados con exito con el nombre 'resultados_formateados_mensual.csv'") 
#print("Datos guardados con exito con el nombre 'precios_mensual.csv'") 


#Impresiones
desc = data.describe()

ruta_arch = 'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-11/describe_df_formateado_11_24.txt'

with open(ruta_arch, 'w') as file:
     file.write(desc.to_string())
#-----------------------------------------------------------------------------------------------------------------------------------------

precios = precios_mensual.copy()

data = data_mensual

data_anterior = pd.read_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-11/data_18_meses_hasta_NOV_2024.csv', index_col = 0) 

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
data_hasta_mes_ant.to_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-11/data_18_meses_hasta_NOV_2024.csv') 

# Recargar el CSV para procesamiento adicional 
#data_hasta_mes_ant = pd.read_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-09/data_18_meses_hasta_SEP_2024.csv') 

  

# Filtrar clientes que no compraron en el último año 
data = data_hasta_mes_ant.copy() 

# Convertir formatos de fecha 
data["fecha"] = pd.to_datetime(data["fecha"]) 

# Ordenar el dataframe por fecha de compra en orden ascendente 
data = data.sort_values("fecha", ascending=True) 

# Agrupar el dataframe por cliente y obtener la última fecha de compra 
ultima_compra_por_cliente = data.groupby("mail").fecha.last()
ultima_compra_por_cliente = ultima_compra_por_cliente.reset_index()

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
data.to_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-11/data_filtrada_hasta_mes_ant_11_24.csv', index=False) 

#---------------------------------------------------------------------------------------------------------------------------
# PATRONES DE CONSUMO

clientes = data['mail'].unique().tolist() 
clientes_features = pd.DataFrame(clientes, columns=['mail']) 

  
#cigarrillos u otros prod
df_otros_productos = data[data['tipo_prd_id'] == 2] 
df_cigarrillos = data[data['tipo_prd_id'] == 1] 
clientes_cigarrillos = df_cigarrillos['mail'].unique().tolist()

#clientes de otros productos
clientes_clientes_otros_prod = df_otros_productos['mail'].unique().tolist() 

#consumen otros prod?
clientes_features['otros_productos'] = 0   
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

#clientes_features_relevantes.to_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-09/clientes_features_mensual_09_24.csv', index=False) 

#print('CSV "clientes_features_relevantes_mensual" creado con éxito') 

#---------------------------------------------------------------------------------------------------------------------------------------


# Cargar los datasets 

# precios = pd.read_csv(r'C:\\Users\\ccendago\\OneDrive - Philip Morris International\\CSV\\precios_mensual.csv')   

#data = pd.read_csv(r'C:\\Users\\ccendago\\OneDrive - Philip Morris International\\CSV\\resultados_formateados_mensual.csv') 

#data = pd.read_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-09/data_filtrada_hasta_mes_ant_09_24.csv', index_col=0) 

print (data.head(5))
# Convertir formatos de fecha 
data["fecha"] = pd.to_datetime(data["fecha"], errors="coerce") 

#Elimino los registrso duplicados de compras en la misma fecha por cliente 
data_unique = data.drop_duplicates(subset=["mail","fecha"])

#Convierto la columna total actualizado a numerico  
data["total_actualizado"] = pd.to_numeric(data["total_actualizado"], errors="coerce")

   
# Valor monetario (Monetary) 
rfm_m = data.groupby("mail")["total_actualizado"].sum().reset_index().round(2) 
#rfm_m =rfm_m.rename(columns={"total_actualizado": "Valor Monetario"})

  
# Frecuencia (Frequency) 
rfm_f = data.groupby("mail")["fecha"].count().reset_index() 
rfm_f = rfm_f.rename(columns={"fecha": "Frecuencia"}) 

  
# Recencia (Recency) 
max_date = max(data["fecha"]) #obtengo la ultima fecha en el dataset 
data["Fecha_Dif"] = max_date - data["fecha"] 
  

# Computar última transacción 
rfm_p = data.groupby("mail")["Fecha_Dif"].min().reset_index() 

  
# Extraer la cantidad de días 
rfm_p["Recencia"] = rfm_p["Fecha_Dif"].dt.days 
rfm_p = rfm_p.drop(columns=["Fecha_Dif"]) #elimino la columna intermedia 

mail_deseado = 'eulpino@gmail.com'
print(f'La recencia del cliente {mail_deseado} es: {rfm_p.loc[rfm_p["mail"]== mail_deseado, 'Recencia'].values[0]} dias'if not rfm_p.loc[rfm_p['mail']== mail_deseado, 'Recencia'].empty else f'El cliente con mail {mail_deseado} no existe en el dataset')  

# Juntar los 3: Monetario, Frecuencia y Recencia 
rfm_data = pd.merge(rfm_m, rfm_f, on="mail", how="inner") 
rfm_data = pd.merge(rfm_data, rfm_p, on="mail", how="inner") 

numeric_columns= ['total_actualizado', 'Frecuencia', 'Recencia']   

Q1 = rfm_data[numeric_columns].quantile(0.25)
Q3 = rfm_data[numeric_columns].quantile(0.75)
IRQ = Q3 - Q1

lower_bound = Q1 - 1.5 * IRQ
upper_bound = Q3 + 1.5 * IRQ

rfm_data_filtrado = rfm_data[

       ~ rfm_data[numeric_columns].apply(
           lambda row: any((row < lower_bound) | (row > upper_bound)), axis=1
       )
] 

# Escalamiento de las variables numéricas 
rfm_data_num = rfm_data_filtrado[['total_actualizado', 'Frecuencia', 'Recencia']] 
  

#Pasamos todos los valores a decimal
from decimal import Decimal
for col in rfm_data.select_dtypes(include=['object']).columns :
    rfm_data[col] = rfm_data[col].apply(lambda x: float(x) if isinstance(x, Decimal)else x)


  
# Escalador 
scaler = StandardScaler() 
rfm_data_num_scl = scaler.fit_transform(rfm_data_num) 

  
# Convertir a DataFrame escalado 
rfm_data_num_scl = pd.DataFrame(rfm_data_num_scl, columns=['total_actualizado', 'Frecuencia', 'Recencia']) 

  

# Clusterización K-Means con K=3 
kmeans_3 = KMeans(n_clusters=3, max_iter=50, random_state=16) 
clusters = kmeans_3.fit_predict(rfm_data_num_scl) 

  

# Asignar etiquetas del cluster al dataframe original 
rfm_data = rfm_data_filtrado
rfm_data["rfm"] = clusters 

stats = rfm_data.groupby("rfm").agg({
    "Recencia":["min", "max", "mean"],
    "Frecuencia": ["min", "max", "mean"],
    "total_actualizado":["min", "max", "mean"]
}).reset_index()

stats.columns = ["rfm",
                 "Recencia Min", "Recencia Max", "Recencia Media",
                 "Frecuencia Min", "Frecuencia Max", "Frecuencia Media",
                 "Monetario Min","Monetario Max","Monetario Media"]
print ("\nEstadisticas por cluster:")
print (stats)


carpeta = "C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-11"
output_file = f"{carpeta}/Estadisticas_cluster.txt"

with open(output_file, "w") as file:
    file.write("Estadisticas por cluster\n")
    file.write("=" * 30 + "\n\n")

    for index, row in stats.iterrows():
        file.write(f"rfm {row['rfm']}\n")
        file.write(f"Recencia: Min= {row['Recencia Min']}, Max={row['Recencia Max']}, Media= {row['Recencia Media']:.2f}\n")
        file.write(f"Frecuencia: Min= {row['Frecuencia Min']}, Max={row['Frecuencia Max']}, Media= {row['Frecuencia Media']:.2f}\n")
        file.write(f"Monetario: Min= {row['Monetario Min']}, Max={row['Monetario Max']}, Media= {row['Monetario Media']:.2f}\n")
        file.write("-" * 30 + "\n\n")

print(f"Estadisticas guardadas en '{output_file}")
# Guardar resultado en un CSV 
#rfm_data.to_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-09/resultados_cluster_rfm_mensual_09_24.csv', index=False) 

#rfm_data['rfm'] = rfm_data['rfm'].replace({2: 'en peligro', 1:'perdido' , 0: 'fidelizado'}) 

# Graficar los resultados (gráficos de violines) 
plt.figure(figsize=(10, 6)) 
sns.violinplot(x='rfm', y='total_actualizado', data=rfm_data, hue='rfm', palette= ['#FF200E','#3346FF','#01D40B']) 
plt.title('Gráfico de violín: Valor monetario por Cluster (RFM)') 
plt.show() 

  
plt.figure(figsize=(10, 6)) 
sns.violinplot(x='rfm', y='Frecuencia', data=rfm_data, hue='rfm', palette= ['#FF200E','#3346FF','#01D40B']) 
plt.title('Gráfico de violín: Frecuencia por Cluster (RFM)') 
plt.show() 

  
plt.figure(figsize=(10, 6)) 
sns.violinplot(x='rfm', y='Recencia', data=rfm_data, hue='rfm', palette= ['#FF200E','#3346FF','#01D40B']) 
plt.title('Gráfico de violín: Recencia por Cluster (RFM)') 
plt.show() 


#Seleccionar las columnas numericas del dataframe


# Agrupar los datos por la columna 'rfm' y calcular la media para cada grupo 

media_por_cluster = rfm_data_num.groupby(rfm_data['rfm']).mean() 
#group_describe = rfm_data.groupby(rfm_data['rfm']).describe()

# Mostrar las medias de cada cluster 
print(media_por_cluster) 

# Agrupar por la columna 'rfm' 
grouped = rfm_data.groupby('rfm') 

# Iterar sobre cada grupo y mostrar el describe() por separado 

for rfm_value, group in grouped: 
    print(f"Cluster RFM: {rfm_value}") 
    print(group.describe()) 
    print("\n" + "="*40 + "\n") 

# Guardar el describe de cada cluster en un archivo txt separado 

for rfm_value, group in grouped: 
    file_name = f"C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-11/describe_cluster_{rfm_value}.txt" 

    # Convertir el describe() a una cadena de texto 
    describe_str = group.describe().to_string() 

    # Escribir la cadena en un archivo txt 
    with open(file_name, 'w') as file: 
        file.write(f"Cluster RFM: {rfm_value}\n") 
        file.write(describe_str) 
        file.write("\n" + "="*40 + "\n") 

    print(f"Describe de cluster RFM {rfm_value} guardado en {file_name}") 
#print(group_describe.T) 

#----------------------------------------------------------------------------------------------------------------------------------------

#Cargar los datos 

#rfm_data = pd.read_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-09/resultados_cluster_rfm_mensual_09_24.csv')  
#data =pd.read_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-09/data_filtrada_hasta_mes_ant_09_24.csv', index_col= 0)
#clientes_features_relevantes = pd.read_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-09/clientes_features_mensual_09_24.csv')
  

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
segmentacion_clientes['fecha_segmentacion'] = '2024-11-01' 
 
print('Segm cliente',segmentacion_clientes.head(5))

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


print(segmentacion_clientes.columns)

# Renombro  
#ultima_fecha_por_cliente.rename(columns={'fecha':'ultima_compra'}, inplace=True)  

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

segmentacion_clientes.rename(columns={'fecha':'ultima_compra'}, inplace=True) 

segmentacion_clientes_0 = segmentacion_clientes.copy()
  
# Importo segmentación vieja  
segmentacio_vieja = pd.read_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-10/segmentacion_18_meses_hasta_OCT_2024.csv')  
segmentacion_clientes = pd.concat([segmentacio_vieja, segmentacion_clientes_0], ignore_index=True)  

# Cambio estrella por fidelizados  
segmentacion_clientes.replace({'estrella':'fidelizado'}, inplace=True)  

print (f'rename a ultima_compra', segmentacion_clientes.columns)  

segmentacion_clientes['ultima_compra'] = pd.to_datetime(segmentacion_clientes['ultima_compra'], errors= 'coerce')
segmentacion_clientes['fecha_segmentacion'] = pd.to_datetime(segmentacion_clientes['fecha_segmentacion'], errors='coerce')

print(f'Segmentacion RFM: {segmentacion_clientes.rfm.unique()}')
#creo los dataset para calcular los gap de compras (se podria hacer en un unico dataset pero el PBI en la VM, por alguna razon, no actualiza las columnas del dataset "Output")
#df con los clientes en peligro
segmentacion_clientes_enpeligro = segmentacion_clientes[segmentacion_clientes['rfm'] == 'en peligro']
segmentacion_clientes_enpeligro = segmentacion_clientes_enpeligro.sort_values(by= ['mail', 'ultima_compra'])
print(f'EnPeligro RFM: {segmentacion_clientes_enpeligro.rfm.unique()}')
#df con los clientes en peligro
segmentacion_clientes_fidelizado = segmentacion_clientes[segmentacion_clientes['rfm'] == 'fidelizado']
segmentacion_clientes_fidelizado = segmentacion_clientes_fidelizado.sort_values(by= ['mail', 'ultima_compra'])
print(f'FIdelizado RFM: {segmentacion_clientes_fidelizado.rfm.unique()}')
#df con los clientes en peligro
segmentacion_clientes_perdido = segmentacion_clientes[segmentacion_clientes['rfm']== 'perdido']
segmentacion_clientes_perdido = segmentacion_clientes_perdido.sort_values(by= ['mail', 'ultima_compra'])
print(f'Perdido RFM: {segmentacion_clientes_perdido.rfm.unique()}')
#creo una columna con la fecha de la compra previa 
#segmentacion_clientes_perdidos['ultima_compra_previa'] = segmentacion_clientes_perdidos.groupby('mail')['ultima_compra'].shift(1)



#calculo el tiempo entre compras(gap) en dias en los tres df nuevos 
segmentacion_clientes_enpeligro['gap_entre_compras'] = segmentacion_clientes_enpeligro.groupby('mail')['ultima_compra'].diff().dt.days

segmentacion_clientes_fidelizado['gap_entre_compras'] = segmentacion_clientes_fidelizado.groupby('mail')['ultima_compra'].diff().dt.days

segmentacion_clientes_perdido['gap_entre_compras'] = segmentacion_clientes_perdido.groupby('mail')['ultima_compra'].diff().dt.days

#definimos los segmentos en funcion de los dias de compra 
#Gap Corto (0-30 días)
#Gap Medio (31-90 días)
#Gap Largo (91-180 días)
#Gap Muy Largo (>180 días)

def clasificar_gap(gap):
    if pd.isna(gap):
        return 'Primera / Única Compra'
    elif gap <= 30:
        return 'Gap Corto'
    elif gap <= 90:
        return 'Gap Medio'
    elif gap <= 183:
        return 'Gap Largo'
    else:
        return 'Gap Muy Largo'  

#Aplico la funcion de clasificacion a cada df
segmentacion_clientes_enpeligro['segmento_gap'] = segmentacion_clientes_enpeligro['gap_entre_compras'].apply(clasificar_gap)

segmentacion_clientes_fidelizado['segmento_gap'] = segmentacion_clientes_fidelizado['gap_entre_compras'].apply(clasificar_gap)

segmentacion_clientes_perdido['segmento_gap'] = segmentacion_clientes_perdido['gap_entre_compras'].apply(clasificar_gap)

#resumen estadistico de los gaps
resumen_gap_enpeligro = segmentacion_clientes_enpeligro['gap_entre_compras'].describe()
print('Resumen estadistico de los gaps "en peligro" (en dias): ')
print(resumen_gap_enpeligro)

resumen_gap_fidelizado = segmentacion_clientes_fidelizado['gap_entre_compras'].describe()
print('Resumen estadistico de los gaps "fidelizado" (en dias): ')
print(resumen_gap_fidelizado)

resumen_gap_perdido = segmentacion_clientes_perdido['gap_entre_compras'].describe()
print('Resumen estadistico de los gaps "perdido" (en dias): ')
print(resumen_gap_perdido)

#Agrupo para contar la cantidad de clientes por segmento 
segmentos_gap_enpeligro = segmentacion_clientes_enpeligro.groupby('segmento_gap').agg({'mail' : 'nunique'}).reset_index()
segmentos_gap_enpeligro.rename(columns={'mail':'cantidad_de_clientes'}, inplace=True)
print(segmentos_gap_enpeligro)

segmentos_gap_fidelizado = segmentacion_clientes_fidelizado.groupby('segmento_gap').agg({'mail' : 'nunique'}).reset_index()
segmentos_gap_fidelizado.rename(columns={'mail':'cantidad_de_clientes'}, inplace=True)
print(segmentos_gap_fidelizado)

segmentos_gap_perdido = segmentacion_clientes_perdido.groupby('segmento_gap').agg({'mail' : 'nunique'}).reset_index()
segmentos_gap_perdido.rename(columns={'mail':'cantidad_de_clientes'}, inplace=True)
print(segmentos_gap_perdido)
#--------------------------------------------------------------------

# Guardo CSV  
segmentacion_clientes.to_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-11/segmentacion_18_meses_hasta_NOV_2024.csv', index=False)  
segmentacion_clientes.to_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-11/output.csv', index=False)  
segmentacion_clientes_enpeligro.to_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-11/output_enpeligro.csv', index=False) 
segmentacion_clientes_fidelizado.to_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-11/output_fidelizado.csv', index=False)  
segmentacion_clientes_perdido.to_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-11/output_perdido.csv', index=False)  


  
# Guardar el DataFrame actualizado de rfm_data en un nuevo CSV
rfm_data.to_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-11/resultados_cluster_rfm_etiquetado_mensual_11_24.csv', index=False) 

  

# Mostrar las primeras filas de segmentacion_clientes 

print(segmentacion_clientes.head())  # Muestra las primeras filas del DataFram

print(f'Fechas de segmentacion: {segmentacion_clientes.fecha_segmentacion.unique()}')
print ('Se ha creado el CSV "resultados_cluster_rfm_etiquetado_mensual" y "segmantacion_clientes_mensual"')
print (' El proceso se ha terminado con exito ')