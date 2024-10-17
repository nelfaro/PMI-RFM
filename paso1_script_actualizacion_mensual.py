# librerías 

#from sqlalchemy import create_engine, text 
#from sqlalchemy.exc import SQLAlchemyError 
import mysql.connector 
from mysql.connector import Error 
import datetime 
import pandas as pd 

  

# Obtener la fecha actual 

today = datetime.date.today() 

  

# Restar 30 días para obtener el mes pasado 

#last_month = today - datetime.timedelta(days=30) 

#anteultimo_mes = today - datetime.timedelta(days=60)

#anteultimo_mes_year = anteultimo_mes.strftime('%Y')
#anteultimo_mes_month = anteultimo_mes.strftime('%m')  

# Obtener el año y el mes del mes pasado en formato de cadena 

#last_month_year = last_month.strftime("%Y") 
#last_month_month = last_month.strftime("%m") 
#print(f'Mes:{last_month_month} / Año:{last_month_year}') 

  

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
                and MONTH(post_date) = 9
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
precios_mensual.to_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-09/precios_mensual_09_24.csv', index=False)
data_mensual.to_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-09/resultados_formateados_mensual_09_24.csv', index=False)  
print(f'Fecha inicial {data['fecha'].min()} / Fecha final {data['fecha'].max()}')
print("Datos guardados con exito con el nombre 'resultados_formateados_mensual.csv'") 
print("Datos guardados con exito con el nombre 'precios_mensual.csv'") 


#Impresiones
desc = data.describe()

ruta_arch = 'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-09/describe_df_formateado_09_24.txt'

with open(ruta_arch, 'w') as file:
     file.write(desc.to_string())

#print(data.describe())