import mysql.connector
from mysql.connector import Error
import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import os # Importar el módulo os para manejo de archivos y carpetas
import locale # Para nombres de meses en español si es necesario

# --- CONFIGURACIÓN INICIAL ---
# Establecer locale a español para nombres de meses (opcional, si se usa strftime con %b)
try:
    locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8') # O 'es_AR.UTF-8' o similar
except locale.Error:
    print("Locale español no encontrado, usando default.")
    pass # Continuar con el locale por defecto si falla

# Obtener la fecha actual para determinar el mes a procesar
today = datetime.date.today()

# El script se ejecuta el 1ro, procesamos el mes anterior.
# Si hoy es 1 de Diciembre 2024, procesamos Noviembre 2024.
# Calculamos la fecha del último día del mes anterior
process_date = today - datetime.timedelta(days=1)
process_year = process_date.year
process_month = process_date.month

# Calculamos la fecha del último día del mes *anterior* al procesado (para leer archivos)
# Si procesamos Noviembre 2024, necesitamos leer de Octubre 2024
previous_month_date = process_date.replace(day=1) - datetime.timedelta(days=1)
prev_month_year = previous_month_date.year
prev_month_month = previous_month_date.month

# Formatear cadenas de mes/año para nombres de carpetas y archivos
# Mes procesado (ej: 2024-11, NOV_2024)
process_month_str = f"{process_year}-{process_month:02d}"
process_month_abbr_year = process_date.strftime("%b_%Y").upper() # ej: NOV_2024

# Mes anterior (ej: 2024-10, OCT_2024)
prev_month_str = f"{prev_month_year}-{prev_month_month:02d}"
prev_month_abbr_year = previous_month_date.strftime("%b_%Y").upper() # ej: OCT_2024

print(f"--- Iniciando proceso para el mes: {process_month_str} ---")
print(f"Leyendo datos del mes anterior desde la carpeta: {prev_month_str}")

# Definir la ruta base (ajusta si es necesario)
base_path = r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy'

# Definir rutas de carpetas dinámicas
current_month_folder = os.path.join(base_path, process_month_str)
previous_month_folder = os.path.join(base_path, prev_month_str)

# Definir y crear carpeta estable para Power BI ---
powerbi_output_folder = os.path.join(base_path, 'PowerBI_Output') # Carpeta estable
os.makedirs(powerbi_output_folder, exist_ok=True)
print(f"Carpeta estable para Power BI: {powerbi_output_folder}")

# Crear la carpeta del mes actual si no existe
os.makedirs(current_month_folder, exist_ok=True)
print(f"Carpeta de resultados para este mes: {current_month_folder}")

# --- CONEXIÓN A BASE DE DATOS ---
try:
    cnx = mysql.connector.connect(user='dbreader', password='ZPO5ZMI8q9$6ncRv',
                                  host='ecomarprddb-20240412104121819200000001.cvpvuylh12yi.eu-west-1.rds.amazonaws.com',
                                  database='gharg-wp-prd',
                                  port=3306)
    cursor = cnx.cursor()
    cursor.execute('SELECT 1')
    result = cursor.fetchone()
    print('Conexión a BD exitosa!!!')

except Error as e:
    print(f'Error de conexión a BD: {e}')
    # Considerar salir del script si la conexión falla
    exit() # O raise SystemExit(f'Error de conexión a BD: {e}')

# --- EXTRACCIÓN DE DATOS ---

# Consulta todos los ids del *mes a procesar* (dinámico)
query_ventas = (f"""SELECT ID FROM pr_2_posts
                   WHERE post_type = 'shop_order' and post_status = 'wc-bill'
                   and YEAR(post_date) = {process_year}
                   and MONTH(post_date) = {process_month}
                   ORDER BY post_date DESC;""")

print(f"Ejecutando consulta para {process_year}-{process_month}...")
try:
    cursor.execute(query_ventas)
    post_id_ventas_hasta_mes_actual = [i[0] for i in cursor]
    print(f"Se encontraron {len(post_id_ventas_hasta_mes_actual)} ventas para el mes.")

    if not post_id_ventas_hasta_mes_actual:
        print("Advertencia: No se encontraron ventas para el mes procesado. Continuando con datos anteriores si existen.")
        # Podrías decidir terminar aquí si no hay datos nuevos
        # exit()

    ventas_hasta_mes_actual = pd.DataFrame()
    ventas_hasta_mes_actual['post_id'] = post_id_ventas_hasta_mes_actual
    ventas_hasta_mes_actual['post_id'] = ventas_hasta_mes_actual['post_id'].astype(int)

    # --- PROCESAMIENTO DE DATOS (usando 'ventas_hasta_mes_actual' si hay datos) ---

    if not ventas_hasta_mes_actual.empty:
        # --- BUSCAR MAIL ---
        def buscar_cuil(df, cursor):
            if df.empty:
                return pd.DataFrame(columns=['post_id', 'mail']) # Devuelve df vacío si no hay post_ids
            post_id_values = tuple(int(x) for x in df['post_id'].values)
            # Usar %s para placeholders y pasar los valores como tupla separada (más seguro)
            query = """SELECT post_id, meta_value FROM pr_2_postmeta
                       WHERE post_id IN %s AND meta_key = '_billing_email';"""
            cursor.execute(query, (post_id_values,)) # Pasar como tupla
            resultado = cursor.fetchall()
            cuil = pd.DataFrame(resultado, columns=['post_id', 'mail'])
            return df.merge(cuil, on='post_id', how='left') # Usar left merge y especificar 'on'

        ventas_hasta_mes_actual = buscar_cuil(ventas_hasta_mes_actual, cursor)
        print(f'Información ventas con mail:\n{ventas_hasta_mes_actual.info()}')

        # --- BUSCAR COMPROBANTES (BILLS) ---
        if not ventas_hasta_mes_actual.empty:
            order_id_values = tuple(int(x) for x in ventas_hasta_mes_actual['post_id'].values)
            query_bills = """SELECT order_id, id, type, creation_date
                             FROM pr_2_pmi_bills
                             WHERE order_id IN %s;"""
            cursor.execute(query_bills, (order_id_values,))
            resultado_bills = cursor.fetchall()
            bills = pd.DataFrame(resultado_bills, columns=['post_id', 'bill_id', 'type', 'fecha'])
            # Añadir el mail (asegurarse que post_id sea la clave correcta para merge)
            ventas_hasta_mes_actual = bills.merge(ventas_hasta_mes_actual.drop_duplicates(subset=['post_id']), on='post_id', how='left')
            print(f'Información ventas con bills:\n{ventas_hasta_mes_actual.info()}')
        else:
             print("No hay ventas con mail para buscar comprobantes.")
             ventas_hasta_mes_actual = pd.DataFrame(columns=['post_id', 'bill_id', 'type', 'fecha', 'mail']) # Crear df vacío

        # --- BUSCAR PRODUCTOS POR BILL ID ---
        if not ventas_hasta_mes_actual.empty and 'bill_id' in ventas_hasta_mes_actual.columns and not ventas_hasta_mes_actual['bill_id'].isnull().all():
            # Filtrar NAs antes de convertir a int y crear tupla
            bill_ids_validos = ventas_hasta_mes_actual['bill_id'].dropna().astype(int).unique()
            if len(bill_ids_validos) > 0:
                bill_id_values = tuple(bill_ids_validos)
                query_items = """SELECT bill_id, product_id, quantity
                                 FROM pr_2_pmi_bill_items
                                 WHERE bill_id IN %s;"""
                cursor.execute(query_items, (bill_id_values,))
                productos_vendidos_hasta_mes_actual = cursor.fetchall()
                productos_vendidos_hasta_mes_actual = pd.DataFrame(productos_vendidos_hasta_mes_actual, columns=['bill_id', 'product_id', 'quantity'])
                productos_vendidos_hasta_mes_actual['bill_id'] = productos_vendidos_hasta_mes_actual['bill_id'].astype(int)
                productos_vendidos_hasta_mes_actual['product_id'] = productos_vendidos_hasta_mes_actual['product_id'].astype(int)
                productos_vendidos_hasta_mes_actual['quantity'] = productos_vendidos_hasta_mes_actual['quantity'].astype(int)
                print(f'Información productos vendidos:\n{productos_vendidos_hasta_mes_actual.info()}')
            else:
                print("No hay bill_ids válidos para buscar items.")
                productos_vendidos_hasta_mes_actual = pd.DataFrame(columns=['bill_id', 'product_id', 'quantity']) # Crear df vacío
        else:
            print("No hay bills para buscar items.")
            productos_vendidos_hasta_mes_actual = pd.DataFrame(columns=['bill_id', 'product_id', 'quantity']) # Crear df vacío

        # --- BUSCAR DESCRIPCIÓN Y TIPO DE PRODUCTO ---
        if not productos_vendidos_hasta_mes_actual.empty:
            product_id_values_desc = tuple(int(x) for x in productos_vendidos_hasta_mes_actual['product_id'].unique()) # Usar unique

            # Descripción
            query_desc = """SELECT id, post_title, post_content
                            FROM pr_2_posts
                            WHERE id IN %s;"""
            cursor.execute(query_desc, (product_id_values_desc,))
            prod_descripcion = cursor.fetchall()
            prod_descripcion = pd.DataFrame(prod_descripcion, columns =['product_id', 'titulo', 'descripcion'])
            prod_descripcion['product_id'] = prod_descripcion['product_id'].astype(int) # Asegurar tipo int

            # Tipo Prd ID
            query_tipo = """SELECT post_id, meta_value
                            FROM pr_2_postmeta
                            WHERE post_id IN %s AND meta_key = 'tipo_prd_id';"""
            cursor.execute(query_tipo, (product_id_values_desc,))
            tipo_prd_id = cursor.fetchall()
            tipo_prd_id = pd.DataFrame(tipo_prd_id, columns =['product_id', 'tipo_prd_id'])
            tipo_prd_id['product_id'] = tipo_prd_id['product_id'].astype(int) # Asegurar tipo int
            # Convertir tipo_prd_id a numérico, manejar errores
            tipo_prd_id['tipo_prd_id'] = pd.to_numeric(tipo_prd_id['tipo_prd_id'], errors='coerce').fillna(0).astype(int)


            # Juntar descripción y tipo
            prod_descripcion = prod_descripcion.merge(tipo_prd_id, on='product_id', how='left')

            # Juntar con productos vendidos
            productos_vendidos_hasta_mes_actual = productos_vendidos_hasta_mes_actual.merge(prod_descripcion, on='product_id', how='left')
            print(f'Información productos vendidos (completo):\n{productos_vendidos_hasta_mes_actual.info()}')
        else:
             print("No hay productos vendidos para buscar descripción.")

        # --- COMBINAR DATOS Y FORMATEAR (DataFrame 'df' o 'data_mes_actual') ---
        if not productos_vendidos_hasta_mes_actual.empty and not ventas_hasta_mes_actual.empty:
             # Asegurarse que 'bill_id' exista y sea del tipo correcto antes de mergear
             if 'bill_id' in productos_vendidos_hasta_mes_actual.columns and 'bill_id' in ventas_hasta_mes_actual.columns:
                 productos_vendidos_hasta_mes_actual['bill_id'] = productos_vendidos_hasta_mes_actual['bill_id'].astype(int)
                 ventas_hasta_mes_actual['bill_id'] = pd.to_numeric(ventas_hasta_mes_actual['bill_id'], errors='coerce').fillna(-1).astype(int) # Manejar posibles NaN
                 
                 df_mes_actual = productos_vendidos_hasta_mes_actual.merge(ventas_hasta_mes_actual, on='bill_id', how='left')

                 # Convertir a tipos correctos, manejando errores
                 df_mes_actual['product_id'] = df_mes_actual['product_id'].astype(int)
                 df_mes_actual['quantity'] = df_mes_actual['quantity'].astype(int)
                 df_mes_actual['tipo_prd_id'] = pd.to_numeric(df_mes_actual['tipo_prd_id'], errors='coerce').fillna(0).astype(int) # Usar 0 o -1 para NaN
                 df_mes_actual['post_id'] = pd.to_numeric(df_mes_actual['post_id'], errors='coerce').fillna(-1).astype(int) # Usar -1 para NaN si 'post_id_y' no existe siempre
                 df_mes_actual['fecha'] = pd.to_datetime(df_mes_actual['fecha'], errors='coerce') # Convertir fecha
                 
                 print("Columnas después del merge inicial:", df_mes_actual.columns)
                 
                 # --- APLICAR LÓGICA DE NEGOCIO AL MES ACTUAL ---
                 # (Título descripción, Notas de crédito, Unidades, Presentación, Combos, Cartones, etc.)

                 data = df_mes_actual.copy() # Renombrar para mantener consistencia con el código original
                 data = data.dropna(subset=['fecha']) # Eliminar filas donde la fecha no se pudo convertir

                 # Resto del preprocesamiento (asegúrate que las columnas existan)
                 if 'titulo' in data.columns and 'descripcion' in data.columns:
                     data['titulo_descripcion'] = data['titulo'].fillna('') + ' ' + data['descripcion'].fillna('')

                 # Notas de crédito (requiere ordenar por fecha)
                 if 'post_id' in data.columns and 'titulo_descripcion' in data.columns and 'fecha' in data.columns and 'type' in data.columns and 'product_id' in data.columns:
                    data.sort_values(['post_id', 'titulo_descripcion', 'fecha'], inplace=True)
                    data.reset_index(drop=True, inplace=True)
                    for i in data.index:
                        # Verificar si el índice anterior existe
                        if i > 0 and data.loc[i, 'type'] == 'pmi-credit-note':
                           # Comparar con la fila anterior (i-1)
                           if (data.loc[i, 'product_id'] == data.loc[i-1, 'product_id']) and \
                              (data.loc[i, 'post_id'] == data.loc[i-1, 'post_id']) and \
                              (data.loc[i, 'fecha'] > data.loc[i-1, 'fecha']):
                                data.loc[i-1, 'quantity'] = data.loc[i-1, 'quantity'] - data.loc[i, 'quantity']
                    # Eliminar las nc y filas con cantidad 0
                    data = data[data.type != 'pmi-credit-note']
                    data = data[data.quantity > 0] # Corregido: != 0 a > 0 si no se venden cantidades negativas
                 else:
                     print("Advertencia: Faltan columnas para procesar notas de crédito.")


                 # Campo unidad
                 if 'titulo_descripcion' in data.columns and 'tipo_prd_id' in data.columns:
                     data["Unidad"] = np.nan # Inicializar
                     uni10_mask = data.titulo_descripcion.str.contains("10", na=False)
                     uni20_mask = data.titulo_descripcion.str.contains("20", na=False)
                     uni12_mask = data.titulo_descripcion.str.contains("12", na=False)
                     otros_mask = data.tipo_prd_id == 2
                     data.loc[uni10_mask, "Unidad"] = 10
                     data.loc[uni20_mask, "Unidad"] = 20
                     data.loc[uni12_mask, "Unidad"] = 12
                     data.loc[otros_mask, "Unidad"] = 1
                     data["Unidad"] = data["Unidad"].fillna(1) # Asignar 1 por defecto si no coincide
                 else:
                     print("Advertencia: Faltan columnas para calcular 'Unidad'.")

                 # Presentación
                 if 'titulo_descripcion' in data.columns and 'tipo_prd_id' in data.columns:
                     data["Presentacion"] = "sin datos" # Inicializar
                     Box_mask = data.titulo_descripcion.str.contains("Box|BOX|box", na=False, regex=True)
                     soft_mask = ~Box_mask # Negación de Box_mask
                     otros_mask_pres = data.tipo_prd_id == 2
                     data.loc[Box_mask, "Presentacion"] = "Box"
                     # Aplicar soft_mask *después* de Box y *antes* de Otros, asegurando que no sea tipo 2
                     data.loc[soft_mask & (data.tipo_prd_id != 2), "Presentacion"] = "Soft_pack"
                     data.loc[otros_mask_pres, "Presentacion"] = "Otros Productos"
                 else:
                     print("Advertencia: Faltan columnas para calcular 'Presentacion'.")

                 # Combos
                 if 'quantity' in data.columns:
                    cantidades_posibles = data['quantity'].unique()
                    cantidades_que_indican_combo = [i for i in cantidades_posibles if (i < 10 or i % 2 != 0) and i != 15 and i != 45 and i != 1] # Excluir 1 también
                    data['Combo'] = 0
                    data.loc[data['quantity'].isin(cantidades_que_indican_combo), 'Combo'] = 1
                    data['Combo'] = pd.to_numeric(data['Combo'])
                 else:
                     print("Advertencia: Falta columna 'quantity' para calcular 'Combo'.")


                 # Periodo y Cartones
                 if 'fecha' in data.columns and 'tipo_prd_id' in data.columns and 'titulo_descripcion' in data.columns and 'quantity' in data.columns:
                     data["Periodo"] = data["fecha"].dt.strftime('%Y-%m')
                     data["Carton_unidad"] = 10 # Default
                     # Corrección: Usar .loc para asignación segura
                     data.loc[data["tipo_prd_id"] == 2, "Carton_unidad"] = 1 # Otros productos unidad 1
                     # Usar str.contains de forma segura con na=False
                     parliament_mask = data["titulo_descripcion"].str.contains("Parliament Super Slims Box 20", na=False)
                     data.loc[parliament_mask, "Carton_unidad"] = 15
                     # Calcular cantidad de cartones, evitar división por cero
                     data["Carton_cantidad"] = np.where(data["Carton_unidad"] > 0, data["quantity"] / data["Carton_unidad"], 0)

                 else:
                     print("Advertencia: Faltan columnas para calcular 'Periodo' o 'Cartones'.")


                 # MARCA Y CATEGORÍA (Leyendo de la carpeta del mes *ANTERIOR* o una fija)
                 # Decidir si este archivo es mensual o estático.
                 # Opción 1: Leer del mes anterior (si se actualiza mensualmente)
                 catalogacion_path = os.path.join(previous_month_folder, 'Catalogación - Sheet1.csv')
                 # Opción 2: Leer de una ruta fija (si es estático)
                 # catalogacion_path = r'C:/ruta/fija/Catalogación - Sheet1.csv'
                 # Opción 3: Leer de la carpeta actual (si se copia/coloca ahí antes de ejecutar)
                 # catalogacion_path = os.path.join(current_month_folder, 'Catalogación - Sheet1.csv')

                 try:
                    # Usaremos la Opción 1 por defecto (leer del mes anterior)
                    print(f"Intentando leer Catalogación desde: {catalogacion_path}")
                    catalogacion = pd.read_csv(catalogacion_path)
                    # Quitar columna Unnamed si existe
                    if 'Unnamed: 0' in catalogacion.columns:
                        catalogacion = catalogacion.drop(columns=['Unnamed: 0'])
                    catalogacion.rename(columns={'PRODUCTO': 'titulo'}, inplace=True)

                    # Acomodar títulos y hacer merge
                    if 'titulo' in data.columns:
                         data['titulo'] = data['titulo'].str.replace('amp;', '', regex=False).str.replace('\xa0', ' ', regex=False)
                         data = data.merge(catalogacion[['titulo', 'MARCA', 'CATEGORÍA']], on='titulo', how='left') # Seleccionar solo columnas necesarias
                         data['MARCA'] = data['MARCA'].fillna('otros')
                         data['CATEGORÍA'] = data['CATEGORÍA'].fillna('otros') # Asumiendo que también quieres rellenar categoría
                    else:
                         print("Advertencia: Falta columna 'titulo' para merge con catalogación.")
                         data['MARCA'] = 'otros' # Crear columnas por defecto
                         data['CATEGORÍA'] = 'otros'


                 except FileNotFoundError:
                    print(f"Error: No se encontró el archivo de catalogación en {catalogacion_path}")
                    print("Continuando sin información de Marca/Categoría para el mes actual.")
                    data['MARCA'] = 'otros' # Crear columnas por defecto si falla la lectura
                    data['CATEGORÍA'] = 'otros'

                 # PRECIOS (Obtener precios actuales)
                 if 'product_id' in data.columns and not data['product_id'].isnull().all():
                    product_id_values_precios = tuple(int(x) for x in data['product_id'].unique()) # Usar unique
                    query_precios = """SELECT product_id, creation_date, neto
                                       FROM pr_2_pmi_prices_log
                                       WHERE product_id IN %s;"""
                    cursor.execute(query_precios, (product_id_values_precios,))
                    resultado_precios = cursor.fetchall()
                    precios_mensual = pd.DataFrame(resultado_precios, columns=['product_id', 'fecha_precio', 'precio'])
                    precios_mensual['fecha_precio'] = pd.to_datetime(precios_mensual['fecha_precio'])
                    precios_mensual['precio'] = pd.to_numeric(precios_mensual['precio'], errors='coerce')
                    precios_mensual.sort_values(['product_id', 'fecha_precio'], inplace=True)
                    precios_mensual = precios_mensual.drop_duplicates(subset=['product_id'], keep='last')
                    precios_mensual = precios_mensual.drop(columns=['fecha_precio'])
                    precios_mensual = precios_mensual.dropna(subset=['precio']) # Eliminar productos sin precio válido

                    # Merge precios con datos del mes
                    data_mensual = data.merge(precios_mensual, on='product_id', how='left')
                    # Calcular total actualizado, manejar precios NaN (quizás asignar 0 o precio promedio?)
                    data_mensual['precio'] = data_mensual['precio'].fillna(0) # Opcional: llenar NaN con 0
                    data_mensual['total_actualizado'] = data_mensual['precio'] * data_mensual['quantity']

                    # Guardar precios y datos formateados del mes actual (opcional pero útil para debug)
                    # precios_mensual.to_csv(os.path.join(current_month_folder, f'precios_mensual_{process_month_abbr_year}.csv'), index=False)
                    # data_mensual.to_csv(os.path.join(current_month_folder, f'resultados_formateados_mensual_{process_month_abbr_year}.csv'), index=False)
                    print(f"Fechas procesadas en el mes actual: {data_mensual['fecha'].min()} / {data_mensual['fecha'].max()}")

                 else:
                    print("Advertencia: No hay product_ids para buscar precios o falta la columna.")
                    data_mensual = data.copy() # Continuar con los datos que hay
                    data_mensual['precio'] = 0 # Añadir columnas faltantes
                    data_mensual['total_actualizado'] = 0

                 # Guardar describe de los datos formateados del mes
                 desc_path = os.path.join(current_month_folder, f'describe_df_formateado_{process_month_abbr_year}.txt')
                 try:
                    with open(desc_path, 'w') as file:
                         file.write(data_mensual.describe(include='all').to_string()) # Incluir todo tipo de columnas
                    print(f"Descripción de datos formateados guardada en: {desc_path}")
                 except Exception as e:
                    print(f"Error al guardar el archivo de descripción: {e}")

             else:
                print("Merge inicial no posible por falta de columna 'bill_id' en uno de los dataframes.")
                data_mensual = pd.DataFrame() # df vacío si no se pudo hacer merge
        else:
             print("No hay datos suficientes de productos o ventas para combinar.")
             data_mensual = pd.DataFrame() # df vacío si no hay datos
    else:
        print("No se procesaron datos nuevos del mes actual porque no se encontraron ventas.")
        data_mensual = pd.DataFrame() # df vacío si no hay datos nuevos

except Exception as e:
    print(f"Error durante la extracción o procesamiento inicial de datos: {e}")
    # Considerar si continuar o no
    data_mensual = pd.DataFrame() # Asegurarse que exista, aunque esté vacío

# --- COMBINAR CON DATOS HISTÓRICOS ---

# Ruta del archivo histórico del mes anterior
data_anterior_path = os.path.join(previous_month_folder, f"data_18_meses_hasta_{prev_month_abbr_year}.csv")
print(f"Intentando leer datos históricos desde: {data_anterior_path}")

try:
    # Leer datos históricos del mes anterior
    data_anterior = pd.read_csv(data_anterior_path) # Quitar index_col=0 si no hay índice guardado explícitamente
    print("Datos históricos del mes anterior cargados.")

    # --- Lógica de Actualización de Precios (si hay datos nuevos y viejos) ---
    if not data_mensual.empty and not data_anterior.empty and 'product_id' in data_mensual.columns and 'product_id' in data_anterior.columns:
        # Asegurarse que las columnas necesarias existen y tienen tipos correctos
        for col in ['product_id', 'quantity', 'total_actualizado']:
            if col in data_mensual.columns:
                data_mensual[col] = pd.to_numeric(data_mensual[col], errors='coerce')
            if col in data_anterior.columns:
                data_anterior[col] = pd.to_numeric(data_anterior[col], errors='coerce')
        
        # Rellenar NaNs antes de calcular
        data_mensual = data_mensual.fillna({'quantity': 0, 'total_actualizado': 0, 'precio': 0})
        data_anterior = data_anterior.fillna({'quantity': 0, 'total_actualizado': 0})

        # Usar precios_mensual si se calculó antes, sino df vacío
        if 'precios_mensual' not in locals():
            precios_mensual = pd.DataFrame(columns=['product_id', 'precio'])

        # Productos más vendidos del *nuevo* mes
        productos_mas_vendidos = data_mensual.groupby('product_id')['quantity'].sum().reset_index().sort_values('quantity', ascending=False).head(4)['product_id'].values

        # Precio de la canasta nueva (usando los precios del mes actual)
        canasta_nueva = precios_mensual[precios_mensual['product_id'].isin(productos_mas_vendidos)]['precio'].sum()

        # Precio viejo de esos productos (del data_anterior)
        productos_viejos = data_anterior[data_anterior['product_id'].isin(productos_mas_vendidos)].copy() # Usar .copy()
        # Calcular precio unitario viejo, evitar división por cero
        productos_viejos['precio_viejo'] = np.where(productos_viejos['quantity'] > 0,
                                                    productos_viejos['total_actualizado'] / productos_viejos['quantity'],
                                                    0)
        productos_viejos = productos_viejos[['product_id', 'precio_viejo']].drop_duplicates(subset=['product_id'])
        canasta_vieja = productos_viejos['precio_viejo'].sum()

        # Índice de actualización (manejar división por cero)
        if canasta_vieja > 0:
            indice_actualizacion = canasta_nueva / canasta_vieja
            print(f"Índice de actualización de precios calculado: {indice_actualizacion:.4f}")
        else:
            indice_actualizacion = 1.0 # No actualizar si no hay canasta vieja o su valor es 0
            print("Advertencia: No se pudo calcular índice de actualización (canasta vieja = 0). Usando índice = 1.0")

        # Actualizar precios en data_anterior
        # Calcular precio viejo unitario en data_anterior
        data_anterior['precio_viejo'] = np.where(data_anterior['quantity'] > 0,
                                               data_anterior['total_actualizado'] / data_anterior['quantity'],
                                               0)
        # Merge con precios nuevos
        data_anterior = data_anterior.merge(precios_mensual[['product_id', 'precio']], on='product_id', how='left')

        # Actualizar precios NaN usando el índice
        # Usar np.isnan para verificar NaN en la columna 'precio' (que viene del merge)
        mask_nan_precio = np.isnan(data_anterior['precio'])
        data_anterior.loc[mask_nan_precio, 'precio'] = data_anterior.loc[mask_nan_precio, 'precio_viejo'] * indice_actualizacion

        # Recalcular total_actualizado en data_anterior con los precios (nuevos o actualizados)
        data_anterior['total_actualizado'] = data_anterior['precio'].fillna(0) * data_anterior['quantity'].fillna(0)
        data_anterior = data_anterior.drop(columns=['precio_viejo']) # Eliminar columna auxiliar

    elif data_mensual.empty:
        print("No hay datos nuevos del mes actual para actualizar precios.")
    else: # data_anterior está vacío o falta product_id
         print("No hay datos históricos válidos para actualizar precios.")


    # Combinar datos antiguos (actualizados) con los nuevos del mes
    # Asegurarse que data_mensual y data_anterior tengan las mismas columnas relevantes antes de concatenar
    cols_anterior = set(data_anterior.columns)
    cols_actual = set(data_mensual.columns)
    common_cols = list(cols_anterior.intersection(cols_actual))
    
    # Si faltan columnas en uno, añadirlas con NaN o valor por defecto si es posible
    for col in cols_anterior - cols_actual:
        data_mensual[col] = np.nan # O un valor por defecto apropiado
    for col in cols_actual - cols_anterior:
        data_anterior[col] = np.nan # O un valor por defecto apropiado

    # Reordenar columnas para que coincidan exactamente antes de concat
    data_mensual = data_mensual[data_anterior.columns] # Ordena columnas de data_mensual igual que data_anterior

    data_hasta_mes_actual_combinado = pd.concat([data_anterior, data_mensual], ignore_index=True)
    print(f"Datos combinados. Total de registros: {len(data_hasta_mes_actual_combinado)}")


except FileNotFoundError:
    print(f"Error: No se encontró el archivo histórico en {data_anterior_path}")
    if not data_mensual.empty:
        print("Continuando solo con los datos del mes actual.")
        data_hasta_mes_actual_combinado = data_mensual.copy()
        # Si es el primer mes, el archivo 'data_18_meses_...' no existirá, así que esto es esperado.
    else:
        print("Error: No hay datos históricos ni datos del mes actual. Terminando.")
        exit() # Salir si no hay nada que procesar
except Exception as e:
    print(f"Error al leer o procesar datos históricos: {e}")
    # Decidir si continuar solo con datos nuevos o terminar
    if not data_mensual.empty:
        print("Continuando solo con los datos del mes actual debido a error con históricos.")
        data_hasta_mes_actual_combinado = data_mensual.copy()
    else:
        print("Error: No hay datos históricos ni datos del mes actual. Terminando.")
        exit()

# Guardar el CSV acumulado actualizado al mes actual
output_acumulado_path = os.path.join(current_month_folder, f"data_18_meses_hasta_{process_month_abbr_year}.csv")
try:
    data_hasta_mes_actual_combinado.to_csv(output_acumulado_path, index=False)
    print(f"Datos acumulados guardados en: {output_acumulado_path}")
except Exception as e:
    print(f"Error al guardar los datos acumulados: {e}")


# --- FILTRAR CLIENTES DEL ÚLTIMO AÑO ---
data_filtrada = data_hasta_mes_actual_combinado.copy()
if not data_filtrada.empty and 'fecha' in data_filtrada.columns and 'mail' in data_filtrada.columns:
    data_filtrada["fecha"] = pd.to_datetime(data_filtrada["fecha"], errors='coerce')
    data_filtrada = data_filtrada.dropna(subset=['fecha', 'mail']) # Quitar filas sin fecha o mail

    # Calcular última compra
    ultima_compra_por_cliente = data_filtrada.loc[data_filtrada.groupby("mail")["fecha"].idxmax()]

    # Fecha límite (un año atrás desde la fecha de *procesamiento*)
    one_year_ago = process_date - pd.Timedelta(days=365)

    # Filtrar clientes activos en el último año
    clientes_activos = ultima_compra_por_cliente[ultima_compra_por_cliente['fecha'] >= one_year_ago]['mail'].unique()

    data_filtrada = data_filtrada[data_filtrada['mail'].isin(clientes_activos)]
    print(f"Filtrado por actividad último año. Clientes restantes: {data_filtrada['mail'].nunique()}")

    # Guardar el CSV final con los datos filtrados
    output_filtrado_path = os.path.join(current_month_folder, f"data_filtrada_hasta_mes_ant_{process_month_abbr_year}.csv")
    try:
        data_filtrada.to_csv(output_filtrado_path, index=False)
        print(f"Datos filtrados guardados en: {output_filtrado_path}")
    except Exception as e:
        print(f"Error al guardar datos filtrados: {e}")
else:
    print("No hay datos suficientes para filtrar por actividad.")
    data_filtrada = pd.DataFrame() # Asegurar que exista vacío

# --- ANÁLISIS RFM Y PATRONES DE CONSUMO (sobre data_filtrada) ---
if not data_filtrada.empty:
    data = data_filtrada.copy() # Usar data_filtrada como base para el resto del análisis
    # Convertir columnas necesarias a tipos correctos
    data['fecha'] = pd.to_datetime(data['fecha'], errors='coerce')
    data['total_actualizado'] = pd.to_numeric(data['total_actualizado'], errors='coerce')
    data['tipo_prd_id'] = pd.to_numeric(data['tipo_prd_id'], errors='coerce').fillna(0).astype(int)
    data['Combo'] = pd.to_numeric(data['Combo'], errors='coerce').fillna(0).astype(int)
    data = data.dropna(subset=['mail', 'fecha', 'total_actualizado']) # Quitar NaNs esenciales

    # --- PATRONES DE CONSUMO ---
    print("Calculando patrones de consumo...")
    clientes = data['mail'].unique().tolist()
    clientes_features = pd.DataFrame(clientes, columns=['mail'])

    # Lógica de patrones (similar a la original, pero usando 'data')
    df_otros_productos = data[data['tipo_prd_id'] == 2]
    df_cigarrillos = data[data['tipo_prd_id'] == 1]
    clientes_otros_prod = df_otros_productos['mail'].unique().tolist()
    clientes_features['otros_productos'] = clientes_features['mail'].isin(clientes_otros_prod).astype(int)

    clientes_multimarcas = []
    if 'MARCA' in df_cigarrillos.columns:
         marca_counts = df_cigarrillos.groupby('mail')['MARCA'].nunique()
         clientes_multimarcas = marca_counts[marca_counts > 1].index.tolist()
    clientes_features['Multimarca'] = clientes_features['mail'].isin(clientes_multimarcas).astype(int)

    clientes_combo = data[data['Combo'] == 1]['mail'].unique().tolist()
    clientes_features['consume_combo'] = clientes_features['mail'].isin(clientes_combo).astype(int)

    clientes_box = []
    clientes_soft_pack = []
    if 'Presentacion' in df_cigarrillos.columns:
        clientes_box = df_cigarrillos[df_cigarrillos['Presentacion'] == 'Box']['mail'].unique().tolist()
        clientes_soft_pack = df_cigarrillos[df_cigarrillos['Presentacion'] == 'Soft_pack']['mail'].unique().tolist()
    clientes_features['box'] = clientes_features['mail'].isin(clientes_box).astype(int)
    clientes_features['soft_pack'] = clientes_features['mail'].isin(clientes_soft_pack).astype(int)


    clientes_que_mezclan_en_misma_compra = []
    if 'bill_id' in df_cigarrillos.columns and 'MARCA' in df_cigarrillos.columns:
        marcas_por_factura = df_cigarrillos.groupby('bill_id')['MARCA'].nunique()
        facturas_donde_se_mezclan_marcas = marcas_por_factura[marcas_por_factura > 1].index.tolist()
        ventas_mezcla_marcas = df_cigarrillos[df_cigarrillos['bill_id'].isin(facturas_donde_se_mezclan_marcas)]
        clientes_que_mezclan_en_misma_compra = ventas_mezcla_marcas['mail'].unique().tolist()
    clientes_features['mezcla_en_misma_compra'] = clientes_features['mail'].isin(clientes_que_mezclan_en_misma_compra).astype(int)

    clientes_no_ff = []
    if 'CATEGORÍA' in data.columns:
        clientes_no_ff = data[data['CATEGORÍA'] != 'FF']['mail'].unique().tolist()
    clientes_features['FF'] = (~clientes_features['mail'].isin(clientes_no_ff)).astype(int) # Invertir la lógica

    # Clusters Patrones Consumo
    clientes_features_relevantes = clientes_features[['mail', 'Multimarca', 'mezcla_en_misma_compra', 'consume_combo', 'FF']].copy() # Usar .copy()
    conditions = [
        (clientes_features_relevantes['Multimarca'] == 0) & (clientes_features_relevantes['FF'] == 1),
        (clientes_features_relevantes['Multimarca'] == 1) & (clientes_features_relevantes['FF'] == 1),
        (clientes_features_relevantes['Multimarca'] == 0) & (clientes_features_relevantes['FF'] == 0),
        (clientes_features_relevantes['Multimarca'] == 1) & (clientes_features_relevantes['FF'] == 0)
    ]
    choices = ["FF - Monomarca", "FF - Multimarca", "No FF - Monomarca", "No FF - Multimarca"]
    clientes_features_relevantes['cluster_patrones_consumo'] = np.select(conditions, choices, default='Indefinido')

    # Guardar CSV Patrones (opcional)
    # output_patrones_path = os.path.join(current_month_folder, f'clientes_features_mensual_{process_month_abbr_year}.csv')
    # clientes_features_relevantes.to_csv(output_patrones_path, index=False)
    # print(f"Patrones de consumo guardados en: {output_patrones_path}")

    # --- ANÁLISIS RFM ---
    print("Calculando RFM...")

    # Calcular M, F, R
    rfm_m = data.groupby("mail")["total_actualizado"].sum().reset_index()
    rfm_f = data.groupby("mail")["fecha"].count().reset_index().rename(columns={"fecha": "Frecuencia"})
    max_date = data["fecha"].max() # Usar la fecha máxima de los datos filtrados
    data["Fecha_Dif"] = max_date - data["fecha"]
    rfm_p = data.groupby("mail")["Fecha_Dif"].min().reset_index()
    rfm_p["Recencia"] = rfm_p["Fecha_Dif"].dt.days
    rfm_p = rfm_p.drop(columns=["Fecha_Dif"])

    # Juntar RFM
    rfm_data = rfm_m.merge(rfm_f, on="mail", how="inner").merge(rfm_p, on="mail", how="inner")
    rfm_data = rfm_data.rename(columns={"total_actualizado": "ValorMonetario"}) # Renombrar para claridad

    # Filtrar outliers (opcional, pero mantenido de tu código)
    numeric_columns= ['ValorMonetario', 'Frecuencia', 'Recencia']
    Q1 = rfm_data[numeric_columns].quantile(0.25)
    Q3 = rfm_data[numeric_columns].quantile(0.75)
    IQR = Q3 - Q1 # Corregido: IRQ a IQR
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    rfm_data_filtrado = rfm_data[
           ~rfm_data[numeric_columns].apply(
               lambda row: any((row < lower_bound) | (row > upper_bound)), axis=1
           )
    ].copy() # Usar .copy()

    if not rfm_data_filtrado.empty:
        # Escalar datos
        rfm_data_num = rfm_data_filtrado[numeric_columns]
        scaler = StandardScaler()
        rfm_data_num_scl = scaler.fit_transform(rfm_data_num)
        rfm_data_num_scl = pd.DataFrame(rfm_data_num_scl, index=rfm_data_filtrado.index, columns=numeric_columns) # Mantener índice

        # K-Means
        kmeans_3 = KMeans(n_clusters=3, n_init=10, max_iter=300, random_state=42) # Usar n_init y random_state
        clusters = kmeans_3.fit_predict(rfm_data_num_scl)
        rfm_data_filtrado["rfm_cluster"] = clusters # Usar nombre diferente para el cluster numérico

        # Calcular estadísticas y guardar
        stats = rfm_data_filtrado.groupby("rfm_cluster").agg(
            {'Recencia': ["min", "max", "mean"],
             'Frecuencia': ["min", "max", "mean"],
             'ValorMonetario': ["min", "max", "mean"]}
             ).reset_index()
        stats.columns = ["rfm_cluster", "Recencia Min", "Recencia Max", "Recencia Media",
                         "Frecuencia Min", "Frecuencia Max", "Frecuencia Media",
                         "Monetario Min", "Monetario Max", "Monetario Media"]
        print("\nEstadísticas por cluster RFM:")
        print(stats)

        stats_output_file = os.path.join(current_month_folder, "Estadisticas_cluster_RFM.txt")
        try:
            with open(stats_output_file, "w") as file:
                 file.write(stats.to_string())
            print(f"Estadísticas RFM guardadas en: {stats_output_file}")
        except Exception as e:
            print(f"Error guardando estadísticas RFM: {e}")


        # Guardar describes por cluster
        grouped = rfm_data_filtrado.groupby('rfm_cluster')
        for rfm_value, group in grouped:
            file_name = os.path.join(current_month_folder, f"describe_cluster_rfm_{rfm_value}.txt")
            try:
                with open(file_name, 'w') as file:
                    file.write(f"Cluster RFM: {rfm_value}\n")
                    file.write(group[numeric_columns].describe().to_string())
                print(f"Describe del cluster RFM {rfm_value} guardado en {file_name}")
            except Exception as e:
                print(f"Error guardando describe cluster {rfm_value}: {e}")

        # Etiquetado semántico de clusters RFM
        cluster_means = rfm_data_filtrado.groupby('rfm_cluster')[numeric_columns].mean()
        # Ordenar clusters basado en Recencia (bajo es mejor), Frecuencia (alto es mejor), Monetario (alto es mejor)
        # Puntuación simple: -Recencia + Frecuencia + ValorMonetario (normalizados o rankeados)
        # O usar lógica original basada en promedios directos:
        fidelizado_cluster = cluster_means['ValorMonetario'].idxmax() # Mayor valor monetario
        perdido_cluster = cluster_means['Recencia'].idxmax() # Mayor recencia (peor)
        # El restante es 'en peligro'
        en_peligro_cluster = [c for c in cluster_means.index if c not in [fidelizado_cluster, perdido_cluster]][0]

        cluster_traducciones = {
            fidelizado_cluster: 'fidelizado',
            perdido_cluster: 'perdido',
            en_peligro_cluster: 'en peligro'
        }
        rfm_data_filtrado['rfm'] = rfm_data_filtrado['rfm_cluster'].map(cluster_traducciones)
        print("Clusters RFM etiquetados.")
        print(rfm_data_filtrado['rfm'].value_counts())

        # Guardar RFM etiquetado
        rfm_etiquetado_path = os.path.join(current_month_folder, f'resultados_cluster_rfm_etiquetado_{process_month_abbr_year}.csv')
        try:
             rfm_data_filtrado.to_csv(rfm_etiquetado_path, index=False)
             print(f"Resultados RFM etiquetados guardados en: {rfm_etiquetado_path}")
        except Exception as e:
            print(f"Error guardando RFM etiquetado: {e}")


        # --- SEGMENTACIÓN FINAL ---
        print("Generando segmentación final...")
        # Juntar patrones y RFM
        segmentacion_clientes = pd.merge(clientes_features_relevantes,
                                         rfm_data_filtrado[['mail', 'rfm', 'Recencia', 'Frecuencia', 'ValorMonetario']], # Seleccionar columnas de RFM
                                         on='mail', how='inner') # Inner join para mantener solo clientes con ambos análisis

        # Añadir fecha de segmentación (primer día del mes *siguiente* al procesado)
        segmentacion_clientes['fecha_segmentacion'] = today.strftime('%Y-%m-%d')


        # Información adicional (Marca monomarca, Última compra, Contenido)
        # Marca que consumen los monomarca
        clientes_monomarca = segmentacion_clientes[segmentacion_clientes['Multimarca'] == 0]['mail'].tolist()
        if clientes_monomarca and 'titulo' in data.columns:
            marca_mono_dict = data[(data['mail'].isin(clientes_monomarca)) & (data['tipo_prd_id'] == 1)]\
                              .groupby('mail')['titulo'].apply(lambda x: ', '.join(x.unique())).to_dict()
            segmentacion_clientes['producto_que_consume'] = segmentacion_clientes['mail'].map(marca_mono_dict)
        else:
            segmentacion_clientes['producto_que_consume'] = np.nan

        # Fecha última compra
        ultima_compra_dict = data.loc[data.groupby('mail')['fecha'].idxmax()][['mail', 'fecha']].set_index('mail')['fecha'].to_dict()
        segmentacion_clientes['ultima_compra'] = segmentacion_clientes['mail'].map(ultima_compra_dict)

        # Contenido última y anteúltima compra
        if 'Periodo' in data.columns and 'titulo' in data.columns:
             contenido_compras = data.groupby(['mail', 'Periodo'])['titulo'].apply(lambda x: ', '.join(x.unique())).reset_index()
             contenido_compras = contenido_compras.sort_values(by=['mail', 'Periodo'])

             contenido_ultima = contenido_compras.drop_duplicates(subset=['mail'], keep='last').set_index('mail')['titulo'].to_dict()
             segmentacion_clientes['Contenido Ultima Compra'] = segmentacion_clientes['mail'].map(contenido_ultima)

             # Anteúltima: quitar la última y tomar la nueva última
             indices_ultimas = contenido_compras.groupby('mail')['Periodo'].idxmax()
             contenido_sin_ultimas = contenido_compras.drop(indices_ultimas)
             contenido_anteultima = contenido_sin_ultimas.drop_duplicates(subset=['mail'], keep='last').set_index('mail')['titulo'].to_dict()
             segmentacion_clientes['Contenido Ante Ultima Compra'] = segmentacion_clientes['mail'].map(contenido_anteultima)
        else:
             segmentacion_clientes['Contenido Ultima Compra'] = np.nan
             segmentacion_clientes['Contenido Ante Ultima Compra'] = np.nan


        # --- COMBINAR CON SEGMENTACIÓN HISTÓRICA Y CÁLCULO DE GAP ---
        segmentacion_actual = segmentacion_clientes.copy()

        # Leer segmentación histórica del mes anterior
        segmentacion_vieja_path = os.path.join(previous_month_folder, f"segmentacion_18_meses_hasta_{prev_month_abbr_year}.csv")
        print(f"Intentando leer segmentación histórica desde: {segmentacion_vieja_path}")
        try:
            segmentacion_vieja = pd.read_csv(segmentacion_vieja_path)
            # Homogeneizar columnas y tipos antes de concatenar (importante!)
            segmentacion_vieja['fecha_segmentacion'] = pd.to_datetime(segmentacion_vieja['fecha_segmentacion'], errors='coerce')
            segmentacion_vieja['ultima_compra'] = pd.to_datetime(segmentacion_vieja['ultima_compra'], errors='coerce')
            # Asegurar que ambas tengan las mismas columnas en el mismo orden
            cols_v = set(segmentacion_vieja.columns)
            cols_a = set(segmentacion_actual.columns)
            for col in cols_v - cols_a: segmentacion_actual[col] = np.nan
            for col in cols_a - cols_v: segmentacion_vieja[col] = np.nan
            segmentacion_actual = segmentacion_actual[segmentacion_vieja.columns] # Ordenar

            segmentacion_combinada = pd.concat([segmentacion_vieja, segmentacion_actual], ignore_index=True)
            print("Segmentación histórica cargada y combinada.")

        except FileNotFoundError:
            print(f"Advertencia: No se encontró segmentación histórica en {segmentacion_vieja_path}. Se usarán solo los datos actuales.")
            segmentacion_combinada = segmentacion_actual.copy()
        except Exception as e:
             print(f"Error leyendo o combinando segmentación histórica: {e}. Usando solo datos actuales.")
             segmentacion_combinada = segmentacion_actual.copy()


        # Cálculo de Gap entre compras sobre la segmentación combinada
        if 'mail' in segmentacion_combinada.columns and 'ultima_compra' in segmentacion_combinada.columns:
            segmentacion_combinada['ultima_compra'] = pd.to_datetime(segmentacion_combinada['ultima_compra'], errors='coerce')
            segmentacion_combinada = segmentacion_combinada.sort_values(by=['mail', 'ultima_compra'])
            segmentacion_combinada['gap_entre_compras'] = segmentacion_combinada.groupby('mail')['ultima_compra'].diff().dt.days

            # Clasificación de Gap
            def clasificar_gap(gap):
                if pd.isna(gap): return 'Primera / Única Compra'
                elif gap <= 30: return 'Gap Corto'
                elif gap <= 90: return 'Gap Medio'
                elif gap <= 183: return 'Gap Largo'
                else: return 'Gap Muy Largo'
            segmentacion_combinada['segmento_gap'] = segmentacion_combinada['gap_entre_compras'].apply(clasificar_gap)
            print("Cálculo y clasificación de Gap completado.")

            # Guardar resúmenes de Gap por segmento RFM (del último mes)
            segmentacion_actual_con_gap = segmentacion_combinada[segmentacion_combinada['fecha_segmentacion'] == today.strftime('%Y-%m-%d')].copy()
            
            output_gap_summary_path = os.path.join(current_month_folder, f'resumen_gap_por_rfm_{process_month_abbr_year}.txt')
            try:
                 with open(output_gap_summary_path, 'w') as f:
                    f.write("Resumen Estadístico de Gap (días) por Segmento RFM (Mes Actual)\n")
                    f.write("=============================================================\n\n")
                    for rfm_segment in segmentacion_actual_con_gap['rfm'].unique():
                         if pd.notna(rfm_segment):
                             f.write(f"Segmento RFM: {rfm_segment}\n")
                             desc = segmentacion_actual_con_gap[segmentacion_actual_con_gap['rfm'] == rfm_segment]['gap_entre_compras'].describe()
                             f.write(desc.to_string())
                             f.write("\n\nDistribución por Clasificación de Gap:\n")
                             dist = segmentacion_actual_con_gap[segmentacion_actual_con_gap['rfm'] == rfm_segment]['segmento_gap'].value_counts()
                             f.write(dist.to_string())
                             f.write("\n---------------------------------------------\n\n")
                 print(f"Resumen de Gaps guardado en: {output_gap_summary_path}")
            except Exception as e:
                 print(f"Error al guardar resumen de gaps: {e}")
                 
        else:
             print("Advertencia: Faltan columnas 'mail' o 'ultima_compra' para calcular Gaps.")


        # --- GUARDAR SALIDAS FINALES ---
        # Segmentación acumulada
        output_segmentacion_acumulada_path = os.path.join(current_month_folder, f"segmentacion_18_meses_hasta_{process_month_abbr_year}.csv")
        try:
            segmentacion_combinada.to_csv(output_segmentacion_acumulada_path, index=False)
            print(f"Segmentación acumulada guardada en: {output_segmentacion_acumulada_path}")
        except Exception as e:
            print(f"Error al guardar segmentación acumulada: {e}")

        # Archivo 'output.csv' (generalmente la segmentación del último mes)
        output_final_path_monthly = os.path.join(current_month_folder, "output.csv")
        output_final_path_powerbi = os.path.join(powerbi_output_folder, "output.csv") # Path para Power BI

        try:
            # Guardar en carpeta mensual (archivo histórico del mes)
            segmentacion_actual_con_gap.to_csv(output_final_path_monthly, index=False)
            print(f"Archivo 'output.csv' (archivo mensual) guardado en: {output_final_path_monthly}")

            # Guardar el MISMO archivo en la carpeta estable de Power BI (sobrescribir)
            segmentacion_actual_con_gap.to_csv(output_final_path_powerbi, index=False)
            print(f"Archivo 'output.csv' (para Power BI) guardado/sobreescrito en: {output_final_path_powerbi}")

        except Exception as e:
            # Informar error si falla cualquiera de las escrituras
            print(f"Error al guardar archivos 'output.csv' (mensual o Power BI): {e}")

        # Archivos 'output_<rfm>.csv' (segmentación del último mes por cluster RFM)
        for rfm_segment in segmentacion_actual_con_gap['rfm'].unique():
             if pd.notna(rfm_segment):
                 df_segment = segmentacion_actual_con_gap[segmentacion_actual_con_gap['rfm'] == rfm_segment]
                 output_rfm_path = os.path.join(current_month_folder, f"output_{rfm_segment}.csv")
                 try:
                    df_segment.to_csv(output_rfm_path, index=False)
                    print(f"Archivo para RFM '{rfm_segment}' guardado en: {output_rfm_path}")
                 except Exception as e:
                    print(f"Error guardando archivo para RFM {rfm_segment}: {e}")

    else: # rfm_data_filtrado estaba vacío
        print("No se generó RFM (probablemente por falta de datos o todos outliers), no se puede continuar con segmentación.")

else: # data_filtrada estaba vacía
    print("No hay datos filtrados disponibles para realizar análisis RFM y Patrones.")


# --- GENERAR GRÁFICOS (si hay datos RFM) ---
if 'rfm_data_filtrado' in locals() and not rfm_data_filtrado.empty and 'rfm' in rfm_data_filtrado.columns:
    print("Generando gráficos de violín RFM...")
    try:
        # Paleta de colores consistente
        palette = {'perdido': '#FF200E', 'en peligro': '#3346FF', 'fidelizado': '#01D40B'}

        plt.figure(figsize=(10, 6))
        sns.violinplot(x='rfm', y='ValorMonetario', data=rfm_data_filtrado, palette=palette, order=['perdido', 'en peligro', 'fidelizado'])
        plt.title(f'Valor Monetario por Cluster RFM ({process_month_str})')
        plt.savefig(os.path.join(current_month_folder, f'violin_monetario_rfm_{process_month_abbr_year}.png'))
        plt.close() # Cerrar la figura para liberar memoria

        plt.figure(figsize=(10, 6))
        sns.violinplot(x='rfm', y='Frecuencia', data=rfm_data_filtrado, palette=palette, order=['perdido', 'en peligro', 'fidelizado'])
        plt.title(f'Frecuencia por Cluster RFM ({process_month_str})')
        plt.savefig(os.path.join(current_month_folder, f'violin_frecuencia_rfm_{process_month_abbr_year}.png'))
        plt.close()

        plt.figure(figsize=(10, 6))
        sns.violinplot(x='rfm', y='Recencia', data=rfm_data_filtrado, palette=palette, order=['perdido', 'en peligro', 'fidelizado'])
        plt.title(f'Recencia por Cluster RFM ({process_month_str})')
        plt.savefig(os.path.join(current_month_folder, f'violin_recencia_rfm_{process_month_abbr_year}.png'))
        plt.close()
        print("Gráficos RFM guardados.")
    except Exception as e:
        print(f"Error al generar gráficos RFM: {e}")
else:
    print("No se generaron gráficos RFM por falta de datos etiquetados.")


# --- CIERRE FINAL ---
# Cerrar conexión a BD
if 'cursor' in locals() and cursor:
    cursor.close()
if 'cnx' in locals() and cnx.is_connected():
    cnx.close()
    print('Conexión a BD cerrada.')

print(f"\n--- Proceso para el mes {process_month_str} finalizado con éxito. ---")
print(f"Resultados guardados en la carpeta: {current_month_folder}")

