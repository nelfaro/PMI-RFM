# Importación de librerías necesarias 

import pandas as pd 

import numpy as np 

import matplotlib.pyplot as plt 

import seaborn as sns 

from sklearn.preprocessing import StandardScaler 

from sklearn.cluster import KMeans 

  

# Cargar los datasets 

# precios = pd.read_csv(r'C:\\Users\\ccendago\\OneDrive - Philip Morris International\\CSV\\precios_mensual.csv')   

#data = pd.read_csv(r'C:\\Users\\ccendago\\OneDrive - Philip Morris International\\CSV\\resultados_formateados_mensual.csv') 

data = pd.read_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-09/data_filtrada_hasta_mes_ant_09_24.csv') 

# Convertir formatos de fecha 

data["fecha"] = pd.to_datetime(data["fecha"]) 

  

# Valor monetario (Monetary) 

rfm_m = data.groupby("mail")["total_actualizado"].sum().reset_index().round(2) 

  

# Frecuencia (Frequency) 

rfm_f = data.groupby("mail")["fecha"].count().reset_index() 

rfm_f = rfm_f.rename(columns={"fecha": "Frecuencia"}) 

  

# Recencia (Recency) 

max_date = max(data["fecha"]) 

data["Fecha_Dif"] = max_date - data["fecha"] 

  

# Computar última transacción 

rfm_p = data.groupby("mail")["Fecha_Dif"].min().reset_index() 

  

# Extraer la cantidad de días 

rfm_p["Fecha_Dif"] = rfm_p["Fecha_Dif"].dt.days 

rfm_p = rfm_p.rename(columns={"Fecha_Dif": "Recencia"}) 

  

# Juntar los 3: Monetario, Frecuencia y Recencia 

rfm_data = pd.merge(rfm_m, rfm_f, on="mail", how="inner") 

rfm_data = pd.merge(rfm_data, rfm_p, on="mail", how="inner") 

  

# Escalamiento de las variables numéricas 

rfm_data_num = rfm_data[['total_actualizado', 'Frecuencia', 'Recencia']] 

  

# Escalador 

scaler = StandardScaler() 

rfm_data_num_scl = scaler.fit_transform(rfm_data_num) 

  

# Convertir a DataFrame escalado 

rfm_data_num_scl = pd.DataFrame(rfm_data_num_scl, columns=['Total_actualizado', 'Frecuencia', 'Recencia']) 

  

# Clusterización K-Means con K=3 

kmeans_3 = KMeans(n_clusters=3, max_iter=50, random_state=16) 

clusters = kmeans_3.fit_predict(rfm_data_num_scl) 

  

# Asignar etiquetas del cluster al dataframe original 

rfm_data["rfm"] = clusters 

  

# Guardar resultado en un CSV 

rfm_data.to_csv(r'C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-09/resultados_cluster_rfm_mensual_09_24.csv', index=False) 

#rfm_data['rfm'] = rfm_data['rfm'].replace({0:'en peligro', 2: 'fidelizado', 1: 'perdido'}) 

# Graficar los resultados (gráficos de violines) 

plt.figure(figsize=(10, 6)) 

sns.violinplot(x='rfm', y='total_actualizado', data=rfm_data, hue='rfm', palette='Set1') 

plt.title('Gráfico de violín: Total Actualizado por Cluster (RFM)') 

plt.show() 

  

plt.figure(figsize=(10, 6)) 

sns.violinplot(x='rfm', y='Frecuencia', data=rfm_data, hue='rfm', palette='Set1') 

plt.title('Gráfico de violín: Frecuencia por Cluster (RFM)') 

plt.show() 

  

plt.figure(figsize=(10, 6)) 

sns.violinplot(x='rfm', y='Recencia', data=rfm_data, hue='rfm', palette='Set1') 

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

    file_name = f"C:/Users/ccendago/OneDrive - Philip Morris International/Sin Clientes Historicos copy/2024-09/describe_cluster_{rfm_value}.txt" 

    # Convertir el describe() a una cadena de texto 

    describe_str = group.describe().to_string() 

    # Escribir la cadena en un archivo txt 

    with open(file_name, 'w') as file: 

        file.write(f"Cluster RFM: {rfm_value}\n") 

        file.write(describe_str) 

        file.write("\n" + "="*40 + "\n") 

    print(f"Describe de cluster RFM {rfm_value} guardado en {file_name}") 
#print(group_describe.T) 
print('Proceso finalizado con éxito')