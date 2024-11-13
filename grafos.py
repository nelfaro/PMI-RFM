import pandas as pd
from pyvis.network import Network
# Carga del dataset
df = pd.read_csv(r"C:/Users/Nelson/Documents/AnalyticsTown/PMI/PMI-RFM/PMI-RFM/segmentacion_18_meses_hasta_SEP_2024.csv")

# Ordenar por cliente y fecha de compra
df = df.sort_values(by=["mail", "fecha_segmentacion"])

# Crear una columna para el estado anterior de cada cliente
df["rfm_anterior"] = df.groupby("mail")["rfm"].shift(1)

# Filtrar registros donde hay cambio de estado
df_cambio = df.dropna(subset=["rfm_anterior"])  # Elimina filas sin estado anterior

# Contar las transiciones entre estados
transiciones = df_cambio.groupby(["rfm_anterior", "rfm"]).size().reset_index(name="cantidad")

# Crear la red en Pyvis
net = Network(height="600px", width="100%", directed=True)

# Definir los colores y tamaños de los nodos
colores = {
    "Nuevos": "white",
    "En Peligro": "red",
    "Fidelizados": "green",
    "Perdidos": "blue"
}
tamaños = {
    "Nuevos": 20,
    "En Peligro": 50,
    "Fidelizados": 40,
    "Perdidos": 30
}

'''# Crear nodos para cada estado único
for estado in df["rfm"].unique():
    cantidad = df[df["rfm"] == estado].shape[0]
    net.add_node(estado, label=f"{estado} - {cantidad}", color=colores.get(estado), size=tamaños.get(estado))'''

# Contar la cantidad de clientes en cada estado actual
estado_counts = df['rfm'].value_counts()

# Agregar nodos para cada estado con el tamaño en función de la cantidad de clientes
for estado, count in estado_counts.items():
    net.add_node(estado, label=f"{estado} - {estado_counts}", size=count * 10, color={
        'Fidelizado': 'green',
        'En Peligro': 'orange',
        'Perdido': 'blue',
        'Nuevos': 'grey'
    }.get(estado))  # Colores personalizados para cada estado

# Agregar aristas basadas en las transiciones
for _, row in transiciones.iterrows():
    origen = row['rfm']
    destino = row['rfm_anterior']
    cantidad = row['cantidad']
    net.add_edge(origen, destino, value =cantidad, title=f"{cantidad} clientes cambiaron de {origen} a {destino}")
    #net.add_edge(row["rfm_anterior"], row["rfm"], value=row["cantidad"], title=str(row["cantidad"]))

# Guardar y mostrar el grafo
net.show("grafico_transiciones_clientes.html")