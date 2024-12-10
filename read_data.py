import pandas as pd
from typing import Tuple

# Crear DataFrame 1
data1 = {
    'id': [1, 2, 3],
    'nombre': ['Alice', 'Bob', 'Charlie']
}
df1 = pd.DataFrame(data1)

# Crear DataFrame 2
data2 = {
    'id': [1, 2, 4],
    'edad': [25, 30, 22]
}
df2 = pd.DataFrame(data2)

class AnalizadorDatos:
    @staticmethod
    def realizar_left_join(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
        return pd.merge(df1, df2, on='id', how='left')

    @staticmethod
    def calcular_estadisticas_edad(df: pd.DataFrame) -> Tuple[float, float, float]:
        max_edad = df['edad'].max()
        min_edad = df['edad'].min()
        promedio_edad = df['edad'].mean()
        return max_edad, promedio_edad, min_edad

if __name__ == "__main__":
    analizador = AnalizadorDatos()
    df_resultado = analizador.realizar_left_join(df1, df2)
    print(df_resultado)
    
    max_edad, promedio_edad, min_edad = analizador.calcular_estadisticas_edad(df_resultado)
    print(f"Edad máxima: {max_edad}, Edad promedio: {promedio_edad}, Edad mínima: {min_edad}")
