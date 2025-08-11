import pytest
import pandas as pd
from read_data import AnalizadorDatos

@pytest.fixture
def dataframes():
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

    return df1, df2

def test_realizar_left_join(dataframes):
    df1, df2 = dataframes
    df_resultado = AnalizadorDatos.realizar_left_join(df1, df2)
    expected_data = {
        'id': [1, 2, 3],
        'nombre': ['Alice', 'Bob', 'Charlie'],
        'edad': [25, 30, None]
    }
    expected_df = pd.DataFrame(expected_data)
    pd.testing.assert_frame_equal(df_resultado, expected_df)

def test_calcular_estadisticas_edad(dataframes):
    df1, df2 = dataframes
    df_resultado = AnalizadorDatos.realizar_left_join(df1, df2)
    max_edad, promedio_edad, min_edad = AnalizadorDatos.calcular_estadisticas_edad(df_resultado)
    assert max_edad == 30
    assert promedio_edad == 27.5  # (25 + 30) / 2
    assert min_edad == 25 