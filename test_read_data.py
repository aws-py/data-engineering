import unittest
import pandas as pd
from read_data import AnalizadorDatos

class TestAnalizadorDatos(unittest.TestCase):

    def setUp(self):
        # Crear DataFrame 1
        data1 = {
            'id': [1, 2, 3],
            'nombre': ['Alice', 'Bob', 'Charlie']
        }
        self.df1 = pd.DataFrame(data1)

        # Crear DataFrame 2
        data2 = {
            'id': [1, 2, 4],
            'edad': [25, 30, 22]
        }
        self.df2 = pd.DataFrame(data2)

    def test_realizar_left_join(self):
        df_resultado = AnalizadorDatos.realizar_left_join(self.df1, self.df2)
        expected_data = {
            'id': [1, 2, 3],
            'nombre': ['Alice', 'Bob', 'Charlie'],
            'edad': [25, 30, None]
        }
        expected_df = pd.DataFrame(expected_data)
        pd.testing.assert_frame_equal(df_resultado, expected_df)

    def test_calcular_estadisticas_edad(self):
        df_resultado = AnalizadorDatos.realizar_left_join(self.df1, self.df2)
        max_edad, promedio_edad, min_edad = AnalizadorDatos.calcular_estadisticas_edad(df_resultado)
        self.assertEqual(max_edad, 30)
        self.assertEqual(promedio_edad, 27.5)  # (25 + 30) / 2
        self.assertEqual(min_edad, 25)

if __name__ == "__main__":
    unittest.main() 