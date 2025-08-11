import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score, confusion_matrix
import seaborn as sns
import matplotlib.pyplot as plt

# Cargar el dataset
df = pd.read_csv('simulated_saas_churn.csv')  # Reemplaza con tu archivo generado

# Vista previa de los datos
print(df.head())

# Análisis de datos
print("\nResumen estadístico:")
print(df.describe())

# Análisis de variables categóricas

# Convertir fechas a tipo datetime
df['subscriptionStartDate'] = pd.to_datetime(df['subscriptionStartDate'])
df['lastActiveDate'] = pd.to_datetime(df['lastActiveDate'])

# Crear antigüedad de suscripción en días
df['subscriptionDays'] = (pd.to_datetime('2024-01-01') - df['subscriptionStartDate']).dt.days

# Crear días desde la última actividad
df['daysSinceLastActive'] = (pd.to_datetime('2024-01-01') - df['lastActiveDate']).dt.days


df.drop(columns=['clientId', 'subscriptionStartDate', 'lastActiveDate'], inplace=True)

print(df.isnull().sum())

# Separar variables
X = df.drop(columns=['isChurn'])  # Variables independientes
y = df['isChurn']                # Variable objetivo

# Dividir en entrenamiento y prueba (80%-20%)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Entrenar modelo Random Forest
model = RandomForestClassifier(random_state=42, n_estimators=100)
model.fit(X_train, y_train)

# Realizar predicciones
y_pred = model.predict(X_test)
y_pred_proba = model.predict_proba(X_test)[:, 1]

print(classification_report(y_test, y_pred))

auc_score = roc_auc_score(y_test, y_pred_proba)
print(f"AUC-ROC Score: {auc_score}")


cm = confusion_matrix(y_test, y_pred)
sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', xticklabels=['No Churn', 'Churn'], yticklabels=['No Churn', 'Churn'])
plt.title('Confusion Matrix')
plt.ylabel('Actual')
plt.xlabel('Predicted')
plt.show()


# Predicción para nuevos datos
new_data = pd.DataFrame({
    'contractValue': [12000],
    'ticketsRaised': [3],
    'avgResponseTime': [15.0],
    'featuresUsed (%)': [85],
    'emailsOpened (%)': [75],
    'npsScore': [8],
    'subscriptionDays': [1095],
    'daysSinceLastActive': [30]
})

# Predecir probabilidad de churn
predicted_proba = model.predict_proba(new_data)[:, 1]
print(f"Probability of churn: {predicted_proba[0]:.2f}")



# 8. Resultados Esperados
# 
#     Métricas del modelo: Precisión, recall, F1-score y AUC-ROC adecuadas para el problema.
# 
#     Importancia de variables:
#         Días desde la última actividad (daysSinceLastActive).
#         Valor del contrato (contractValue).
#         Porcentaje de características usadas (featuresUsed (%)).
# 
#     Acción basada en predicciones: Los clientes con alta probabilidad de churn pueden ser objetivo de campañas de retención.
# 
# Siguientes Pasos
# 
#     Optimización del Modelo:
#         Ajustar hiperparámetros con GridSearchCV o RandomizedSearchCV.
#         Probar otros modelos como XGBoost o LightGBM.
# 
#     Automatización:
#         Crear pipelines para integrar el modelo con sistemas internos usando Flask o FastAPI.
# 
#     Monitoreo:
#         Implementar monitoreo continuo para validar el rendimiento del modelo en producción.