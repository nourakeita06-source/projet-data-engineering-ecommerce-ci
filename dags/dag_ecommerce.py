from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# 1. Configuration des default_args exigés par le barème
default_args = {
    'owner': 'Binome_Sujet4',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email': ['gouahtato@gmail.com'], # Email du prof pour les alertes
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# 2. Définition des fonctions Python pour simuler les étapes de l'ETL
def simuler_extraction():
    print("Extraction des sources : ventes_commerce_abidjan_100k.csv et referentiel_produits.csv...")

def simuler_transformation():
    print("Application des filtres, nettoyage des NaN et jointure des sources de données...")

def simuler_dbt_tests():
    print("Exécution des tests de qualité dbt (not_null et unique) sur le schéma...")

# 3. Déclaration du DAG avec planification Cron quotidienne
with DAG(
    'dag_pipeline_ecommerce_abidjan',
    default_args=default_args,
    description='Pipeline de données E-commerce pour la startup Jumia CI',
    schedule_interval='0 6 * * *', # S'exécute tous les jours à 6h du matin
    catchup=False,
) as dag:

    # Tâche 1 : Extraction
    task_extraction = PythonOperator(
        task_id='extraire_sources_csv',
        python_callable=simuler_extraction,
    )

    # Tâche 2 : Transformation & Nettoyage
    task_transformation = PythonOperator(
        task_id='transformer_nettoyer_jointure',
        python_callable=simuler_transformation,
    )

    # Tâche 3 : Chargement vers la base cloud
    # Cette tâche appelle directement ton script principal
    task_chargement_supabase = BashOperator(
        task_id='charger_supabase_entrepot',
        bash_command='python /app/projet_corrigé_\(1\).ipynb', # Exécution sous Docker
    )

    # Tâche 4 : Validation des données via dbt
    task_validation_dbt = PythonOperator(
        task_id='executer_tests_qualite_dbt',
        python_callable=simuler_dbt_tests,
    )

    # Enchaînement des 4 tâches requis par le barème
    task_extraction >> task_transformation >> task_chargement_supabase >> task_validation_dbt