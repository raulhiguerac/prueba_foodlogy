import requests
import pandas as pd
import json
import uuid
import psycopg2
import yagmail 

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from decouple import config
from email.mime.multipart import MIMEMultipart

def extract_data(url):
    try:
        response = requests.get(url)
        df = pd.DataFrame(json.loads(response.content))
        df.to_csv(r"D:\prueba_t\airflow\data\output.csv",index=False)
    except:
        return "Hubo un problema con la extraccion"
    
    return "Success"

def transform_data(src_file):

    try:

        df = pd.read_csv(src_file)

        df.rename(columns = {
            'd_a':'DIA',
            'campaero':'CAMPERO',
            'via_1':'VIA',
            'nombrecomuna':'NOMBRE_COMUNA',
            'propietario_de_veh_culo':'PROPIETARIO',
            'diurnio_nocturno':'HORARIO',
            'hora_restriccion_moto':'RESTRICCION_MOTO'

        }
        , inplace = True
        )

        df = df.drop(['orden','entidad','a_o','mes'], axis=1)

        df['ID'] = [uuid.uuid4() for _ in range(len(df.index))]
        df['ID_TIEMPO'] = [uuid.uuid4() for _ in range(len(df.index))]
        df['ID_LUGAR'] = [uuid.uuid4() for _ in range(len(df.index))]
        df['ID_VEHICULO'] = [uuid.uuid4() for _ in range(len(df.index))]

        df.columns = df.columns.str.upper()

        df.FECHA = pd.to_datetime(df.FECHA)
        df.DIA = df.DIA.astype(str).str.upper()
        df.GRAVEDAD = df.GRAVEDAD.astype(str).str.upper()
        df.PEATON = pd.to_numeric(df.PEATON, downcast="integer")
        df.AUTOMOVIL = pd.to_numeric(df.AUTOMOVIL, downcast="integer")
        df.CAMPERO = pd.to_numeric(df.CAMPERO, downcast="integer")
        df.CAMIONETA = pd.to_numeric(df.CAMIONETA, downcast="integer")
        df.MICRO = pd.to_numeric(df.MICRO, downcast="integer")
        df.BUSETA = pd.to_numeric(df.BUSETA, downcast="integer")
        df.BUS = pd.to_numeric(df.BUS, downcast="integer")
        df.CAMION = pd.to_numeric(df.CAMION, downcast="integer")
        df.VOLQUETA = pd.to_numeric(df.VOLQUETA, downcast="integer")
        df.MOTO = pd.to_numeric(df.MOTO, downcast="integer")
        df.BICICLETA = pd.to_numeric(df.BICICLETA, downcast="integer")
        df.OTRO = pd.to_numeric(df.OTRO, downcast="integer")
        df.VIA = df.VIA.astype(str).str.upper()
        df.BARRIO = df.BARRIO.astype(str).str.upper()
        df.HORA = pd.to_datetime(df.HORA).dt.time
        df.NOMBRE_COMUNA = df.NOMBRE_COMUNA.astype(str).str.upper()
        df.PROPIETARIO = df.PROPIETARIO.astype(str).str.upper()
        df.HORARIO = df.HORARIO.astype(str).str.upper()
        df.RESTRICCION_MOTO = df.RESTRICCION_MOTO.astype(str).str.upper()
        df['ID'] = df.ID.astype(str)
        df['ID_TIEMPO'] = df.ID_TIEMPO.astype(str)
        df['ID_LUGAR'] = df.ID_LUGAR.astype(str)
        df['ID_VEHICULO'] = df.ID_VEHICULO.astype(str)

        df['DIA'] = df['DIA'].str.split('.').str[-1]
        df['DIA'] = df['DIA'].str.strip()
        df['NOMBRE_COMUNA'] = df['NOMBRE_COMUNA'].str.split('.').str[-1]
        df['NOMBRE_COMUNA'] = df['NOMBRE_COMUNA'].str.strip()

        Tiempo = df[['ID_TIEMPO','FECHA','DIA','HORA','HORARIO']]
        Lugar = df[['ID_LUGAR','NOMBRE_COMUNA','VIA','BARRIO']]
        Vehiculo = df[['ID_VEHICULO','PEATON','AUTOMOVIL','CAMPERO','CAMIONETA','MICRO','BUSETA','BUS','CAMION','VOLQUETA','MOTO','BICICLETA','OTRO']]
        Accidentes = df[['ID','ID_TIEMPO','ID_VEHICULO','ID_LUGAR','GRAVEDAD','PROPIETARIO','RESTRICCION_MOTO']]

        Tiempo.to_csv(r'D:\prueba_t\airflow\data\Tiempo.csv',index=False)
        Lugar.to_csv(r'D:\prueba_t\airflow\data\Lugar.csv',index=False)
        Vehiculo.to_csv(r'D:\prueba_t\airflow\data\Vehiculo.csv',index=False)
        Accidentes.to_csv(r'D:\prueba_t\airflow\data\Accidentes.csv',index=False)
    
    except:
        return "Problema con la transformacion"

    return "Success"

def load_data():

    try:

        # Dejo el usuario y la contraseña ya que no es nada productivo

        conn = psycopg2.connect(
            database="accidentes",
            user='airflow', 
            password='airflow', 
            host='host.docker.internal', 
            port='5432'
        )
        cur = conn.cursor()

        tablas = ['accidentes','lugar','vehiculo','tiempo']
        rutas = [r'D:\prueba_t\airflow\data\Accidentes.csv',r'D:\prueba_t\airflow\data\Lugar.csv',r'D:\prueba_t\airflow\data\Vehiculo.csv',r'D:\prueba_t\airflow\data\Tiempo.csv']

        contador = 0

        for ruta in rutas:
            with open(ruta, 'r') as f:
                cur.copy_expert(sql=
                                f"""
                                    COPY {tablas[contador]} FROM stdin WITH CSV HEADER
                                    DELIMITER as ','
                                    """, 
                                file=f)
                conn.commit()
            contador += 1
        
        cur.close()
    
    except:
        return "Problemas cargando las tablas en bd"
    
    return "Success"

def send_mail():

    correo = config('MAIL')
    contraseña = config('PASSWORD')

    yag = yagmail.SMTP(user=correo, password=contraseña)

    destinatarios = [config('MAIL')]
    asunto = 'Prueba correo'
    mensaje = 'Mensaje de prueba'

    yag.send(destinatarios,asunto,mensaje)

    # mensaje = MIMEMultipart("plain")
    # mensaje["From"] = correo
    # mensaje["To"] = correo
    # mensaje["Subject"] = "Prueba correo"

    # smtp = smtp("smtp.live.com")
    # smtp.starttls()
    # smtp.login(correo,contraseña)
    # smtp.sendmail(correo,correo,mensaje.as_string())
    # smtp.quit()

    return "Success"

default_args = {
    "owner": "Raul Higuera",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="prueba_tecnica",
    schedule="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['prueba'],
) as dag:

    extract_info_task = PythonOperator(
        task_id="extract_info_task",
        python_callable=extract_data,
        op_kwargs={
            "url": 'https://datos.gov.co/resource/7cci-nqqb.json',
        },
        dag=dag
    )

    transform_data_task = PythonOperator(
        task_id="transform_data_task",
        python_callable=transform_data,
        op_kwargs={
            "src_file": "D:\prueba_t\data\output.csv",
        },
        dag=dag
    )

    create_tables_task = PostgresOperator(
        task_id="create_tables_into_db_task",
        postgres_conn_id='postgres_localhost',
        sql = """
            drop table if exists Accidentes;
            drop table if exists Lugar;
            drop table if exists Tiempo;
            drop table if exists Vehiculo;

            create table if not exists Accidentes (
                ID text,
                ID_TIEMPO text,
                ID_VEHICULO text,
                ID_LUGAR text,
                GRAVEDAD VARCHAR(50),
                PROPIETARIO VARCHAR(50),
                RESTRICCION_MOTO VARCHAR(50)
            );

            create table if not exists Lugar (
                ID_LUGAR text,
                NOMBRE_COMUNA VARCHAR(50),
                VIA VARCHAR(50),
                BARRIO VARCHAR(50)
            );

            create table if not exists Tiempo (
                ID_TIEMPO text,
                FECHA date,
                DIA VARCHAR(50),
                HORA time,
                HORARIO VARCHAR(50)
            );

            create table if not exists Vehiculo (
                ID_VEHICULO text,
                PEATON SMALLINT,
                AUTOMOVIL SMALLINT,
                CAMPERO SMALLINT,
                CAMIONETA SMALLINT,
                MICRO SMALLINT,
                BUSETA SMALLINT,
                BUS SMALLINT,
                CAMION SMALLINT,
                VOLQUETA SMALLINT,
                MOTO SMALLINT,
                BICICLETA SMALLINT,
                OTRO SMALLINT
            );
        """
    )

    load_data_task = PythonOperator(
        task_id="load_data_into_db_task",
        python_callable=load_data,
        dag=dag
    )

    notify_success_task = PythonOperator(
        task_id="Notify_task",
        python_callable=send_mail,
        dag=dag
    )



    extract_info_task >> transform_data_task >> create_tables_task >> load_data_task >> notify_success_task
    
    