import pandas as pd
import numpy as np
import datetime
from kafka import *
from minio import Minio
from minio.error import S3Error
import datetime
import json

"""
Mini-Projet : Traitement de l'Intelligence Artificielle
Contexte : Allier les concepts entre l'IA, le Big Data et IoT

Squelette pour simuler un capteur qui est temporairement stocké sous la forme de Pandas
"""

"""
    Dans ce fichier capteur.py, vous devez compléter les méthodes pour générer les données brutes vers Pandas 
    et par la suite, les envoyer vers Kafka.
    ---
    Le premier devra prendre le contenue du topic donnees_capteurs pour le stocker dans un HDFS. (Fichier Kafka-topic.py)
    Le deuxième devra prendre le contenue du HDFS pour nettoyer les données que vous avez besoin avec SPARK, puis le stocker en HDFS.
    Le troisième programme est un entraînement d'un modèle du machine learning (Vous utiliserez TensorFlow, avec une Régression Linéaire) (Fichier train.py)
    Un Quatrième programme qui va prédire une valeur en fonction de votre projet. (Fichier predict.py)
"""

def generate_dataFrame(col):
    """
    Cette méthode permet de générer un DataFrame Pandas pour alimenter vos data
    """
    df = pd.DataFrame(columns=col)
    add_data(df)
    return df

def add_data(df: pd.DataFrame):
    """
    Cette méthode permet d'ajouter de la donnée vers votre DataFrame Pandas
    """
    # temps_execution =
    current_timestamp = datetime.datetime.now()

    
    duration = 60 # Nombre de secondes pour la génération des données.

    i = 0
    while i < 10000:
        ## Dans cette boucle, vous devez créer et ajouter des données dans Pandas à valeur aléatoire.
        ## Chaque itération comportera 1 ligne à ajouter.
        new_timestamp = current_timestamp + datetime.timedelta(seconds=1)
        entrance_amount = np.random.randint(low=0, high=50)
        exit_amount = np.random.randint(low=0, high=50)
        temperature = np.random.normal(loc=20, scale=5)
        humidity = np.random.normal(loc=50, scale=10)
        parking_entrance = np.random.randint(low=1, high=5)
        parking_exit = np.random.randint(low=1, high=5)
        parking_actual_vehicle = np.random.randint(low=0, high=500)
    
        # ajouter une ligne au DataFrame
        df.loc[i] = [new_timestamp, entrance_amount, exit_amount, temperature, humidity, parking_entrance, parking_exit, parking_actual_vehicle]
        i += 1
    return df


def add_datatokafka():
    """
    Cette méthode permet d'ajouter de la donnée vers votre DataFrame Pandas
    """
    # temps_execution =
    current_timestamp = datetime.datetime.now()
    while 1:
        ## Dans cette boucle, vous devez créer et ajouter des données dans Pandas à valeur aléatoire.
        ## Chaque itération comportera 1 ligne à ajouter.
        new_timestamp = current_timestamp + datetime.timedelta(seconds=1)
        entrance_amount = np.random.randint(low=0, high=50)
        exit_amount = np.random.randint(low=0, high=50)
        temperature = np.random.normal(loc=20, scale=5)
        humidity = np.random.normal(loc=50, scale=10)
        parking_entrance = np.random.randint(low=1, high=5)
        parking_exit = np.random.randint(low=1, high=5)
        parking_actual_vehicle = np.random.randint(low=0, high=500)

        liste = [new_timestamp.isoformat(), entrance_amount, exit_amount, temperature, humidity, parking_entrance, parking_exit,
         parking_actual_vehicle]

        # ajouter une ligne au DataFrame
        producer = KafkaProducer( bootstrap_servers=['127.0.0.1:9092'],value_serializer=lambda x: json.dumps(x).encode('utf-8'))
        producer.send("capteur", liste)



def write_data_kafka(df: pd.DataFrame):
    """
    Cette méthode permet d'écrire le DataFrame vers Kafka.
    (Optionnel)
    """
    csv_string = df.to_csv(index=False)
    producer = KafkaProducer( bootstrap_servers=['127.0.0.1:9092'])
    producer.send('nom_du_topic', value=bytes(csv_string, 'utf-8'))
    producer.close()

def write_data_minio(df: pd.DataFrame):
    """
    Cette méthode permet d'écrire le DataFrame vers Minio.
    (Obligatoire)
    """
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    found = client.bucket_exists("donnes-capteurs")
    if not found:
        client.make_bucket("donnes-capteurs")
    else:
        print("Bucket 'donnes-capteurs' existe déjà")

    timestamp = datetime.datetime.now().strftime('%d-%m-%y')
    df.to_csv("donnes_capteurs_" + str(timestamp) + ".csv", encoding='utf-8', index=False)
    client.fput_object(
        "donnes-capteurs", "donnes_capteurs_" + str(timestamp) + ".csv",  "donnes_capteurs_" + str(timestamp) + ".csv")

if __name__ == "__main__":

    # sans kafka
    columns = ["timestamp", "entrance_amount", "exit_amount", "temperature", "humidity", "parking_entrance", "parking_exit", "parking_actual_vehicle"]

    df = generate_dataFrame(columns)
    df = add_data(df)
    write_data_minio(df)

    #add_datatokafka()
    #write_data_kafka(df)
    #write_data_kafka(df)
    #add_datatokafka()