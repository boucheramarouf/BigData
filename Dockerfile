# Base image pour Spark (ajuste selon ton image actuelle)
FROM bde2020/spark-master:3.0.0-hadoop3.2

# Installer Python 3 et pip via apk
RUN apk update && apk add --no-cache \
    python3 \
    py3-pip \
    && python3 --version \
    && pip3 --version

# Installer les dépendances Python nécessaires pour Spark
RUN pip3 install pyspark==3.0.0

# Copier le répertoire src dans le container
COPY src /opt/spark/src

# Définir l'environnement Python 3 comme version par défaut
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 1
RUN update-alternatives --install /usr/bin/pip pip /usr/bin/pip3 1

# Exposer les ports nécessaires pour Spark
EXPOSE 7077 8080 4040

# Commande pour démarrer Spark
CMD ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"]
