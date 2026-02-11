# Base image pour Spark (ajuste selon ton image actuelle)
FROM bde2020/spark-master:3.0.0-hadoop3.2

# Installer Java 11, Python 3 et pip via apk/apt
RUN set -eux; \
    if command -v apt-get >/dev/null 2>&1; then \
      apt-get update && apt-get install -y openjdk-11-jdk python3 python3-pip && rm -rf /var/lib/apt/lists/*; \
    elif command -v apk >/dev/null 2>&1; then \
      apk add --no-cache openjdk11 python3 py3-pip; \
    else \
      echo "No supported package manager found" && exit 1; \
    fi

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk

# Vérifier les versions
RUN python --version && pip --version

# Installer les dépendances Python nécessaires pour Spark
RUN python -m pip install pyspark==3.0.0

# Copier le répertoire src dans le container
COPY src /opt/spark/src

# Définir l'environnement Python 3 comme version par défaut
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 1
RUN update-alternatives --install /usr/bin/pip pip /usr/bin/pip3 1

# Exposer les ports nécessaires pour Spark
EXPOSE 7077 8080 4040

# Commande pour démarrer Spark
CMD ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"]
