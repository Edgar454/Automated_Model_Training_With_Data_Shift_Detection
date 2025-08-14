FROM apache/airflow:3.0.1

USER airflow

# Installer poetry
RUN pip install "poetry==2.1.3" 

# Copier les fichiers de dépendances
COPY pyproject.toml poetry.lock ./

# Installer les dépendances
RUN poetry install --no-root
