# Docker and Docker Compose
sudo apt update
sudo apt install docker.io -y
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker $USER
newgrp docker  # May not persist in non-interactive shells

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" \
  -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Verify Installation
docker --version
docker-compose --version

# Airflow setup
echo "export AIRFLOW_HOME=/home/azureuser/fitness_monitor" >> ~/.bashrc
source ~/.bashrc

# Ensure docker-compose.yml is in $AIRFLOW_HOME
cd $AIRFLOW_HOME
docker compose up airflow-init -d
docker compose up -d
