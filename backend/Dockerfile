FROM python:3.9
WORKDIR /app
COPY backend/ .  # Copier le code source dans le conteneur
RUN pip install -r requirements.txt
CMD ["python", "app.py"]
