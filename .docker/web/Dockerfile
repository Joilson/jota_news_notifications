FROM python:3.10

WORKDIR /app

RUN apt-get update && apt-get install -y

COPY . .

RUN pip install --no-cache-dir -r ./requirements.txt

CMD ["python", "-u", "main.py"]
