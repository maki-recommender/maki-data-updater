FROM python:3.10-slim-bullseye

COPY ./requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./anilistdataupdater.py .
COPY ./common.py .
COPY ./database.py .
COPY ./main.py .

CMD python main.py