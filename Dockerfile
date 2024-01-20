FROM python:3.9

# By default, listen on port 8501
EXPOSE 8501

COPY requirements.txt /

RUN apt-get update

RUN apt install -y unixodbc unixodbc-dev

RUN python -m pip install -r /requirements.txt

COPY ./app /app

WORKDIR /app/

# execute the command python main.py (in the WORKDIR) to start the app
CMD ["streamlit", "run","Home.py"]

