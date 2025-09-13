FROM flink:latest

RUN apt-get update -y && apt-get install -y python3 python3-pip python3-dev && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python

# Installing required libraries
RUN mkdir -p /tmp/pyflink_build
WORKDIR /tmp/pyflink_build
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# Copying data input and script code into image
COPY df_json.py .
COPY stock_1mo.csv .

#Ensuring that script code can write the file to container
RUN chown -R flink:flink /tmp/pyflink_build

CMD ["python3", "df_json.py"]