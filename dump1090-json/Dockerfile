FROM iqtlabs/edgetech-core
RUN apt-get update \
    && apt-get install -y \
       curl \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
COPY poetry.lock .
RUN pip3 install poetry==1.5.1
RUN poetry config virtualenvs.create false
RUN poetry install --without dev

RUN curl -LO \
    https://github.com/IQTLabs/edgetech-core/raw/main/core/base_mqtt_pub_sub.py
COPY ./dump1090-json_pub_sub.py .

ENTRYPOINT ["python3", "dump1090-json_pub_sub.py"]

# TODO: Review
## docker run -it --network host --device /dev/ttyACM0:/dev/serial0 <container name>
