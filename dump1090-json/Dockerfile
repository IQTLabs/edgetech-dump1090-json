FROM iqtlabs/edgetech-core
COPY pyproject.toml .
COPY poetry.lock .
RUN pip3 install poetry==1.1.5
RUN poetry config virtualenvs.create false
RUN poetry install --no-dev
RUN pip install coloredlogs python-dateutil
RUN apt-get install -y python3-pandas
COPY ./dump1090-json_pub_sub.py .
ENTRYPOINT ["python3", "dump1090-json_pub_sub.py"]

## docker run -it --network host --device /dev/ttyACM0:/dev/serial0 <container name>
