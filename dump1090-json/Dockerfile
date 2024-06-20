FROM iqtlabs/edgetech-core

COPY pyproject.toml .
COPY poetry.lock .
RUN pip3 install poetry==1.5.1
RUN poetry config virtualenvs.create false
RUN poetry install --without dev

COPY ./dump1090-json_pub_sub.py .

ENTRYPOINT ["python3", "dump1090-json_pub_sub.py"]