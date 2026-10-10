FROM docker.io/library/python:3.12-slim
RUN pip install --no-cache-dir pyzmq==27.1.0 msgspec==0.19.0
COPY fake_vllm.py /app/fake_vllm.py
USER 1000:1000
ENTRYPOINT ["python3", "-u", "/app/fake_vllm.py"]
