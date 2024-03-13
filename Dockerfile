FROM python:3.8
RUN apt-get -y update && apt-get install -y --no-install-recommends \
         wget \
         python3 \
         nginx \
         ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN wget https://bootstrap.pypa.io/get-pip.py && python3 get-pip.py && \
    pip install flask gevent gunicorn && \
        rm -rf /root/.cache

#pre-trained model package installation
RUN apt-get update && apt-get install ffmpeg libsm6 libxext6  -y
RUN wget https://raw.githubusercontent.com/dns-smitk/kinesis_stream_processing_pipeline/main/requirement.txt | pip install -r requirement.txt
RUN sh start_default.sh 

# Set environment variables
ENV PYTHONUNBUFFERED=TRUE
ENV PYTHONDONTWRITEBYTECODE=TRUE
ENV PATH="/opt/program:${PATH}"

COPY NER /opt/program
WORKDIR /opt/program