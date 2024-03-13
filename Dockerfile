FROM python:3.8
RUN apt-get -y update && apt-get install -y --no-install-recommends \
         wget \
         python3 \
         ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN wget https://bootstrap.pypa.io/get-pip.py && python3 get-pip.py 

#pre-trained model package installation
RUN apt-get update && apt-get install ffmpeg libsm6 libxext6  -y
RUN pip install boto3 botocore certifi charset-normalizer idna jmespath numpy opencv-python python-dateutil requests s3transfer setuptools six urllib3 wheel




# Set environment variables
ENV PYTHONUNBUFFERED=TRUE
ENV PYTHONDONTWRITEBYTECODE=TRUE
ENV PATH="/opt/program:${PATH}"

COPY Pipeline /opt/program
WORKDIR /opt/program
CMD sh start_default.sh