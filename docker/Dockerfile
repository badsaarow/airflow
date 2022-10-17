FROM ubuntu:22.04

ENV DEBIAN_FRONTEND noninteractive
ENV HOME /root

RUN apt-get update \
    && apt-get install -y  --no-install-recommends procps apt-utils build-essential \
      iproute2 net-tools supervisor qemu-utils wget bridge-utils dnsmasq \
      python3.10  python3.10-venv  zsh htop vim git curl ca-certificates \
    && apt-get autoclean \
    && apt-get autoremove
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && python3 get-pip.py
RUN python3 -m venv /root/venv
RUN . /root/venv/bin/activate && pip install --upgrade pip && pip install wheel pandas protobuf boto3
RUN pip install "apache-airflow[celery]==2.4.1" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.4.1/constraints-3.7.txt"

ENTRYPOINT ["/startup.sh"]
