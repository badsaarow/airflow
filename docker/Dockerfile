FROM ubuntu:22.04

ENV DEBIAN_FRONTEND noninteractive
ENV HOME /root

WORKDIR /root

RUN apt-get update \
    && apt-get install -y  --no-install-recommends procps apt-utils build-essential \
      iproute2 net-tools supervisor qemu-utils wget bridge-utils dnsmasq \
      python3.9  zsh htop vim git curl ca-certificates  postgresql \
    && apt-get autoclean \
    && apt-get autoremove
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && python3 get-pip.py
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
ENTRYPOINT ["/startup.sh"]
