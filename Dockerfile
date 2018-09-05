FROM registry.docker-cn.com/python:3.6

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

COPY . ./

VOLUME [ "/logs" ]

ENV BITFINEX /app/bitfinex
ENV BINANCE /app/binance

RUN echo 'Asia/Shanghai' >/etc/timezone & cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
