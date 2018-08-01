FROM daocloud.io/xingetouzi/python3-cron:latest

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

COPY . ./

VOLUME [ "/logs" ]

ENV BITFINEX /app/bitfinex
ENV BINANCE /app/binance

CMD ["/bin/bash", "command.sh"]