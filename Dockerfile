FROM daocloud.io/xingetouzi/python3-cron:latest

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

COPY . ./

CMD ["/bin/bash", "command.sh"]