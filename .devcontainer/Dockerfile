# imageは使いたいバージョン、ディストリビューションなどに変更することができます。
FROM ubuntu:20.04

# 必要そうなものをinstall
RUN apt-get update && apt-get install -y --no-install-recommends wget build-essential libreadline-dev \ 
libncursesw5-dev libssl-dev libsqlite3-dev libgdbm-dev libbz2-dev liblzma-dev zlib1g-dev uuid-dev libffi-dev libdb-dev libsnappy-dev libleveldb-dev

#任意バージョンのpython install
# RUN wget --no-check-certificate https://www.python.org/ftp/python/3.9.5/Python-3.9.5.tgz \
# && tar -xf Python-3.9.5.tgz \
# && cd Python-3.9.5 \
# && ./configure --enable-optimizations\
# && make \
# && make install

# RUNでコンテナ生成時に実行する
RUN apt-get update
RUN apt-get install -y python3 python3-pip

#サイズ削減のため不要なものは削除
RUN apt-get autoremove -y

#requirements.txtなら以下のように
# RUN pip3 install -r ./requirements.txt