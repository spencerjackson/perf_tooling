# docker run -it --mount type=bind,source=$HOME/.evergreen.yml,target=/root/.evergreen.yml tooling

FROM amazonlinux:2 as base

RUN yum -y groupinstall Development Tools
RUN yum -y install python3 python3-pip sudo bash git

RUN mkdir /perftooling
WORKDIR /perftooling

RUN curl https://s3.amazonaws.com/boxes.10gen.com/build/curator/curator-dist-rhel70-3df28d2514d4c4de7c903d027e43f3ee48bf8ec1.tar.gz  | tar -xvzf -

COPY requirements.txt .

RUN pip3 install -r requirements.txt

COPY . .

ENTRYPOINT ["python3", "src/perf_tools/cli.py"]