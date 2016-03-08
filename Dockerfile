FROM python:2.7
MAINTAINER indieman<ivan.kobzev@gmail.com>

ADD . /project
WORKDIR /project

RUN rm -rf .git && \
    rm -rf tests && \
    rm -f .gitignore && \
    rm -f .gitlab-ci.yml && \
    rm -f Dockerfile && \
    rm -f docker-compose.yml && \
    rm -f run_tests.sh

RUN pip install -r config/requirements.txt

EXPOSE 8888
CMD python -m box_longer
