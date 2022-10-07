FROM ubuntu:22.04

LABEL author="Tonny-Bright"

EXPOSE 8080 5555 8793

SHELL ["/bin/bash", "-c"]

# no interaction
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux
ENV SLUGIFY_USES_TEXT_UNIDECODE=yes

# airflow
ENV AIRFLOW=/opt/airflow
ENV AIRFLOW_HOME=$AIRFLOW
ENV AIRFLOW__CORE__DAGS_FOLDER=$AIRFLOW/dags
ENV AIRFLOW__CORE__PLUGINS_FOLDER=$AIRFLOW/plugins
ENV AIRFLOW__CORE__BASE_LOG_FOLDER=$AIRFLOW/logs
ENV AIRFLOW_KEYS=$AIRFLOW/keys
ENV AIRFLOW_VERSION=2.3.0
ENV AIRFLOW_COMPONENTS=all_dbs,async,celery,cloudant,crypto,gcp_api,google_auth,hdfs,hive,jdbc,mysql,oracle,password,postgres,rabbitmq,redis,s3,samba,slack,ssh,github_enterprise
ENV AIRFLOW_GPL_UNIDECODE=yes
ENV C_FORCE_ROOT=true

# language
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

# pip install extensions
ENV PYTHON_PACKAGES=
ENV PYTHONDONTWRITEBYTECODE=true

# base
RUN mkdir -p $AIRFLOW_HOME && \
    mkdir -p $AIRFLOW_KEYS && \
    mkdir -p $AIRFLOW__CORE__DAGS_FOLDER && \
    mkdir -p $AIRFLOW__CORE__BASE_LOG_FOLDER && \
    mkdir -p $AIRFLOW__CORE__PLUGINS_FOLDER
# ADD airflow/home /opt/airflow/
# ADD instantclient-basic-linux.x64-19.3.0.0.0dbru.zip /tmp/
WORKDIR /opt/airflow

RUN apt-get update -y \
    && apt-get install -y \
                        python3-pip \
                        python3-dev \
                        python3-setuptools \
                        zip \
                        wget \
                        git \
                        vim \
                        locales \
                        build-essential \
                        curl \
                        default-libmysqlclient-dev \
                        freetds-dev \
                        libkrb5-dev \
                        libsasl2-dev \
                        libssl-dev \
                        libffi-dev \
                        libpq-dev \
                        libaio1 \
                        openjdk-8-jre \
                        openjdk-8-jdk \
                        dumb-init \
    && sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && locale-gen \
    && apt-get clean

RUN ln -sf $(which pip3) /usr/bin/pip \
    && ln -sf $(which python3) /usr/bin/python



## Install Airflow
# RUN pip install "apache-airflow[${AIRFLOW_COMPONENTS}]==${AIRFLOW_VERSION}" --no-cache-dir

# RUN mkdir -p /airflow_custom
# ADD airflow/airflow_custom /airflow_custom
# RUN python -m pip install -e /airflow_custom

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -U -r /tmp/requirements.txt

COPY dags /opt/airflow/dags
COPY airflowproj /opt/airflow/airflowproj

# remove apt cache
RUN apt-get clean --dry-run

COPY airflow.cfg /opt/airflow/
COPY scripts/airflowinit.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT [ "/entrypoint.sh" ]
CMD ["/usr/bin/dumb-init", "--", "/entrypoint.sh"]
