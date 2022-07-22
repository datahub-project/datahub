FROM acryldata/acryl-datahub-actions:head
USER root
RUN chown -R datahub:datahub /usr/local/bin/

ENV CLI_TELEMETRY_ENABLED=false

#ENV PIP_INDEX_URL=<override pypi url if needed>
ENV PIP_VERBOSE=3
ENV PIP_DEFAULT_TIMEOUT=2
ENV VENV_DIR=/etc/datahub/venv
ENV VENV_NAME=$VENV_DIR/datahub/ingest


RUN mv /usr/local/bin/run_ingest.sh /usr/local/bin/run_ingest.sh.bak
COPY ./docker/datahub-actions/files/run_ingest.sh /usr/local/bin/run_ingest.sh
COPY ./docker/datahub-actions/files/requirement.txt $VENV_DIR/requirement.txt
COPY ./docker/datahub-actions/files/start_datahub_actions.sh /start_datahub_actions.sh

RUN mkdir -p $VENV_DIR/datahub
RUN python3 -m venv $VENV_NAME
RUN $VENV_NAME/bin/python3 -m pip install --upgrade pip wheel setuptools
RUN $VENV_NAME/bin/python3 -m pip install -r $VENV_DIR/requirement.txt

RUN chmod -R 755 $VENV_DIR
RUN chmod -R 755 /usr/local/bin/run_ingest.sh
RUN chmod 755 /start_datahub_actions.sh
USER datahub
