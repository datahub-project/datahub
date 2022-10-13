FROM elasticdump/elasticsearch-dump:latest
RUN apt update && apt install -y curl cron && service cron start
RUN apt install -y python3
COPY script.py /script.py
RUN mkdir /dump
COPY schedule.cron /etc/cron.d/
RUN chmod 0644 /etc/cron.d/schedule.cron \
  && chmod 0644 /script.py \
  && crontab /etc/cron.d/schedule.cron
RUN touch /var/log/cron.log
CMD cron && tail -f /dump/cron.log

