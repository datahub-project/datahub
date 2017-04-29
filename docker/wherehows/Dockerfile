FROM centos:7.3.1611

RUN adduser wherehows

WORKDIR /home/wherehows

# install wherehows.
ADD archives/jdk-8u101-linux-x64.tar.gz /home/wherehows
ADD archives/activator-1.3.11-minimal.tar.gz /home/wherehows
ADD archives/wherehows-built-lite.tar.gz /home/wherehows
ADD archives/ivy2.tar.gz /home/wherehows

COPY binary/* /home/wherehows/
COPY conf/backend/* /home/wherehows/WhereHows/backend-service/conf/
COPY conf/web/* /home/wherehows/WhereHows/web/conf/

RUN mkdir logs; echo "alias ll=ls -al" > /etc/profile; \
    ln -s activator-1.3.11-minimal activator; \
    ln -s jdk1.8.0_101 jdk8; \
    chown -R wherehows:wherehows ./*; \
    chown -R wherehows:wherehows .ivy2

USER wherehows

VOLUME /home/wherehows/logs

ENV ACTIVATOR_HOME=/home/wherehows/activator \
    JAVA_HOME=/home/wherehows/jdk8 \
    MYSQL_HOME=/home/wherehows/mysql \
    SBT_OPTS="-Xms1G -Xmx1G -Xss2M -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled" \
    PATH=/home/wherehows/activator/bin:/home/wherehows/jdk8/bin:/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/home/wherehows/.local/bin:/home/wherehows/bin

EXPOSE 9000 9008

CMD /home/wherehows/run.sh
