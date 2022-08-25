FROM node:18.5-alpine

MAINTAINER devicehive

LABEL org.label-schema.url="https://devicehive.com" \
      org.label-schema.vendor="DeviceHive" \
      org.label-schema.vcs-url="https://github.com/devicehive/devicehive-ws-proxy" \
      org.label-schema.name="devicehive-ws-proxy" \
      org.label-schema.version="development"

ENV WORK_DIR=/usr/src/app/
ENV CONF_DIR=/usr/src/app/conf
RUN mkdir -p ${WORK_DIR} \
    && mkdir -p ${CONF_DIR} \
    && cd ${WORK_DIR}

WORKDIR ${WORK_DIR}

COPY . ${WORK_DIR}

RUN apk update && npm install && npm cache clean --force

RUN npm install pm2 -g

EXPOSE 3000
VOLUME ["/usr/src/app/conf"]
CMD ["pm2-docker", "src/proxy.js"]
