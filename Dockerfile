FROM node:8.8.1-alpine
ENV WORK_DIR=/usr/src/app/
ENV CONF_DIR=/usr/src/app/conf
RUN mkdir -p ${WORK_DIR} \
    && mkdir -p ${CONF_DIR} \
    && cd ${WORK_DIR}

WORKDIR ${WORK_DIR}

COPY . ${WORK_DIR}

RUN apk update \
    && apk add --no-cache --virtual .gyp python make g++ \
    && npm install \
    && apk del .gyp


EXPOSE 3000
VOLUME ["/usr/src/app/conf"]
CMD ["node", "example/wskafka-server.js"]
