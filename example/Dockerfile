FROM node:8.1.4-alpine
ENV WORK_DIR=/usr/src/app/
RUN mkdir -p ${WORK_DIR} \
    && cd ${WORK_DIR}

WORKDIR ${WORK_DIR}

COPY . ${WORK_DIR}

RUN npm install ws debug pino
ENTRYPOINT ["node"]
