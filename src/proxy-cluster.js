const Const = require(`./constants.json`);
const Config = require(`../config`).proxy;
const ApplicationLogger = require(`./ApplicationLogger`);
const WRRPool = require('wrr-pool');
const cluster = require('cluster');
const net = require('net');
const os = require('os');

const logger = new ApplicationLogger(Const.APPLICATION_TAG, Config.APP_LOG_LEVEL);

process.env.IS_CLUSTER_MODE = true;

if (cluster.isMaster) {
    const clusterWorkers = Config.CLUSTER_WORKERS;
    const amountOfWorkers = clusterWorkers === Const.CPU_TAG ? os.cpus().length : clusterWorkers;
    const pool = new WRRPool();
    const workers = [];
    const spawn = (i) => {
        workers[i] = cluster.fork();
        workers[i].on('exit', () => spawn(i));
    };

    logger.info(`Process will be started in cluster mode with ${amountOfWorkers} proxy workers`);

    for (let i = 0; i < amountOfWorkers; i++) {
        pool.add(i, 1);
        spawn(i);
    }

    net.createServer({ pauseOnConnect: true }, (connection) =>
        workers[pool.next()].send(Const.STICKY_SESSION_TAG, connection))
        .listen(Config.WEB_SOCKET_SERVER_PORT, Config.WEB_SOCKET_SERVER_HOST);
} else {
    logger.info(`Child proxy process with PID: ${process.pid} has been started`);

    require(`./proxy`);
}