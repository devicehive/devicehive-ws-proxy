const Const = require(`./constants.json`);
const Config = require(`../config`).proxy;
const ApplicationLogger = require(`./ApplicationLogger`);
const cluster = require('cluster');


process.env.IS_CLUSTER_MODE = true;


const logger = new ApplicationLogger(Const.APPLICATION_TAG, Config.APP_LOG_LEVEL);


if (cluster.isMaster) {
    const url = require('url');
    const http = require('http');
    const os = require('os');
    const httpProxy = require('http-proxy');
    const LoadBalancer = require(`./LoadBalancer`);

    const clusterWorkers = Config.CLUSTER_WORKERS;
    const amountOfWorkers = clusterWorkers === Const.CPU_TAG ? os.cpus().length : clusterWorkers;

    const ports = Array.from({length: amountOfWorkers}, (value, index) => Config.WS_WORKER_BASE_PORT + index);
    const lb = new LoadBalancer(amountOfWorkers);
    const workers = [];
    const spawn = (i) => {
        workers[i] = cluster.fork();
        workers[i].on('exit', () => spawn(i));
        workers[i].on('message', (message) => {
            if (message.message === `close`) {
                lb.freeWorker(i, message.type, message.role);
            }
        });
        workers[i].send({ message: `port`, port: ports[i] });
    };

    for (let i = 0; i < amountOfWorkers; i++) {
        spawn(i);
    }

    const proxies = Array.from({ length: amountOfWorkers }, (value, index) =>
        new httpProxy.createProxyServer({ target: { host: Config.WEB_SOCKET_SERVER_HOST, port: ports[index] } }));

    logger.info(`Process will be started in cluster mode with ${amountOfWorkers} proxy workers`);

    const proxyServer = http
        .createServer()
        .listen(Config.WEB_SOCKET_SERVER_PORT, Config.WEB_SOCKET_SERVER_HOST);

    proxyServer.on('upgrade', (req, socket, head) => {
        const { type, role } = url.parse(req.url, true).query;
        proxies[lb.getWorker(type, role)].ws(req, socket, head);
    });

} else {
    logger.info(`Child proxy process with PID: ${process.pid} has been started`);

    require(`./proxy`);
}