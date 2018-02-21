const cluster = require('cluster');
const net = require('net');
const WRRPool = require('wrr-pool');

const port = 3000;
const num_processes = require('os').cpus().length;

if (cluster.isMaster) {
    const pool = new WRRPool();
    const workers = [];
    const spawn = (i) => {
        workers[i] = cluster.fork();
        workers[i].on('exit', (code, signal) => {
            console.log('respawning worker', i);
            spawn(i);
        });
    };

    for (let i = 0; i < num_processes; i++) {
        pool.add(i, 1);
        spawn(i);
    }

    net.createServer({ pauseOnConnect: true }, function(connection) {
        const worker = workers[pool.next()];
        worker.send('sticky-session:connection', connection);
    }).listen(port);
} else {
    require(`./proxy`);
}