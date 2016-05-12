#rabbit alarm utility
#pulls in specified queues from the specified rabbit server
#

import argparse
import time
import pika
import resource
import boto3
from datetime import datetime

import logging

LOGGER = None


def init_logging(log_level='INFO', logfile=None):
    global LOGGER

    log_format = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) -35s %(lineno) -5d: %(message)s')

    if logfile is None:
        logging.basicConfig(level=log_level, format=log_format)
    else:
        logging.basicConfig(level=log_level, format=log_format, filename=logfile)

    LOGGER = logging.getLogger('Rabbitalarm')


class Args:
    pass

def post(length):
    LOGGER.info(" Length of %s is %d", args.queues, length)
    try:
        client = boto3.client('cloudwatch')
        #client.put_metric_data(Namespace='Meshfire-Production', MetricData=[{'MetricName':'Queue-length', 'Timestamp':datetime.utcnow(), 'Value':length,'Unit':'Count'}])

    except Exception as ex:
        LOGGER.error(" Failed to post queue length data to AWS Cloudwatch because of error: %s", ex.message)


def on_callback(msg):
    LOGGER.info(" Pika callback message %s", msg)

def start(post):
    LOGGER.info(" Starting rabbit alarm")
    LOGGER.info( '  rabbit host: %s', args.rabbitserver)
    LOGGER.info('  rabbit user: %s', args.rabbituser)
    LOGGER.info('  rabbit pwd: *********')
    LOGGER.info('  logging level: %s', args.loglevel)
    LOGGER.info('  logging file: %s', args.logfile)
    LOGGER.info('  queues monitored: %s', args.queues)
    LOGGER.info('  monitoring interval %d', args.interval)
    querycount = 0
    try:
        params = pika.ConnectionParameters(
            host=args.rabbitserver,
            port=5672,
            credentials=pika.credentials.PlainCredentials(args.rabbituser, args.rabbitpwd)
        )
        connection = pika.BlockingConnection(parameters=params)
        channel = connection.channel()

        while True:
            LOGGER.debug("Memory current level %d", resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
            total = 0
            for queue in args.queues:
                LOGGER.debug('getting length of queue %s', queue)
                res = channel.queue_declare(queue=queue, durable=True, exclusive=False, auto_delete=False, passive=True)
                total += res.method.message_count
                LOGGER.debug('queue %s has length of %d.', queue, res.method.message_count)
            post(total)
            querycount += 1
            time.sleep(args.interval*60)

    except Exception as ex:
        LOGGER.error(" Error %s on query count %d", ex.message, querycount)

if __name__ == '__main__':

    args = Args()

    parser = argparse.ArgumentParser(description="Meshfire Rabbitmq monitoring service")
    parser.add_argument('-rs', '--rabbitserver', default='localhost', help='RabbitMQ Server name')
    parser.add_argument('-ru', '--rabbituser', default='guest', help='RabbitMQ user name')
    parser.add_argument('-pwd', '--rabbitpwd', default='guest', help='RabbitMQ password')
    parser.add_argument('-q','--queues', nargs='+',  help="List of queues to report. If none then all queues counts are counted", required=True)
    parser.add_argument('-ll', '--loglevel', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help='Logging level')
    parser.add_argument('-log', '--logfile', default=None, help='Set log file')
    parser.add_argument('-i', '--interval', type=int, default=1, help='Set interval check time in minutes')

    parser.parse_args(namespace=args)
    init_logging(log_level=args.loglevel, logfile=args.logfile)

    print(' Meshfire Rabbitmq monitoring tool. Posts number of messages in monitored queues to AWS Cloudwatch')
    print '  rabbit host: %s' % args.rabbitserver
    print '  rabbit user: %s' % args.rabbituser
    print '  rabbit pwd: *********'
    print '  logging level: %s' % args.loglevel
    print '  logging file: %s' % args.logfile
    print '  queues monitored: %s' % args.queues
    print '  monitoring interval %d' % args.interval

    start(post)



