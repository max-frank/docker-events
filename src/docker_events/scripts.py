"""The CLI to start listen on docker events."""

import gevent
import gevent.pool
import gevent.queue
import gevent.monkey as gMonKey

gMonKey.patch_all()

# pylama: ignore=E402
import inspect
try:
    from itertools import imap
except ImportError:
    # Python 3...
    imap=map
import logging
import logging.config

import sys
import os
import click
import docker
import yaml
import simplejson as json

from . import event


if sys.version_info[:2] >= (3, 3):
    from importlib.machinery import SourceFileLoader
    def load_source(name, path):
        if not os.path.exists(path):
            return {}
        return vars(SourceFileLoader(name, path).load_module())
else:
    import imp
    def load_source(name, path):
        if not os.path.exists(path):
            return {}
        return vars(imp.load_source(name, path))

LOG = logging.getLogger('docker_events')


def setup_logging(logging_config, log_level=None):
    """Setup logging config."""

    if logging_config is not None:
        logging.config.fileConfig(logging_config)
    else:
        logging.basicConfig(level=logging.ERROR, format='%(levelname)s: %(name)s: %(message)s')
    if log_level:
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, log_level.upper()))


def join_configs(configs):

    """Join all config files into one config."""

    joined_config = {}

    for config in configs:
        joined_config.update(yaml.load(config))

    return joined_config


def load_modules(modules):
    """Load a module."""

    for dotted_module in modules:
        try:
            __import__(dotted_module)

        except ImportError as e:
            LOG.error("Unable to import %s: %s", dotted_module, e)


def load_files(files):
    """Load and execute a python file."""

    for py_file in files:
        LOG.debug("load source %s", py_file)
        load_source(py_file[:-3],py_file)


def summarize_events():
    """Some information about active events and callbacks."""

    for ev in event.events:
        if ev.callbacks:
            LOG.info("subscribed to %s by %s", ev, ', '.join(imap(repr, ev.callbacks)))


class DockerEvents(gevent.Greenlet):
    def __init__(self, docker_base_url, config, logger=None):
        super().__init__()
        self.client = docker.DockerClient(base_url=docker_base_url)
        self.config = config
        self.log = logger or logging.getLogger('docker_events')
        self.greenlets = gevent.pool.Group()
        self.events = gevent.queue.Queue(0)
        self._keep_going = True

    def _run(self):
        self.log.info('Starting {0}'.format(
            self.__class__.__name__
        ))

        # Startup event for handler initialization.
        self.dispatch_event({
            'status': 'docker_events.startup',
            'id': None,
            'from': None,
            'time': None,
        })

        self.greenlets.spawn(self.__collect)
        self.greenlets.spawn(self.__dispatch)

        # Fake a 'running' event for all running containers.
        for container in self.client.containers.list():
            event_data = {
                'status': 'running',
                'id': container.attrs['Id'],
                'from': container.attrs['Image'],
                'time': container.attrs['Created'],
            }
            self.events.put(event_data)

        self.greenlets.join()

    def stop(self):
        self.log.info('Stopping {0}'.format(
            self.__class__.__name__,
        ))
        self._keep_going = False
        self.greenlets.kill()

        # Shutdown event for handlers.
        self.dispatch_event({
            'status': 'docker_events.shutdown',
            'id': None,
            'from': None,
            'time': None,
        })

        self.kill()

    def __collect(self):
        while self._keep_going:
            for event_data in self.client.events(decode=True):
                #self.log.debug('incomming event: %s', event_data)
                self.events.put(event_data)
                gevent.sleep()

    def __dispatch(self):
        while self._keep_going:
            event_data = self.events.get()
            self.dispatch_event(event_data)
            gevent.sleep()

    def dispatch_event(self, event_data):
        self.log.debug('dispatching event: %s', event_data)
        callbacks = event.filter_callbacks(self.client, event_data)
        for callback in event.filter_callbacks(self.client, event_data):
            self.greenlets.add(gevent.spawn(callback, self.client, event_data, self.config))


@click.command()
@click.option("--sock", "-s",
              default="unix://var/run/docker.sock",
              help="the docker socket")
@click.option("configs", "--config", "-c",
              multiple=True,
              type=click.File("r"),
              help="a config yaml")
@click.option("modules", "--module", "-m",
              multiple=True,
              help="a python module to load")
@click.option("files", "--file", "-f",
              multiple=True,
              type=click.Path(exists=True),
              help="a python file to load")
@click.option("--log", "-l",
              type=click.Path(exists=True),
              help="logging config")
@click.option('--verbose', '-v', 'log_level', flag_value='info', help='set log level to info', envvar='DOCKER_EVENTS_LOG_LEVEL')
@click.option('--debug', '-d', 'log_level', flag_value='debug', help='set log level to debug', envvar='DOCKER_EVENTS_LOG_LEVEL')
def cli(sock, configs, modules, files, log, log_level):
    """The CLI."""

    setup_logging(log, log_level)

    config = join_configs(configs)

    # load python modules
    load_modules(modules)

    # load python files
    load_files(files)

    # summarize active events and callbacks
    summarize_events()

    server = DockerEvents(sock, config=config)
    try:
        server.start()
        server.join()
    except KeyboardInterrupt:
        pass # Press Ctrl+C to stop
    finally:
        server.stop()
