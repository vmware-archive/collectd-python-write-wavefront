#! /usr/bin/env python

# TODOs:
# - Clean up logging (Propose prefixing all messages with something identifying the plugin)
# - Mock collectd functionality for testing / development
# - Evaluate whether or not we care that sendall won't let you know what you sent, which
#   will include a lot of buffer management and metric boundaries.
# - Self monitoring - publish i.e. reconnect tries, seconds connected vs. seconds not connected, queue size

# Example config:
#
# LoadPlugin python
# <Plugin python>
#   ModulePath "/etc/collectd/py"
#   Import wavefront_push
#
#   <Module wavefront_push>
#     server 10.255.254.255
#     port 2878
#     prefix collectd
#     tag dc dallas
#     tag use prod
#   </Module>
# </Plugin>

import Queue, socket, threading, time, re
from collections import namedtuple

try:
    import collectd
except ImportError, e:
    # This lets me at least parse the plugin before I feed it to collectd
    pass

# Hack!
# collectd < 5.5 does not have get_dataset, this should "mock" get_dataset and have
# approximately the same behavior. ( collectd.get_dataset(typename) -> (name, type, min val, max val)
class CollectdDS:
    """Emulate collectd.get_dataset as it's a relatively "new" addition to collectd-python
    CollectDS(filename) - load tyeps.db given by filename
    CollectDS.__call__(typename) -> type description (name, type, min max)

    drop-in replacement for collectd.get_dataset
    """
    ds_value_type = namedtuple("ds_value_type", ["name", "type", "min", "max"])
    # Collectd types
    DS_TYPE_COUNTER=0
    DS_TYPE_GAUGE=1
    DS_TYPE_DERIVE=2
    DS_TYPE_ABSOLUTE=3
    TYPES={ 'DERIVE': DS_TYPE_DERIVE, 'GAUGE': DS_TYPE_GAUGE, 'ABSOLUTE': DS_TYPE_ABSOLUTE, 'COUNTER': DS_TYPE_COUNTER }
    def __init__(self, ds_file=None):
        """__init__(ds_file)
        - ds_file(None) -> Empty types.db
        - ds_file(filename) -> attempt to laod filename as collectd types.db
        """
        self.__typesdb = {}
        if ds_file is not None:
            self.load_file(ds_file)
    def load_file(self, filename):
        with open(filename, "r") as file:
            for line in file:
                line = line.rstrip('\r\n')
                line = line[:line.find('#')]
                if len(line) == 0:
                    continue
                try:
                    values = []
                    (ds_name, values_string) = re.split('\s+', line, 1)
                    for (name, type, min, max) in [ e.split(':') for e in re.split(',\s*', values_string) ]:
                        if min == 'U' or min == '':
                            min = '-inf'
                        if max == 'U' or max == '':
                            max = 'inf'
                        values.append( self.ds_value_type(name, self.TYPES[type], float(min), float(max)) )
                        self.__typesdb[ds_name] = values
                except Exception, err:
                    raise
    def __call__(self, name):
        return self.__typesdb[name]
    def clear(self):
        self.__typesdb = {}

# FIXME: Global variable, anecodotally this needs to be a reference, "seems to work" sharing between threads
#        We can potentially avoid the ambiguity around the threading by passing copies in the callback closures.
CONFIG={'prefix': 'collectd', 'qsize': 10240}

def retry_connect(host, port, socket_timeout=15):
    """Retries connection indefinitely
    On connect: - Close receiving direction, we don't expect anything nor do we ever read from it.
                - Set the timeout on the socket (default %ds), the rationale for this is that if for
                  some reason send blocks, we don't want to block infinitely.
    """ % ( socket_timeout )
    while True:
        try:
            s = socket.create_connection( (socket.gethostbyname(host), port) )
            s.shutdown(socket.SHUT_RD) # We're not receiving anything, close RX stream
            s.settimeout(socket_timeout) # We do not want to block indefinitely
            return s
        except (IOError, socket.error, socket.timeout, socket.gaierror), e:
            collectd.error("Failed to connect: " + str(e))
            time.sleep(1)

def send_wavefront(host, port, item_queue):
    """send_wavefront(host, port, item_queue) - collectd-wavefront plugin eventloop
    connects to host:port, continually checks item_queue for messages and sends them.
    Runs as a separate thread. item_queue should be a thread-safe, i.e. from python's
    Queue module.
    """
    collectd.info("starting wavefront sender")
    connection = None
    message = None
    
    while True:
        if connection is None:
            connection = retry_connect(host, port)

        if message is None:
            try:
                message = item_queue.get(False)
            except Queue.Empty:
                time.sleep(1)
                continue
        
        try:
            #strip white spaces and quotes for bad plugins
            epoch=message.split(' ')[2]
            if not re.search("^1[2-9]\d{8}$", epoch):
                regex=  '(\S+.*)(\s+(\d+\.\d+|\d+)\s+1\d{9}.*)'
                m = re.search(regex, message)
                if m:
                    print "match"
                    string= m.groups()[0]
                    string = re.sub(r'[ |"|$|#|\']', '_', string)
                    remainder= m.groups()[1]
                    message= string+ remainder+"\n"
            
            # Lazy "send everything", loosing messages is very much a possibility
            # we should know that we failed to send "something".
            connection.sendall(message)
            message = None
        except (socket.timeout, IOError, socket.error), e:
            # Lazy error handling; "something" went wrong, let's
            # give up and toss the message
            collectd.error("Failed to send items:" + str(e))
            try:
                connection.shutdown(socket.SHUT_WR)
                connection.close()
            except Exception, e:
                collectd.info("wavefront-connection close failed:" + str(e))
                pass
            connection = None

# Currently, the following configuration is accepted
#
# Server -> hostname/ip to connect to
# Port -> port to connect to
# Prefix -> what to prefix the netric name with
# qsize -> internal queue size
# types_db -> Where to find a collectd dypes.db file (if collectd < 5.5)
# Any number of
#   Tag <tag name> <value>
#
def configure_callback(conf):
    """Collectd configuration callback.
    - Parameter conf: Collectd configuration tree"""
    tags={}
    CONFIG['prefix'] = 'collectd'
    override_types_db = []
    for node in conf.children:
        config_key = node.key.lower()
        if config_key == 'tag':
            try:
                (tag, value) = node.values
            except ValueError, e:
                collectd.error("Config tag %s malformed" % (node.values))
                continue
            tags[tag] = value
        elif config_key == 'server':
            CONFIG['server'] = node.values[0]
        elif config_key == 'port':
            try:
                CONFIG['port'] = int(node.values[0])
            except ValueError:
                collectd.error("Port non-integer")
        elif config_key == 'prefix':
            CONFIG['prefix'] = node.values[0].lower()
        elif config_key == 'qsize':
            CONFIG['qsize'] = int(node.values[0])
        elif config_key == 'types_db':
            override_types_db.append(node.values[0])
        else:
            collectd.error("config: %s unknown config key" % (config_key))
    CONFIG['tags_append'] = " " +  " ".join(["%s=%s" % (t,v) for (t,v) in tags.iteritems() ])

    if not 'get_dataset' in dir(collectd):
        collectd.info("Trying to shoehorn in collectd types.db")
        candidate_file_list = override_types_db + [ '/usr/share/collectd/types.db' ] + [ None ]
        for candidate_file in candidate_file_list:
            try:
                setattr(collectd, 'get_dataset', CollectdDS(candidate_file))
                collectd.info("Loaded %s as types.db" % (candidate_file))
                break
            except Exception, e:
                collectd.warning("Tried and failed to load %s: %s" % ( candidate_file, str(e) ))
        
    collectd.info("config(%s,tags='%s'" % ( ",".join(["%s=%s" % (k,v) for (k,v) in CONFIG.iteritems()]), CONFIG['tags_append']))

def init_callback():
    """Collectd initialization callback.
    Responsible for starting the sending thread"""
    try:
        queue_size = CONFIG['qsize']
    except KeyError:
        queue_size = 1024
    CONFIG['queue']=Queue.Queue(queue_size)
    if CONFIG['server'] and CONFIG['port']:
        sender = threading.Thread(target=send_wavefront, args=(CONFIG['server'], CONFIG['port'], CONFIG['queue']))
        sender.setDaemon(True)
        sender.start()
        collectd.info("init: starting wavefront sender")
    else:
        collectd.error("wavefront-forwarder has no destination to send data")

def write_callback(value):
    """Collectd write callback, responsible for formatting and writing messages onto
    the send queue"""

    # collectd will feed us metrics as value objects
    #  value.plugin
    #       .plugin_instance
    #       .type
    #       .type_instance
    #       .time
    #       .host
    #       .values (which is a list of metric values)
    #
    # the value.values will have "extra names" in types.db
    #
    # Example:
    # The interfaces plugin reports on the type if_octets (among others)
    # value.plugin          = interface
    #      .plugin_instance = em1
    #      .type            = if_octets
    #      .type_instance   =
    #      .time            = some instant
    #      .host            = some host
    #      .values          = [ 10, 20 ]
    #
    # types.db will have an entry for if_octets which defines
    #   tx:DS_TYPE_COUNTER:0:U
    #   rx:DS_TYPE_COUNTER:0:U
    #
    # Which lets us map concrete names to each value
    # i.e.
    #  collectd.interface-em1.if_octets.tx = 10
    #  collectd.interface-em1.if_octets.rx = 20
    #
    
    metric_name = "%s%s.%s%s" % ( value.plugin,
                                  '.' + value.plugin_instance if len(value.plugin_instance) > 0 else '',
                                  value.type,
                                  '.' + value.type_instance if len(value.type_instance) > 0 else '' )

    try:
        prefix = CONFIG['prefix']
        if not prefix.endswith('.'):
            prefix = prefix + '.'
    except KeyError, e:
        prefix = ''

    try:
        tags_append = ' ' + CONFIG['tags_append']
    except KeyError, e:
        tags_append = ''

    append_names = [ '.' + append_name if append_name != 'value' else ''
                     for (append_name, _, _, _)
                     in collectd.get_dataset(value.type) ]

    if len(append_names) != len(value.values):
        collectd.error("len(ds_names) != len(value.values)")
        return
    
    msg = "".join([ "%s %f %d host=%s%s\n" % (prefix + metric_name + postfix, metric_value, value.time, value.host, tags_append)
                      for (postfix, metric_value)
                      in zip(append_names, value.values) ])
    try:
        CONFIG['queue'].put(msg, block=False)
    except Exception, e:
        collectd.error("Failed to message:" + str(e))

if 'collectd' in globals().keys():
    collectd.register_config(configure_callback)
    collectd.register_write(write_callback)
    collectd.register_init(init_callback)
else:
    print "Not running under collectd"
    import sys
    sys.exit(1)
