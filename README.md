# collectd Point Tagging Plugin
Wavefront can easily be integrated with collectd via the write_graphite plugin. While this provides a straight forward integration path, it does not provide any options for adding point tags to metrics. This plugin provides an alternative which supports point tagging.

## Prerequisites
1. collectd 5 or higher (tested with collectd 5.4)
2. collectd python plugin
3. The wavefront proxy installed on a host and port accessible to the server where collectd is installed.
4. The wavefront proxy should *NOT* be configured to use Graphite format. This plugin uses the standard Wavefront format.

## Installation Instructions
After collectd and the collectd python plugin are installed

1. create a directory for the plugin i.e. `/etc/collectd/py`
2. In the new directory, place `wavefront_push.py`
3. In your collectd config file (usually `/etc/collectd.conf`) add the sample config below to your file:
4. Save the file and restart collectd

### Sample Config
```xml
LoadPlugin python
<Plugin python>
   ModulePath "/etc/collectd/py"
   Import wavefront_push

   <Module wavefront_push>
     server 10.255.254.255
     port 2878
     prefix collectd
     tag dc dallas
     tag use prod
   </Module>
 </Plugin>
```

### Config Parameters
- `server` - The server address of the wavefront proxy
- `port` - The port of the wavefront proxy
- `prefix` - The prefix to be applied to points from collectd
- `tag <name> <value>` - A name and value for a tag


