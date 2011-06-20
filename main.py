from twisted.internet import epollreactor
epollreactor.install()

from minitree.db.postgres import dbBackend

import logging
import sys


def configure(ini_file):
    from ConfigParser import SafeConfigParser as ConfigParser
    import codecs
    from StringIO import StringIO

    default = """
[server:main]
port = 8000

[backend:main]
dsn = host=%(server)s port=%(port)s dbname=%(database)s \
user=%(user)s password=%(password)s
user =
password =
"""
    p = ConfigParser()
    p.readfp(StringIO(default))
    with codecs.open(ini_file, "r", encoding="utf-8") as f:
        p.readfp(f)
    return p


def main(c):
    from twisted.python import log
    from twisted.web import server

    observer = log.PythonLoggingObserver()
    observer.start()
    logging.basicConfig(file=sys.stderr, level=logging.DEBUG)

    dbBackend.connect(c.get("backend:main", "dsn"))

    from minitree.service import root as site_root
    site = server.Site(site_root)

    from twisted.internet import reactor
    reactor.listenTCP(int(c.get("server:main", "port")), site)
    reactor.run()

if __name__ == '__main__':
    main(configure(sys.argv[1]))
