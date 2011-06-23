from twisted.internet import epollreactor
epollreactor.install()

from minitree.db.postgres import dbBackend

import logging
import sys


def main(c):
    from twisted.python import log
    from twisted.web import server

    observer = log.PythonLoggingObserver()
    observer.start()
    logging.basicConfig(file=sys.stderr, level=logging.DEBUG)

    dbBackend.connect(c.get("backend:main", "dsn"), cp_min=8, cp_max=16)

    from minitree.service import site_configure
    site_root = site_configure(c)
    site = server.Site(site_root)

    from twisted.internet import reactor
    reactor.listenTCP(int(c.get("server:main", "port")), site)
    reactor.run()

if __name__ == '__main__':
    from minitree import configure
    main(configure(sys.argv[1]))
