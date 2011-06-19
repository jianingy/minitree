from twisted.internet import epollreactor
epollreactor.install()

from minitree.db.postgres import dbBackend

import logging
import sys


def main():
    from twisted.python import log
    from twisted.web import server

    observer = log.PythonLoggingObserver()
    observer.start()
    logging.basicConfig(file=sys.stderr, level=logging.DEBUG)

    dbBackend.connect('host=localhost dbname=jianingy user=jianingy')

    from minitree.service import root as site_root
    site = server.Site(site_root)

    from twisted.internet import reactor
    reactor.listenTCP(8000, site)
    reactor.run()

if __name__ == '__main__':
    main()
