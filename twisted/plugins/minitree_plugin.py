from zope.interface import implements

from twisted.python import usage
from twisted.plugin import IPlugin
from twisted.application.service import IServiceMaker
from twisted.application import internet


class Options(usage.Options):
    optParameters = [
        ["socket", "s", "",
         "Path (or name) of UNIX/TCP socket to bind to. Overrides --port"],
        ["config", "c", "etc/default.ini",
         "Path (or name) of minitree configuration."],
        ["port", "p", 0, "The port number to listen on."],
    ]


class MiniTreeServiceMaker(object):
    implements(IServiceMaker, IPlugin)
    tapname = "minitree"
    description = "minitree service"
    options = Options

    def makeService(self, options):
        """
        Construct a TCPServer from a factory defined in myproject.
        """
        from minitree import configure
        c = configure(options["config"])
        from minitree.db.postgres import dbBackend
        dbBackend.connect(c.get("backend:main", "dsn"),
                          cp_min=int(c.get("backend:main", "cp_min")),
                          cp_max=int(c.get("backend:main", "cp_max")))

        from minitree.service import site_configure
        site_root = site_configure(c)
        from twisted.web import server
        site = server.Site(site_root)

        if "socket" in options and options["socket"]:
            return internet.UNIXServer(options["socket"], site)
        else:
            return internet.TCPServer(int(options["port"] or
                                          c.get("server:main", "port")), site)


# Now construct an object which *provides* the relevant interfaces
# The name of this variable is irrelevant, as long as there is *some*
# name bound to a provider of IPlugin and IServiceMaker.

serviceMaker = MiniTreeServiceMaker()
