from zope.interface import implements

from twisted.python import usage
from twisted.plugin import IPlugin
from twisted.application.service import IServiceMaker
from twisted.application import internet


class Options(usage.Options):
    optParameters = [
        ["port", "p", 0, "The port number to listen on."],
        ["server", "s", "localhost", "The miniserver to use"],
        ["template", "t", "webadmin.html", "The HTML template to use"],
    ]


class MiniTreeWebAdminServiceMaker(object):
    implements(IServiceMaker, IPlugin)
    tapname = "minitree-webadmin"
    description = "minitree webadmin"
    options = Options

    def makeService(self, options):

        from webadmin import service
        from twisted.web import server
        site = server.Site(service(options["server"], options["template"]))

        return internet.TCPServer(int(options["port"]), site)


serviceMaker = MiniTreeWebAdminServiceMaker()
