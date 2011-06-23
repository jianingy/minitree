from twisted.web import resource
from minitree.service.nodeservice import NodeService

__all__ = ['site_configure']


def site_configure(c):
    root = resource.Resource()
    root.putChild(NodeService.serviceName, NodeService(c))
    return root
