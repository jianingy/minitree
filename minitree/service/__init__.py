from twisted.web import resource
from minitree.service.nodeservice import NodeService

__all__ = ['root']

root = resource.Resource()
root.putChild(NodeService.serviceName, NodeService())
