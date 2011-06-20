from twisted.internet.threads import deferToThread
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
from twisted.python import log
from twisted.python.failure import Failure
from minitree.db.postgres import dbBackend
from cjson import encode as json_encode, decode as json_decode
import time
import minitree.db
import logging


class UnsupportedGetNodeMethod(Exception):
    pass


class NodeService(Resource):

    isLeaf = True
    serviceName = "node"
    defaultFormat = "json"
    allowedFormat = ("json", "xml")

    @staticmethod
    def _buildQuery(request):
        uri = request.path[len(NodeService.serviceName) + 1:].rstrip("/")

        if uri.find(".") > -1:
            node_path, format = uri.split(".", 1)
            format = format.lower()
            if format not in NodeService.allowedFormat:
                format = NodeService.defaultFormat
        else:
            node_path = uri
            format = NodeService.defaultFormat

        return node_path, format

    def cancel(self, err, call):
        log.msg("Request cancelled.")
        call.cancel()

    def createNode(self, content, node_path):
        # content must be first argument
        def _success(rowcount):
            return dict(success="%d node(s) has been created" % rowcount,
                        affected=rowcount)

        d = dbBackend.createNode(node_path, content)
        d.addCallback(_success)
        return d

    def deleteNode(self, content, node_path, cascade):
        # content must be first argument
        def _success(rowcount):
            return dict(success="%d node(s) has been modified" % rowcount,
                        affected=rowcount)

        d = dbBackend.deleteNode(node_path, content, cascade)
        d.addCallback(_success)
        return d

    def getNode(self, node_path, method):
        if method == 'override':
            d = dbBackend.getOverridedNode(node_path)
        elif method == 'combo':
            d = dbBackend.getComboNode(node_path)
        else:
            raise UnsupportedGetNodeMethod()
        return d

    def selectNode(self, node_path):
        d = dbBackend.selectNode(node_path)
        return d

    def finish(self, value, request, format):
        log.msg("finish value = %s" % str(value), level=logging.DEBUG)
        request.setHeader('Content-Type', 'application/json;charset=UTF-8')
        if isinstance(value, Failure):
            err = value.value
            request.setResponseCode(500)
            error = dict(error="unknown error occurred")
            if isinstance(err, minitree.db.NodeNotFound):
                request.setResponseCode(404)
                error = dict(error="node not found")
            elif isinstance(err, minitree.db.PathDuplicatedError):
                request.setResponseCode(400)
                error = dict(error=str(err))
            elif isinstance(err, ValueError):
                request.setResponseCode(400)
                error = dict(error=str(err))
            request.write(json_encode(error) + "\n")
        else:
            request.setResponseCode(200)
            request.write(json_encode(value) + "\n")

        log.msg("respone time: %.3fms" % (
                (time.time() - self.startTime) * 1000))
        request.finish()

    def updateNode(self, content, node_path):
        # content must be first argument
        def _success(rowcount):
            return dict(success="%d node(s) has been modified" % rowcount,
                        affected=rowcount)

        d = dbBackend.updateNode(node_path, content)
        d.addCallback(_success)
        return d

    def render(self, *args, **kwargs):
        self.startTime = time.time()
        return Resource.render(self, *args, **kwargs)

    def render_DELETE(self, request):
        node_path, format = NodeService._buildQuery(request)
        cascade = False
        if "cascade" in request.args:
            cascade = request.args["cascade"][0]

        request.content.seek(0, 0)
        content = request.content.read()
        d = deferToThread(lambda x: json_decode(x or '""'), content.strip())
        d.addCallback(self.deleteNode, node_path, cascade)
        d.addBoth(self.finish, request, format)
        request.notifyFinish().addErrback(lambda e, d: d.cancel(), d)
        return NOT_DONE_YET

    def render_GET(self, request):
        node_path, format = NodeService._buildQuery(request)
        if "method" in request.args:
            method = request.args["method"][0].lower()
            d = self.getNode(node_path, method)
        else:
            d = self.selectNode(node_path)
        d.addBoth(self.finish, request, format)
        request.notifyFinish().addErrback(lambda e, d: d.cancel(), d)
        return NOT_DONE_YET

    def render_POST(self, request):
        node_path, format = NodeService._buildQuery(request)
        request.content.seek(0, 0)
        content = request.content.read()
        d = deferToThread(lambda x: json_decode(x), content)
        d.addCallback(self.updateNode, node_path)
        d.addBoth(self.finish, request, format)
        request.notifyFinish().addErrback(lambda e, d: d.cancel(), d)
        return NOT_DONE_YET

    def render_PUT(self, request):
        node_path, format = NodeService._buildQuery(request)
        request.content.seek(0, 0)
        content = request.content.read()
        d = deferToThread(lambda x: json_decode(x), content)
        d.addCallback(self.createNode, node_path)
        d.addBoth(self.finish, request, format)
        request.notifyFinish().addErrback(lambda e, d: d.cancel(), d)
        return NOT_DONE_YET
