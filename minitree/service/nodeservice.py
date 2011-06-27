from twisted.internet import defer
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
from twisted.python import log
from twisted.python.failure import Failure
from hashlib import md5 as md5sum
from collections import namedtuple
from minitree.db.postgres import dbBackend
from ujson import encode as json_encode, decode as json_decode
import time
import minitree.db
import logging

INode = namedtuple("Inode", ["node_path", "format", "data", "user", "passwd"])


class UnsupportedGetNodeMethod(Exception):
    pass


class ServiceAuthenticationError(Exception):
    pass


class InvalidInputData(Exception):

    def __init__(self, message=""):
        self.value = "the input data must be a JSON dict"
        if message:
            self.value = message

    def __repr__(self):
        return self.value

    def __str__(self):
        return str(self.value)

    def __unicode__(self):
        return unicode(self.value)


class NodeService(Resource):

    isLeaf = True
    serviceName = "node"
    defaultFormat = "json"
    allowedFormat = ("json", "xml")

    # privilege bits
    X_GET    = 8
    X_PUT    = 4
    X_POST   = 2
    X_DELETE = 1

    @staticmethod
    def _buildQuery(request):
        decoded = request.path.decode("UTF-8")
        uri = decoded[len(NodeService.serviceName) + 1:].rstrip("/")

        if uri.find(".") > -1:
            node_path, format = uri.split(".", 1)
            format = format.lower()
            if format not in NodeService.allowedFormat:
                format = NodeService.defaultFormat
        else:
            node_path = uri
            format = NodeService.defaultFormat

        return node_path, format

    def __init__(self, c, *args, **kwargs):
        self.config = c
        self.admin_user = self.config.get("server:main", "admin_user")
        self.admin_passwd = self.config.get("server:main", "admin_pass")
        Resource.__init__(self, *args, **kwargs)

    def auth(self, inode, bits):

        def _auth(user, inode):
            if user["password"] != inode.passwd:
                raise ServiceAuthenticationError()
            ns = user["ns"].split(",")
            rns = inode.node_path.split("/")
            if len(rns) > 1:
                rns = '.'.join(rns[0:2])
            else:
                rns = rns[0]
            if rns.lstrip(".") not in ns:
                raise ServiceAuthenticationError("this ns is not allowed")
            return inode

        def _fail(e):
            log.msg("Authentication failed: %s" % str(e.value),
                    level=logging.DEBUG)
            raise ServiceAuthenticationError()

        # if no admin_user, disable authentication
        if self.admin_user == '':
            return inode

        # admin user has all privileges
        if (inode.user == self.admin_user and
            inode.passwd == self.admin_passwd):
            return inode
        d = dbBackend.selectNode("_meta.users." + inode.user)
        d.addCallbacks(_auth, _fail, callbackArgs=(inode,))
        return d

    def createNode(self, inode):
        # content must be first argument

        def _success(rowcount):
            if not rowcount:
                rowcount = 0
            return dict(success="%d node(s) has been created" % rowcount,
                        affected=rowcount)

        if not isinstance(inode.data, dict):
            raise InvalidInputData()
        d = dbBackend.createNode(inode.node_path, inode.data)
        d.addCallback(_success)
        return d

    def deleteNode(self, inode, cascade):
        # content must be first argument
        def _success(rowcount):
            return dict(success="%d node(s) has been modified" % rowcount,
                        affected=rowcount)

        d = dbBackend.deleteNode(inode.node_path, inode.data, cascade)
        d.addCallback(_success)
        return d

    def getNode(self, inode, method):
        node_path = inode.node_path
        if method == 'override':
            d = dbBackend.getOverridedNode(node_path)
        elif method == 'combo':
            d = dbBackend.getComboNode(node_path)
        elif method == 'ancestors':
            d = dbBackend.getAncestors(node_path)
        elif method == 'children':
            d = dbBackend.getChildren(node_path)
        elif method == 'descendants':
            d = dbBackend.getDescendants(node_path)
        else:
            raise UnsupportedGetNodeMethod()
        return d

    def searchNode(self, inode, q):
        d = dbBackend.searchNode(inode.node_path, q)
        return d

    def selectNode(self, inode):
        d = dbBackend.selectNode(inode.node_path)
        return d

    def finish(self, value, request):
        log.msg("finish value = %s" % str(value), level=logging.DEBUG)
        request.setHeader('Content-Type', 'application/json;charset=UTF-8')
        if isinstance(value, Failure):
            err = value.value
            request.setResponseCode(500)
            error = dict(error="unknown error occurred")
            if isinstance(err, defer.CancelledError):
                log.msg("Request cancelled.", level=logging.DEBUG)
                return None
            elif isinstance(err, minitree.db.NodeNotFound):
                request.setResponseCode(404)
                error = dict(error="node not found", message=err.message,
                             instance="db.NodeNotFound")
            elif isinstance(err, minitree.db.ParentNotFound):
                request.setResponseCode(400)
                error = dict(error="parent node not found",
                             message=err.message,
                             instance="db.ParentNotFound")
            elif isinstance(err, minitree.db.PathDuplicatedError):
                request.setResponseCode(400)
                error = dict(error=str(err),
                             instance="db.PathDuplicatedError")
            elif isinstance(err, minitree.db.PathError):
                request.setResponseCode(400)
                error = dict(error=str(err),
                             instance="db.PathError")
            elif isinstance(err, minitree.db.DataTypeError):
                request.setResponseCode(400)
                error = dict(error=str(err),
                             instance="db.DataTypeError")
            elif isinstance(err, InvalidInputData):
                request.setResponseCode(400)
                error = dict(error=str(err),
                             instance="service.NodeSerivce.InvalidInputData")
            elif isinstance(err, ValueError):
                request.setResponseCode(400)
                error = dict(error=str(err), instance="ValueError")
            elif isinstance(err, ServiceAuthenticationError):
                request.setResponseCode(403)
                error = dict(error="forbidden",
                             message=str(err),
                             instance="service.NodeService."
                             "ServiceAuthenticationError")
            elif isinstance(err, UnicodeDecodeError):
                request.setResponseCode(400)
                error = dict(error=str(err),
                             instance="UnicodeDecodeError")
            request.write(json_encode(error) + "\n")
        else:
            request.setResponseCode(200)
            request.write(json_encode(value) + "\n")

        log.msg("respone time: %.3fms" % (
                (time.time() - self.startTime) * 1000))
        request.finish()

    def updateNode(self, inode):
        # content must be first argument
        def _success(rowcount):
            return dict(success="%d node(s) has been modified" % rowcount,
                        affected=rowcount)

        if not isinstance(inode.data, dict):
            raise InvalidInputData()

        d = dbBackend.updateNode(inode.node_path, inode.data)
        d.addCallback(_success)
        return d

    def render(self, *args, **kwargs):
        self.startTime = time.time()
        return Resource.render(self, *args, **kwargs)

    def cancel(self, err, call):
        log.msg("Request cancelling.", level=logging.DEBUG)
        call.cancel()

    def _prepare(self, request, content=True):
        node_path, format = NodeService._buildQuery(request)

        if content:
            request.content.seek(0, 0)
            content = request.content.read().strip() or "{}"
            try:
                data = json_decode(content)
            except:
                raise InvalidInputData("Invalid JSON")
        else:
            data = dict()
        return INode(node_path=node_path,
                     format=format,
                     data=data,
                     user=request.getUser(),
                     passwd=md5sum(request.getPassword()).hexdigest())

    def prepare(self, request, content=True):
        try:
            return defer.succeed(self._prepare(request, content))
        except Exception as e:
            return defer.fail(Failure(e))

    def render_DELETE(self, request):
        cascade = False
        if "cascade" in request.args:
            cascade = request.args["cascade"][0]

        d = self.prepare(request)
        request.notifyFinish().addErrback(self.cancel, d)
        d.addCallback(self.auth, self.X_DELETE)
        d.addCallback(self.deleteNode, cascade)
        d.addBoth(self.finish, request)
        return NOT_DONE_YET

    def render_GET(self, request):
        d = self.prepare(request, False)
        request.notifyFinish().addErrback(self.cancel, d)
        d.addCallback(self.auth, self.X_GET)
        if "q" in request.args:
            d.addCallback(self.searchNode, request.args["q"][0])
        elif "method" in request.args:
            d.addCallback(self.getNode, request.args["method"][0].lower())
        else:
            d.addCallback(self.selectNode)
        d.addBoth(self.finish, request)
        return NOT_DONE_YET

    def render_POST(self, request):
        d = self.prepare(request)
        request.notifyFinish().addErrback(self.cancel, d)
        d.addCallback(self.auth, self.X_POST)
        d.addCallback(self.updateNode)
        d.addBoth(self.finish, request)
        return NOT_DONE_YET

    def render_PUT(self, request):
        d = self.prepare(request)
        request.notifyFinish().addErrback(self.cancel, d)
        d.addCallback(self.auth, self.X_PUT)
        d.addCallback(self.createNode)
        d.addBoth(self.finish, request)
        return NOT_DONE_YET
