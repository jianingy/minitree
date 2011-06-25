#!/usr/bin/env python
# -*- python -*-
# -*- coding: utf-8 -*-

from twisted.web.resource import Resource
from twisted.internet import defer
from twisted.internet.defer import DeferredList
from twisted.web.server import NOT_DONE_YET
from twisted.python.failure import Failure
from twisted.web import client
from cjson import encode as json_encode, decode as json_decode


class WebAdmin(Resource):

    isLeaf = True

    def __init__(self, server, tpl):
        if not server.startswith("http://"):
            server = "http://" + server
        self.server = server.rstrip("/")
        self.template = file(tpl).read()
        Resource.__init__(self)

    def has_children(self, data):

        def _check_return(ret, value):
            name = value.split(".")[-1]
            if isinstance(ret, Failure):
                return dict(data=name, state="", attr=dict(id=value))
            else:
                return dict(data=name, state="closed", attr=dict(id=value))

        def _check(value):
            url = "%s/node/%s?method=children" % \
                (self.server, value.encode("UTF-8").replace(".", "/"))
            d = client.getPage(url, followRedirect=True)
            d.addBoth(_check_return, value)
            return d

        def _json_encode(value):
            data = map(lambda y: y[1], filter(lambda z: z[0], value))
            return json_encode(data)

        dl = DeferredList(map(_check, data), consumeErrors=True)
        dl.addCallback(_json_encode)
        return dl

    def finish(self, value, request):
        if isinstance(value, Failure):
            err = value.value
            if isinstance(err, defer.CancelledError):
                return None
        print value
        request.setHeader("Content-Type", "text/html; charset=UTF-8")
        request.write(value)
        request.finish()

    def render_GET(self, request):
        try:
            node_path, html = request.path.split(".")
        except ValueError:
            node_path, html = request.path, None
        if "node_path" in request.args:
            node_path = "/" + request.args["node_path"][0].replace(".", "/")
        if html:
            return self.template % dict(
                node_path=node_path.lstrip("/").replace("/", "."),
                server=self.server)
        elif "chld" in request.args:
            d = client.getPage("%s/node%s?method=children" % \
                                   (self.server, node_path),
                               followRedirect=True)
            d.addCallback(lambda x: json_decode(x))
            d.addCallback(self.has_children)
        else:
            d = client.getPage("%s/node%s" % (self.server, node_path),
                               followRedirect=True)
        request.notifyFinish().addErrback(lambda e, d: d.cancel(), d)
        d.addBoth(self.finish, request)
        return NOT_DONE_YET

service = WebAdmin
