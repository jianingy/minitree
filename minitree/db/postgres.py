# -*- coding: utf-8 -*-
from twisted.internet import defer
from twisted.python.failure import Failure
from minitree.db import PathError, NodeNotFound, NodeCreationError
from minitree.db import DataTypeError
from minitree.db import PathDuplicatedError
from collections import defaultdict
from txpostgres import txpostgres
from os.path import splitext
import psycopg2
import re

__all__ = ["dbBackend"]


class Postgres(object):

    selectOneSQL = "SELECT 1 FROM %s WHERE node_path = %%(node_path)s LIMIT 1"
    selectSQL = "SELECT key, value FROM each( \
(SELECT node_value FROM %s WHERE node_path = %%(node_path)s LIMIT 1))"
    selectOverrideSQL = "SELECT key, value FROM each( \
(SELECT hstore_override(node_value order by node_path asc) AS node_value \
FROM %s WHERE node_path @> %%(node_path)s))"
    selectComboSQL = "SELECT (each(node_value)).key, (each(node_value)).value \
FROM %s WHERE node_path @> %%(node_path)s"
    selectReverseComboSQL = "SELECT (each(node_value)).key, (each(node_value)).value \
FROM %s WHERE node_path <@ %%(node_path)s"
    selectAncestorSQL = "SELECT node_path FROM %s \
WHERE node_path @> %%(node_path)s AND node_path != %%(node_path)s"
    selectAllSQL = "SELECT node_path FROM %s WHERE node_path ~ %%(q)s"
    selectDescentantsSQL = "SELECT node_path, node_value FROM %s \
WHERE node_path <@ %%(node_path)s AND node_path != %%(node_path)s"
    selectTablesSQL = "SELECT (schemaname || '.' || tablename) AS node_path \
FROM pg_tables WHERE schemaname=%(name)s;"
    searchNodeSQL = "SELECT node_path FROM %s WHERE node_path ~ %%(q)s"
    updateSQL = "UPDATE %s SET node_value = node_value || %%s, \
last_modification = now() \
WHERE node_path = %%s"
    deleteSQL = "UPDATE %s SET node_value = delete(node_value, %%s), \
last_modification = now() \
WHERE node_path = %%s"
    deleteNodeSQL = "DELETE FROM %s WHERE node_path = %%s"
    deleteNodeCascadedSQL = "DELETE FROM %s WHERE node_path <@ %%s"
    dropTableSQL = "DROP TABLE %s"
    createSQL = "INSERT INTO %s(node_path, node_value) VALUES(%%s, %%s)"
    createTableSQL = "CREATE TABLE %s(id SERIAL PRIMARY KEY, \
node_path ltree unique, node_value hstore, \
last_modification timestamp default now())"
    createSchemaSQL = "CREATE SCHEMA %s"
    initTableSQL = "INSERT INTO %s(node_path) VALUES('')"

    regexNoTable = re.compile(r"relation \"[^\"]+\" does not exist")
    regexNoSchema = re.compile(r"schema \"[^\"]+\" does not exist")

    def __init__(self):
        self.pool = None

    @staticmethod
    def _buildTableName(schema, table):
        def _quote(s):
            return s.replace("\"", "\\\"")

        return "\"%s\".\"%s\"" % (_quote(schema), _quote(table))

    @staticmethod
    def _serialize_hstore(val):
        """
        Serialize a dictionary into an hstore literal. Keys and values
        must both be strings.
        """
        def esc(s, position):
            try:
                if isinstance(s, dict):
                    raise DataTypeError("dict is not allowed")
                elif isinstance(s, list):
                    raise DataTypeError("list is not allowed")
                return unicode(s).replace('"', r'\"').encode('UTF-8')
            except AttributeError:
                raise ValueError("%r in %s position is not a string." %
                                 (s, position))
        return ', '.join('"%s"=>"%s"' % (esc(k, 'key'), esc(v, 'value'))
                         for k, v in val.iteritems())

    @staticmethod
    def _splitPath(path, encode=True):
        parts = path.replace("/", ".").lstrip(".").split(".", 2)

        if len(parts) == 3:
            schema, table, node_path = parts
        elif len(parts) == 2:
            schema, table = parts
            node_path = ""
        else:
            raise PathError("Not enough level")

        if encode:
            schema = schema.encode("UTF-8")
            table = table.encode("UTF-8")
            node_path = node_path.encode("UTF-8")

        return (schema, table, node_path)

    def connect(self, *args, **kwargs):
        assert(self.pool == None)
        self.pool = txpostgres.ConnectionPool(None, *args, **kwargs)
        return self.pool.start()

    def _selectPath(self, c, path, sql, q=None):

        def _exists(c):
            if not c.fetchone():
                raise  NodeNotFound()

        schema, table, node_path = self._splitPath(path)
        tablename = self._buildTableName(schema, table)
        try:
            d = c.execute(self.selectOneSQL % tablename,
                          dict(node_path=node_path))
            d.addCallback(_exists)
            if q:
                d.addCallback(lambda _, c: c.execute(sql % tablename,
                                                     dict(q=q)), c)
            else:
                d.addCallback(lambda _, c: c.execute(
                        sql % tablename, dict(node_path=node_path)), c)
            d.addCallback(lambda c: map(lambda x: x[0].decode("UTF-8"),
                                        c.fetchall()))
            return d
        except psycopg2.ProgrammingError as e:
            err = str(e)
            if self.regexNoSchema.match(err):
                raise NodeNotFound("schema not found")
            elif self.regexNoTable.match(err):
                raise NodeNotFound("collection not found")
            else:
                raise

    def _patch_path_heading(self, value, path):
        schema, table, node_path = self._splitPath(path, False)
        prefix = "%s.%s" % (schema, table)
        return map(lambda x: ("%s.%s" % (prefix, x)).rstrip("."),
                   value)

    def getAncestors(self, path):
        d = self.pool.runInteraction(self._selectPath, path,
                                     self.selectAncestorSQL)
        d.addCallback(self._patch_path_heading, path)

        return d

    def getChildren(self, path):
        p = path.lstrip("/").split("/")
        n = len(p)

        if n == 1:
            d = self.pool.runInteraction(self._selectDBObject, p[0],
                                         self.selectTablesSQL)
        elif n == 2:
            d = self.pool.runInteraction(self._selectPath,
                                         ".".join(p),
                                         self.selectAllSQL, q="*{1}")
            d.addCallback(self._patch_path_heading, path)
        else:
            d = self.pool.runInteraction(self._selectPath,
                                         ".".join(p),
                                         self.searchNodeSQL,
                                         q="%s.*{1}" % ".".join(p[2:]))
            d.addCallback(self._patch_path_heading, path)
        return d

    def getDescendants(self, path):
        d = self.pool.runInteraction(self._selectPath, path,
                                     self.selectDescentantsSQL)
        d.addCallback(self._patch_path_heading, path)

        return d

    def getOverridedNode(self, path):
        def _decode(x):
            return (x[0].decode("UTF-8"), x[1].decode("UTF-8"))

        d = self.pool.runInteraction(self._selectNode, path,
                                     self.selectOverrideSQL)
        d.addCallback(lambda x: dict(map(_decode, x)))

        return d

    def getComboNode(self, path):

        def _combo(result):
            combo = defaultdict(list)
            map(lambda x: combo[x[0]].append(x[1].decode("UTF-8")), result)
            return combo

        d = self.pool.runInteraction(self._selectNode, path,
                                     self.selectComboSQL)
        return d.addCallback(_combo)

    def getReverseComboNode(self, path):

        def _rcombo(result):
            rcombo = defaultdict(list)
            map(lambda x: rcombo[x[0]].append(x[1].decode("UTF-8")), result)
            return rcombo

        d = self.pool.runInteraction(self._selectNode, path,
                                     self.selectReverseComboSQL)
        return d.addCallback(_rcombo)

    def _selectDBObject(self, c, name, sql):

        def _finish(result, c):
            if isinstance(result, list) and result:
                return map(lambda x: x[0].decode("UTF-8"), result)
            else:
                raise NodeNotFound()

        d = c.execute(sql, dict(name=name))
        d.addCallback(lambda c: c.fetchall())
        d.addBoth(_finish, c)
        return d

    def _selectNodeFinish(self, c):
        if isinstance(c, Failure):
            exc = c.value
            s_exc = str(exc)
            if isinstance(exc, psycopg2.ProgrammingError):
                if self.regexNoSchema.match(s_exc):
                    raise NodeNotFound("schema not found")
                elif self.regexNoTable.match(s_exc):
                    raise NodeNotFound("collection not found")

            raise c.value

        return c.fetchall()

    def _selectNode(self, c, path, sql):

        def _exists(c):
            if not c.fetchone():
                raise  NodeNotFound()

        schema, table, node_path = self._splitPath(path)
        tablename = self._buildTableName(schema, table)

        d = c.execute(self.selectOneSQL % tablename, dict(node_path=node_path))
        d.addCallback(_exists)
        d.addCallback(lambda _, c: c.execute(sql % tablename,
                                             dict(node_path=node_path)), c)
        d.addBoth(self._selectNodeFinish)

        return d

    def selectNode(self, path):
        def _decode(x):
            return (x[0].decode("UTF-8"), x[1].decode("UTF-8"))

        d = self.pool.runInteraction(self._selectNode, path, self.selectSQL)
        d.addCallback(lambda x: dict(map(_decode, x)))
        return d

    def searchNode(self, path, q):
        prefix = path.lstrip("/").replace("/", ".") + "."
        d = self.pool.runInteraction(self._selectPath, path,
                                     self.searchNodeSQL, q)
        d.addCallback(lambda r: map(lambda x: prefix + x, r))

        return d

    def _createFinish(self, e, c, inode, icall):
        if isinstance(e, Failure):
            schema, tablename, node_path = inode
            path, content, ncall = icall
            exc = e.value
            s_exc = str(exc)
            if isinstance(exc, psycopg2.IntegrityError):
                if s_exc.startswith("duplicate key value violates"):
                    raise PathDuplicatedError("%s already exists" % node_path)
            elif isinstance(exc, psycopg2.ProgrammingError):
                if self.regexNoSchema.match(s_exc):
                    d = c.execute("ROLLBACK")
                    d.addCallback(lambda c:
                                      c.execute(self.createSchemaSQL % schema))
                    d.addCallback(self._createNode, path, content,
                                  ncall=ncall + 1)
                    return d
                elif self.regexNoTable.match(s_exc):
                    d = c.execute("ROLLBACK")
                    d.addCallback(lambda c: c.execute(
                            self.createTableSQL % tablename))
                    if node_path:
                        d.addCallback(lambda c: c.execute(
                                self.initTableSQL % tablename))
                    d.addCallback(self._createNode, path, content,
                                  ncall=ncall + 1)
                    return d
            raise exc
        else:
            return c._cursor.rowcount

    def _createNode(self, c, path, content, ncall=0):

        def _exists(c):
            if not c.fetchone():
                raise  NodeNotFound()

        if ncall > 3:
            raise NodeCreationError("internal error")
        schema, table, node_path = self._splitPath(path)
        tablename = self._buildTableName(schema, table)
        hstore_value = self._serialize_hstore(content)

        parent_path, rest = splitext(node_path)
        d = c.execute(self.selectOneSQL % tablename,
                      dict(node_path=parent_path))
        if rest:  # check exists when first execution
            d.addCallback(_exists)
        d.addCallback(lambda _, c: c.execute(self.createSQL % tablename,
                                             [node_path, hstore_value]), c)
        d.addBoth(self._createFinish, c, (schema, tablename, node_path),
                  (path, content, ncall))

        return d

    def createNode(self, path, content):
        return self.pool.runInteraction(self._createNode, path, content)

    def _deleteNode(self, c, path, content, cascade=False):
        schema, table, node_path = self._splitPath(path)
        tablename = self._buildTableName(schema, table)
        if content:
            hstore_key = content.keys()
            d = c.execute(self.deleteSQL % tablename, [hstore_key, node_path])
            d.addCallback(lambda c: c._cursor.rowcount)
            return d
        elif node_path:
            if cascade:
                d = c.execute(self.deleteNodeCascadedSQL % tablename,
                            [node_path])
            else:
                d = c.execute(self.deleteNodeSQL % tablename, [node_path])
            d.addCallback(lambda c: c._cursor.rowcount)
            return d
        elif cascade:
            d = c.execute(self.dropTableSQL % tablename)
            d.addCallback(lambda c: c._cursor.rowcount)
            return d
        else:
            return defer.succeed(0)

    def deleteNode(self, path, content, cascade):
        return self.pool.runInteraction(self._deleteNode, path, content,
                                        cascade)

    def _updateNodeFinish(self, c):
        if isinstance(c, Failure):
            exc = c.value
            s_exc = str(exc)
            if isinstance(exc, psycopg2.ProgrammingError):
                if self.regexNoSchema.match(s_exc):
                    raise NodeNotFound("schema not found")
                elif self.regexNoTable.match(s_exc):
                    raise NodeNotFound("collection not found")

            raise c.value

        return c._cursor.rowcount

    def _updateNode(self, c, path, content):
        schema, table, node_path = self._splitPath(path)
        tablename = self._buildTableName(schema, table)
        hstore_value = self._serialize_hstore(content)
        try:
            d = c.execute(self.updateSQL % tablename,
                          [hstore_value, node_path])
            d.addBoth(self._updateNodeFinish)
            return d
        except:
            return 0

    def updateNode(self, path, content):
        return self.pool.runInteraction(self._updateNode, path, content)

dbBackend = Postgres()
