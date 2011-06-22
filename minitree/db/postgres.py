from twisted.enterprise import adbapi
from minitree.db import PathError, NodeNotFound, NodeCreationError
from minitree.db import DataTypeError
from minitree.db import PathDuplicatedError
from collections import defaultdict
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
    def _splitPath(path):
        parts = path.replace("/", ".").lstrip(".").split(".", 2)

        if len(parts) == 3:
            schema, table, node_path = parts
        elif len(parts) == 2:
            schema, table = parts
            node_path = ""
        else:
            raise PathError("Not enough level")

        return (schema, table, node_path)

    def connect(self, *args, **kwargs):
        assert(self.pool == None)
        self.pool = adbapi.ConnectionPool("psycopg2", *args, **kwargs)

    def _selectPath(self, txn, path, sql, q=None):
        schema, table, node_path = self._splitPath(path)
        tablename = self._buildTableName(schema, table)
        try:
            txn.execute(self.selectOneSQL % tablename, dict(node_path=node_path))
            if not txn.fetchall():
                raise NodeNotFound()
            if q:
                txn.execute(sql % tablename, dict(q=q))
            else:
                txn.execute(sql % tablename, dict(node_path=node_path))
            return map(lambda x: x[0], txn.fetchall())
        except psycopg2.ProgrammingError as e:
            err = unicode(e)
            txn.execute("ROLLBACK")
            if self.regexNoSchema.match(err):
                raise NodeNotFound("schema not found")
            elif self.regexNoTable.match(err):
                raise NodeNotFound("collection not found")
            else:
                raise

    def _patch_path_heading(self, value, path):
        schema, table, node_path = self._splitPath(path)
        return map(lambda x: ("%s.%s.%s" % (schema, table, x)).rstrip("."),
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
        d = self.pool.runInteraction(self._selectNode, path,
                                     self.selectOverrideSQL)
        d.addCallback(lambda r: dict(map(lambda x: (x[0].decode("UTF-8"),
                                                    x[1].decode("UTF-8")), r)))
        return d

    def getComboNode(self, path):

        def _combo(result):
            combo = defaultdict(list)
            map(lambda x: combo[x[0]].append(x[1].decode("UTF-8")), result)
            return combo

        d = self.pool.runInteraction(self._selectNode, path,
                                     self.selectComboSQL)
        d.addCallback(_combo)

        return d

    def _selectDBObject(self, txn, name, sql):
        txn.execute(sql, dict(name=name))
        result = txn.fetchall()
        if result:
            return map(lambda x: x[0], result)
        else:
            raise NodeNotFound()

    def _selectNode(self, txn, path, sql):
        schema, table, node_path = self._splitPath(path)
        tablename = self._buildTableName(schema, table)
        try:
            txn.execute(self.selectOneSQL % tablename, dict(node_path=node_path))
            if not txn.fetchall():
                raise NodeNotFound()
            txn.execute(sql % tablename, dict(node_path=node_path))
            return txn.fetchall()
        except psycopg2.ProgrammingError as e:
            err = unicode(e)
            txn.execute("ROLLBACK")
            if self.regexNoSchema.match(err):
                raise NodeNotFound("schema not found")
            elif self.regexNoTable.match(err):
                raise NodeNotFound("collection not found")
            else:
                raise

    def selectNode(self, path):
        d = self.pool.runInteraction(self._selectNode, path, self.selectSQL)
        d.addCallback(lambda r: dict(map(lambda x: (x[0].decode("UTF-8"),
                                                    x[1].decode("UTF-8")), r)))
        return d

    def searchNode(self, path, q):
        prefix = path.lstrip("/").replace("/", ".") + "."
        d = self.pool.runInteraction(self._selectPath, path,
                                     self.searchNodeSQL, q)
        d.addCallback(lambda r: map(lambda x: prefix + x, r))

        return d

    def _createNode(self, txn, path, content, ncall=0):
        if ncall > 3:
            raise NodeCreationError("internal error")
        schema, table, node_path = self._splitPath(path)
        tablename = self._buildTableName(schema, table)
        hstore_value = self._serialize_hstore(content)
        try:
            txn.execute(self.createSQL % tablename, [node_path, hstore_value])
            return txn._cursor.rowcount
        except psycopg2.IntegrityError as e:
            err = unicode(e)
            if err.startswith("duplicate key value violates"):
                raise PathDuplicatedError("%s already exists" % node_path)
        except psycopg2.ProgrammingError as e:
            err = unicode(e)
            if self.regexNoSchema.match(err):
                txn.execute("ROLLBACK")
                txn.execute(self.createSchemaSQL % schema)
                return self._createNode(txn, path, content,
                                        ncall=ncall + 1)
            elif self.regexNoTable.match(err):
                txn.execute("ROLLBACK")
                txn.execute(self.createTableSQL % tablename)
                return self._createNode(txn, path, content,
                                        ncall=ncall + 1)

    def createNode(self, path, content):
        return self.pool.runInteraction(self._createNode, path, content)

    def _deleteNode(self, txn, path, content, cascade=False):
        schema, table, node_path = self._splitPath(path)
        tablename = self._buildTableName(schema, table)
        if content:
            hstore_key = content.keys()
            txn.execute(self.deleteSQL % tablename, [hstore_key, node_path])
            return txn._cursor.rowcount
        elif node_path:
            if cascade:
                txn.execute(self.deleteNodeCascadedSQL % tablename,
                            [node_path])
            else:
                txn.execute(self.deleteNodeSQL % tablename, [node_path])
            return txn._cursor.rowcount
        elif cascade:
            txn.execute(self.dropTableSQL % tablename)
            return txn._cursor.rowcount
        else:
            return 0

    def deleteNode(self, path, content, cascade):
        return self.pool.runInteraction(self._deleteNode, path, content,
                                        cascade)

    def _updateNode(self, txn, path, content):
        schema, table, node_path = self._splitPath(path)
        tablename = self._buildTableName(schema, table)
        hstore_value = self._serialize_hstore(content)
        try:
            txn.execute(self.updateSQL % tablename, [hstore_value, node_path])
            return txn._cursor.rowcount
        except:
            return 0

    def updateNode(self, path, content):
        return self.pool.runInteraction(self._updateNode, path, content)

dbBackend = Postgres()
