from twisted.internet.threads import deferToThread
from twisted.enterprise import adbapi
from minitree.db import InvalidPathError, NodeNotFound, NodeCreationError
from minitree.db import PathDuplicatedError
import psycopg2
import re

__all__ = ["dbBackend"]

HSTORE_PAIR_RE = re.compile(r"""
    (
        (?P<key> [^" ] [^= ]* )            # Unquoted keys
      | " (?P<key_q> ([^"] | \\ . )* ) "   # Quoted keys
    )
    [ ]* => [ ]*    # Pair operator, optional adjoining whitespace
    (
        (?P<value> [^" ] [^, ]* )          # Unquoted values
      | " (?P<value_q> ([^"] | \\ . )* ) " # Quoted values
    )
    """, re.VERBOSE)

HSTORE_DELIMITER_RE = re.compile(r"""
    [ ]* , [ ]*
    """, re.VERBOSE)


class HStoreSyntaxError(Exception):
    """Indicates an error unmarshalling an hstore value."""
    def __init__(self, hstore_str, pos):
        self.hstore_str = hstore_str
        self.pos = pos

        CTX = 20
        hslen = len(hstore_str)

        parsed_tail = hstore_str[max(pos - CTX - 1, 0):min(pos, hslen)]
        residual = hstore_str[min(pos, hslen):min(pos + CTX + 1, hslen)]

        if len(parsed_tail) > CTX:
            parsed_tail = '[...]' + parsed_tail[1:]
        if len(residual) > CTX:
            residual = residual[:-1] + '[...]'

        super(HStoreSyntaxError, self).__init__(
                "After %r, could not parse residual at position %d: %r" %
                (parsed_tail, pos, residual))


class Postgres(object):

    selectSQL = "SELECT key, value FROM each( \
(SELECT node_value FROM %s WHERE node_path = %%s LIMIT 1))"

    selectAncestorSQL = "SELECT node_value FROM %s \
WHERE node_path @> %%s ORDER BY node_path ASC"
    updateSQL = "UPDATE %s SET node_value = node_value || %%s \
WHERE node_path = %%s"
    deleteSQL = "UPDATE %s SET node_value = delete(node_value, %%s) \
WHERE node_path = %%s"
    deleteNodeSQL = "DELETE FROM %s WHERE node_path = %%s"
    deleteNodeCascadedSQL = "DELETE FROM %s WHERE node_path <@ %%s"
    dropTableSQL = "DROP TABLE %s"
    createSQL = "INSERT INTO %s(node_path, node_value) VALUES(%%s, %%s)"
    createTableSQL = "CREATE TABLE %s(id SERIAL PRIMARY KEY, \
node_path ltree unique, node_value hstore)"
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
    def _convert(result):
        if result:
            return dict(map(lambda x: (x[0], x[1]), result))
        else:
            raise NodeNotFound()

    @staticmethod
    def _parse_hstore(hstore_str):
        """
        Parse an hstore from it's literal string representation.

        Attempts to approximate PG's hstore input parsing rules as
        closely as possible. Although currently this is not strictly
        necessary, since the current implementation of hstore's output
        syntax is stricter than what it accepts as input, the
        documentation makes no guarantees that will always be the
        case.

        Throws HStoreSyntaxError if parsing fails.
        """
        result = {}
        pos = 0
        pair_match = HSTORE_PAIR_RE.match(hstore_str)

        while pair_match is not None:
            key = pair_match.group('key') or pair_match.group('key_q')
            key = key.decode('UTF-8')
            value = pair_match.group('value') or pair_match.group('value_q')
            value = value.decode('UTF-8')
            result[key] = value
            pos += pair_match.end()

            delim_match = HSTORE_DELIMITER_RE.match(hstore_str[pos:])
            if delim_match is not None:
                pos += delim_match.end()

            pair_match = HSTORE_PAIR_RE.match(hstore_str[pos:])

        if pos != len(hstore_str):
            raise HStoreSyntaxError(hstore_str, pos)

        return result

    @staticmethod
    def _serialize_hstore(val):
        """
        Serialize a dictionary into an hstore literal. Keys and values
        must both be strings.
        """
        def esc(s, position):
            try:
                return unicode(s).replace('"', r'\"').encode('UTF-8')
            except AttributeError:
                raise ValueError("%r in %s position is not a string." %
                                 (s, position))
        return ', '.join('"%s"=>"%s"' % (esc(k, 'key'), esc(v, 'value'))
                         for k, v in val.iteritems())

    def _splitPath(self, path):
        parts = path.replace("/", ".").lstrip(".").split(".", 2)

        if len(parts) == 3:
            schema, table, node_path = parts
        elif len(parts) == 2:
            schema, table = parts
            node_path = ""
        else:
            raise InvalidPathError("Not enough level")

        return (schema, table, node_path)

    def connect(self, *args, **kwargs):
        assert(self.pool == None)
        self.pool = adbapi.ConnectionPool("psycopg2", *args, **kwargs)

    def getOverridedNode(self, path):

        def _update(x, y):
            x.update(y)
            return x

        def _override(result):
            return reduce(lambda x, y: _update(x, y),
                          map(lambda x: self._parse_hstore(x[0]), result))

        schema, table, node_path = self._splitPath(path)
        tablename = Postgres._buildTableName(schema, table)
        d = self.pool.runQuery(self.selectAncestorSQL % tablename, [node_path])
        d.addCallback(lambda x: deferToThread(_override, x))

        return d

    def getComboNode(self, path):

        def _concat(x, y, m):
            if m in x:
                l = ([x[m]], x[m])[isinstance(x[m], list)]
                if m in y:
                    l.append(y[m])
                    return (m, l)
                else:
                    return (m, l)
            elif m in y:
                return (m, [y[m]])
            else:
                raise Exception("What's wrong with your python")

        def _combine(x, y):
            return dict(map(lambda m: _concat(x, y, m),
                            list(set(x.keys() + y.keys()))))

        def _combo(result):
            return reduce(lambda x, y: _combine(x, y),
                          map(lambda x: self._parse_hstore(x[0]), result))

        schema, table, node_path = self._splitPath(path)
        tablename = Postgres._buildTableName(schema, table)
        d = self.pool.runQuery(self.selectAncestorSQL % tablename, [node_path])
        d.addCallback(lambda x: deferToThread(_combo, x))

        return d

    def selectNode(self, path):
        schema, table, node_path = self._splitPath(path)
        tablename = Postgres._buildTableName(schema, table)
        d = self.pool.runQuery(self.selectSQL % tablename, [node_path])
        d.addCallback(Postgres._convert)
        return d

    def _createNode(self, txn, path, content, ncall=0):
        if ncall > 3:
            raise NodeCreationError("internal error")
        schema, table, node_path = self._splitPath(path)
        tablename = Postgres._buildTableName(schema, table)
        hstore_value = Postgres._serialize_hstore(content)
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
        tablename = Postgres._buildTableName(schema, table)
        if content:
            hstore_key = content.keys()
            txn.execute(self.deleteSQL % tablename, [hstore_key, node_path])
            return txn._cursor.rowcount
        elif node_path:
            if cascade:
                txn.execute(self.deleteNodeCascadedSQL % tablename, [node_path])
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
        tablename = Postgres._buildTableName(schema, table)
        hstore_value = Postgres._serialize_hstore(content)
        try:
            txn.execute(self.updateSQL % tablename, [hstore_value, node_path])
            return txn._cursor.rowcount
        except:
            return 0

    def updateNode(self, path, content):
        return self.pool.runInteraction(self._updateNode, path, content)

dbBackend = Postgres()
