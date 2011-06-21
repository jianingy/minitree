# -*- coding: utf-8 -*-
from cjson import decode as json_decode, encode as json_encode
import unittest
import psycopg2
import urllib2
import os


def url_access(url, data="", method="GET"):
    opener = urllib2.build_opener(urllib2.HTTPHandler)
    request = urllib2.Request(url, data)
    request.get_method = lambda: method
    return opener.open(request)


class TestSelectFunctions(unittest.TestCase):

    base = None
    conn = None

    @classmethod
    def setUpClass(cls):
        cls.base = os.environ["MINITREE_SERVER"]
        cls.conn = psycopg2.connect(os.environ["MINITREE_DSN"])
        url_access(cls.base + "/node/test/table/",
                   json_encode(dict(key1="value1-1", key4="value4-1",
                                    key5="value5", key6=u"中文测试")),
                   method="PUT").read()
        url_access(cls.base + "/node/test/table/a",
                   json_encode(dict(key1="value1-2", key2="value2-1",
                                    key4="value4-2")),
                   method="PUT").read()
        url_access(cls.base + "/node/test/table/a/b",
                   json_encode(dict(key1="value1-3", key2="value2-2",
                                    key3="value3")),
                   method="PUT").read()

    @classmethod
    def tearDownClass(cls):
        cursor = cls.conn.cursor()
        cursor.execute("DROP SCHEMA test CASCADE")
        cls.conn.commit()

    def setUp(self):
        pass

    def test_select_ancestors_normal(self):
        ret = url_access(self.base
                         + "/node/test/table/a/b?method=ancestors").read()
        data = json_decode(ret)
        self.assertEqual(data, ["test.table", "test.table.a"])

    def test_select_ancestors_nonexists(self):
        code = 200
        try:
            url_access(self.base
                         + "/node/test/table/a/b/x/y?method=ancestors").read()
        except urllib2.HTTPError as e:
            code = e.code
            ret = e.read()

        self.assertEqual(code, 404)
        self.assertTrue("error" in ret)

    def test_select_children_normal(self):
        ret = url_access(self.base
                         + "/node/test/table/a?method=children").read()
        data = json_decode(ret)
        self.assertEqual(data, ["test.table.a.b"])

    def test_select_children_nonexists(self):
        code = 200
        try:
            url_access(self.base
                         + "/node/test/table/a/b/x/y?method=children").read()
        except urllib2.HTTPError as e:
            code = e.code
            ret = e.read()

        self.assertEqual(code, 404)
        self.assertTrue("error" in ret)

    def test_select_node_normal(self):
        ret = url_access(self.base + "/node/test/table/a/b").read()
        data = json_decode(ret)
        self.assertEqual(data["key1"], "value1-3")
        self.assertEqual(data["key2"], "value2-2")
        self.assertEqual(data["key3"], "value3")

    def test_select_node_override(self):
        ret = url_access(self.base +
                         "/node/test/table/a/b?method=override").read()
        data = json_decode(ret)
        self.assertEqual(data["key1"], "value1-3")
        self.assertEqual(data["key2"], "value2-2")
        self.assertEqual(data["key3"], "value3")
        self.assertEqual(data["key4"], "value4-2")
        self.assertEqual(data["key5"], "value5")
        self.assertEqual(data["key6"], u"中文测试")

    def test_select_node_combo(self):
        ret = url_access(self.base +
                         "/node/test/table/a/b?method=combo").read()
        data = json_decode(ret)
        self.assertEqual(data["key1"], ["value1-1", "value1-2", "value1-3"])
        self.assertEqual(data["key2"], ["value2-1", "value2-2"])
        self.assertEqual(data["key3"], ["value3"])
        self.assertEqual(data["key4"], ["value4-1", "value4-2"])
        self.assertEqual(data["key5"], ["value5"])
        self.assertEqual(data["key6"], [u"中文测试"])

    def test_select_node_non_exist(self):
        code = 200
        try:
            url_access(self.base + "/node/test/table/x/y/z").read()
        except urllib2.HTTPError as e:
            code = e.code
            ret = e.read()

        self.assertEqual(code, 404)
        self.assertTrue("error" in ret)

    def test_select_schema_non_exist(self):
        code = 200
        try:
            url_access(self.base + "/node/test_test/table/x/y/z").read()
        except urllib2.HTTPError as e:
            code = e.code
            ret = e.read()

        self.assertEqual(code, 404)
        self.assertTrue("error" in ret)

    def test_select_table_non_exist(self):
        code = 200
        try:
            url_access(self.base + "/node/test/table_table/x/y/z").read()
        except urllib2.HTTPError as e:
            code = e.code
            ret = e.read()

        self.assertEqual(code, 404)
        self.assertTrue("error" in ret)

    def test_select_table_invalid_path(self):
        code = 200
        try:
            url_access(self.base + "/node/invalid").read()
        except urllib2.HTTPError as e:
            code = e.code
            ret = e.read()

        self.assertEqual(code, 400)
        self.assertTrue("error" in ret)

    def test_select_node_non_exist_override(self):
        code = 200
        try:
            url_access(self.base +
                       "/node/test/table/x/y/z?method=override").read()
        except urllib2.HTTPError as e:
            code = e.code
            ret = e.read()

        self.assertEqual(code, 404)
        self.assertTrue("error" in ret)

    def test_select_schema_non_exist_override(self):
        code = 200
        try:
            url_access(self.base +
                       "/node/test_test/table/x/y/z?method=override").read()
        except urllib2.HTTPError as e:
            code = e.code
            ret = e.read()

        self.assertEqual(code, 404)
        self.assertTrue("error" in ret)

    def test_select_table_non_exist_override(self):
        code = 200
        try:
            url_access(self.base +
                       "/node/test/table_table/x/y/z?method=override").read()
        except urllib2.HTTPError as e:
            code = e.code
            ret = e.read()

        self.assertEqual(code, 404)
        self.assertTrue("error" in ret)

    def test_select_table_invalid_path_override(self):
        code = 200
        try:
            url_access(self.base
                       + "/node/invalid?method=override").read()
        except urllib2.HTTPError as e:
            code = e.code
            ret = e.read()

        self.assertEqual(code, 400)
        self.assertTrue("error" in ret)

    def test_select_node_non_exist_combo(self):
        code = 200
        try:
            url_access(self.base +
                       "/node/test/table/x/y/z?method=combo").read()
        except urllib2.HTTPError as e:
            code = e.code
            ret = e.read()

        self.assertEqual(code, 404)
        self.assertTrue("error" in ret)

    def test_select_schema_non_exist_combo(self):
        code = 200
        try:
            url_access(self.base +
                       "/node/test_test/table/x/y/z?method=combo").read()
        except urllib2.HTTPError as e:
            code = e.code
            ret = e.read()

        self.assertEqual(code, 404)
        self.assertTrue("error" in ret)

    def test_select_table_non_exist_combo(self):
        code = 200
        try:
            url_access(self.base +
                       "/node/test/table_table/x/y/z?method=combo").read()
        except urllib2.HTTPError as e:
            code = e.code
            ret = e.read()

        self.assertEqual(code, 404)
        self.assertTrue("error" in ret)

    def test_select_table_invalid_path_combo(self):
        code = 200
        try:
            url_access(self.base
                       + "/node/invalid?method=combo").read()
        except urllib2.HTTPError as e:
            code = e.code
            ret = e.read()

        self.assertEqual(code, 400)
        self.assertTrue("error" in ret)
