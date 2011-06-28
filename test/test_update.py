# -*- coding: utf-8 -*-
from cjson import decode as json_decode, encode as json_encode
import unittest2
import psycopg2
import urllib2
import os


def url_access(url, data="", method="GET"):
    opener = urllib2.build_opener(urllib2.HTTPHandler)
    request = urllib2.Request(url, data)
    request.get_method = lambda: method
    return opener.open(request)


class TestUpdateFunctions(unittest2.TestCase):

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
        url_access(cls.base + "/node/test/table/a/c",
                   json_encode(dict(key1="value1-3", key2="value2-2",
                                    key3="value3")),
                   method="PUT").read()
        url_access(cls.base + "/node/test/table/a/c/d",
                   json_encode(dict(key1="value1-3", key2="value2-2",
                                    key3="value3")),
                   method="PUT").read()
        url_access(cls.base + "/node/test/table/empty",
                   json_encode(dict()),
                   method="PUT").read()

    @classmethod
    def tearDownClass(cls):
        cursor = cls.conn.cursor()
        cursor.execute("DROP SCHEMA test CASCADE")
        cls.conn.commit()

    def setUp(self):
        pass

    def test_update_node_non_exist(self):
        code = 200
        date = "{}"
        try:
            ret = url_access(self.base + "/node/test/table/x/y/z",
                             date, method="POST").read()
        except urllib2.HTTPError as e:
            code = e.code
            ret = e.read()

        self.assertEqual(code, 200)
        self.assertTrue("success" in ret)
        self.assertTrue("0 node" in ret)

    def test_update_schema_non_exist(self):
        code = 200
        data = "{}"
        try:
            url_access(self.base + "/node/test_test/table/x/y/z",
                       data, method="GET").read()
        except urllib2.HTTPError as e:
            code = e.code
            ret = e.read()

        self.assertEqual(code, 404)
        self.assertTrue("error" in ret)

    def test_update_table_non_exist(self):
        code = 200
        data = "{}"
        try:
            url_access(self.base + "/node/test/table_table/x/y/z",
                       data, method="GET").read()
        except urllib2.HTTPError as e:
            code = e.code
            ret = e.read()

        self.assertEqual(code, 404)
        self.assertTrue("error" in ret)

if __name__ == "__main__":
    unittest2.main()
