# -*- coding: utf-8 -*-
from cjson import encode as json_encode
import unittest2
import psycopg2
import urllib2
import os


def url_access(url, data="", method="GET"):
    opener = urllib2.build_opener(urllib2.HTTPHandler)
    request = urllib2.Request(url, data)
    request.get_method = lambda: method
    return opener.open(request)


class TestCreateFunctions(unittest2.TestCase):

    base = None
    conn = None

    @classmethod
    def setUpClass(cls):
        cls.base = os.environ["MINITREE_SERVER"]
        cls.conn = psycopg2.connect(os.environ["MINITREE_DSN"])

    def setUp(self):
        cursor = self.conn.cursor()
        try:
            cursor.execute("DROP SCHEMA test CASCADE")
            self.conn.commit()
        except:
            self.conn.rollback()

    def test_create_retval_success(self):
        data = dict(key_a="value_a", key_b="\"H\"'E'%l%@l@(e)")
        data[u"中文键"] = u"中文值"
        ret = url_access(self.base + "/node/test/table/success",
                         json_encode(data), "PUT").read()
        self.assertTrue("success" in ret)

    def test_create_retval_invalid_data(self):
        data = "Invalid Data"
        code = 200
        try:
            url_access(self.base + "/node/test/table/invalid1",
                       data, "PUT").read()
        except urllib2.HTTPError as e:
            code = e.code
            ret = e.read()

        self.assertEqual(code, 400)
        self.assertTrue("JSON" in ret)

    def test_create_retval_trailing(self):
        data = dict(key_a="value_a")
        data[u"中文键"] = u"中文值"
        ret = url_access(self.base + "/node/test/table/trailing",
                         json_encode(data), "PUT").read()
        self.assertTrue("success" in ret)

    def test_create_retval_wrong_type_1(self):
        """dict value should not be acceptable"""
        data = dict(key_a=dict(key_a=1))
        data[u"中文键"] = u"中文值"
        code = 200
        try:
            ret = url_access(self.base + "/node/test/table/wrong_type1",
                             json_encode(data), "PUT").read()
        except urllib2.HTTPError as e:
            code = e.code
            ret = e.read()
        self.assertTrue("error" in ret)

    def test_create_retval_wrong_type_2(self):
        """list value should not be acceptable"""
        data = dict(key_a=[1, 2])
        data[u"中文键"] = u"中文值"
        code = 200
        try:
            ret = url_access(self.base + "/node/test/table/wrong_type2",
                             json_encode(data), "PUT").read()
        except urllib2.HTTPError as e:
            code = e.code
            ret = e.read()
        self.assertTrue("error" in ret)

    def test_create_retval_dup(self):
        code = 200
        data = dict(key_a="value_a")
        data[u"中文键"] = u"中文值"
        url_access(self.base + "/node/test/table/dup",
                   json_encode(data), "PUT")
        try:
            ret = url_access(self.base + "/node/test/table/dup",
                             json_encode(data), "PUT").read()
        except urllib2.HTTPError as e:
            code = e.code
            ret = e.read()

        self.assertEqual(code, 400)
        self.assertTrue(ret.find("dup") > -1)
        self.assertTrue(ret.find("already exists") > -1)

    def test_create_dbval(self):
        cursor = self.conn.cursor()
        data = dict(key_a="value_a")
        data[u"中文键"] = u"中文值"
        url_access(self.base + "/node/test/table/dbval",
                   json_encode(data), method="PUT").read()
        cursor.execute("SELECT node_value FROM test.table \
WHERE node_path='dbval' LIMIT 1")
        data = cursor.fetchall()
        self.assertEqual(data[0][0], '"key_a"=>"value_a", "中文键"=>"中文值"')


if __name__ == "__main__":
    unittest2.main()
