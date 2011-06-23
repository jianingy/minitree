__all__ = ["configure"]


def configure(ini_file):
    from ConfigParser import SafeConfigParser as ConfigParser
    import codecs
    from StringIO import StringIO

    default = """
[server:main]
port = 8000
admin_user =
admin_pass =

[backend:main]
dsn = host=%(server)s port=%(port)s dbname=%(database)s \
user=%(user)s password=%(password)s
user =
password =
cp_min = 2
cp_max = 4
"""
    p = ConfigParser()
    p.readfp(StringIO(default))
    with codecs.open(ini_file, "r", encoding="utf-8") as f:
        p.readfp(f)
    return p
