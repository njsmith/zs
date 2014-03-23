# This file is part of ZSS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

from contextlib import contextmanager
import subprocess
from tempfile import mkstemp
import os
import os.path
import requests

from nose.plugins.skip import SkipTest

from .test import test_data_path

# a random number
PORT = 43124

def find_nginx():
    for loc in [os.environ.get("NGINX_PATH"), "/usr/sbin/nginx"]:
        if loc is not None and os.path.exists(loc):
            return loc
    return None

@contextmanager
def tempname(suffix=""):
    try:
        fd, path = mkstemp(suffix=suffix)
        os.close(fd)
        yield path
    finally:
        os.unlink(path)

def test_tempname():
    with tempname(".asdf") as name:
        assert os.path.exists(name)
        assert name.endswith(".asdf")
    assert not os.path.exists(name)

@contextmanager
def web_server(root, port, error_exc=SkipTest):
    nginx = find_nginx()
    if nginx is None:
        raise error_exc
    with tempname(".conf") as conf_path, tempname(".pid") as pid_path:
        with closing(open(conf, "wb")) as conf_path:
            conf_path.write(
                "daemon off;\n"
                "worker_processes 1;\n"
                "pid %s;\n"
                "error_log stderr;\n"
                "events {}\n"
                "http {\n"
                "  access_log off;\n"
                "  server {\n"
                "    listen 127.0.0.1:%s;\n"
                "    location / {\n"
                "      root %s;\n"
                "    }\n"
                "  }\n"
                "}\n"
                % (pid_path, port, root))
        try:
            process = subprocess.Popen([nginx, "-c", conf_path])
            yield "http://127.0.0.1:%s/" % (port,)
        finally:
            process.poll()
            if process.returncode is not None:
                raise IOError("nginx exited with error: %s"
                              % (process.returncode,))
            process.terminate()
            process.wait()

def test_web_server():
    with web_server(test_data_path("http-test"), PORT) as url:
        response = requests.get(url + "subdir/foo")
        assert response.content == b"foo"
    # to check it shut down properly, start another one immediately on the
    # same port, with a different root
    with web_server(test_data_path("http-test/subdir"), PORT) as url:
        assert requests.get(url + "subdir/foo").status_code == 404
        assert requests.get(url + "foo").status_code == 200
