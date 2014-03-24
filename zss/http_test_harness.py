# This file is part of ZSS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

from contextlib import contextmanager
import subprocess
from tempfile import mkstemp
import os
import os.path
import time
import socket
import threading
import sys

import requests

from nose.plugins.skip import SkipTest

from .test_util import test_data_path, tempname

# a random number
PORT = 43124

def find_nginx():
    for loc in [os.environ.get("NGINX_PATH"), "/usr/sbin/nginx"]:
        if loc is not None and os.path.exists(loc):
            return loc
    return None

def _copy_to_stdout(handle):
    while True:
        byte = handle.read(1)
        if not byte:
            break
        sys.stdout.write(byte)

@contextmanager
def web_server(root, port, error_exc=SkipTest):
    nginx = find_nginx()
    if nginx is None:
        raise error_exc
    with tempname(".conf") as conf_path, tempname(".pid") as pid_path:
        with open(conf_path, "wb") as conf:
            conf.write(
                "daemon off;\n"
                "worker_processes 1;\n"
                "pid %s;\n"
                "error_log stderr;\n"
                "events {}\n"
                "http {\n"
                "  access_log /dev/stderr;\n"
                "  server {\n"
                "    listen 127.0.0.1:%s;\n"
                "    location / {\n"
                "      root %s;\n"
                "    }\n"
                "  }\n"
                "}\n"
                % (pid_path, port, root))
        try:
            process = subprocess.Popen([nginx, "-c", conf_path],
                                       stderr=subprocess.PIPE)
            # We capture nginx's chatter and redirect it to sys.stdout, where
            # it is then captured by nose and only displayed if the test
            # fails.
            nginx_stderr_thread = threading.Thread(target=_copy_to_stdout,
                                                   args=(process.stderr,))
            nginx_stderr_thread.daemon = True
            nginx_stderr_thread.start()
            # need to wait for it to be ready to accept connections!
            TIMEOUT = 5.0
            POLL = 0.01
            for i in xrange(int(TIMEOUT / POLL)):
                try:
                    s = socket.create_connection(("127.0.0.1", port))
                except socket.error:
                    continue
                else:
                    s.close()
                    break
                time.sleep(POLL)
            else:
                raise AssertionError("nginx not listening after %s seconds"
                                     % (TIMEOUT,))
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
        assert response.content == b"foo\n"
    # to check it shut down properly, start another one immediately on the
    # same port, with a different root
    with web_server(test_data_path("http-test/subdir"), PORT) as url:
        assert requests.get(url + "subdir/foo").status_code == 404
        assert requests.get(url + "foo").status_code == 200
