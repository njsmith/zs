# This file is part of ZSS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

# We can't use HTTPSimpleServer for testing ZSS-over-HTTP, because
# HTTPSimpleServer doesn't support Range requests. So instead this file sets
# up a little harness for spawning a little temporary static-only
# localhost-only nginx server.

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
import six

from nose.plugins.skip import SkipTest

from .util import test_data_path, tempname

# a random number
PORT = 43124

def find_nginx():  # pragma: no cover
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

def wait_for_tcp(port):  # pragma: no cover
    TIMEOUT = 5.0
    POLL = 0.01
    now = time.time()
    while time.time() - now < TIMEOUT:
        try:
            s = socket.create_connection(("127.0.0.1", port))
        except socket.error:
            continue
        else:
            s.close()
            break
        time.sleep(POLL)
    else:
        raise IOError("server not listening after %s seconds" % (TIMEOUT,))

def spawn_server(argv, port, **kwargs):
    process = subprocess.Popen(argv,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT,
                               **kwargs)
    # We capture the server's chatter and redirect it to sys.stdout, where it
    # is then captured by nose and only displayed if the test fails.
    stdout_thread = threading.Thread(target=_copy_to_stdout,
                                     args=(process.stdout,))
    stdout_thread.start()
    wait_for_tcp(port)
    return (process, stdout_thread)

def shutdown_server(process_thread, name):
    if process_thread is None:
        return
    process, stdout_thread = process_thread
    if process.poll() is not None:  # pragma: no cover
        raise IOError("%s exited with error: %s" % (name, process.returncode))
    process.terminate()
    process.wait()
    stdout_thread.join()

@contextmanager
def nginx_server(root, port=PORT, error_exc=SkipTest):
    nginx = find_nginx()
    if nginx is None:  # pragma: no cover
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
        server = None
        try:
            server = spawn_server([nginx, "-c", conf_path], port)
            yield "http://127.0.0.1:%s/" % (port,)
        finally:
            shutdown_server(server, "nginx")

@contextmanager
def simplehttpserver(root, port=PORT, error_exc=SkipTest):
    if six.PY2:  # pragma: no cover
        mod = "SimpleHTTPServer"
    else:
        mod = "http.server"
    server = None
    try:
        server = spawn_server([sys.executable, "-m", mod, str(port)], port,
                               cwd=root)
        yield "http://127.0.0.1:%s/" % (port,)
    finally:
        shutdown_server(server, mod)

web_server = nginx_server

def test_web_server():
    for server in [nginx_server, simplehttpserver]:
        with server(test_data_path("http-test")) as url:
            response = requests.get(url + "subdir/foo")
            assert response.content == b"foo\n"
        # to check it shut down properly, start another one immediately on the
        # same port, with a different root
        with server(test_data_path("http-test/subdir")) as url:
            assert requests.get(url + "subdir/foo").status_code == 404
            assert requests.get(url + "foo").status_code == 200
