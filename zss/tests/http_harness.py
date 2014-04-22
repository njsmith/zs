# This file is part of ZS
# Copyright (C) 2013-2014 Nathaniel Smith <njs@pobox.com>
# See file LICENSE.txt for license information.

# We can't use HTTPSimpleServer for testing ZS-over-HTTP, because
# HTTPSimpleServer doesn't support Range requests. So instead this file sets
# up a little harness for spawning a little temporary static-only
# localhost-only nginx server.

from contextlib import contextmanager, closing
import subprocess
import os
import os.path
import time
import socket
import threading
import sys
import random

import requests
import six

from nose.plugins.skip import SkipTest

from .util import test_data_path, tempname

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
        sys.stdout.write(byte.decode("ascii"))

def find_port():
    # We used to just use a fixed port, but it turns out that because of
    # TIME_WAIT annoyances it's possible for our server socket to become
    # un-bindable even after the process that was using it goes away:
    #   http://hea-www.harvard.edu/~fine/Tech/addrinuse.html
    # So, merely waiting for the web server process to exit does not mean that
    # its socket can be reused. Instead, we must pick a nice clean port
    # from scratch each time.
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        # Get the kernel to assign a port.
        s.bind(("127.0.0.1", 0))
        # We made no connections to/from this port, so it can't get stuck in
        # TIME_WAIT. So when we exit the context manager, it will close, and
        # its port is guaranteed to be free. (Of course it could still get
        # grabbed by someone else before we start our web server on it, but
        # that race condition is unavoidable.)
        return s.getsockname()[1]

def wait_for_http(url, timeout=10.0, poll_wait=0.01):  # pragma: no cover
    start = time.time()
    while time.time() - start < timeout:
        try:
            requests.head(url)
        except requests.ConnectionError:
            time.sleep(poll_wait)
        else:
            print("waited %s for server to come UP" % (time.time() - start,))
            break
    else:
        raise IOError("server not listening after %s seconds" % (timeout,))

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
def server_manager(name, argv, port, **kwargs):
    server = spawn_server(argv, port, **kwargs)
    try:
        url = "http://127.0.0.1:%s/" % (port,)
        wait_for_http(url)
        yield url
    finally:
        shutdown_server(server, name)

@contextmanager
def nginx_server(root, error_exc=SkipTest):
    port = find_port()
    nginx = find_nginx()
    if nginx is None:  # pragma: no cover
        raise error_exc
    with tempname(".conf") as conf_path, tempname(".pid") as pid_path:
        with open(conf_path, "w") as conf:
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
        # the nginx on Travis insists on always looking for conf files
        # relative to /etc/nginx
        absolute_conf_path = os.path.join(*([".."] * 20
                                            + [os.path.abspath(conf_path)]))
        with server_manager("nginx", [nginx, "-c", conf_path], port) as url:
            yield url

def simplehttpserver(root, error_exc=SkipTest):
    port = find_port()
    if six.PY2:  # pragma: no cover
        mod = "SimpleHTTPServer"
    else:
        mod = "http.server"
    return server_manager(mod, [sys.executable, "-m", mod, str(port)], port,
                          cwd=root)

def web_server(root, error_exc=SkipTest, range_support=True):
    if range_support:
        return nginx_server(root, error_exc=error_exc)
    else:
        return simplehttpserver(root, error_exc=error_exc)

def test_web_servers():
    for server in [nginx_server, simplehttpserver]:
        with server(test_data_path("http-test")) as url:
            response = requests.get(url + "subdir/foo")
            assert response.content == b"foo\n"
        # to check it shut down properly, start another one immediately on the
        # same port, with a different root
        with server(test_data_path("http-test/subdir")) as url:
            assert requests.get(url + "subdir/foo").status_code == 404
            assert requests.get(url + "foo").status_code == 200

def test_range_support():
    with web_server(test_data_path("http-test"), range_support=False) as url:
        response = requests.get(url + "subdir/foo",
                                headers={"Range": "bytes=0-2"})
        # regular response, ignoring Range:
        assert response.status_code == 200
        assert "Content-Range" not in response.headers
    with web_server(test_data_path("http-test"), range_support=True) as url:
        response = requests.get(url + "subdir/foo",
                                headers={"Range": "bytes=0-2"})
        # Partial data response
        assert response.status_code == 206
        assert "Content-Range" in response.headers
