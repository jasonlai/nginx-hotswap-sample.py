#!/usr/bin/env python

import os
import sys

import signal
import socket
import time
import traceback

from datetime import datetime


DATETIME_FORMAT = '%b %d %H:%M:%S'
ENV_SOCKET = 'HOTSWAP_SOCKET'
MAX_CONNECTIONS = 3

HOST        = ''
PORT        = 7890
LOG_FILE    = '/tmp/hotswap.log'
PID_FILE    = '/var/run/hotswap.pid'

MSG_PING    = 'Hello, world\n'
MSG_QUIT    = 'Bye-bye\n'


def log(message):
    with open(LOG_FILE, 'a') as logfile:
        print >>logfile, '%s %s[%d]: %s' % (datetime.now().strftime(DATETIME_FORMAT), os.path.basename(sys.argv[0]), os.getpid(), message)


def daemonize(working_dir='/'):
    pid = os.fork()
    if pid > 0:
        # In the parent process, exit...
        print 'Start the daemon process ID=%d' % pid
        sys.exit(0)
    os.umask(027)
    # Change the working directory
    if working_dir:
        os.chdir(working_dir)
    # Obtain a new process group
    os.setsid()
    # Detach from the default STDIN, STDOUT & STDERR in the daemon process
    fd = os.open('/dev/null', os.O_RDWR)
    for io in (sys.stdin, sys.stdout, sys.stderr):
        to_fd = io.fileno()
        os.close(to_fd)
        os.dup2(fd, to_fd)
    os.close(fd)


def log_exception():
    exc_output = traceback.format_exception(*sys.exc_info())
    for lines in exc_output:
        for line in lines.splitlines():
            log(line)


def log_child_exit(pid, exit_status):
    log('Child process #%d exits with status %s' % (pid, exit_status))


def shutdown_child(child_pid):
    try:
        log('Shutting down child process #%d....' % child_pid)
        exit_status = os.kill(child_pid, signal.SIGQUIT)
        log_child_exit(child_pid, exit_status)
    except:
        log_exception()


class Master(object):
    def __init__(self, host, port, working_dir, max_connections=MAX_CONNECTIONS):
        self.sock = None
        self.host = host
        self.port = port
        self.child_pids = []
        self.working_dir = working_dir
        self.max_connections = max_connections

    def _register_signal_handlers(self):
        if not getattr(self, '_handlers_registered', False):
            signal.signal(signal.SIGTERM, self._shutdown_gracefully)
            signal.signal(signal.SIGQUIT, self._shutdown_gracefully)
            signal.signal(signal.SIGUSR2, self._hotswap)
            self._handlers_registered = True

    def _listen(self):
        if ENV_SOCKET in os.environ:
            sock_fd = int(os.environ[ENV_SOCKET])
            self.sock = socket.fromfd(sock_fd, socket.AF_INET, socket.SOCK_STREAM)
        else:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.bind((self.host, self.port))
        log('Starting server at %s:%d....' % (self.host, self.port))
        self.sock.listen(5)

    def _accept(self):
        while len(self.child_pids) < self.max_connections:
            conn, addr = self.sock.accept()
            # Fork out a child process to handle the request
            pid = os.fork()
            if pid > 0:
                # Close the request connection in the master process as it's only used in the child process
                conn.close()
                self.child_pids.append(pid)
            else:
                self._close()
                # Serve the request with the child process
                worker = Worker(conn, addr)
                worker.serve()
                break

    def _wait(self):
        while self.child_pids:
            pid, exit_status = os.wait()
            self.child_pids.remove(pid)

    def _close(self):
        try:
            self.sock.close()
        finally:
            self.sock = None

    def _shutdown_gracefully(self, signum, frame):
        log('Gracefully shutting down all child processes....')
        while self.child_pids:
            shutdown_child(self.child_pids.pop())
        sys.exit(0)

    def _hotswap(self, signum, frame):
        log('Received SIGUSR2, gracefully shutting down all child processes....')
        while self.child_pids:
            shutdown_child(self.child_pids.pop())
        # Fork a child process to take over control of the listening socket
        pid = os.fork()
        if pid > 0:
            # In parent master process
            log('Starting hotswapping process #%d....' % pid)
        else:
            # In child process
            log('Now in the hotswapping process, loading new code....')
            try:
                os.chdir(self.working_dir)
                env = dict(os.environ)
                env[ENV_SOCKET] = str(self.sock.fileno())
                # Replace the child process with the new code
                os.execvpe(sys.executable, ['Master Process: python', __file__] + sys.argv[1:], env)
            except:
                raise
        sys.exit(0)

    def serve(self):
        try:
            self._register_signal_handlers()
            self._listen()
            self._accept()
            self._wait()
        finally:
            if self.sock:
                self._close()
                log('Exiting master process....')


class Worker(object):
    def __init__(self, sock, addr):
        self.sock = sock
        self.host, self.port = addr

    def _shutdown_gracefully(self, signum, frame):
        try:
            self.sock.send(MSG_QUIT)
        finally:
            log('Received SIGQUIT, shutting down gracefully for request from %s:%d....' % (self.host, self.port))
            sys.exit(0)

    def serve(self):
        signal.signal(signal.SIGQUIT, self._shutdown_gracefully)
        log('Serving request from %s:%d....' % (self.host, self.port))
        try:
            while True:
                self.sock.send(MSG_PING)
                time.sleep(5)
        except socket.error as error:
            if error.errno != 32:
                raise
        finally:
            log('Stopped serving request from %s:%d' % (self.host, self.port))
            self.sock.close()


def main():
    try:
        working_dir = os.getcwd()
        daemonize()
        master = Master(HOST, PORT, working_dir, MAX_CONNECTIONS)
        master.serve()
    except SystemExit:
        pass
    except:
        log_exception()
        sys.exit(1)


if __name__ == '__main__':
    main()
