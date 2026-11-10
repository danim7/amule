#!/usr/bin/env python3
# Copyright (c) 2026 aMule Team
# SPDX-License-Identifier: GPL-2.0-or-later
"""An upload socket whose kernel send queue is still draining does not time out.

Three loopback peers get a slot and request three blocks, which the kernel takes from the daemon
at once. One reads them at about 4 KiB/s, so it needs minutes. One reads the same way but stops for
30 s after 15 s: less than the 40 s timeout without progress, though longer than half of it. The
last never reads, and only it may hit the timeout. Needs a platform with a send-queue query (Linux,
macOS, BSD).
Example: python3 UploadDrainTimeoutIntegrationTest.py build/src/amuled
"""
import argparse
import hashlib
import os
from pathlib import Path
import re
import socket
import struct
import sys
import tempfile
import threading
import time

from AllSearchIntegrationTest import free_ports, retry_port_collisions, running_daemon, wait_for
from DaemonConfig import write_amule_conf
from ShareWatcherMoveIntegrationTest import wait_shared
from UploadRefillIntegrationTest import BLOCK, packet, send

OP_HELLO = 0x01
OP_SETREQFILEID = 0x4f
OP_STARTUPLOADREQ = 0x54
OP_ACCEPTUPLOADREQ = 0x55
OP_REQUESTPARTS = 0x47
SLOW = 'drain-slow-peer'
PAUSING = 'drain-pausing-peer'
STALLED = 'drain-stalled-peer'


def deleted(log, name):
    match = re.search(rf'client D:\d+ U:\d+ "[^"]*{name}.*Reason was (.*)', log)
    return match[1].strip() if match else None


def peer(port, address, name, file_hash, read_rate, pause, stop, errors):
    try:
        sock = socket.socket()
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        sock.bind((address, 0))
        sock.settimeout(30)
        sock.connect(('127.0.0.1', port))
        tag = name.encode()
        tags = b'\x02\x01\x00\x01' + struct.pack('<H', len(tag)) + tag
        send(sock, OP_HELLO, b'\x10' + hashlib.md5(tag).digest() + socket.inet_aton(address)
             + struct.pack('<HI', sock.getsockname()[1], 1) + tags + bytes(6))
        send(sock, OP_SETREQFILEID, file_hash)
        send(sock, OP_STARTUPLOADREQ, file_hash)
        while packet(sock)[1] != OP_ACCEPTUPLOADREQ:
            pass
        starts = [0, BLOCK, 2 * BLOCK]
        send(sock, OP_REQUESTPARTS, file_hash + struct.pack('<6I', *starts, *(s + BLOCK for s in starts)))
        sock.settimeout(1)
        started = time.monotonic()
        while not stop.is_set():
            elapsed = time.monotonic() - started
            if not read_rate or (pause and pause[0] <= elapsed < pause[0] + pause[1]):
                stop.wait(1)
                continue
            try:
                if not sock.recv(read_rate // 4):
                    return
            except TimeoutError:
                pass
            stop.wait(0.25)
    except Exception as error:  # reported by the main thread
        errors.append(f'{name}: {error!r}')
    finally:
        sock.close()


def run(binary):
    with tempfile.TemporaryDirectory(prefix='amule-upload-drain-') as directory:
        root = Path(directory)
        (root / 'Incoming').mkdir()
        (root / 'Incoming' / 'shared.bin').write_bytes(os.urandom(8 << 20))
        ec_port, peer_port = free_ports(2, peer_index=1)
        write_amule_conf(root, ec_port, Port=peer_port, UDPEnable=0, FilterLanIPs=0, MaxUpload=0,
                         SlotAllocation=10, VerboseDebug=1, VerboseDebugLogfile=1,
                         sections={'Debug': {'Cat_ED2k Client': 1}})
        stop = threading.Event()
        errors = []
        with running_daemon(binary, root, (ec_port, peer_port)) as (proc, ec):
            file_hash = wait_shared(proc, ec, ['shared.bin'], 60)['shared.bin']
            threads = [threading.Thread(target=peer, args=(peer_port, address, name, file_hash,
                                                           rate, pause, stop, errors))
                       for address, name, rate, pause in (('127.0.0.2', SLOW, 4096, None),
                                                          ('127.0.0.4', PAUSING, 4096, (15, 30)),
                                                          ('127.0.0.3', STALLED, 0, None))]
            for thread in threads:
                thread.start()
            try:
                log = root / 'logfile'
                reason = wait_for(proc, 'the stalled peer to time out',
                                  lambda: deleted(log.read_text(errors='replace'), STALLED), 90)
                assert reason == 'Timeout', f'the stalled peer ended with {reason!r}'
                # The others started at the same time. The pausing peer resumes 45 s in, and
                # would time out 55-60 s in had it not.
                time.sleep(25)
                text = log.read_text(errors='replace')
                assert not errors, errors
                for name in (SLOW, PAUSING):
                    assert deleted(text, name) is None, f'{name} was dropped: {deleted(text, name)!r}'
            finally:
                stop.set()
                for thread in threads:
                    thread.join(10)
    print('OK: the stalled peer timed out, the draining and pausing ones kept their slots')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('binary')
    args = parser.parse_args()
    if sys.platform.startswith('win'):
        print('skipped: no send-queue query on Windows')
        sys.exit(77)
    retry_port_collisions(run, str(Path(args.binary).resolve()))
