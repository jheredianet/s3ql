import oauth2client.client
import apiclient.discovery
import apiclient.http
import os
import sys
import tempfile
import time
import argparse
from queue import Queue, Full as QueueFull

basedir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
if (os.path.exists(os.path.join(basedir, 'setup.py')) and
    os.path.exists(os.path.join(basedir, 'src', 's3ql', '__init__.py'))):
    sys.path = [os.path.join(basedir, 'src')] + sys.path

from s3ql.logging import logging, setup_logging, QuietError
from s3ql.common import AsyncFn, handle_on_return
from s3ql.backends.common import DanglingStorageURLError, NoSuchObject
from s3ql import BUFSIZE
from s3ql.parse_args import ArgumentParser, storage_url_type

logging.getLogger("googleapiclient.discovery").setLevel(logging.WARNING)
log = logging.getLogger(__name__)
def parse_args(args):
    '''Parse command line'''

    parser = ArgumentParser(
                description='Upgrade Gdrive Folder structure.')

    parser.add_quiet()
    parser.add_log()
    parser.add_debug()
    parser.add_backend_options()
    parser.add_version()

    parser.add_argument("--target-path", metavar='<target-path>', type=str,
                        help='Target path to move the FS blocks, default is same')

    # Can't use parser.add_storage_url(), because we need both a source
    # and destination.
    parser.add_argument("--authfile", type=str, metavar='<path>',
                      default=os.path.expanduser("~/.s3ql/authinfo2"),
                      help='Read authentication credentials from this file '
                      '(default: `~/.s3ql/authinfo2)`')
    parser.add_argument("storage_url", metavar='<storage-url>',
                        type=storage_url_type,
                        help='Storage URL of the source backend that contains the file system')

    options = parser.parse_args(args)
    setup_logging(options)

    # Print message so that the user has some idea what credentials are
    # wanted (if not specified in authfile).
    log.info('Connecting to source backend...')
    parser._init_backend_factory(options)
    src_options = argparse.Namespace()
    src_options.__dict__.update(options.__dict__)
    options.src_backend_factory = lambda: src_options.backend_class(src_options)
    options.threads=10
    return options

@handle_on_return
def mv_loop(queue, src_backend_factory, target_drive, on_return):
    backend = on_return.enter_context(src_backend_factory())

    while True:
        drive_object = queue.get()
        if drive_object is None:
            break

        log.debug('moving object %s', drive_object['name'])
        target_folder = backend._get_object_folder_id(drive_object['name'],target_drive)
        if drive_object['parents'][0]!=target_folder['id']:
            new_f = backend._change_parent_object(drive_object['id'],drive_object['parents'][0],target_folder['id'])
            if new_f['id']!=drive_object['id']:
                raise  Exception("Invalid ID {} after move from object {}".format(new_f['id'],drive_object['id']))





def main(args=None):
    options = parse_args(args)

    src_backend_factory = options.src_backend_factory
    target_path = options.target_path

    # Try to access both backends before starting threads
    try:
        src_backend_factory().lookup('s3ql_metadata')
    #except:
    #    pass
    except DanglingStorageURLError as exc:
        raise QuietError(str(exc)) from None


    queue = Queue(maxsize=options.threads)
    threads = []
    target_drive=src_backend_factory().folder
    if options.target_path is not None:
        target_drive=src_backend_factory()._initialize_folder(options.target_path)
        if target_drive is None:
            target_drive = src_backend_factory()._create_folder(options.target_path)


    for _ in range(options.threads):
        t = AsyncFn(mv_loop, queue, src_backend_factory,
                    target_drive)
        # Don't wait for worker threads, gives deadlock if main thread
        # terminates with exception
        t.daemon = True
        t.start()
        threads.append(t)


    stamp1 = 0
    moved=0
    for drive_object in src_backend_factory()._list_files([src_backend_factory().folder], "name,parents,id,mimeType,properties"):
        stamp2 = time.time()
        moved+=1
        if stamp2 - stamp1 > 1:
            stamp1 = stamp2
            sys.stdout.write('\rMoved %d objects so far...' % moved)
            sys.stdout.flush()

            # Terminate early if any thread failed with an exception
            for t in threads:
                if not t.is_alive():
                    t.join_and_raise()

        # Avoid blocking if all threads terminated
        while True:
            try:
                if drive_object['mimeType'] != 'application/vnd.google-apps.folder':
                    queue.put(drive_object, timeout=1)
            except QueueFull:
                pass
            else:
                break
            for t in threads:
                if not t.is_alive():
                    t.join_and_raise()
    sys.stdout.write('\n')

    queue.maxsize += len(threads)
    for t in threads:
        queue.put(None)

    for t in threads:
        t.join_and_raise()
    sys.stdout.write('Completed!!!\n')

if __name__ == '__main__':
    main(sys.argv[1:])
