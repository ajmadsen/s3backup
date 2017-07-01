#!/usr/bin/env python

'''Backs up and prunes a set of backups in S3'''


import os
from os.path import exists, join, abspath, relpath, normpath

from datetime import timedelta, datetime

import sys
import yaml
import boto3
import pytz

def _upload_files(bucket, root_path, key_base):
    root_path = abspath(root_path)
    print "Root path: {}".format(root_path)
    for dirname, _, files in os.walk(root_path):
        for fname in files:
            relative = relpath(join(dirname, fname), root_path)

            fstat = os.lstat(join(dirname, fname))
            def _new_callback(fstat, relative):
                def _callback(bnew):
                    _callback.bdone += bnew
                    print "Uploaded {}/{} kB of {}".format(_callback.bdone/1024,
                                                           fstat.st_size/1024,
                                                           relative)
                _callback.bdone = 0
                return _callback

            bucket.upload_file(join(dirname, fname), join(key_base, relative),
                               Callback=_new_callback(fstat, relative))
            os.remove(join(dirname, fname))

def _prune(bucket, key_base):
    key_base = normpath(key_base) + '/'

    cutoff = datetime.utcnow().replace(tzinfo=pytz.utc) - timedelta(weeks=1)

    to_delete = []
    for obj in bucket.objects.filter(Prefix=key_base):
        if obj.last_modified < cutoff:
            print 'Deleting {}'.format(obj.key)
            to_delete.append(obj)

    def _transform(obj):
        return {'Key': obj.key}

    start = 0
    while start < len(to_delete):
        chunk = map(_transform, to_delete[start:start+1000])

        print 'Deleting {}-{} of {}'.format(start, start+len(chunk),
                                            len(to_delete))

        bucket.delete_objects(Delete={'Objects': chunk})
        start += len(chunk)

def main():
    '''Main entry'''
    config_file = os.environ.get('S3BACKUPCONFIG')
    if not config_file:
        print 'Please set S3BACKUPCONFIG and run again'
        sys.exit(2)

    if not exists(config_file):
        print 'Config file {} does not exist'.format(config_file)
        sys.exit(2)

    with open(config_file, 'rb') as fp_:
        config = yaml.safe_load(fp_.read())

    s3res = boto3.resource('s3')
    bucket = s3res.Bucket(config['bucket'])

    _upload_files(bucket, config['root_path'], config['key_base'])
    _prune(bucket, config['key_base'])

if __name__ == "__main__":
    main()
