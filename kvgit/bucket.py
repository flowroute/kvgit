import os
import sys
from time import time
from StringIO import StringIO
from dulwich.repo import Repo
from dulwich.objects import Blob, Commit
from dulwich.client import get_transport_and_path
from dulwich.errors import NotGitRepository


class BucketError(Exception):
    pass


class Bucket(object):
    def __init__(self, name, opts):
        self.name = name

        try:
            config = opts['kvgit.buckets'][name]
        except KeyError:
            raise BucketError('Bucket config missing')

        try:
            remote = config['remote']
        except KeyError:
            raise BucketError('Bucket remote missing from config')

        path = '{0}/kvgit/{1}'.format(opts['cachedir'], name)

        self.client, self.remote_path = get_transport_and_path(remote)

        try:
            r = self.repo = Repo(path)
            self.fetch()
        except NotGitRepository:
            r = self.repo = self.init_repo(path)

        self.tree = r.get_object(r.get_object(r.head()).tree)
        self.blobs = []

    def fetch(self):
        def determine_wants(heads):
            return heads.values()
            refs = dict([(k, (v, None)) for (k, v) in heads.iteritems()])
            return [sha1 for (sha1, revid) in refs.itervalues()]

        graphwalker = self.repo.get_graph_walker()
        f = StringIO()
        remote_refs = self.client.fetch_pack(
            self.remote_path,
            determine_wants,
            graphwalker, f.write)
        f.seek(0)
        self.repo.object_store.add_thin_pack(f.read, None)
        self.repo['HEAD'] = remote_refs['refs/heads/master']

    def init_repo(self, path):
        os.makedirs(path)
        repo = Repo.init(path)

        remote_refs = self.client.fetch(
            self.remote_path, repo,
            determine_wants=repo.object_store.determine_wants_all)

        repo['HEAD'] = remote_refs['refs/heads/master']
        return repo

    def get(self, key, default=None):
        r = self.repo
        try:
            o = r.get_object(self.tree.lookup_path(r.get_object, key)[1])
        except KeyError:
            return default
        return o.data

    def set(self, key, value):
        if not isinstance(value, str):
            raise BucketError('Value must be a string')

        blob = Blob.from_string(value)
        self.tree.add(key, 0100644, blob.id)
        self.blobs.append(blob)

    def _reset(self):
        self.blobs = []
    rollback = _reset

    def commit(self, message='autocommit'):
        before_commit = self.repo.head()

        for b in self.blobs:
            self.repo.object_store.add_object(b)
        self.repo.object_store.add_object(self.tree)

        # commit = self.repo.do_commit(message, 'test-committer', tree=self.tree.id)
        commit = Commit()
        commit.tree = self.tree.id
        commit.parents = [before_commit]
        commit.author = commit.committer = 'test-committer'
        commit.commit_time = commit.author_time = int(time())
        commit.commit_timezone = commit.author_timezone = 0
        commit.encoding = 'UTF-8'
        commit.message = 'autocommit'
        self.repo.object_store.add_object(commit)

        self._reset()

        gen_pack = self.repo.object_store.generate_pack_contents

        def get_refs(refs):
            return {"refs/heads/master": commit.id}

        try:
            self.client.send_pack(self.remote_path, get_refs, gen_pack,
                                  progress=sys.stdout.write)
        except:
            self.repo['HEAD'] = before_commit
