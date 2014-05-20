import os
import sys
from time import time
from collections import deque
from StringIO import StringIO
from dulwich.repo import Repo
from dulwich.objects import Blob, Commit, Tree
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
            self.repo = Repo(path)
            self.fetch()
        except NotGitRepository:
            self.repo = self.init_repo(path)

        self._reset()

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
            print key
            _, oid = self.tree.lookup_path(r.get_object, key)
            o = r.get_object(oid)
        except KeyError:
            return default
        return o.data

    def set(self, key, value):
        if not isinstance(value, str):
            raise BucketError('Value must be a string')

        parts = deque(key.split('/'))
        sha = self.tree.id
        transversed = []

        while parts:
            p = parts.popleft()
            transversed.append(p)
            if not p:
                continue
            obj = self.repo.get_object(sha)
            if not isinstance(obj, Tree):
                raise BucketError('{0} is a document'.format(
                    '/'.join(transversed)))
            try:
                _, sha = obj[p]
            except KeyError:
                tree = obj
                break

        new_trees = [(p, Tree()) for p in parts]
        if new_trees:
            tree = new_trees[-1]

        blob = Blob.from_string(value)
        tree.add(parts[-1], 0100644, blob.id)
        self.objects.append(blob)

        name = None
        if new_trees:
            while new_trees:
                n, t = new_tress.pop()
                if name:


    def _reset(self):
        r = self.repo
        self.tree = r.get_object(r.get_object(r.head()).tree)
        self.objects = []
        self.objects.append(self.tree)
        self.tree_id = self.tree.id
    rollback = _reset

    def commit(self, message='autocommit'):

        previous_commit = self.repo.head()
        print self.objects
        for o in self.objects:
            self.repo.object_store.add_object(o)
        commit = Commit()
        commit.tree = self.tree.id
        commit.parents = [previous_commit]
        commit.author = commit.committer = 'test-committer <foo@foo.com>'
        commit.commit_time = commit.author_time = int(time())
        commit.commit_timezone = commit.author_timezone = 0
        commit.encoding = 'UTF-8'
        commit.message = message
        self.repo.object_store.add_object(commit)
        self.repo['HEAD'] = commit.id

        gen_pack = self.repo.object_store.generate_pack_contents

        def get_refs(refs):
            return {"refs/heads/master": commit.id}

        try:
            self.client.send_pack(self.remote_path, get_refs, gen_pack,
                                  progress=sys.stdout.write)
        except Exception as e:
            raise e
            self.repo['HEAD'] = previous_commit
        self._reset()
