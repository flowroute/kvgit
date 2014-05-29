import time
import json
import pygit2
import errors


def _check_key(key):
    """
    Raise InvalidKey exception if key has leading, trailing,
    or double slashes.
    """
    if '' in key.split('/'):
        raise errors.InvalidKey(key)


class Bucket(object):
    def __init__(self, path, remote=None, author=None, committer=None,
                 timezone_offset=0, credentials=None, loader=None,
                 dumper=None):
        """
        Load or initialize a repository as a bucket.
        :param path: Local path to load. If the path does not exist,
            clone from remote (if provided) or initialize a new git repo.
        :param remote: (optional) Remote from which to clone. If path already
            exists, ensures remote is equal to the local repo's remote.
        :param author: (optional) A tuple of (user, email) to be used as author
            in commits. Defaults to values returned by git config for user.name
            and user.email.
        :param commmitter: (optional) A tuple of (user, email) to be use as
            committer in commits. Defaults to same as author.
        :param timezone_offset: (optional) Offset from UTC in minutes.
            Defaults to 0.
        :param credentials: A `pygit2.Keypair` or `pygit2.UserPass` instance.
            Required for SSH remotes.

        :raises:
            :class:`RemoteMismatch`
        """
        def get_credentials(*args, **kwargs):
            return credentials

        self._loader = loader
        self._dumper = dumper
        self._remote = None
        self._staged = {}

        try:
            self._repo = pygit2.Repository(path)
            if remote:
                try:
                    if remote != self._repo.remotes[0].url:
                        raise errors.RemoteMismatch(
                            '{} is not the same as '
                            '{}'.format(remote, self._repo.remotes[0].url))
                except IndexError:
                    raise errors.RemoteMismatch('Existing path has no remote.')
                self._remote = self._repo.remotes[0]
                if credentials:
                    self._remote.credentials = get_credentials
                self.update()
        except KeyError:
            if remote:
                self._repo = pygit2.clone_repository(remote, path, bare=True,
                                                     credentials=credentials)
                self._remote = self._repo.remotes[0]
                if credentials:
                    self._remote.credentials = get_credentials
            else:
                self._repo = pygit2.init_repository(path, bare=True)
        c = self._repo.config
        self._author = author or (c['user.name'], c['user.email'])
        self._committer = committer or self._author
        self._timezone_offset = timezone_offset

    def _signatures(self):
        """
        Generate `pygit2.Signature` instances for author and committer.
        """
        curtime = time.time()
        author = pygit2.Signature(self._author[0], self._author[1],
                                  curtime, self._timezone_offset)
        committer = pygit2.Signature(self._committer[0], self._committer[1],
                                     curtime, self._timezone_offset)
        return author, committer

    def _navigate_tree(self, oid, path):
        """
        Find an OID inside a nested tree.
        """
        steps = path.split('/')
        for step in steps:
            oid = self._repo.get(oid)[step].oid
        return oid

    def __getitem__(self, key):
        NotFound = object()
        val = self.get(key, default=NotFound)
        if val == NotFound:
            raise KeyError(key)
        return val

    def __setitem__(self, key, value):
        _check_key(key)
        if self._dumper:
            value = self._dumper(value)
        self._staged[key] = value

    def get(self, key, default=None, staged=True):
        _check_key(key)
        if staged and key in self._staged:
            value = self._staged[key]
        else:
            try:
                oid = self._repo.revparse_single('master').tree.oid
                value = self._repo[self._navigate_tree(oid, key)].data
            except KeyError:
                return default
        if self._loader:
            value = self._loader(value)
        return value

    def update(self):
        """
        Update local path to remote's head.

        :raises:
            :class:`ChangesNotCommitted`
        """
        if not self._remote:
            raise errors.NoREmote()
        if self._staged:
            raise errors.ChangesNotCommitted()
        self._remote.fetch()
        self._repo.reset(self._repo.revparse_single(
            'refs/remotes/origin/master').oid, pygit2.GIT_RESET_SOFT)

    def rollback(self, key=None):
        if key:
            if key in self._staged:
                del self._staged[key]
        else:
            self._staged = {}

    def commit(self, key=None, value=None, message='', tag=None, push=True):
        author, committer = self._signatures()
        idx = self._repo.index
        try:
            idx.read_tree(self._repo.head.get_object().tree.id)
        except pygit2.GitError:
            pass
        if key and value:
            if self._dumper:
                value = self._dumper(value)
            items = ((key, value),)
            if key in self._staged:
                del self._staged[key]
        elif not self._staged:
            raise errors.BucketError('Nothing to commit')
        else:
            items = self._staged.items()
        for k, v in items:
            blob_id = self._repo.create_blob(v)
            idx.add(pygit2.IndexEntry(k, blob_id, pygit2.GIT_FILEMODE_BLOB))
        tree_id = idx.write_tree(self._repo)

        try:
            parents = [self._repo.head.target]
        except pygit2.GitError:
            parents = []
        self._repo.create_commit('refs/heads/master', author, committer,
                                 message, tree_id, parents)
        self._staged = {}
        if push and self._remote:
            try:
                self.push()
            except pygit2.GitError:
                self.update()
                raise errors.CommitError('Push failed. Changes rolled '
                                         'back to remote.')

    def push(self):
        if not self._remote:
            raise errors.NoREmote()
        self._remote.push('refs/heads/master')
        self._remote.fetch()


class JSONBucket(Bucket):
    def __init__(self, *args, **kwargs):
        Bucket.__init__(self, *args, loader=json.loads,
                        dumper=json.dumps, **kwargs)
