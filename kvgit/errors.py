class BucketError(Exception):
    pass


class NoRepository(BucketError):
    pass


class InvalidKey(BucketError):
    pass


class ChangesNotCommitted(BucketError):
    pass


class RemoteMismatch(BucketError):
    pass


class CommitError(BucketError):
    pass


class NoRemote(BucketError):
    pass
