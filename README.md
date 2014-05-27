kvgit
=====

kvgit is a git backed key/value store.
It implements a dictionary-like interface, and supports rollback.

A remote can be configured, to which commits will be pushed.

``kvgit.bucket.Bucket`` can be passed a remote url, to which commits will
be pushed; serialization functions may be passed as well.

# Example Usage

```python
import kvgit.bucket


bucket = kvgit.bucket.Bucket('/tmp/test', author=('Bob', 'bob@test.com'))

bucket['foo'] = 'bar'
bucket['bar/baz'] = 'biz'

print bucket['foo']
# 'bar'

print bucket['bar']
# raises KeyError

bucket.get('bar')
# None

bucket.commit(message='test commit')

bucket['foo'] = 'foo'
bucket.rollback()

bucket['foo']
# 'bar'
```
