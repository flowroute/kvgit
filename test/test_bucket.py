import os
import unittest
import shutil
import yaml
import kvgit.bucket
import kvgit.errors


class BucketTestCase(unittest.TestCase):
    def setUp(self):
        """
        Create a (local) test repository.
        """
        if os.path.lexists('_test_temp'):
            shutil.rmtree('_test_temp')
        os.mkdir('_test_temp')
        self.test_repo_path = '_test_temp/test.git'
        self.bucket = kvgit.bucket.Bucket(path=self.test_repo_path,
                                          author=('test', 'test@test'))

    def tearDown(self):
        """
        Remove temporary files.
        """
        shutil.rmtree('_test_temp')

    def test_clone(self):
        path = '_test_temp/cloned.git'
        kvgit.bucket.Bucket(path=path, remote=self.test_repo_path)
        self.assertTrue(os.path.isdir(path))

    def test_init_existing(self):
        kvgit.bucket.Bucket(path=self.test_repo_path)

    def test_remote_mismatch(self):
        with self.assertRaises(kvgit.errors.RemoteMismatch):
            kvgit.bucket.Bucket(path=self.test_repo_path, remote='foo')
        path = '_test_temp/cloned.git'
        kvgit.bucket.Bucket(path=path, remote=self.test_repo_path)
        with self.assertRaises(kvgit.errors.RemoteMismatch):
            kvgit.bucket.Bucket(path=path, remote='foo')

    def test_commit_after_update(self):
        path = '_test_temp/cloned.git'
        bucket = kvgit.bucket.Bucket(path=path, remote=self.test_repo_path)
        bucket['foo'] = 'bar'
        bucket.commit()
        bucket.update()
        bucket['foo'] = 'foo'
        bucket.commit()

    def test_commit_and_update(self):
        b1 = kvgit.bucket.Bucket(path='_test_temp/clone1.git',
                                 remote=self.test_repo_path)
        b2 = kvgit.bucket.Bucket(path='_test_temp/clone2.git',
                                 remote=self.test_repo_path)
        b1['foo'] = 'bar'
        b1.commit()
        self.assertEqual(b2.get('foo'), None)
        b2.update()
        self.assertEqual(b2.get('foo'), 'bar')

    def test_update_conflict(self):
        b1 = kvgit.bucket.Bucket(path='_test_temp/clone1.git',
                                 remote=self.test_repo_path)
        b2 = kvgit.bucket.Bucket(path='_test_temp/clone2.git',
                                 remote=self.test_repo_path)
        b1['foo'] = 'bar'
        b1.commit()
        with self.assertRaises(kvgit.errors.CommitError):
            b2['foo'] = 'foo'
            b2.commit()
        self.assertEqual(b2['foo'], 'bar')

    def test_check_key(self):
        for key in ('/', '//', '/foo', '/foo/bar', 'foo/', 'foo//bar'):
            with self.assertRaises(kvgit.errors.InvalidKey):
                kvgit.bucket._check_key(key)

    def test_set(self):
        self.bucket['foo/bar'] = 'bar'
        self.assertEqual(self.bucket['foo/bar'], 'bar')

    def test_list(self):
        items = ['foo', 'bar', 'biz/baz']
        for i in items:
            self.bucket[i] = ''
        self.assertEqual(sorted(self.bucket.list()), sorted(items))
        self.assertEqual(self.bucket.list('biz'), ['baz'])

    def test_delete(self):
        self.bucket['was/here'] = 'something'
        self.assertEqual(self.bucket['was/here'], 'something')
        del self.bucket['was/here']
        with self.assertRaises(KeyError):
            self.bucket['was/here']

    def test_get_absent(self):
        self.assertEqual(self.bucket.get('not/here'), None)
        with self.assertRaises(KeyError):
            self.bucket['not/here']

    def test_commit(self):
        self.bucket['foo/bar'] = 'bar'
        self.bucket.commit()
        self.assertEqual(self.bucket.get('foo/bar'), 'bar')
        self.bucket['biz/baz'] = 'bar'
        self.bucket.commit()
        self.assertEqual(self.bucket.get('biz/baz'), 'bar')
        self.assertEqual(self.bucket.get('foo/bar'), 'bar')

    def test_rollback(self):
        self.bucket['foo/bar'] = 'bar'
        self.bucket.commit()
        self.assertEqual(self.bucket.get('foo/bar'), 'bar')
        self.bucket['foo/bar'] = 'baz'
        self.assertEqual(self.bucket.get('foo/bar'), 'baz')
        self.bucket.rollback()
        self.assertEqual(self.bucket.get('foo/bar'), 'bar')

    def test_rollback_key(self):
        self.bucket['foo/bar'] = 'bar'
        self.bucket['foo/baz'] = 'bar'
        self.bucket.commit()
        self.bucket['foo/bar'] = 'baz'
        self.bucket['foo/baz'] = 'baz'
        self.bucket.rollback('foo/baz')
        self.bucket['foo/bar'] = 'baz'
        self.bucket['foo/baz'] = 'bar'

    def test_get_not_staged(self):
        self.bucket['foo'] = 'foo'
        self.bucket.commit()
        self.bucket['foo'] = 'bar'
        self.assertEqual(self.bucket.get('foo', staged=False), 'foo')

    def test_multiple_commits(self):
        self.bucket['foo'] = 'bar'
        self.bucket.commit()
        bucket = kvgit.bucket.Bucket(path=self.test_repo_path,
                                     author=('test', 'test@test'))
        bucket['bar'] = 'foo'
        bucket.commit()
        self.assertEqual(bucket['foo'], 'bar')

    def test_json_load_dump(self):
        bucket = kvgit.bucket.JSONBucket(path=self.test_repo_path,
                                         author=('test', 'test@test'))
        bucket['foo'] = {'foo': 'bar'}
        self.assertEqual(bucket.get('foo'), {'foo': 'bar'})
        bucket.commit()
        self.assertEqual(bucket.get('foo'), {'foo': 'bar'})

    def test_yaml_load_dump(self):
        bucket = kvgit.bucket.Bucket(path=self.test_repo_path,
                                     author=('test', 'test@test'),
                                     loader=yaml.load,
                                     dumper=yaml.dump)
        bucket['foo'] = {'foo': 'bar'}
        self.assertEqual(bucket.get('foo'), {'foo': 'bar'})
        bucket.commit()
        self.assertEqual(bucket.get('foo'), {'foo': 'bar'})
