import json
import os
import time
import fnmatch
import threading

class JSONStorage:
    def __init__(self, filename='storage.json'):
        self.filename = filename
        self.lock = threading.RLock()
        self._load_storage()

    def _load_storage(self):
        with self.lock:
            if os.path.exists(self.filename):
                with open(self.filename, 'r') as file:
                    self.storage = json.load(file)
                    for key, value in self.storage.items():
                        if isinstance(value, list) and all(isinstance(item, (int, str)) for item in value):
                            self.storage[key] = set(value)
            else:
                self.storage = {}

    def _save_storage(self):
        with self.lock:
            with open(self.filename, 'w') as file:
                json.dump({k: list(v) if isinstance(v, set) else v for k, v in self.storage.items()}, file)

    def set(self, key, value):
        with self.lock:
            self.storage[key] = value
            self._save_storage()

    def setex(self, key, ttl, value):
        with self.lock:
            self.storage[key] = {'value': value, 'expires_at': time.time() + ttl}
            self._save_storage()

    def get(self, key):
        with self.lock:
            item = self.storage.get(key)
            if isinstance(item, dict) and 'expires_at' in item:
                if time.time() > item['expires_at']:
                    del self.storage[key]
                    self._save_storage()
                    return None
                return item['value']
            return item

    def delete(self, key):
        with self.lock:
            if key in self.storage:
                del self.storage[key]
                self._save_storage()

    def sadd(self, key, value):
        with self.lock:
            if key not in self.storage:
                self.storage[key] = set()
            elif not isinstance(self.storage[key], set):
                raise TypeError(f"The value for key '{key}' is not a set.")
            self.storage[key].add(value)
            self._save_storage()

    def srem(self, key, value):
        with self.lock:
            if key in self.storage and value in self.storage[key]:
                self.storage[key].remove(value)
                self._save_storage()

    def sismember(self, key, value):
        with self.lock:
            return key in self.storage and value in self.storage[key]

    def smembers(self, key):
        with self.lock:
            if key in self.storage:
                return self.storage[key]
            return set()

    def hset(self, key, field, value):
        with self.lock:
            if key not in self.storage:
                self.storage[key] = {}
            self.storage[key][field] = value
            self._save_storage()

    def hget(self, key, field):
        with self.lock:
            if key in self.storage and field in self.storage[key]:
                return self.storage[key][field]
            return None

    def hdel(self, key, field):
        with self.lock:
            if key in self.storage and field in self.storage[key]:
                del self.storage[key][field]
                self._save_storage()

    def hincrby(self, key, field, increment):
        with self.lock:
            if key not in self.storage:
                self.storage[key] = {}
            if field not in self.storage[key]:
                self.storage[key][field] = 0
            self.storage[key][field] += increment
            self._save_storage()
            return self.storage[key][field]

    def lpush(self, key, value):
        with self.lock:
            if key not in self.storage:
                self.storage[key] = []
            elif not isinstance(self.storage[key], list):
                raise TypeError(f"The value for key '{key}' is not a list.")
            self.storage[key].insert(0, value)
            self._save_storage()

    def rpush(self, key, value):
        with self.lock:
            if key not in self.storage:
                self.storage[key] = []
            elif not isinstance(self.storage[key], list):
                raise TypeError(f"The value for key '{key}' is not a list.")
            self.storage[key].append(value)
            self._save_storage()

    def lpop(self, key):
        with self.lock:
            if key in self.storage and isinstance(self.storage[key], list) and self.storage[key]:
                value = self.storage[key].pop(0)
                self._save_storage()
                return value
            return None

    def rpop(self, key):
        with self.lock:
            if key in self.storage and isinstance(self.storage[key], list) and self.storage[key]:
                value = self.storage[key].pop()
                self._save_storage()
                return value
            return None

    def llen(self, key):
        with self.lock:
            if key in self.storage and isinstance(self.storage[key], list):
                return len(self.storage[key])
            return 0

    def expire(self, key, ttl):
        with self.lock:
            if key in self.storage:
                if isinstance(self.storage[key], dict) and 'expires_at' in self.storage[key]:
                    self.storage[key]['expires_at'] = time.time() + ttl
                else:
                    self.storage[key] = {'value': self.storage[key], 'expires_at': time.time() + ttl}
                self._save_storage()

    def keys(self, pattern='*'):
        with self.lock:
            return [key for key in self.storage.keys() if fnmatch.fnmatch(key, pattern)]

    def incrby(self, key, increment):
        with self.lock:
            if key not in self.storage:
                self.storage[key] = 0
            if not isinstance(self.storage[key], int):
                raise TypeError(f"The value for key '{key}' is not an integer.")
            self.storage[key] += increment
            self._save_storage()
            return self.storage[key]

    def decrby(self, key, decrement):
        return self.incrby(key, -decrement)

    def exists(self, key):
        with self.lock:
            return key in self.storage

    def rename(self, old_key, new_key):
        with self.lock:
            if old_key in self.storage:
                self.storage[new_key] = self.storage.pop(old_key)
                self._save_storage()
            else:
                raise KeyError(f"The key '{old_key}' does not exist.")

    def type(self, key):
        with self.lock:
            if key in self.storage:
                value = self.storage[key]
                if isinstance(value, str):
                    return 'string'
                elif isinstance(value, list):
                    return 'list'
                elif isinstance(value, set):
                    return 'set'
                elif isinstance(value, dict):
                    return 'hash'
                elif isinstance(value, int):
                    return 'integer'
                else:
                    return 'unknown'
            return 'none'

    def append(self, key, value):
        with self.lock:
            if key in self.storage:
                if isinstance(self.storage[key], str):
                    self.storage[key] += value
                else:
                    raise TypeError(f"The value for key '{key}' is not a string.")
            else:
                self.storage[key] = value
            self._save_storage()

    def lindex(self, key, index):
        with self.lock:
            if key in self.storage and isinstance(self.storage[key], list):
                try:
                    return self.storage[key][index]
                except IndexError:
                    return None
            return None

    def lrange(self, key, start, end):
        with self.lock:
            if key in self.storage and isinstance(self.storage[key], list):
                return self.storage[key][start:end + 1]
            return []

    def scard(self, key):
        with self.lock:
            if key in self.storage and isinstance(self.storage[key], set):
                return len(self.storage[key])
            return 0

    def sdiff(self, key1, key2):
        with self.lock:
            if key1 in self.storage and key2 in self.storage and isinstance(self.storage[key1], set) and isinstance(self.storage[key2], set):
                return self.storage[key1] - self.storage[key2]
            return set()

    def sunion(self, key1, key2):
        with self.lock:
            if key1 in self.storage and key2 in self.storage and isinstance(self.storage[key1], set) and isinstance(self.storage[key2], set):
                return self.storage[key1] | self.storage[key2]
            return set()

    def hkeys(self, key):
        with self.lock:
            if key in self.storage and isinstance(self.storage[key], dict):
                return list(self.storage[key].keys())
            return []

    def hvals(self, key):
        with self.lock:
            if key in self.storage and isinstance(self.storage[key], dict):
                return list(self.storage[key].values())
            return []

    def hlen(self, key):
        with self.lock:
            if key in self.storage and isinstance(self.storage[key], dict):
                return len(self.storage[key])
            return 0

    def zadd(self, key, score, value):
        with self.lock:
            if key not in self.storage:
                self.storage[key] = []
            self.storage[key].append((score, value))
            self.storage[key].sort()
            self._save_storage()

    def zrange(self, key, start, end):
        with self.lock:
            if key in self.storage and isinstance(self.storage[key], list):
                return [v for s, v in self.storage[key][start:end + 1]]
            return []

    def zscore(self, key, value):
        with self.lock:
            if key in self.storage and isinstance(self.storage[key], list):
                for score, val in self.storage[key]:
                    if val == value:
                        return score
            return None

