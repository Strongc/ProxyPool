import redis,time
from proxypool.error import PoolEmptyError
from proxypool.setting import HOST, PORT


class RedisClient(object):

    def __init__(self, host=HOST, port=PORT):
        self._db = redis.Redis(host, port)
        self.dbname = "proxies"

    #取得时间最老的数据去校验，同时删除这些数据
    #这些数据如果校验成功，会被重新插入
    #限制：保留至少100个数据备用

    def get(self, count=1):
        """
        get proxies from redis
        """
        #proxies = self._db.lrange("proxies", 0, count - 1)
        #self._db.ltrim("proxies", count, -1)
        proxies = list(self._db.zrange(self.dbname,0,count-1))
        proxieslen = self._db.zcard(self.dbname)

        #至少留100个
        if count > proxieslen:
            count = proxieslen-100
        self._db.zremrangebyrank(self.dbname,0,count)

        return proxies
    def put(self, proxy):
        """
        add proxy to right top
        """
        self._db.zadd(self.dbname, proxy,time.time())

    def pop(self):
        """
        get proxy from right.
        """
        try:
            proxi =  self._db.zrange(self.dbname,-1,-1).decode('utf-8')
            #把获得的数据时间标签减去一个值，使得其变得比较老，避免下次请求被使用
            self._db.zincrby(self.dbname,proxi[0],-60)
        except:
            raise PoolEmptyError

    @property
    def queue_len(self):
        """
        get length from queue.
        """
        return self._db.zcard(self.dbname)

    def flush(self):
        """
        flush db
        """
        self._db.flushall()


if __name__ == '__main__':
    conn = RedisClient()
    print(conn.pop())
