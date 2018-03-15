import time

def dict_sorted(d):
    return sorted(d.items(), key=lambda item:item[1])


class KylinCache(object):
    TIMEOUT = 10
    MAX_SIZE = 100
    CLEAN_DATA_PERCENT = 2 / 3.0

    def __init__(self, timeout=None, max_size=None):
        self.data = {}
        self.data_expire = {} 
        self.timeout = timeout or self.TIMEOUT
        self.max_size = max_size or self.MAX_SIZE
        self.count = 0

    def set(self, key, value, timeout=None):
        if key in self.data:
            return None
        self._clean_data()
        self.data[key] = value
        timeout = timeout or self.timeout
        self.data_expire[key] = time.time() + timeout
        self.count += 1
        return value
        
    def get(self, key):
        try:
            data = self.data[key]
        except KeyError:
            return None
        data_expire = self.data_expire[key]
        if data_expire < time.time():
            del self.data[key]
            del self.data_expire[key]
            return None
        return data

    def _clean_data(self):
        if self.count > self.max_size - 1:
            data_expire_list = dict_sorted(self.data_expire)
            
            clean_len = int(self.max_size * (1 - self.CLEAN_DATA_PERCENT))
            
            for i in range(clean_len):
                key = data_expire_list[i][0]
                del self.data[key]
                del self.data_expire[key]
                self.count -= 1

        data_expire_list = dict_sorted(self.data_expire)
        for d in data_expire_list:
            key = d[0]
            expire = d[1]
            if expire < time.time():
                del self.data[key]
                del self.data_expire[key]
                self.count -= 1
            else:
                return


    def print_data(self):
        print self.data
        print self.data_expire
        print '-'*80


r = KylinCache(max_size=10)
r.set(1,'a')
r.set(2,'a')
r.set(3,'a')
r.set(4,'a')
r.set(5,'a')
r.set(6,'a')
r.set(7,'a')
r.set(8,'a')
r.set(9,'a')
r.print_data()

r.set(10,'a')
r.set(11,'a')
r.print_data()

time.sleep(5)
print time.time()
r.print_data()

time.sleep(11)
print time.time()
r.set(12, 'a')
r.print_data()













