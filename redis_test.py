from pytebis import tebis
import redis
import datetime
import time

configuration = {
        'host': '10.15.239.202',
        'configfile': 'd:/tebis/Anlage/Config.txt',        
    }
#teb = tebis.Tebis(configuration=configuration)


r = redis.Redis(host='localhost', port=6379, db=0)
p = r.pubsub()
#p.psubscribe('hello*')
for i in range(0,100000):
	r.publish('hello', f'New Test Message 1 - {datetime.datetime.now()}')
	print(f'publish {i}')
	time.sleep(1)
	i +=1

