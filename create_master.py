import zerorpc
import zmaster
import sys
from apscheduler.schedulers.background import BackgroundScheduler
import time

PORT = 1400


def hello(fmt):
    print "hi at %s" % time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def main(argv):

    # easy way to create master on port other than 1400 if needed
    if argv and argv[0].isdigit():
        port = argv[0]
    else:
        port = PORT

    # this schedules background tasks in separate thread
    # scheduler = BackgroundScheduler()
    # scheduler.add_job(hello, 'interval', minutes=0.1)
    # scheduler.start()

    s = zerorpc.Server(zmaster.ZMaster())
    # connect to master
    s.bind('tcp://*:%d' % port)

    print 'Registering master on port %s' % port

    try:
        s.run()
    except:
        print 'Closing master on port %s' % port
        s.close()
        # scheduler.shutdown()
        
if __name__ == '__main__':
    main(sys.argv[1:])
