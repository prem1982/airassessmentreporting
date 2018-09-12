from threading import Thread
import time

# Define a function for the thread
def print_time( threadName, delay):
    count = 0
    while count < 5:
        time.sleep(delay)
        count += 1
        print "%s: %s" % ( threadName, time.ctime(time.time()) )
    print "Exit Child thread"
# Create two threads as follows

thread1 = Thread( target=print_time, args=("Thread-1", 2, ) )
thread2 = Thread( target=print_time, args=("Thread-2", 5, ) )


thread1.start()
thread2.start()

threads = []

threads.append(thread1)
threads.append(thread2)


# for t in threads:
#     print 'join is running'
#     t.join()

print "Exit Main threads"