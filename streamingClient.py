# Class Client shamelessly copied from 
# http://arstechnica.com/open-source/guides/2010/04/tutorial-use-twitters-new-real-time-stream-api-in-python.ars/2
import pycurl
import os,sys
#import simplejson as json
from optparse import OptionParser
import time
from datetime import datetime
import os
import gzip
import threading
#import logging
#import logging.handlers
from aws import Aws
from scribe_logger.scribe_logger import Scribe_logger

#sudo apt-get install python-pycurl if necessary.  This version of pycurl works for python2.5 only.

STREAM_URL = "Your GNIP URL goes here."
USER = "username"  
PASS = "password"  
ROLL_DURATION = 120  #rolls to a new file (in seconds)
SAVE_PATH = "/mnt/"
FEED_NAME = "decahose"
SAVE_FILE_LENGTH= 1*1024*1024  # in MB unzipped
json_data = ""


def option_parser():
   usage = "Usage: %prog -o output_dir [options] arg"
   parser = OptionParser(usage)
   parser.add_option("-d","--debug",dest="debug",action="store_true",
                     help="Prints all Debug messages")
   parser.add_option("-o","--output",dest="output_dir",
                     help="Stores the parsed content here")
   parser.add_option("-u","--upload",dest="s3_upload_dir",
                     help="Syncs with S3")
   parser.add_option("-l", "--scribe-host", action="store", dest="scribe_host", default="localhost",
                     help="The host of the scribe server to forward log messages to.", metavar="SCRIBE_HOST")
   parser.add_option("-p", "--scribe-port", action="store", dest="scribe_port", default=61463,
                     help="The port of the scribe server to forward log messages to.", metavar="SCRIBE_PORT")


   (options,args) = parser.parse_args()
   return options, args 


(options, args) = option_parser()
category = "spider"
logger = Scribe_logger(options.scribe_host, options.scribe_port)


#Client Class starts here.
class Client:  
    time_start = time.time()
    content = ""
    def __init__(self,options):
        self.options = options  
        self.buffer = ""  
        self.conn = pycurl.Curl()  
        self.conn.setopt(pycurl.USERPWD, "%s:%s" % (USER, PASS))  
	self.conn.setopt(pycurl.ENCODING,'gzip')
        self.conn.setopt(pycurl.URL, STREAM_URL)  
        self.conn.setopt(pycurl.WRITEFUNCTION, self.on_receive)  
        self.conn.setopt(pycurl.FOLLOWLOCATION,1)
        self.conn.setopt(pycurl.MAXREDIRS, 5)
        self.conn.setopt(pycurl.COOKIEFILE,"cookie.txt")
        try:
            self.conn.perform()  
        except Exception,e:
            logger.log(category,"ERROR",e.message)
           
    def on_receive(self, data):  
        self.buffer += data  
        #logger.log(category,"INFO","on_receive method")
        if data.endswith("\r\n") and self.buffer.strip():  
            if(self.triggered()):
		#Start the save thread.
		if(len(self.buffer) != 0 ):
                    try:
		        SaveThread(self.buffer).start()
                    except Exception, e:
                        logger.log(category,"ERROR","There was an error creating thread.  Killing the main process..")
                        system.exit(1) 
                self.buffer = ""
                #logger.log(category,"INFO","Save thread running...")

    def triggered(self):
        # First trigger based on size then based on time..
        if (len(self.buffer) > SAVE_FILE_LENGTH):
	   return True
        time_end = time.time()
        if (((time_end - self.time_start) > ROLL_DURATION)):  #for the time frame 
             self.time_start=time.time()
             return True
        return False

class SaveThread( threading.Thread):
    def __init__(self,buffer):
        #logger.log(category,"INFO","Saving now...inside thread")
        self.buffer = buffer
        threading.Thread.__init__ (self)

    def run(self) :
        #logger.log(category,"INFO","Inside SaveThread run method")
        try:
            self.saveAs(self.buffer)
        except EOFError,e:
            logger.log(category,"ERROR","EOF error")
            sys.exit(1)

    def saveAs(self,buffer):
          date_time = datetime.today()
          year,month,day,hour = date_time.year,date_time.month,date_time.day,date_time.hour
          minute, microsecond = date_time.minute, date_time.microsecond
  	  if (hour < 10):
               hour = "0" + str(hour)
 	  if (day < 10):
               day = "0" + str(day)
	  if (month < 10):
               month = "0" + str(month)

                                       
          base_path = options.output_dir 
          suffix_path = str(year)+"/"+str(month)+"/"+str(day)+"/"+str(hour)+"_"+str(hour)
          file_path = base_path+ "/" + suffix_path 
          try:
              os.makedirs(file_path)
          except:
              pass
              #logger.log(category,"DEBUG","Directory exists: " + file_path)
                                                                                                                                                 
          fhostname = os.popen('hostname')
          hostname = fhostname.read().rstrip()
          fhostname.close()
          hosts = hostname.split('.')
          host = '_'.join(hosts)
          name = FEED_NAME + "_" +host+"_"+str(year)+str(month)+str(day)+str(hour)+str(minute)+str(microsecond) + ".json"     
          suffix = suffix_path +"/"+ name+".gz"
          file_name = file_path + "/" + name +".gz"
          print file_name
          #fp = open(file_name,"w")
          fp = gzip.open(file_name,"w")
          fp.write(buffer)
          fp.close()
	  logger.log(category,"INFO","Saved file.."+file_name)
          #Exit thread now.
          sys.exit(0)
          #return (file_name,suffix) 

if __name__ == "__main__":
#    OUTPUT_DIR = options.output_dir
    #S3_DATA_STORE = "s3://"+options.s3_upload_dir

    client = Client(options) 

