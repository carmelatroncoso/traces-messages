from collections import namedtuple
from collections import Counter
import email
import getopt, getpass
import hashlib
import heapq
import imaplib
import os
import random
import re
import sys
import traceback
import time

from scipy.stats import gamma

import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator, FormatStrFormatter

from pylab import subplot as subplot
from pylab import savefig as savefig
from pylab import hold as hold
from pylab import figure as figure
from pylab import close as close
from pylab import legend as legend
from numpy import arange 
# GLOBAL VARIABLES
SECONDSMONTH = 2629743
HOURSMONTH = 730
SECONDSHOUR = 3600
SECONDSDAY = 86400

## Do logging properly
import logging
logging.basicConfig(level=logging.DEBUG) # set to .DEBUG for gory details

# named tuple for messages
Msg = namedtuple('Msg', 'ID, time, replyto, sender, receiver, cc')

# regex for emails
email_pattern = re.compile('[a-zA-Z0-9+_\-\.]+@[0-9a-zA-Z][.-0-9a-zA-Z]*.[a-zA-Z]+')

def processEnron(rootfolder="C:/Users/ctroncoso/Documents/Research/email/enron_mail_20110402/maildir/"):

####################################################################
##        SAMPLE EMAIL FROM ENRON DATASET (fields of interest)
##
##        Message-ID: <2901330.1075859176788.JavaMail.evans@thyme>
##        Date: Tue, 18 Sep 2001 09:24:04 -0700 (PDT)
##        From: jaime.gualy@enron.com
##        To: w..white@enron.com
##        Subject: New NatGas Portfolios/Books
##        Cc: robert.stalford@enron.com, harry.arora@enron.com
##        Mime-Version: 1.0
##        Content-Type: text/plain; charset=us-ascii
##        Content-Transfer-Encoding: 7bit
##        Bcc: robert.stalford@enron.com, harry.arora@enron.com
####################################################################

    # create/open dir in which logs are to be stored
    parsedLogsFolder = '../scratch/enronParsed/'
    if not os.path.exists(parsedLogsFolder): 
        os.makedirs(parsedLogsFolder)

    cnt_msgs = 0

    for userfolder in os.listdir(rootfolder):
        logging.info( "Parsing user: %s", userfolder )
        # check that this user was not already parsed
        if os.path.exists(parsedLogsFolder+userfolder+'.txt'):
            logging.debug("User %s was already parsed", userfolder)
            continue
            
        log = []
        heapq.heapify(log)    # create log for user
        seenMsgs = []         # create list to detect duplicate messages

        sentFolders = [folder for folder in os.listdir(rootfolder+'/'+userfolder) if 'sent' in folder] # only process files with sent messages
        # only process user if there are more than 20 sent messages
        counter = 0
        for folder in sentFolders:
            counter += len(os.listdir(rootfolder+'/'+userfolder+'/'+folder))
        if counter < 20:
            logging.info('User %s does not have enough sent messages', userfolder)
            continue

    
        for folder in sentFolders:
        
            files = os.listdir(rootfolder+'/'+userfolder+'/'+folder)
            for currentfile in files:                
                filePath = rootfolder+'/'+userfolder+'/'+folder + '/' + currentfile
                f = open( filePath, 'r' )
                full_message = f.read()
                msg_content = email.message_from_string(full_message)
                
                try:
                    auxstring = msg_content['Message-ID']
                    mID =  int(hashlib.sha1(auxstring.lower()).hexdigest(),16)
                    if mID in seenMsgs:
                        print '*',
                        continue
                except:
                    mID = 'there is no Message-ID'


                # ReplyTo
                try:
                    auxstring = msg_content['In-Reply-to']
                    mReplyTo =  int(hashlib.sha1(auxstring.lower()).hexdigest(),16)
                except:
                    mReplyTo = 'there is no ReplyTo'

                # time
                try:
                    mtime = time.mktime(email.utils.parsedate(msg_content['date']))
                except:
                    mtime = 'there is no date'
                

                # senderID
                temp = msg_content['from']
                try:
                    temp = email_pattern.findall(temp)
                    sen = temp[0].lower()
                    mSenderID = int(hashlib.sha1(sen).hexdigest(),16) 
                    
                except:
                    mSenderID = 'there is no From'

                    
                # receiversID
                # To field
                tList = []
                temp = msg_content['to']
                try:
                    temp = email_pattern.findall(temp)
                    for e in temp:
                        tList += [e.lower()]
                    mReceiversID = [int(hashlib.sha1(rec).hexdigest(),16) for rec in tList]
                except:
                    mReceiversID = 'there is no To'

                # CC field
                tList = []
                try:
                    temp = msg_content ['cc']
                    temp = email_pattern.findall(temp)
                    for e in temp:
                        tList += [e.lower()]
                    mCCID = [ int(hashlib.sha1(rec).hexdigest(),16) for rec in tList]
                except:
                    mCCID = 'there is no CC'


                if cnt_msgs % 1000 == 0:
                    logging.debug('Parsing message %d',cnt_msgs)

                            
                # msg = namedtuple('msg', 'ID, time, replyto, sender, receiver, cc')
                msg = Msg._make((mID, mtime, mReplyTo, mSenderID, mReceiversID, mCCID)) 

                
                heapq.heappush(log, (msg.time,msg)) # update the log
                seenMsgs += [mID] # update the list of duplicates

                cnt_msgs += 1        

        # when all messages for one user are parsed, store the log
        outputfile = parsedLogsFolder+userfolder
     
        # store data in hard drive
        f = open(outputfile+'.txt','w')

        # dump data
        while log:
            d = heapq.heappop(log)
            d = d[1]
            # Msg = namedtuple('Msg', 'ID, time, replyto, sender, receiver, cc')
            s = "%s\t%s\t%s\t%s\t%s\t%s" % (d.ID,d.time,d.replyto,d.sender,d.receiver,d.cc)
            f.write(s+'\n')

        f.close()


    return log


    

def main():

    processEnron()

    
if __name__ == "__main__":
    main()
