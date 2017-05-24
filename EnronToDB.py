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
from math import log


from operator import itemgetter
from itertools import groupby

from collections import Counter

from cPickle import *

from scipy.stats import gamma
from numpy import zeros
import numpy

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


def enronToPeriod():

####################################################################
##        SAMPLE LINE FROM PARSED ENRON DATA 
##
##        Message-ID\ttime\tfrom\tto\tCC\tBCC 
####################################################################

    # create/open dir in which logs are be stored
    usersDataFolder = 'data/'

    # create file in which DB will be stored
    DBFile = 'data/DB.dat'
    fDB = open(DBFile, 'w')
    DB = []
    DBFile = 'data/DBzero.dat'
    fDBzero = open(DBFile, 'w')
    DBzero = []

    cntUsers = 0

    files = os.listdir(usersDataFolder)

    gapsSize = zeros([150])
    gapXaxis = arange(1,151)
    continuinousSize = zeros([100])
    continuinousXaxis = arange(1,101)

    lens = []

    totZeroUp = 0
    totZeroZero = 0
    totUpZero = 0
    totUpUp = 0
    

    for nU, userFile in enumerate(files):
        if '.dat' in userFile and not 'DB' in userFile:
##            print "Parsing file %s " % userFile
            fUser = file(usersDataFolder+userFile, "r")
            dataUser = load(fUser)
        else:
            continue

        distinctReceivers = dataUser[0]
        userPeriods = dataUser[1:]


        recsPerPeriod=[[0]*len(userPeriods) for i in xrange(len(distinctReceivers))]
        timeFirstMessagePeriod=[[0]*len(userPeriods) for i in xrange(len(distinctReceivers))]
        maxMsgPeriod = 0

        temp = [] # container for trace with users in a continuous 
        for period, tuples in userPeriods:
            for (r,c,times) in tuples:
                recsPerPeriod[r][period]+=c
                timeFirstMessagePeriod[r][period]=times[0]
                
                if c > maxMsgPeriod:
                    maxMsgPeriod = c
        
  
        if maxMsgPeriod > 15:
            print "\nSen: %s---------------" % userFile[:-4]
        for rec,history in enumerate(recsPerPeriod):

            # zero -> greater
            zeroUp = [(i,i+1) for i,c in  enumerate(history[:-2]) if ((c <=5) and (history[i+1] > 5))]
            # zero -> zero
            zeroZero = [(i,i+1) for i,c in  enumerate(history[:-2]) if ((c <=5) and (history[i+1] <=5 ))]
            # greater -> zero
            UpZero = [(i,i+1) for i,c in  enumerate(history[:-2]) if ((c > 5) and (history[i+1] <=5 ))] 
            # greater -> greater      
            UpUp = [(i,i+1) for i,c in  enumerate(history[:-2]) if ((c > 5) and (history[i+1] > 5))]

            # total transitions
            allTransitions =  [(i,i+1) for i,c in  enumerate(history[:-2]) ]

            totZeroUp += len(zeroUp)
            totZeroZero += len(zeroZero)
            totUpZero += len(UpZero)
            totUpUp += len(UpUp)
            
            
            # lambda > 0
            nonzero = [i for i,c in  enumerate(history) if c > 0]
##            print "sen=%s NZ=%s" % (userFile[:-4],nonzero)
            # study gap size
            gaps = [c-nonzero[i]-1 for i,c in enumerate(nonzero[1:])]
            gapsSize[gaps] += 1

            # study contionuous periods
            if nonzero: 
                for k, g in groupby(enumerate(nonzero), lambda (i,x):i-x):
                    # g is a group of continuous epoch with nonzero messages
                    g = map(itemgetter(1), g)
                    cnt = len(g)
                    continuinousSize[cnt] += 1
                    if cnt >= 5:
                        v = [(i,c) for i,c in enumerate(history) if c > 0 and i in g]
##                        print "NZ - Period=%d (%f) - Rec: %d  - len=%d" % (g[0],timeFirstMessagePeriod[rec][g[0]],  rec, len(v))
                        lens += [len(v)]                            
                        DB += [(userFile[:-4], rec, g[0], len(v))] # (userFile, receiver, starting period, length period)
        


            # lambda = 0
            zero = [i for i,c in enumerate(history) if c == 0]
##            print "sen=%s Z=%s" % (userFile[:-4], zero)

            # study contionuous periods
            if zero: 
                for k, g in groupby(enumerate(zero), lambda (i,x):i-x):
                    # g is a group of continuous epoch with nonzero messages
                    g = map(itemgetter(1), g)
                    cnt = len(g)
                    if cnt >= 5:
                        v = [(i,c) for i,c in enumerate(history) if c == 0 and i in g]
##                        print "Z - Period=%d (%f) - Rec: %d  - len=%d" % (g[0],timeFirstMessagePeriod[rec][g[0]],  rec, len(v))
                        lens += [len(v)]                            
                        DBzero += [(userFile[:-4], rec, g[0], len(v))] # (userFile, receiver, starting period, length period)


    allall = totZeroUp + totZeroZero + totUpZero + totUpUp
    allzero = totZeroUp +totZeroZero
    allup = totUpZero +totUpUp
    print "all=%d  allZero=%d  allUp=%d" % (allall, allzero, allup)
    print "zeroUp=%d\t\tPr[zeroUp]=%f (log=%f)" % (totZeroUp, float(totZeroUp)/allzero, log(float(totZeroUp)/allzero))
    print "zeroZero=%d\tPr[zeroZero]=%f (log=%f)" % (totZeroZero, float(totZeroZero)/allzero, log(float(totZeroZero)/allzero))
    print "UpZero=%d\t\tPr[UpZero]=%f (log=%f)" % (totUpZero, float(totUpZero)/allup, log(float(totUpZero)/allup))
    print "UpUp=%d\t\tPr[UpUp]=%f (log=%f)" % (totUpUp, float(totUpUp)/allup, log(float(totUpUp)/allup))
    print ""

    # Gap size
    print "\n=========================================================="
    print "GAPS -- There are:"
    print "%d gaps in total(ALL USERS)" % sum(gapsSize)
    print "%d gaps of zero epochs" % gapsSize[1]    
    print "%d gaps of more than 5 epochs" % sum(gapsSize[5:])
    print "%d gaps of more than 10 epochs" % sum(gapsSize[10:])
    print "%d gaps of more than 15 epochs" % sum(gapsSize[15:])
    print "%d gaps of more than 20 epochs" % sum(gapsSize[20:])
    print "%d gaps of more than 25 epochs" % sum(gapsSize[25:])
    print "%d gaps of more than 30 epochs" % sum(gapsSize[30:])
    print "Maximum gap %d" % numpy.nonzero(gapsSize)[0][-1]
    print "============================================================"

    normalizedGapSize = [float(g)/sum(gapsSize) for g in gapsSize]

    figure()
    ax = subplot(111)
    tit= "Periods between silences" 
    plt.title(tit)
    plt.plot(gapXaxis,normalizedGapSize)
    ax.set_xlabel("Gap size (number of epochs)")
    ax.set_ylabel("Pr[Gap size]")
    filename= usersDataFolder+"gapSize" 
    savefig(filename+'.png', bbox_inches=0)
##    plt.show()



    # Continuous size
    print "\n=========================================================="
    print "CONTINUOUS -- There are:"
    print "%d periods in total (ALL USERS)" % sum(continuinousSize)
    print "%d periods of more than 5 epochs" % sum(continuinousSize[5:])
    print "%d periods of more than 10 epochs" % sum(continuinousSize[10:])
    print "%d periods of more than 15 epochs" % sum(continuinousSize[15:])
    print "%d periods of more than 20 epochs" % sum(continuinousSize[20:])
    print "%d periods of more than 25 epochs" % sum(continuinousSize[25:])
    print "%d periods of more than 30 epochs" % sum(continuinousSize[30:])
    print "Maximum length %d" % numpy.nonzero(continuinousSize)[0][-1]
    print "============================================================"



    normalizedContinuinousSize = [float(g)/sum(continuinousSize) for g in continuinousSize]

    cdf = []
    acc = 0
    for i,g in enumerate(normalizedContinuinousSize):
        acc+=g
        cdf += [acc]
        

    figure()
    ax = subplot(111)
    tit= "CDF length of continuous periods with messages"
    plt.title(tit)
    plt.plot(range(len(cdf)),cdf)
    ax.set_xlabel("Number of periods")
    ax.set_ylabel("Pr[Number of periods]")
    filename= usersDataFolder+"continuousSize" 
    savefig(filename+'.png', bbox_inches=0)
##    plt.show()


    dump(DB, fDB)
    fDB.close()
    dump(DBzero, fDBzero)
    fDB.close()

def main():
    
    enronToPeriod()

    
if __name__ == "__main__":
    main()
