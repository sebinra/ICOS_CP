#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 26 09:39:55 2019

take a datalogger table. 
adapt the table to fullfill ICOS requirement :
        generate TIMESTAMP in the correct format 
        complete missing row record
        upload the file to the carbon portal
        save a local copy of the file

currently work only on a linux PC (use of unix command)

gestion du temps : 
    actuellement genere le fichier ICOS pour XX jours   (tous les jours disponibles)
    envoi le fichier pour le dernier jour. 
    
input files : 
    data logger file
    ICOS header file (with the columns in the same order than the data logger file)


@author: christophe.chipeaux@inra.fr
adapted by sebastien.lafont@inra.fr
"""




import matplotlib
#matplotlib.use('Agg')
import pandas as pd  #use to read the data file, and complete  missing rows
import numpy as np
from datetime import datetime,timedelta
import numpy
import os.path
from math import *
import matplotlib.pyplot as plt #pour la visualisation (http://pandas.pydata.org/pandas-docs/stable/visualization.html)
import time
import csv
import sys
from os.path import basename
from matplotlib.ticker import AutoMinorLocator
from matplotlib.ticker import MultipleLocator
from matplotlib.dates import DayLocator, HourLocator, DateFormatter, drange
import hashlib  # library need to create md5

def tail(filename, count=1, offset=1024):
    """
    A more efficent way of getting the last few lines of a file.
    Depending on the length of your lines, you will want to modify offset
    to get better performance.
    """
    f_size = os.stat(filename).st_size
    if f_size == 0:
        return []
    with open(filename, 'r') as f:
        if f_size <= offset:
            offset = int(f_size / 2)
        while True:
            seek_to = min(f_size - offset, 0)
            f.seek(seek_to)
            lines = f.readlines()
            # Empty file
            if seek_to <= 0 and len(lines) == 0:
                return []
            # count is larger than lines in file
            if seek_to == 0 and len(lines) < count:
                return lines
            # Standard case
            if len(lines) >= (count + 1):
                return lines[count * -1:]



def head(filename, count=1):
    """
    This one is fairly trivial to implement but it is here for completeness.
    """
    with open(filename, 'r') as f:
        lines = [f.readline() for line in range(1, count+1)]
        #return filter(len, lines)
        return lines

#""" ADAPT THIS PART TO YOUR SITE """"
sitename="FR-Bil"

carbon_portal_user="FR-Bil"
carbon_portal_password="XXX"
#matplotlib.rc('figure', figsize=(10, 5))
monchemin='/home/cchipeaux/regional/donnees/sites/SALLES_ICOS/ANNEEENCOURS/icos/rayonnement/'
monchemin='/media/slafont/ec323b73-9bf9-4119-8d2f-3ec27b0660e9/Dropbox (TLD_LOUSTAU)/Station de Bilos/data/rayonnement/'
monfichier=monchemin+'CR3000_rayonnement_ICOS_RAY_20S.dat'


monchemin_entete='/media/slafont/ec323b73-9bf9-4119-8d2f-3ec27b0660e9/Dropbox (TLD_LOUSTAU)/Station de Bilos/STEP2/'
monfichierentete=sitename+'_BMHEADER_201711010000_L02_F01.csv'

pathout="/home/cchipeaux/regional/donnees/sites/SALLES_ICOS/ANNEEENCOURS/tmp/"
pathout="/media/slafont/MSATA/DATA/FR-BIL/DATA_FOR_CP"
dossiersauvegarde="/media/slafont/MSATA/DATA/FR-BIL/DATA_FOR_CP"

#""" DO NOT CHANGE AFTER THIS POINT """

entetefic=monfichierentete[0:7]  #FR_Bil_
extension=monfichierentete[-12:-4] #_L10_F01

#fichier entree

name=basename(monfichier)
path=os.path.dirname(monfichier)
path2=os.path.abspath(monfichier)

#fichier sortie

fichiercsv=path2[:-3]+"csv"
fichiertmp=path2[:-3]+"tmp"

# version PC
# transfere l'entete dans un fichier temporaire
#macommande="head -4 "+monfichier+" > \'"+fichiertmp+"\'"
#os.system(macommande)
# recupere la fin du fichier data dans le fichier tmp (pour extraire la derniere journée)
#macommande="tail -n 11000 "+monfichier+" >> \'"+fichiertmp+"\'"
#os.system(macommande)

#head using python
# par souci d'efficacité si le fichier source est gros
# Cette partie créer un fichier temporaire qui contient 
#seulement une partie (la fin) du fichier original.
# 11000 ligne avec une frequence de 20 s represente environ 2.5 jours 
# 11000*20/3600/24


tmpfile=head(monfichier, count=4)
tmpfile2=tail(monfichier, count=11000)
a=tmpfile+tmpfile2
with open(fichiertmp, 'w') as f:
        f.writelines(a) 
        

#pour creer chaque jour le fichier
#macommande="cat "+monfichier+" > \'"+fichiertmp+"\'"

# lecture du fichier TMP
path7d=fichiertmp
kw = dict( parse_dates=[1],dayfirst=True,squeeze=False,header=1, index_col=0, engine='python',skiprows=[2])
#df_data  = pd.read_csv('/Users/christophec/Desktop/profilco2.dat', **kw)
df_data  = pd.read_csv(path7d, **kw)
unitdata = df_data[:1] # premiere ligne unité
df_data  = df_data[1:] # remove 1ere ligne
df_data  = df_data.drop_duplicates()
df_data = df_data[~df_data.index.duplicated(keep='last')]
df_data.index=df_data.index.astype('datetime64[ns]')

# compute sampling interval using the first 3 intervals
freqfile=pd.infer_freq(df_data.index)
print('frequency')
if freqfile[-1]!='S':
    p1=df_data.index[2]-df_data.index[1]
    p2=df_data.index[3]-df_data.index[2]
    p3=df_data.index[4]-df_data.index[3]
    l=[p1,p2,p3]    
    l = sorted(l)
    #keep the median interval
    freqint=l[2].seconds
else:
    freqint=int(freqfile[0:-1])
print('1er freqint=',freqint)

if freqint >0 and freqint < 60:
    formatdate='%Y%m%d%H%M%S'
if freqint >=60:
    formatdate='%Y%m%d%H%M'
    
#freqfile=str(freqint)+"S"

# generate date vector without gaps 
di = pd.date_range(start=df_data.index[0],
                      end=df_data.index[-1],
                      freq=freqfile)

df_data=df_data.reindex(di, fill_value='NaN')

# remove 2 first columns : RECORD and icostimestamps
# same index than df_data;
df1 = df_data.iloc[:,2:]


#df1.index=df1.index.astype('datetime64[ns]')
#df1.index= df1.index.strftime('%Y%m%d%H%M')
#on recupere la premiere date et ka derniere date 
#Mydt=df1.index[1]
datetimedepart=df1.index[1].strftime('%Y-%m-%d %H:%M:%S')
datefinfichierjour=df1.index[-1].strftime('%Y-%m-%d %H:%M:%S')

# creer un index journalier
indexbyday=df1.resample('D').sum().index
indexbyday=indexbyday.strftime('%Y-%m-%d %H:%M:%S')
nbfichier=len(indexbyday)-2   #on enleve le premier et le dernier

#fichierjour=df1.loc[datetimedepart:indexbyday[1]]
#fichierjour.index=fichierjour.index.strftime(formatdate)
#datefic=df1.index[1].strftime('%Y%m%d')
#nbfichier=len(ndexbyday)-2   #on enleve le premier et le dernier
#namefichiercsvtmp=entetefic+datefic+extension+".tmp"
#namefichiercsv=entetefic+datefic+extension+".csv"
#fichierjour.to_csv(pathout+namefichiercsvtmp)
#macommande="head -1 "+monfichierentete+" > \'"+pathout+namefichiercsv+"\'"
#os.system(macommande)
#macommande="tail -n +2 "+pathout+namefichiercsvtmp+" >> \'"+pathout+namefichiercsv+"\'"
#os.system(macommande)
#macommande3="perl -pi -e 's/\\r\\n|\\n|\\r/\\r\\n/g' \'" +pathout+namefichiercsv+"\'"  #conversionunix to dos
#os.system(macommande3)
#macommande="rm "+pathout+namefichiercsvtmp
#os.system(macommande)

#print(len(fichierjour))
for i in range(0, nbfichier+1):
    fichierjour=df1.loc[indexbyday[i]:indexbyday[i+1]]
    print(len(fichierjour))
    datefic=fichierjour.index[0].strftime('%Y%m%d')
    print(datefic)
    fichierjour.index=fichierjour.index.strftime(formatdate)
    namefichiercsvtmp=entetefic+datefic+extension+".tmp"
    namefichiercsv=entetefic+datefic+extension+".csv"
    fichierjour.to_csv(pathout+namefichiercsvtmp)
    # ecrit le fichier en 2 parties 
    # 1) entete
    # 2) append data
    # 3 conversion unix to dos 
    macommande="head -1 "+monchemin_entete+'/'+monfichierentete+" > \'"+pathout+namefichiercsv+"\'"
    os.system(macommande)
    macommande="tail -n +3 "+pathout+namefichiercsvtmp+" >> \'"+pathout+namefichiercsv+"\'"
    os.system(macommande)
#    if sys.platform=='linux':
#        macommande3="perl -pi -e 's/\\r\\n|\\n|\\r/\\r\\n/g' \'" +pathout+namefichiercsv+"\'"  #conversionunix to dos
#        os.system(macommande3)

    macommande="rm "+pathout+namefichiercsvtmp
    os.system(macommande)

#TO DO PRINT CORRESPONDANCE OLD ENTETE NEW ENTETE

# envoie le nouveau fichier 
fichiericos=pathout+namefichiercsv
md5=hashlib.md5(open(fichiericos,'rb').read()).hexdigest()
commandecurl="curl --upload-file "+fichiericos+" https://"+carbon_portal_user+":"+carbon_portal_password+"@data.icos-cp.eu/upload/etc/"+md5+"/"+namefichiercsv
#os.system(commandecurl)  # to be UNCOMMENTED 

# fait une sauvegarde
commandesauvegarde="rsync --remove-source-files -avz "+fichiericos+" "+dossiersauvegarde
os.system(commandesauvegarde)
#commandesup="rm "+pathout+"*"+namefichiercsv[-12:]
#os.system(commandesup)
