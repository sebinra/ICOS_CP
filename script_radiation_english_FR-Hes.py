#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 26 09:39:55 2019

take a datalogger table. 
adapt the table to fullfill ICOS requirement :
        use a correspondace table between the ICOS header nameand the datalloger header name
        generate TIMESTAMP in the correct format 
        complete missing row record
        upload the file to the carbon portal
        save a local copy of the file

currently works on  a linux or windows PC 

gestion du temps : 
    actuellement genere le fichier ICOS pour XX jours   (tous les jours disponibles)
    envoi le fichier pour le dernier jour. 
    
input files : 
    data logger file
    ICOS header file (with the columns in the same order than the data logger file)


@author: christophe.chipeaux@inra.fr
adapted by sebastien.lafont@inra.fr
adapted by jean-baptiste.lily@inra.fr for Hesse site
"""


#import matplotlib
#matplotlib.use('Agg')
import numpy as np
import pandas as pd  #use to read the data file, and complete  missing rows

#from datetime import datetime,timedelta

import os.path
from math import *
import matplotlib.pyplot as plt #pour la visualisation (http://pandas.pydata.org/pandas-docs/stable/visualization.html)

import time
import sys
import hashlib  # library need to create md5
import pycurl # library for curl 
from io import StringIO,BytesIO
from os.path import basename

def tail(filename, count=1, offset=1024):
    """
    A more efficent way of getting the last few lines of a file.
    Depending on the length of your lines, you will want to modify offset
    to get better performance.
    """
    f_size = os.stat(filename).st_size
    if f_size == 0:
        return []
    with open(filename, 'r',newline=None) as f:
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
    with open(filename, 'r',newline=None) as f:
        lines = [f.readline() for line in range(1, count+1)]
        #return filter(len, lines)
        return lines

#""" ADAPT THIS PART TO YOUR SITE """"
sitename="FR-Hes"
typefic="BM_"  # could be "ST_"

carbon_portal_user="FR-Hes"
carbon_portal_password="XXX"


#matplotlib.rc('figure', figsize=(10, 5))
monchemin='D:/Docs/Doc_technique/Projets/Hesse/Prog_Christophe_Cportal/' 
monchemin='/home/slafont/PYTHON/ICOS_CP/JB/Prog_Christophe_Cportal/'
  #'/home/cchipeaux/regional/donnees/sites/SALLES_ICOS/ANNEEENCOURS/icos/rayonnement/'
###monchemin='/media/slafont/ec323b73-9bf9-4119-8d2f-3ec27b0660e9/Dropbox (TLD_LOUSTAU)/Station de Bilos/data/rayonnement/'
monfichier=monchemin+'CR1000_E_fdhesse1_moyennes.dat'  #'CR3000_rayonnement_ICOS_RAY_20S.dat'
reference_table='L05_F01' #'L02_F01'

#monchemin_entete='D:/Docs/Doc_technique/Projets/Hesse/Prog_Christophe_Cportal/'  #'/media/slafont/ec323b73-9bf9-4119-8d2f-3ec27b0660e9/Dropbox (TLD_LOUSTAU)/Station de Bilos/STEP2/Envoi_fichier_exemple'
monchemin_entete=monchemin
monfichierentete=sitename+'_BMHEADER_201503181400_'+reference_table+'.csv'
fullheaderfile=os.path.join(monchemin_entete,monfichierentete)
# for python above 3.4 use patthlib
#from pathlib import Path
# datafolder=Path(monchemin_entete)
#fullheaderfile = data_folder / monfichierentete

#la table de corespondance est un fichier csv qui contient 2 lignes
# la premiere ligne contient les noms de variables ICOS (dans le meme ordre que dans le fichier HEADER ICOS)
# la deuxième ligne contient les noms de variables corresondant dans la table du logger. 
# les variables qui sont dans le logger mais pas dans la table de correspondance seront ignorées!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# le code utilise les noms de variables, pas l'ordre des colonnes.

monchemin_table_correspondance=monchemin_entete
monfichiercorrespondance=sitename+'_tablecorrespondance_'+reference_table+'.csv'
fullcorrespfile=os.path.join(monchemin_table_correspondance,monfichiercorrespondance)

#pathout="D:/Docs/Doc_technique/Projets/Hesse/Prog_Christophe_Cportal/" 
pathout=monchemin
###pathout="/media/slafont/MSATA/DATA/FR-BIL/DATA_FOR_CP/"
dossiersauvegarde="D:/Docs/Doc_technique/Projets/Hesse/Prog_Christophe_Cportal/Sauvegarde/"  #"/media/slafont/MSATA/DATA/FR-BIL/DATA_FOR_CP/"

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

# read ICOS header file
icos_header=pd.read_csv(fullheaderfile)

# read table de correspondance
tab_corresp=pd.read_csv(fullcorrespfile)

# version PC

#head using python
# par souci d'efficacité si le fichier source est gros
# Cette partie créer un fichier temporaire qui contient 
#seulement une partie (la fin) du fichier original.
# 11000 ligne avec une frequence de 20 s represente environ 2.5 jours 
# 11000*20/3600/24

tmpfile=head(monfichier, count=4)
tmpfile2=tail(monfichier, count=11000)
a=tmpfile+tmpfile2
with open(fichiertmp, 'w',newline=os.linesep) as f:
        f.writelines(a) 
        

# read short file (TMP file)
path7d=fichiertmp

# changed option :
# 25/11 change for better windows integration
# add lineterminator=os.linesep
# implys change parser to C 
# read all variables without index 
# the create index with TIMESTAMP column

#kw = dict( parse_dates=[0],dayfirst=True,squeeze=False,header=1, index_col=0, engine='c',skiprows=[2],lineterminator=os.linesep)
#kw = dict( parse_dates=[0],dayfirst=True,squeeze=False,header=1, index_col='TIMESTAMP', engine='c',skiprows=[2],lineterminator=os.linesep)
kw = dict( parse_dates=[0],dayfirst=True,squeeze=False,header=1, index_col=False, engine='c',skiprows=[3],lineterminator=os.linesep)
#df_data  = pd.read_csv('/Users/christophec/Desktop/profilco2.dat', **kw)
df_data  = pd.read_csv(path7d, **kw)
    
df_data.index=df_data.TIMESTAMP
unitdata = df_data[:1] # premiere ligne unité
df_data  = df_data[1:] # remove 1ere ligne
df_data  = df_data.drop_duplicates()
df_data = df_data[~df_data.index.duplicated(keep='last')]
df_data.index=df_data.index.astype('datetime64[ns]')
#
# remove TMP file once read
os.remove(path7d)

# compute sampling interval using the first 3 intervals
freqfile=pd.infer_freq(df_data.index)
print('frequency')
print(freqfile)
# if automatic detection do not work use manual detection
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
    

# generate date vector without gaps 
di = pd.date_range(start=df_data.index[0],
                      end=df_data.index[-1],
                      freq=freqfile)

df_data=df_data.reindex(di, fill_value='NaN')

# conserve uniquement les variables dans tab_corresp
#et uniquement dans l'order de tab_corresp
list_variable=[tab_corresp.values[0,i] for i in range(0,len(tab_corresp.columns))]
df1 =df_data[list_variable]

#remplace les entetes de df1 par les entetes officiels ICOS
df1.columns=tab_corresp.columns
df1=df1.drop(columns='TIMESTAMP')
df1.index.name='TIMESTAMP'
# get 1st and last date in the subset file

datetimedepart=df1.index[1].strftime('%Y-%m-%d %H:%M:%S')
datefinfichierjour=df1.index[-1].strftime('%Y-%m-%d %H:%M:%S')

# create a daily index
indexbyday=df1.resample('D').sum().index
indexbyday=indexbyday.strftime('%Y-%m-%d %H:%M:%S')
nbfichier=len(indexbyday)-2   #on enleve le premier et le dernier

#print(len(fichierjour))
# skip the first day of the extraction that could be incomplete
# start i at one

#for i in range(1, nbfichier+1):
# to test
for i in range(1, nbfichier):
    fichierjour=df1.loc[indexbyday[i]:indexbyday[i+1]]
    print('length=',len(fichierjour))
    datefic=fichierjour.index[0].strftime('%Y%m%d')
    print('date=',datefic)
    fichierjour.index=fichierjour.index.strftime(formatdate)
    
    namefichiercsv=entetefic+typefic+datefic+extension+".csv"
    fichiericos=pathout+namefichiercsv
    fichierjour.to_csv(fichiericos,index_label='TIMESTAMP')
    
# send the new file 
# in this configuration only the last one. 
# add the following part in the for loop to send all the generated files    

#md5=hashlib.md5(open(fichiericos,'rb').read()).hexdigest()
## old upload (unix)
#commandecurl="curl --upload-file "+fichiericos+" https://"+carbon_portal_user+":"+carbon_portal_password+"@data.icos-cp.eu/upload/etc/"+md5+"/"+namefichiercsv
##os.system(commandecurl)  # to be UNCOMMENTED 
#
##universal upload (windows and unix)
#url="https://"+carbon_portal_user+":"+carbon_portal_password+"@data.icos-cp.eu/upload/etc/"+md5+"/"+namefichiercsv
#
## the equivalent command of curl upload-file is UPLOAD (put command)
#
#c = pycurl.Curl()
#c.setopt(c.VERBOSE, True)
#c.setopt(c.UPLOAD, 1)
#c.setopt(c.URL, url)
##c.setopt(c.HTTPHEADER,['Content-Type:text/csv'])
##c.setopt(c.HTTPPOST, [('title', 'test'), (('file', (c.FORM_FILE, fichiericos)))])
#file=open(fichiericos)
#c.setopt(c.READDATA, file)
##c.setopt(c.HTTPPOST, [('fileupload',(c.FORM_FILE, fichiericos))])
#
#bodyOutput = BytesIO()
#headersOutput = StringIO()
#c.setopt(c.WRITEFUNCTION, bodyOutput.write)
##c.setopt(c.HEADERFUNCTION, headersOutput.write)
#c.perform()
##
#print(bodyOutput.getvalue().decode('UTF-8'))
#c.close()

# *******************************************
#last part : backup of the generated and uploaded files on local computer 
print('sauvegarde fichier\n')
commandesauvegarde="rsync --remove-source-files -avz "+fichiericos+" "+dossiersauvegarde
#os.system(commandesauvegarde)
#commandesup="rm "+pathout+"*"+namefichiercsv[-12:]
#os.system(commandesup)
