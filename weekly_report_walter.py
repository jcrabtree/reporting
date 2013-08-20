# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>
# Electricity Authority Weekly Report

import matplotlib as mpl
mpl.use('Agg')
import datetime as dt
from datetime import datetime, date, timedelta
import os
import pandas as pd
import matplotlib.pyplot as plt
import EAtools as ea

ea.set_options()
ea.ea_report_style()
connection = ea.DW_connect(linux=True)
mpl.rcParams['font.size'] = 24   
mpl.rcParams['xtick.labelsize']=20            #setting tick label size
mpl.rcParams['ytick.labelsize']=20 
mpl.rcParams['legend.fontsize']=20

# Hydro Risk Curves and current storage situation
path = '''/home/dw/comit/data'''
os.chdir(path)

connection = ea.DW_connect(linux=True)

print "Getting COMIT and HRC data"
inflow_file = os.path.join('/home','dw','comit','data','inflows.pickle')
storage_file =os.path.join('/home','dw','comit','data','storage.pickle')
ea_hrc = os.path.join('/home','dw','comit','data','hydroriskcurves','HRC_History_20130618.csv')   
so_hrc ='''http://www.systemoperator.co.nz/f3933,83729137/HRC_data_2013_2014.xlsx'''
    
inflows,storage = ea.get_comit_data(inflow_file,storage_file)

NZ =     ea.panel_beater(storage.sum(axis=1),inflows.sum(axis=1),365)
Taupo =  ea.panel_beater(storage['Taupo'],inflows['Taupo'],365)
Tekapo = ea.panel_beater(storage['Tekapo'],inflows['Tekapo'],365)
Pukaki = ea.panel_beater(storage['Pukaki'],inflows['Pukaki'],365)
Hawea =  ea.panel_beater(storage['Hawea'],inflows['Hawea'],365)
TeAnauManapouri = ea.panel_beater(storage['TeAnau']+storage['Manapouri'],inflows['TeAnau']+inflows['Manapouri'],365)

NZ_HRC_SO,SI_HRC_SO = ea.get_SO_HRC(so_hrc)
NZ_HRC,SI_HRC = ea.get_EA_HRC(ea_hrc)
SI_HRC2 = SI_HRC_SO['6/2013':].append(SI_HRC['12/2012':'5/2013'])
NZ_HRC2 = NZ_HRC_SO['6/2013':].append(NZ_HRC['12/2012':'5/2013'])

history_repeats = ea.calc_mean_percentile(storage.sum(axis=1),80,future=True)
history_repeats = history_repeats['12/'+str(dt.datetime.now().year-1):'2/1/'+str(dt.datetime.now().year+1)]

path = '''/home/dave/python/reporting/'''
os.chdir(path)


p=ea.hrc_plot(1,SI_HRC2['12/'+str(dt.datetime.now().year-1):'2/1/'+str(dt.datetime.now().year+1)],\
         history_repeats['Actual'],\
         history_repeats['mean'],'figures/hrc.pdf')

p=ea.hrc_plot(1,SI_HRC2['12/'+str(dt.datetime.now().year-1):'2/1/'+str(dt.datetime.now().year+1)],\
         history_repeats['Actual'],\
         history_repeats['mean'],'figures/hrc.png')

print "Printing Hydrology data to " + path + "/figures"

ea.hydrology_plot(2,NZ,'figures/nz.pdf')
ea.hydrology_plot(2,NZ,'figures/nz.png')

ea.hydrology_plot(3,Taupo,'figures/taupo.pdf')
ea.hydrology_plot(3,Taupo,'figures/taupo.png')

ea.hydrology_plot(4,Tekapo,'figures/tekapo.pdf')
ea.hydrology_plot(4,Tekapo,'figures/tekapo.png')

ea.hydrology_plot(5,Pukaki,'figures/pukaki.pdf')
ea.hydrology_plot(5,Pukaki,'figures/pukaki.png')

ea.hydrology_plot(6,Hawea,'figures/hawea.pdf')
ea.hydrology_plot(6,Hawea,'figures/hawea.png')

ea.hydrology_plot(7,TeAnauManapouri,'figures/teanaumanapouri.pdf')
ea.hydrology_plot(7,TeAnauManapouri,'figures/teanaumanapouri.png')


print "Getting Meridian snow picture"
G = ea.get_web_pics()
G.get_snow_pic()
print "Getting NIWA Rainfall outlook"
G.get_niwa_pic()

print "Getting pricing info from DW - THIS IS SLOW!"
lwaps = ea.get_lwaps(connection)
ea.plot_lwap(8,lwaps,path + '/figures/lwap.pdf')
ea.plot_lwap(8,lwaps,path + '/figures/lwap.png')

# Hedge Market
print "Getting Hedge Market data"
os.chdir('/home/dave/python/asx')

CQ=ea.current_quarter()
#Convert hdf5 data to pickled data 
BEN_ZFT = pd.read_hdf('asx_futures.h5','BEN_ZFT')
OTA_ZFT = pd.read_hdf('asx_futures.h5','OTA_ZFT')
BEN_ZFT.to_pickle('BEN_ZFT.pickle')
OTA_ZFT.to_pickle('OTA_ZFT.pickle')
#Read the pickled data
ben = pd.read_pickle('BEN_ZFT.pickle')
ota = pd.read_pickle('OTA_ZFT.pickle')
#Convert to quarters
ben.axes[1] = pd.period_range(ben.axes[1][0], ben.axes[1][-1], freq='Q')
ota.axes[1] = pd.period_range(ota.axes[1][0], ota.axes[1][-1], freq='Q')

#Get Spread data and convert to pickled data 
benmore = pd.read_hdf('futures_spread_ben.h5','Benmore')
otahuhu = pd.read_hdf('futures_spread_ota.h5','Otahuhu')
benmore.to_pickle('benmore.pickle')
otahuhu.to_pickle('otahuhu.pickle')
#Read the pickled data
benmore = pd.read_pickle('benmore.pickle')
otahuhu = pd.read_pickle('otahuhu.pickle')

#Get OTA and BEN prices for current quarter
connection = ea.DW_connect(linux=True)
prices = ea.timeseries_convert( \
     ea.FP_getter(connection, \
     ea.query_prices(ea.current_quarter().start_time,datetime.now().date(),nodelist=['BEN2201','OTA2201'])))
#Calculate expanding mean over the quarter
spot_OTA = prices.price.OTA2201.groupby(lambda x: x.date()).mean()
spot_BEN = prices.price.BEN2201.groupby(lambda x: x.date()).mean()
spot_OTA_EXMEAN = pd.expanding_mean(spot_OTA) #Otahuhu expanding mean
spot_BEN_EXMEAN = pd.expanding_mean(spot_BEN) #Benmore expanding mean

OTA_BA = ea.CQ_data(otahuhu,ota,spot_OTA_EXMEAN,CQ,'otahuhu')
BEN_BA = ea.CQ_data(benmore,ben,spot_BEN_EXMEAN,CQ,'benmore')

print "Printing Hedge Market data"

ea.forward_price_curve(9,ota,"Reds",path + '/figures/ota_fpc.pdf')
ea.forward_price_curve(9,ota,"Reds",path + '/figures/ota_fpc.png')
ea.forward_price_curve(10,ben,"Blues",path + '/figures/ben_fpc.pdf')
ea.forward_price_curve(10,ben,"Blues",path + '/figures/ben_fpc.png')
ea.bid_ask_plot(11,OTA_BA,BEN_BA,path + '/figures/cq_trend.pdf')
ea.bid_ask_plot(11,OTA_BA,BEN_BA,path + '/figures/cq_trend.png')
ea.plot_monthly_volumes(12,ota,ben,path + '/figures/asx_volumes.pdf')
ea.plot_monthly_volumes(12,ota,ben,path + '/figures/asx_volumes.png')
ea.plot_open_interest(13,ota,ben,path + '/figures/asx_opint.pdf')
ea.plot_open_interest(13,ota,ben,path + '/figures/asx_opint.png')
mpl.rcParams['xtick.labelsize']=16            #setting tick label size
ota_sum,ota_win = ea.filter_last_year(ota,CQ)
ben_sum,ben_win = ea.filter_last_year(ben,CQ)
ea.plot_last_year(14,ota_sum,ota_win,path + '/figures/asx_ota_year.pdf')
ea.plot_last_year(14,ota_sum,ota_win,path + '/figures/asx_ota_year.png')
ea.plot_last_year(15,ben_sum,ben_win,path + '/figures/asx_ben_year.pdf')
ea.plot_last_year(15,ben_sum,ben_win,path + '/figures/asx_ben_year.png')

ben_minus_ota_sum = {}
ben_minus_ota_win = {}

if ota_sum['Current quarter'] is not None:
    ben_minus_ota_sum['Current quarter'] = ben_sum['Current quarter']-ota_sum['Current quarter']
ben_minus_ota_sum['Future quarters'] = ben_sum['Future quarters']-ota_sum['Future quarters']
ben_minus_ota_sum['Past quarters'] = ben_sum['Past quarters']-ota_sum['Past quarters']

if ota_win['Current quarter'] is not None:
    ben_minus_ota_win['Current quarter'] = ben_win['Current quarter']-ota_win['Current quarter']
ben_minus_ota_win['Future quarters'] = ben_win['Future quarters']-ota_win['Future quarters']
ben_minus_ota_win['Past quarters'] = ben_win['Past quarters']-ota_win['Past quarters']

ea.plot_last_year(16,ben_minus_ota_sum,ben_minus_ota_win,path + '/figures/asx_ben_minus_ota_year.pdf')
ea.plot_last_year(16,ben_minus_ota_sum,ben_minus_ota_win,path + '/figures/asx_ben_minus_ota_year.png')




