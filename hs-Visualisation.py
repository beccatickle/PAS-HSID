import folium
from folium import plugins
from folium.plugins import HeatMap

import pandas as pd

hsPath = "hs-delTh19-dr3-HB-4hrs.csv"
hsDelim = "\t"

print('Loading hotspots...')
df = pd.read_csv(hsPath,sep=hsDelim,float_precision='round_trip',index_col=False,header=0)
dfForPlot = df.drop(columns=['Age','Road','Bearing'])

hsMap = folium.Map(location=[54.7, -4.36], zoom_start=6, tiles='OpenStreetMap')

plugins.Fullscreen(
    position='topright',
    title='Fullscreen',
    title_cancel='Exit',
    force_separate_button=True
).add_to(hsMap)

dfForPlot['Latitude'] = dfForPlot['Latitude'].astype(float)
dfForPlot['Longitude'] = dfForPlot['Longitude'].astype(float)

print('Creating map...')

## for static map ##
# heatmapData = [[row['Latitude'],row['Longitude']] for index, row in dfForPlot.iterrows()]
# HeatMap(heatmapData).add_to(hsMap)

## for animated map ##
heatmapData = [[[row['Latitude'],row['Longitude']] for index, row in dfForPlot[dfForPlot['DayID'] == i].iterrows()] for i in range(1,559)]
heatmapData = list(filter(lambda x: len(x) > 30,heatmapData))
hm = plugins.HeatMapWithTime(heatmapData,auto_play=True,max_opacity=0.8)
hm.add_to(hsMap)

hsMap.save('hsMap.html')

