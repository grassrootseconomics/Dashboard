import numpy as np
import pandas as pd
from sklearn import preprocessing
import matplotlib.pyplot as plt
import matplotlib
matplotlib.pyplot.ion()
matplotlib.use('TKAgg',warn=False, force=True)

df = pd.read_csv('lat-lon-bal-only.csv')
df.head()
min_max_scaler = preprocessing.MinMaxScaler()
#BBox = ((df.longitude.min(),   df.longitude.max(),
#         df.latitude.min(), df.latitude.max()))

BBox = (33.245, 41.067, -5.025,2.087 )

print(BBox)

ruh_m = plt.imread('map4.png')
fig, ax = plt.subplots(figsize = (8,7))
ax.scatter(df.longitude, df.latitude, zorder=1, alpha= 0.5, c='b', s=200*min_max_scaler.fit_transform(df[['balance']]))
ax.set_title('Sarafu Holding Communities by Total Balances')
ax.set_xlim(BBox[0],BBox[1])
ax.set_ylim(BBox[2],BBox[3])
ax.imshow(ruh_m, zorder=0, extent = BBox, aspect= 'equal')
plt.savefig('out.png',dpi=600)#, bbox_inches='tight'