import matplotlib.pyplot as plt 

from db import *

df_plot=df_serie_calcolate[df_serie_calcolate["Serie calcolata"].str.contains("Totale produttività NAZIONALE")]
print(df_plot)

x=df_plot['Anno']
y=df_plot['Valore']

plt.bar(x, y, color ='maroon', 
        width = 0.4)

plt.xlabel("Anno")
plt.ylabel("Produttività in migliaia di euro")
plt.title("Totale produttività NAZIONALE per anno")
plt.show()
