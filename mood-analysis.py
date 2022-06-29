from collections import namedtuple
import pandas as pd, numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from song_features import build_df

sns.set(font='georgia')
sns.set_style("ticks")

#build_df()

df = pd.read_csv('output/mood.csv', index_col = 0)

df['secPlayed'] = df['msPlayed'] / 1000
df = df[df.columns[:-1].insert(4, df.columns[-1])]
df = df[df.secPlayed > 60]

df['month'] = df.endTime.str.split('-').apply(lambda x: (x[0], x[1]))
months = list(set(df.month.values))
months.sort()

features = ['danceability', 'energy', 'instrumentalness', 'loudness', 
            'speechiness', 'tempo', 'valence']
for feature in features:
    df[f'{feature}_zscore'] = ( df[feature] - df[feature].mean() ) / df[feature].std()
df[[feature + '_zscore' for feature in features]].describe().loc['mean':'std'].T

Month = namedtuple('Month', features)
avg_features_months = []
for month in months:
    df_month = df[df['month'] == month]
    avg_features = df_month.describe().loc['mean'][[feature + '_zscore' for feature in features]]
    month = Month(*avg_features)
    avg_features_months.append(month)

month_labels = [f'{month[1]}/{month[0]}' for month in months]
month_labels_short = [m[:3]+m[-2:] for m in month_labels]


def features_sns(features):
    fig, ax = plt.subplots(figsize = (10,5))
    x = [x for x in range(14)]

    for feature in features:
        y = [getattr(month, feature) for month in avg_features_months]
        fig = sns.lineplot(x,y, label=feature, linewidth=6, alpha=.7, marker='o', markersize=15)
    
    ax.set_xticks([x for x in range(14)])
    ax.set_xticklabels(labels=month_labels_short, rotation=45, ha='right', size=10)

    for tick in ax.yaxis.get_major_ticks():
                tick.label.set_fontsize(10) 

    leg = ax.legend(loc = 'upper left', bbox_to_anchor=(.85,1), prop={'size': 10})

    for line in leg.get_lines():
        line.set_linewidth(10)
    
    ax.set_title('Normalized mood (according to Spotify)', size = 15, pad = 10, fontname = 'sans-serif')

    return ax


features_sns(['valence', 'energy'])
x = [-1] + [x for x in range(13)]
alpha = .25
x_col = -.5
y_col = 2
y_text = 1.25
plt.ylim([-.5, 2])
plt.xlim([0, 12])

plt.fill_between(x[:3], x_col, y_col, alpha=alpha)
plt.fill_between(x[2:8], x_col, y_col, alpha=alpha)
plt.fill_between(x[7:], x_col, y_col, alpha=alpha)

plt.annotate('Bachelor\n Thesis', (0.1, y_text), size=10)
plt.annotate(' Working\nFull-Time', (2, y_text), size=10,)
plt.annotate('    Death of\nFamily Member', (3.3, 0.75), size=10,)
plt.annotate('Christmas', (7.5, 0.75), size=10,)
plt.annotate('    Master\nof Desaster', (9.5, y_text), size=10,)

plt.axhline(y=0, color='b', linestyle='--')
plt.vlines(x=8, ymin=-.5, ymax=2, color='r', linestyle='--')
plt.vlines(x=4, ymin=-.5, ymax=2, color='r', linestyle='--')

#plt.show()
plt.savefig('output/mood-plot.png')