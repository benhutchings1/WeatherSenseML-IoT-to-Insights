from matplotlib import pyplot as plt

def plot_data(df, out="out.png"):
    '''
    Plots dataframe data into lineplot

    Attributes:
        df: pandas DataFrame
        out: Saves plot to specified path, defaults to "out.png"
    '''
    x = df["Timestamp"].tolist()
    y = df["Value"].tolist()

    plt.figure(figsize=(15,10))
    plt.title("AVG DAILY PM2.5")
    plt.ylabel("AVG PM2.5 (μg/m^3)")
    plt.plot(x, y)
    
    # Rotate x ticks and change font size
    plt.xticks(rotation = 45, fontsize=8)
    # Hide every nth label
    n = 2
    ax = plt.gca()
    xtik = ax.xaxis.get_ticklabels()
    for tick in xtik[::n]:
        tick.set_visible(False)

    print(f"Saving images to {out}", flush=True)
    plt.savefig(f"{out}")
