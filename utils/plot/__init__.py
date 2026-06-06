import base64
import random
import itertools
import numpy as np
import pandas as pd
from io import BytesIO
import matplotlib as mpl
import matplotlib.pyplot as plt
from dagster import MetadataValue
from matplotlib.figure import Figure

mesh_grid_points = 100

legend_elements = [
    mpl.lines.Line2D(
        [0],
        [0],
        marker="o",
        markerfacecolor="green",
        label="Real",
        color="k",
        markersize=13,
        linestyle="None",
    ),
    mpl.lines.Line2D(
        [0],
        [0],
        marker="o",
        markerfacecolor="magenta",
        label="Predicted",
        color="k",
        markersize=13,
        linestyle="None",
    ),
]

def plot_data(data: pd.DataFrame, target_name: str)->Figure:
    fig = Figure()
    features = data.loc[:, data.columns != target_name]
    target = data[target_name]
    if features.shape[1] == 2:
        ax = fig.subplots()
        ax.scatter(features.iloc[:, 0], features.iloc[:, 1], c=target, marker="o", s =5, cmap=plt.get_cmap("jet_r"))
        ax.set_xticks([])
        ax.set_yticks([])
        ax.set_xlabel(features.columns[0])
        ax.set_ylabel(features.columns[1])
    else:
        number_plots = 3
        ax = fig.subplots(nrows=1, ncols=number_plots)
        all_pairs = list(itertools.combinations(range(features.shape[1]), 2))
        random_pairs = random.sample(all_pairs, number_plots)
        for i in range(number_plots):
            x_col, y_col = random_pairs[i]
            ax[i].scatter(features.iloc[:, x_col], features.iloc[:, y_col], c=target, marker="o", s =5, cmap=plt.get_cmap("jet_r"))
            ax[i].set_xticks([])
            ax[i].set_yticks([])
            ax[i].set_xlabel(features.columns[x_col])
            ax[i].set_ylabel(features.columns[y_col])

    buffer = BytesIO()
    fig.savefig(buffer)
    image_data = base64.b64encode(buffer.getvalue())

    return MetadataValue.md(f"![img](data:image/png;base64,{image_data.decode()})")

def plot_model_chara(model, data: pd.DataFrame, test_data:pd.DataFrame, target_name: str, task: str)->Figure:
    fig = Figure()
    features = data.loc[:, data.columns != target_name]
    test_features = test_data.loc[:, test_data.columns != target_name].values
    test_target = test_data[target_name].values

    if task == "classification":
        if features.shape[1] == 2:
            real_predicted_decision(fig, model, features, test_features, test_target)
        else:
            real_predicted(fig, model, test_features, test_target)
    elif task == "regression":
        real_predicted(fig, model, test_features, test_target)
    else:
        raise NotImplementedError(f"Plot function for {task} not implemented.")

    buffer = BytesIO()
    fig.savefig(buffer)
    image_data = base64.b64encode(buffer.getvalue())

    return MetadataValue.md(f"![img](data:image/png;base64,{image_data.decode()})")

def real_predicted(fig, model, test_features, test_target):
    ax = fig.subplots()
    ax.plot(test_target, marker="o", markersize=5, color="green", linestyle="None")
    ax.plot(model.predict(test_features), marker="o", markersize=5, color="magenta", linestyle="None")
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_title("Real-Predicted Comparison")
    ax.legend(
        handles=legend_elements,
        loc="lower center",
        borderaxespad=-2.5,
        handletextpad=0.1,
        ncol=2,
        frameon=True,
    )

    return ax

def real_predicted_decision(fig, model, features, test_features, test_target):
    ax = fig.subplots(nrows=1, ncols=2)

    x_min = features.iloc[:, 0].min()
    x_max = features.iloc[:, 0].max()
    y_min = features.iloc[:, 1].min()
    y_max = features.iloc[:, 1].max()
    xx, yy = np.meshgrid(np.linspace(x_min, x_max, num=mesh_grid_points),
                         np.linspace(y_min, y_max, num=mesh_grid_points), )
    z = model.predict(np.c_[xx.ravel(), yy.ravel()])
    z = z.reshape(xx.shape)
    ax[0].contourf(xx, yy, z, alpha=0.6, cmap=plt.get_cmap("jet_r"))
    ax[0].set_xticks([])
    ax[0].set_yticks([])
    ax[0].set_title("Decision function")
    ax[1].plot(test_target, marker="o", markersize =5, color="green", linestyle="None")
    ax[1].plot(model.predict(test_features), marker="o", markersize =5, color="magenta", linestyle="None")
    ax[1].set_xticks([])
    ax[1].set_yticks([])
    ax[1].set_title("Real-Predicted Comparison")
    ax[1].legend(
        handles=legend_elements,
        loc="lower center",
        borderaxespad=-2.5,
        handletextpad=0.1,
        ncol=2,
        frameon=True,
    )

    return ax