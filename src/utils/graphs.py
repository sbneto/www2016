__author__ = 'Samuel'


import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
from mpl_toolkits.mplot3d import Axes3D


def create_new_figure(w, h):
    fig = plt.figure()
    fig.set_size_inches(w, h)

    params = {'backend': 'eps',
              'font.family': 'serif',
              'font.size': 8,
              'axes.labelsize': 6,
              'legend.fontsize': 6,
              'xtick.labelsize': 6,
              'ytick.labelsize': 6}
    plt.rcParams.update(params)
    fig.set_tight_layout(True)
    return fig


def count_list_structure(original):
    size = 0
    for e in original:
        try:
            size += count_list_structure(iter(e))
        except TypeError:
            size += 1
    return size


def copy_list_structure(original, val):
    structure = []
    for e in original:
        try:
            structure.append(copy_list_structure(iter(e), val))
        except TypeError:
            structure.append(val)
    return structure


def weight_structure(original, y_sum):
    return copy_list_structure(original, y_sum/count_list_structure(original)) if y_sum else None


def get_grayscale_colors(data_points, gradient_start=0.2):
    try:
        if len(data_points) > 0:
            iter(data_points[0])
            n_data_points = len(data_points)
        else:
            n_data_points = 0
    except TypeError:
        n_data_points = 1
    return plt.cm.gray(np.arange(gradient_start, 1.0, (1.0 - gradient_start)/n_data_points))


def subplot_histogram(figure, data_points, title,
                      labels=None, position=111, bins=20,
                      y_sum=None, x_range=None, normed=False,
                      gradient_start=0.2, log=False):
    ax = figure.add_subplot(position)
    ax.set_title(title)
    ret = ax.hist(data_points, label=labels, bins=bins,
                  weights=weight_structure(data_points, y_sum), range=x_range, stacked=True,
                  normed=normed, rwidth=0.9, #color=get_grayscale_colors(data_points, gradient_start),
                  log=log)
    if labels:
        handles, labels = ax.get_legend_handles_labels()
        ax.legend(handles[::-1], labels[::-1])
    return list(ret) + [ax]


def get_kwargs(kwargs, *args):
    return dict((k, v) for k, v in kwargs.items() if k in args)


def formated_plot(plot_function, data, labels, axes, loc, normalize, show, **kwargs):
    # identify sets
    data_x = data[0]
    data_sets = data[1]
    data_y = data[2]
    ax = plt.gca()
    for s in np.nditer(np.unique(data_sets), ['refs_ok']):
        if 'x' in normalize:
            norm = np.max(data_x[data_sets == s])
        else:
            norm = 1
        x_plot = data_x[data_sets == s]/norm

        if 'sum' in normalize:
            norm = np.sum(data_y[data_sets == s])
        elif 'y' in normalize:
            norm = np.max(data_y[data_sets == s])
        else:
            norm = 1
        y_plot = data_y[data_sets == s]/norm
        plot_function(x_plot, y_plot, **get_kwargs(kwargs, 'width', 'linewidth'))
    plt.xlabel(labels[0].replace('_', ' '), fontsize=kwargs['x_label_size'])
    plt.ylabel(labels[2].replace('_', ' '), fontsize=kwargs['y_label_size'])
    ax.xaxis.get_major_formatter().set_useOffset(False)
    ax.yaxis.get_major_formatter().set_useOffset(False)
    tkr = ticker.MultipleLocator(base=1)
    ax.xaxis.set_major_locator(tkr)
    x_extra_gap = kwargs['width'] if 'width' in kwargs else 0.0
    plt.xlim(kwargs['x_min'] if 'x_min' in kwargs else data_x.min(),
             kwargs['x_max'] if 'x_max' in kwargs else data_x.max() + x_extra_gap)
    plt.ylim(kwargs['y_min'] if 'y_min' in kwargs else data_y.min(),
             kwargs['y_max'] if 'y_max' in kwargs else data_y.max())
    if axes[0] == 'log':
        ax.set_xscale('log')
    if axes[1] == 'log':
        ax.set_yscale('log')
    plt.tick_params(axis='both', which='major',
                    labelsize=kwargs['tick_size'],
                    width=kwargs['tick_width'],
                    length=kwargs['tick_length'])
    plot_legend = kwargs['legend'] if 'legend' in kwargs else True
    if plot_legend:
        if 'legend_size' in kwargs:
            ax.legend(np.unique(data_sets), loc=loc, fontsize=kwargs['legend_size'])
        else:
            ax.legend(np.unique(data_sets), loc=loc)
    plt.grid(True)
    plt.tight_layout()
    if 'filename' in kwargs:
        plt.savefig(**kwargs)
    if show:
        fmng = plt.get_current_fig_manager()
        fmng.window.showMaximized()
        plt.show()
    plt.close()


def multiplot(data, labels, axes=('linear', 'linear'),
              loc='upper left', normalize='', show=True, **kwargs):
    kwargs['x_label_size'] = kwargs.get('x_label_size', 18)
    kwargs['y_label_size'] = kwargs.get('y_label_size', 18)
    kwargs['tick_size'] = kwargs.get('tick_size', 18)
    kwargs['tick_width'] = kwargs.get('tick_width', 2)
    kwargs['tick_length'] = kwargs.get('tick_length', 12)
    formated_plot(plt.plot, data, labels, axes, loc, normalize, show, **kwargs)
    # data_x = data[0]
    # data_sets = data[1]
    # data_y = data[2]
    # ax = plt.gca()
    # for s in np.nditer(np.unique(data_sets), ['refs_ok']):
    #     if 'x' in normalize:
    #         norm = np.max(data_x[data_sets == s])
    #     else:
    #         norm = 1
    #     x_plot = data_x[data_sets == s]/norm
    #
    #     if 'sum' in normalize:
    #         norm = np.sum(data_y[data_sets == s])
    #     elif 'y' in normalize:
    #         norm = np.max(data_y[data_sets == s])
    #     else:
    #         norm = 1
    #     y_plot = data_y[data_sets == s]/norm
    #     plt.plot(x_plot, y_plot)
    # plt.xlabel(labels[0].replace('_', ' '), fontsize=18)
    # plt.ylabel(labels[2].replace('_', ' '), fontsize=18)
    # ax.xaxis.get_major_formatter().set_useOffset(False)
    # ax.yaxis.get_major_formatter().set_useOffset(False)
    # plt.xlim(kwargs['x_min'] if 'x_min' in kwargs else data_x.min(),
    #          kwargs['x_max'] if 'x_max' in kwargs else data_x.max())
    # plt.ylim(kwargs['y_min'] if 'y_min' in kwargs else data_y.min(),
    #          kwargs['y_max'] if 'y_max' in kwargs else data_y.max())
    # if axes[0] == 'log':
    #     ax.set_xscale('log')
    # if axes[1] == 'log':
    #     ax.set_yscale('log')
    # plot_legend = kwargs['legend'] if 'legend' in kwargs else True
    # if plot_legend:
    #     if 'legend_size' in kwargs:
    #         ax.legend(np.unique(data_sets), loc=loc, fontsize=kwargs['legend_size'])
    #     else:
    #         ax.legend(np.unique(data_sets), loc=loc)
    # plt.grid(True)
    # if 'filename' in kwargs:
    #     plt.savefig(**kwargs)
    # if show:
    #     plt.show()
    # plt.close()


def scatter(data, labels, axes=('linear', 'linear'), loc='upper right'):
    data_x = data[0]
    data_sets = data[1]
    data_y = data[2]
    ax = plt.gca()
    for s in np.nditer(np.unique(data_sets), ['refs_ok']):
        plt.scatter(data_x[data_sets == s], data_y[data_sets == s])
    plt.xlabel(labels[0], fontsize=18)
    plt.ylabel(labels[2], fontsize=18)
    if axes[0] == 'log':
        ax.set_xscale('log')
        plt.xlim(10**np.floor(np.log10(np.min(data_x))),
                 10**np.ceil(np.log10(np.max(data_x))))
    else:
        ax.xaxis.get_major_formatter().set_useOffset(False)
    if axes[1] == 'log':
        ax.set_yscale('log')
        plt.ylim(10**np.floor(np.log10(np.min(data_y))),
                 10**np.ceil(np.log10(np.max(data_y))))
    else:
        ax.yaxis.get_major_formatter().set_useOffset(False)
    ax.legend(np.unique(data_sets), loc=loc)
    plt.grid(True)
    plt.show()
    plt.close()


def bars(data, labels, axes=('linear', 'linear'),
              loc='upper right', normalize='', show=True, **kwargs):
    kwargs['width'] = kwargs.get('width', 0.8)
    kwargs['linewidth'] = kwargs.get('linewidth', 0.8)
    formated_plot(plt.bar, data, labels, axes, loc, normalize, show, **kwargs)
    # data_x = data[0]
    # data_sets = data[1]
    # data_y = data[2]
    # plt.figure(figsize=(32, 18))
    # ax = plt.gca()
    # kwargs['width'] = kwargs.get('width', 0.8)
    # for s in np.nditer(np.unique(data_sets), ['refs_ok']):
    #     if 'x' in normalize:
    #         norm = np.max(data_x[data_sets == s])
    #     else:
    #         norm = 1
    #     x_plot = data_x[data_sets == s]/norm
    #
    #     if 'sum' in normalize:
    #         norm = np.sum(data_y[data_sets == s])
    #     elif 'y' in normalize:
    #         norm = np.max(data_y[data_sets == s])
    #     else:
    #         norm = 1
    #     y_plot = data_y[data_sets == s]/norm
    #     plt.bar(x_plot, y_plot, width=kwargs['width'], linewidth=kwargs['linewidth'])
    # plt.xlabel(labels[0].replace('_', ' '), fontsize=18)
    # plt.ylabel(labels[2].replace('_', ' '), fontsize=18)
    # ax.xaxis.get_major_formatter().set_useOffset(False)
    # ax.yaxis.get_major_formatter().set_useOffset(False)
    # plt.xlim(kwargs['x_min'] if 'x_min' in kwargs else data_x.min(),
    #          kwargs['x_max'] if 'x_max' in kwargs else data_x.max() + kwargs['width'])
    # plt.ylim(0,
    #          kwargs['y_max'] if 'y_max' in kwargs else data_y.max())
    # if axes[0] == 'log':
    #     ax.set_xscale('log')
    # if axes[1] == 'log':
    #     ax.set_yscale('log')
    # plot_legend = kwargs['legend'] if 'legend' in kwargs else True
    # if plot_legend:
    #     if 'legend_size' in kwargs:
    #         ax.legend(np.unique(data_sets), loc=loc, fontsize=kwargs['legend_size'])
    #     else:
    #         ax.legend(np.unique(data_sets), loc=loc)
    # plt.grid(True)
    # if 'filename' in kwargs:
    #     plt.savefig(**kwargs)
    # if show:
    #     fmng = plt.get_current_fig_manager()
    #     fmng.window.showMaximized()
    #     plt.show()
    # plt.close()


def scatter3d(data, labels, axes=('linear', 'linear', 'linear')):
    data_x = data[0]
    data_y = data[1]
    if axes[2] == 'log':
        data_z = np.log10(data[2])
    else:
        data_z = data[2]
    fig = plt.figure()
    ax = Axes3D(fig)
    ax.set_xlim3d(data_x.min(), data_x.max())
    ax.set_ylim3d(data_y.min(), data_y.max())
    ax.set_zlim3d(data_z.min(), data_z.max())
    ax.xaxis.get_major_formatter().set_scientific(False)
    ax.yaxis.get_major_formatter().set_scientific(False)
    ax.zaxis.get_major_formatter().set_scientific(False)
    ax.xaxis.get_major_formatter().set_useOffset(False)
    ax.yaxis.get_major_formatter().set_useOffset(False)
    ax.zaxis.get_major_formatter().set_useOffset(False)
    ax.set_xlabel(labels[0], fontsize=18)
    ax.set_ylabel(labels[1], fontsize=18)
    ax.set_zlabel(labels[2], fontsize=18)
    if axes[0] == 'log':
        ax.xaxis.set_scale('log')
    if axes[1] == 'log':
        ax.yaxis.set_scale('log')
    if axes[2] == 'log':
        ax.zaxis.set_scale('log')
    ax.scatter(data_x, data_y, data_z)
    plt.grid(True)
    plt.show()
    plt.close()


def wireframe(data, labels, axes=('linear', 'linear', 'linear'), show=False,
              **kwargs):
    data_x = data[0]
    data_y = data[1]
    if axes[2] == 'log':
        data_z = np.log10(data[2])
    else:
        data_z = data[2]
    fig = plt.figure()
    ax = Axes3D(fig)
    ax.set_xlim3d(kwargs['x_min'] if 'x_min' in kwargs else data_x.min(),
                  kwargs['x_max'] if 'x_max' in kwargs else data_x.max())
    ax.set_ylim3d(kwargs['y_min'] if 'y_min' in kwargs else data_y.min(),
                  kwargs['y_max'] if 'y_max' in kwargs else data_y.max())
    ax.set_zlim3d(kwargs['z_min'] if 'z_min' in kwargs else data_z.min(),
                  kwargs['z_max'] if 'z_max' in kwargs else data_z.max())
    ax.xaxis.get_major_formatter().set_scientific(False)
    ax.yaxis.get_major_formatter().set_scientific(False)
    ax.zaxis.get_major_formatter().set_scientific(False)
    ax.xaxis.get_major_formatter().set_useOffset(False)
    ax.yaxis.get_major_formatter().set_useOffset(False)
    ax.zaxis.get_major_formatter().set_useOffset(False)
    ax.set_xlabel(labels[0], fontsize=18)
    ax.set_ylabel(labels[1], fontsize=18)
    ax.set_zlabel(labels[2], fontsize=18)
    if axes[0] == 'log':
        ax.xaxis.set_scale('log')
    if axes[1] == 'log':
        ax.yaxis.set_scale('log')
    if axes[2] == 'log':
        ax.zaxis.set_scale('log')
    data_x = np.array([data_x[data_y == year] for year in np.unique(data_y)])
    data_z = np.array([data_z[data_y == year] for year in np.unique(data_y)])
    data_y = np.array([data_y[data_y == year] for year in np.unique(data_y)])
    ax.plot_wireframe(data_x, data_y, data_z)
    plt.grid(True)
    if 'filename' in kwargs:
        ax.view_init(kwargs['elev'], kwargs['azim'])
        plt.savefig(**kwargs)
    if show:
        plt.show()
    plt.close()


def multiplot3d(data, labels, axes=('linear', 'linear', 'linear')):
    data_x = data[0]
    data_y = data[1]
    if axes[2] == 'log':
        data_z = np.log10(data[2])
    else:
        data_z = data[2]
    fig = plt.figure()
    ax = fig.gca(projection='3d')
    ax.set_xlim3d(data_x.min(), data_x.max())
    ax.set_ylim3d(data_y.min(), data_y.max())
    ax.set_zlim3d(data_z.min(), data_z.max())
    ax.set_xlabel(labels[0], fontsize=18)
    ax.set_ylabel(labels[1], fontsize=18)
    ax.set_zlabel(labels[2], fontsize=18)
    ax.xaxis.get_major_formatter().set_scientific(False)
    ax.yaxis.get_major_formatter().set_scientific(False)
    ax.zaxis.get_major_formatter().set_scientific(False)
    ax.xaxis.get_major_formatter().set_useOffset(False)
    ax.yaxis.get_major_formatter().set_useOffset(False)
    ax.zaxis.get_major_formatter().set_useOffset(False)
    if axes[0] == 'log':
        ax.xaxis.set_scale('log')
    if axes[1] == 'log':
        ax.yaxis.set_scale('log')
    if axes[2] == 'log':
        ax.zaxis.set_scale('log')
    for i in np.nditer(np.unique(data_y), ['refs_ok']):
        ax.plot(data_x[data_y == i], data_y[data_y == i], data_z[data_y == i])
    plt.grid(True)
    plt.show()
    plt.close()


def histogram(data, labels, bins=100, axes=('linear', 'linear'), log_bin=None):
    data = data[0]
    if log_bin:
        bins = np.logspace(log_bin[0], log_bin[1], bins)

    fig = create_new_figure(4, 3)
    subplot_histogram(fig, data, 'bla', bins=bins)
    plt.xlabel(labels[0], fontsize=18)

    if axes[0] == 'log':
        plt.gca().set_xscale('log')
    if axes[1] == 'log':
        plt.gca().set_yscale('log')
    plt.grid(True)
    plt.show()
    plt.close()
