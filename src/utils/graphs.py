import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

__author__ = 'Samuel'


# Necessary to produce only Type 1 fonts, can be remove otherwise
# To run with these, latex binaries, dvipng and Ghostscript MUST be in your PATH
# http://matplotlib.org/users/usetex.html
matplotlib.rcParams['ps.useafm'] = True
matplotlib.rcParams['text.usetex'] = True


def get_kwargs(kwargs, *args):
    return dict((k, v) for k, v in kwargs.items() if k in args)


def formated_plot(plot_function, data, labels, axes, loc, normalize, show, **kwargs):
    # identify sets
    data_x = data[0]
    data_sets = data[1]
    data_y = data[2]
    ax = plt.gca()
    cm = plt.get_cmap('viridis')
    sets = np.unique(data_sets)
    i = 0
    for s in np.nditer(sets, ['refs_ok']):
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

        if 'linestyle' in kwargs:
            style = kwargs['linestyle'][i % len(kwargs['linestyle'])]
        else:
            style = 'solid'

        y_plot = data_y[data_sets == s]/norm
        i += 1
        plot_function(x_plot,
                      y_plot,
                      color=cm(int(256*(len(sets) - i)/len(sets))),
                      linestyle=style,
                      **get_kwargs(kwargs, 'width', 'linewidth'))

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


def bars(data, labels, axes=('linear', 'linear'),
         loc='upper right', normalize='', show=True, **kwargs):
    kwargs['width'] = kwargs.get('width', 0.8)
    kwargs['linewidth'] = kwargs.get('linewidth', 0.8)
    formated_plot(plt.bar, data, labels, axes, loc, normalize, show, **kwargs)
