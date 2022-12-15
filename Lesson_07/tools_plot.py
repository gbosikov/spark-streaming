from sklearn.metrics import roc_curve
from sklearn.metrics import roc_auc_score
import matplotlib.pyplot as plt


def get_plot_roc(y_valid, 
                 y_pred, 
                 pos_label:str=1, 
                 title_plot:str='', 
                 title_line:str='', 
                 plot=None, 
                 fig=None, 
                 ax=None):
    """Рисует график "ROC Curve" для оценки предсказательной силы модели классификации.
    Может рисовать несколько графиков на одном изображении, если переданы 
    параметры plot, fig и ax от предыдущего вызова функции.
    Вместе с графиком возвращает ещё и оценку площади под ним
    :param y_valid: массив из верных данных
    :param y_pred: массив предсказанных значений
    :param pos_label: текст верного предсказания. Это может быть цифра 1
    :param title: подпись кривой на графике
    :param plot:
    :param fig:
    :param ax:
    """
    fpr, tpr, thresholds = roc_curve(y_valid, y_pred, pos_label=pos_label)

    if not plot:
        fig, ax = plt.subplots(figsize=(8,5))
        plt.plot([0, 1],
                 [0, 1],
                 linestyle='--',
                 color='gray',
                 label='Случайное угадывание'
                 )
        
        plt.plot([0, 0, 1],
                 [0, 1, 1],
                 linestyle='-.',
                 alpha=0.5,
                 color='red',
                 label='Идеальное угадывание'
                 )

    score = roc_auc_score(y_valid, y_pred)
    
    plt.plot(fpr, tpr, lw=2, label=title_line + f' ({score.round(4)})')

    plt.xlim([-0.05, 1.05])
    plt.ylim([-0.05, 1.05])
    plt.xlabel('FPR')
    plt.ylabel('TPR')
    if title_plot:
        plt.title(title_plot)
    plt.legend(loc="lower right")
    plt.tight_layout()
    plt.legend(loc=4, prop={'size': 18})
    for item in ([ax.title, ax.xaxis.label, ax.yaxis.label] +
                 ax.get_xticklabels() + ax.get_yticklabels()):
        item.set_fontsize(20)
    for item in (ax.get_xticklabels() + ax.get_yticklabels()):
        item.set_fontsize(15)
        
    return plt, fig, ax, score
