from utils.parse_data import read_inferences
from utils.plots import plot_inferences


def get_inferences(subject="001"):
    path = 'data/' + subject + '/MyData/'
    plot_path = 'output/' + subject + '/inferences.png'

    print('Retrieving inferences for subject ' + subject)
    values = read_inferences(path)

    plot_inferences(values, plot_path)


if __name__ == "__main__":
    get_inferences("001")