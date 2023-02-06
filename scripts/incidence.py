import numpy as np

def incidence(total, group):
    incidence_matrix = np.zeros(total.shape)
    for i in range(total.shape[0]):
        for j in range(total.shape[1]):
            incidence_matrix[i, j] = group[i, j] / total[i, j] * 100
    return incidence_matrix

total_cases = np.array([[1000, 2000, 1500, 1700], [900, 1850, 1700, 1500], [1100, 1700, 1600, 1500]])
group_cases = np.array([[100, 200, 150, 170], [90, 185, 170, 150], [110, 170, 160, 150]])
incidence_result = incidence(total_cases, group_cases)
print(incidence_result)
