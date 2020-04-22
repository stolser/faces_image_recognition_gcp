numbers = list([34, 1, 23, 4, 3, 3, 12, 4, 3, 1])

n = len(numbers)
print("count = ", n)
mean = float(sum(numbers) / n)
print("mean = ", mean)

sorted_numbers = sorted(numbers)
print("sorted = ", sorted_numbers)
print("median = ", (sorted_numbers[5] + sorted_numbers[6]) / 2)

standard_deviation = pow(sum(map(lambda x: pow((x - mean), 2), numbers)) / n, 0.5)
print("standard deviation = ", standard_deviation)
print("kurtosis = ", sum(map(lambda x: pow((x - mean), 4), numbers)) / (n * pow(standard_deviation, 4)))

first = list([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
second = list([7, 6, 5, 4, 5, 6, 7, 8, 9, 10])

n1 = len(first)
n2 = len(second)
print("count1 = ", n1)
print("count2 = ", n2)
mean1 = float(sum(first) / n1)
mean2 = float(sum(second) / n2)
print("mean1 = ", mean1)
print("mean2 = ", mean2)

standard_deviation1 = pow(sum(map(lambda x: pow((x - mean1), 2), first)) / n1, 0.5)
standard_deviation2 = pow(sum(map(lambda x: pow((x - mean2), 2), second)) / n2, 0.5)
print("standard_deviation1 = ", standard_deviation1)
print("standard_deviation2 = ", standard_deviation2)

tupled = list(zip(first, second))
print(tupled)

sum_ = 0
for x, y in tupled:
    sum_ = sum_ + (x - mean1) * (y - mean2)

covariance = sum_ / n1
print("covariance = ", covariance)

correlation = covariance / (standard_deviation1 * standard_deviation2)
print("correlation = ", correlation)

