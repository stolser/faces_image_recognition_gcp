range_numbers = range(10, 101, 10)
print("range_numbers = {}".format(list(range_numbers)))

list1 = ["one", [2], range_numbers]
print("list = {}".format(list1))
print("list = {}".format(list1))

lengths = map(lambda x: len(x), list1)
print("lengths = {}".format(list(lengths)))

json_dict = {
    "one": 1,
    "two": 2,
    "three": [1, 2, 3],
    "four": {"one": "1"}
}
print("json = {}".format(str(json_dict)))

all_numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9]
numbers_ = all_numbers != 2
print("numbers_ = ", numbers_)
print("only odd numbers = {}".format(all_numbers[numbers_]))
