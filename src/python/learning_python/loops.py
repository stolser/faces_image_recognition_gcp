import time


def countdown(start_from):
    """It's a docstring.
    Put here your documentation!"""

    if start_from <= 0:
        print("Parameter start_from must be greater than 0!")
        return

    current = start_from
    while current:
        if current == 5:
            print("I don't have time to wait... Stop.")
            break
        print("countdown {}...".format(current))
        current = current - 1
    else:
        print("start!!!!")


countdown(start_from=10)

time_now = time.time()
print("time now in seconds = {}".format(time_now))
print("time now in nanoseconds = {}".format(time.time_ns()))
print("time now = {}".format(time.localtime(time_now)))
