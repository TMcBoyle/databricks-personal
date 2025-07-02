
class A:
    def __init__(self):
        self.value = 5

class B:
    def __init__(self):
        self.value = 10

lookup = { 'A': A, 'B': B }

config = ['A', 'B', 'A', 'A', 'B']

objects = [lookup[x]() for x in config]
values = [x.value for x in objects]
print(objects)
print(values)
