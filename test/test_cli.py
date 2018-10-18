import unittest

# def func(x):
#     return x + 1

# def test_answer():
#     assert func(1) == 5

# group multiple tests in a class

class TestCLI(unittest.TestCase):
    def test_one(self):
        x = "this"
        assert 'h' in x
