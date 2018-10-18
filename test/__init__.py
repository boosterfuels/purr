import unittest

def fun(x):
  return x + 1

class MyTest(unittest.TestCase):
  def func(x):
      return x + 1

  def test_answer():
      assert func(3) == 5

if __name__ == '__main__':
    unittest.main()


