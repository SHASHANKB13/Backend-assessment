import unittest

from backend_assessment.main import hello


class BasicTest(unittest.TestCase):
    def test_hello(self):
        self.assertEqual(hello(), "Hello, world!")


if __name__ == "__main__":
    unittest.main()
