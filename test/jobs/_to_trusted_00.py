import unittest
from src.jobs.to_trusted_00 import ToTrusted

class ToTrustedTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(ToTrustedTest, self).__init__(*args, **kwargs)

    def test_process(self):
        processor = ToTrusted()
        result = processor.process()

        self.assertEqual(1,result)
