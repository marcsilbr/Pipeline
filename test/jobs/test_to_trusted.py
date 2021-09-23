import unittest, os
from src.jobs.to_trusted import ToTrusted
from pyspark.sql import SparkSession
from pyspark.sql import functions as f


class ToTrustedTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(ToTrustedTest, self).__init__(*args, **kwargs)

    def test_process(self):
        # execution transformation job
        path = os.path.dirname(__file__)
        fr_path = '{}/../locallake/raw/students.csv'.format(path)
        to_path = '{}/../locallake/trusted/students'.format(path)

        processor = ToTrusted(fr_path, to_path)
        processor.process()

        # validate process output
        spark = SparkSession.builder.getOrCreate()
        df = spark.read.format('delta').load(to_path)

        self.assertEqual(5, df.count())

        # lazy ops vs eager ops
        lines = df.groupby(['genre']).count().orderBy(f.asc('genre')).collect()
        self.assertEqual(2, lines[0]['count'])
        self.assertEqual(3, lines[1]['count'])
