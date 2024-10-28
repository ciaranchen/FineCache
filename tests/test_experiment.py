import unittest

from FineCache import FineCache


class TestExperiment(unittest.TestCase):
    def test_exp(self):
        fc = FineCache('.exp_log')

        class Trainer:
            @fc.cache()
            def load_data(self):
                return [1, 1, 4], [5, 1, 4]

            def train(self, data):
                print(f'Train with data {data} ...')

            @fc.record()
            def test(self, data):
                print(f'Test with data {data} ...')

        # 主函数
        trainer = Trainer()
        train_data, test_data = trainer.load_data
        trainer.train(train_data)
        trainer.test(test_data)

        # 进行测试


if __name__ == '__main__':
    unittest.main()
