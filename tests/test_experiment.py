import os
import unittest
from shutil import rmtree

from FineCache import FineCache, IncrementDir


class TestExperiment(unittest.TestCase):
    def setUp(self) -> None:
        # self.fc = FineCache('.exp_log')
        pass

    def tearDown(self):
        super().tearDown()
        # Clear folders...
        if os.path.exists('.exp_log'):
            rmtree('.exp_log')

    def test_exp(self):
        fc = FineCache('.exp_log', "exp{id}-{name}", name="DeepLearningModel")

        class Trainer:
            @fc.cache()
            def load_data(self):
                return [1, 1, 4], [5, 1, 4]

            def train(self, data):
                print(f'Train with data {data} ...')

            @fc.record_output()
            def test(self, data):
                print(f'Test with data {data} ...')

        # 主函数
        @fc.record_main()
        def main():
            trainer = Trainer()
            train_data, test_data = trainer.load_data()
            trainer.train(train_data)
            trainer.test(test_data)

        # TODO: 进行测试
        # self.assertEqual()


if __name__ == '__main__':
    unittest.main()
