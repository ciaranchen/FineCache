from FineCache import FineCache

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


if __name__ == '__main__':
    # 主函数
    with fc.record_main():
        trainer = Trainer()
        train_data, test_data = trainer.load_data()
        trainer.train(train_data)
        trainer.test(test_data)
