from FineCache import FineCache

fc = FineCache('.exp_log', "exp{id}-{name}", name="DeepLearningModel")
# 存储到 `./.exp_log/exp1-DeepLearningModel`
fc.save_changes()  # 保存改动到 patch 文件


class Trainer:
    @fc.cache()
    def load_data(self):
        return [1, 1, 4], [5, 1, 4]

    def train(self, data):
        print(f'Train with data {data} ...')

    @fc.save_console()
    def test(self, data):
        print(f'Test with data {data} ...')


if __name__ == '__main__':
    # 主函数
    with fc.record():
        trainer = Trainer()
        train_data, test_data = trainer.load_data()
        trainer.train(train_data)
        trainer.test(test_data)
