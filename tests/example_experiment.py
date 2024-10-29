from FineCache import FineCache

fc = FineCache('.exp_log', "exp{id}-{name}", name="DeepLearningModel")
# 存储到 `./.exp_log/exp1-DeepLearningModel`
fc.save_changes()  # 保存改动到 patch 文件


@fc.cache()  # 保存并自动取用中间结果
def preprocess_data():
    return [1, 1, 4], [5, 1, 4]


class Trainer:
    def train(self, data):
        print(f'Train with data {data} ...')

    @fc.save_console()  # 将命令行输出保存到特定文件
    def test(self, data):
        print(f'Test with data {data} ...')


if __name__ == '__main__':
    with fc.record():  # 在离开时记录关键信息和文件
        # 主函数
        trainer = Trainer()
        train_data, test_data = preprocess_data()
        trainer.train(train_data)
        trainer.test(test_data)
