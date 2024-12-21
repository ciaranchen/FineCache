import json
import os
import unittest
from shutil import rmtree

from FineCache import FineCache, IncrementDir


class TestExperiment(unittest.TestCase):
    def test_exp(self):
        fc = FineCache('.exp_log', "exp{id}-{name}", name="DeepLearningModel")
        fc.save_changes('changes.patch')

        class Trainer:
            @fc.cache(lambda f, c: f"{c.__class__.__name__}.{f.__name__}.pk")
            def load_data(self):
                return [1, 1, 4], [5, 1, 4]

            def train(self, data):
                print(f'Train with data {data} ...')

            @fc.save_console()
            def test(self, data):
                print(f'Test with data {data} ...')
                # Assume to plot picture
                open('result_image.jpg', 'w').close()
                fc.tracking_files.append(r'result_image\.jpg')

        # 主函数
        @fc.record()
        def main():
            trainer = Trainer()
            train_data, test_data = trainer.load_data()
            trainer.train(train_data)
            trainer.test(test_data)

        main()

        # 进行测试
        exp_dir = os.path.join('.exp_log', 'exp1-DeepLearningModel')
        self.assertTrue(os.path.exists(exp_dir))
        patch_path = os.path.join(exp_dir, 'changes.patch')
        self.assertTrue(os.path.exists(patch_path))
        info_path = os.path.join(exp_dir, 'information.json')
        self.assertTrue(os.path.exists(info_path))
        with open(info_path) as fp:
            info_data = json.load(fp)

        keys = ['commit', 'project_root', 'patch_time', 'main_start', 'main_end', 'tracking_records']
        for k in keys:
            self.assertIn(k, info_data)
        self.assertIn(r'result_image\.jpg', info_data['tracking_records'])
        self.assertTrue(os.path.exists(os.path.join(exp_dir, 'tests', 'result_image.jpg')))

        console_path = os.path.join(exp_dir, 'console.log')
        self.assertTrue(os.path.exists(console_path))
        with open(console_path) as fp:
            text = fp.read()
        self.assertEqual(text.strip(), 'Test with data [5, 1, 4] ...')

        exp_cache_path = os.path.join(exp_dir, 'Trainer.load_data.pk')
        self.assertTrue(os.path.exists(exp_cache_path))

        # Clean
        if os.path.exists('.exp_log'):
            rmtree('.exp_log')
        os.remove('result_image.jpg')


if __name__ == '__main__':
    unittest.main()
