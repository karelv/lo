import unittest
import yaml
from liftoff import do_get_search_query, load_cfg, bdb_cfg


class TestLO(unittest.TestCase):

    def setUp(self):
        pass

    def test_get_search_query(self):
        global bdb_cfg
        load_cfg ('liftoff.yml')
        with open('liftoff.yml', 'r') as f:
          cfg = yaml.load(f)
        self.assertEqual( do_get_search_query ("playground", "groningen"), '"__liftoff__{} playground groningen "'.format(cfg['headers']['app_key']))


if __name__ == '__main__':
    unittest.main()