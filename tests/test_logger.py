from unittest import TestCase
import raylink.util.log.utils as ul


class TestLogger(TestCase):
    def test_get_logger_name(self):
        name = ul.get_logger_name('BRAIN', 'BRAIN')
        self.assertEqual(name, 'BRAIN')
        name = ul.get_logger_name('worker-0', 'manager-0/runner-0/worker-0')
        self.assertEqual(name, '0/0/worker-0')
