from unittest import TestCase
from raylink.data.ps.ps_captain import cal_single_nid_route_map, \
    cal_nid_route_map, PS_TREE_K


class TestPS(TestCase):
    def setUp(self) -> None:
        self.maxDiff = None

    def test_cal_single_nid_route_map(self):
        nids = [str(i) for i in range(PS_TREE_K)]
        true = {'0': [], '1': [], '2': [], '3': []}
        self.assertDictEqual(cal_single_nid_route_map(nids), true)
        nids = [str(i) for i in range(PS_TREE_K * 5)]
        true = {'0': ['4', '8', '12', '16'], '1': ['5', '9', '13', '17'],
                '2': ['6', '10', '14', '18'], '3': ['7', '11', '15', '19']}
        self.assertDictEqual(cal_single_nid_route_map(nids), true)
        nids = [str(i) for i in range(PS_TREE_K * 5)]
        true = {'0': ['4', '8', '12', '16'], '1': ['5', '9', '13', '17'],
                '2': ['6', '10', '14', '18'], '3': ['7', '11', '15', '19']}
        self.assertDictEqual(cal_single_nid_route_map(nids), true)
        nids = [str(i) for i in range(PS_TREE_K * 5 + 2)]
        true = {'0': ['4', '8', '12', '16', '20'],
                '1': ['5', '9', '13', '17', '21'],
                '2': ['6', '10', '14', '18'],
                '3': ['7', '11', '15', '19']}
        self.assertDictEqual(cal_single_nid_route_map(nids), true)
        nids = [str(i) for i in range(80)]
        true = {'0': ['4', '8', '12', '16', '20', '24', '28', '32', '36', '40',
                      '44', '48', '52', '56', '60', '64', '68', '72', '76'],
                '1': ['5', '9', '13', '17', '21', '25', '29', '33', '37', '41',
                      '45', '49', '53', '57', '61', '65', '69', '73', '77'],
                '2': ['6', '10', '14', '18', '22', '26', '30', '34', '38', '42',
                      '46', '50', '54', '58', '62', '66', '70', '74', '78'],
                '3': ['7', '11', '15', '19', '23', '27', '31', '35', '39', '43',
                      '47', '51', '55', '59', '63', '67', '71', '75', '79']}
        self.assertDictEqual(cal_single_nid_route_map(nids), true)

    def test_cal_nid_route_map(self):
        nids = [str(i) for i in range(PS_TREE_K)]
        true = {'0': {}, '1': {}, '2': {}, '3': {}}
        self.assertDictEqual(cal_nid_route_map(nids), true)
        nids = [str(i) for i in range(PS_TREE_K * 5)]
        true = {'0': {'4': {}, '8': {}, '12': {}, '16': {}},
                '1': {'5': {}, '9': {}, '13': {}, '17': {}},
                '2': {'6': {}, '10': {}, '14': {}, '18': {}},
                '3': {'7': {}, '11': {}, '15': {}, '19': {}}}
        self.assertDictEqual(cal_nid_route_map(nids), true)
        nids = [str(i) for i in range(PS_TREE_K * 5 + 2)]
        true = {'0': {'4': {'20': {}}, '8': {}, '12': {}, '16': {}},
                '1': {'5': {'21': {}}, '9': {}, '13': {}, '17': {}},
                '2': {'6': {}, '10': {}, '14': {}, '18': {}},
                '3': {'7': {}, '11': {}, '15': {}, '19': {}}}
        self.assertDictEqual(cal_nid_route_map(nids), true)
        nids = [str(i) for i in range(80)]
        true = {'0': {'4': {'20': {}, '36': {}, '52': {}, '68': {}},
                      '8': {'24': {}, '40': {}, '56': {}, '72': {}},
                      '12': {'28': {}, '44': {}, '60': {}, '76': {}},
                      '16': {'32': {}, '48': {}, '64': {}}},
                '1': {'5': {'21': {}, '37': {}, '53': {}, '69': {}},
                      '9': {'25': {}, '41': {}, '57': {}, '73': {}},
                      '13': {'29': {}, '45': {}, '61': {}, '77': {}},
                      '17': {'33': {}, '49': {}, '65': {}}},
                '2': {'6': {'22': {}, '38': {}, '54': {}, '70': {}},
                      '10': {'26': {}, '42': {}, '58': {}, '74': {}},
                      '14': {'30': {}, '46': {}, '62': {}, '78': {}},
                      '18': {'34': {}, '50': {}, '66': {}}},
                '3': {'7': {'23': {}, '39': {}, '55': {}, '71': {}},
                      '11': {'27': {}, '43': {}, '59': {}, '75': {}},
                      '15': {'31': {}, '47': {}, '63': {}, '79': {}},
                      '19': {'35': {}, '51': {}, '67': {}}}}
        self.assertDictEqual(cal_nid_route_map(nids), true)
