from unittest import TestCase
from raylink.util.resource import Machine, ResourceManager


class TestResource(TestCase):
    def setUp(self):
        cluster_nodes = {
            'worker': {
                'worker-0': {'cpu': 10},
                'worker-1': {'cpu': 10},
                'worker-2': {'cpu': 10}
            },
            'server': {
                'server-0': {'cpu': 10, 'gpu': 1}
            },
            'scheduler': {
                'scheduler-0': {'cpu': 10}
            }
        }
        cluster_res = {}
        for role, identities in cluster_nodes.items():
            for identity, resource in identities.items():
                additional = {}
                if role == 'worker' or role == 'scheduler':
                    additional['ps'] = 2
                m = Machine(role=role, identity=identity,
                            basic=resource['cpu'], additional=additional)
                cluster_res[m.identity] = m
        self.cluster_res = cluster_res

    def test_allocate(self):
        node_res = {'worker': {'num_cpus': 1, 'resources': {'worker': 1}}}
        rm = ResourceManager(self.cluster_res, node_res)
        resources = rm.allocate('worker')
        true = {'num_cpus': 1, 'resources': {'worker': 1, 'worker-0': 1}}
        self.assertDictEqual(resources, true)
        node_res = {
            'worker': {'num_cpus': 0.1, 'resources': {'worker': 0.1, 'ps': 1}}}
        rm = ResourceManager(self.cluster_res, node_res)
        resources = rm.allocate('worker')
        true = {'num_cpus': 0.1, 'resources': {'worker': 0.1, 'worker-1': 0.1}}
        self.assertDictEqual(resources, true)

    def test_allocate_same(self):
        node_res = {'worker': {'num_cpus': 1, 'resources': {'worker': 1}}}
        rm = ResourceManager(self.cluster_res, node_res)
        parent_res = rm.allocate('worker')
        true = {'num_cpus': 1, 'resources': {'worker': 1, 'worker-0': 1}}
        self.assertDictEqual(parent_res, true)
        res = rm.allocate('worker', parent_res)
        true = {'num_cpus': 1, 'resources': {'worker': 1, 'worker-0': 1}}
        self.assertDictEqual(res, true)

    def test_allocate_2(self):
        node_res = {
            'ps': {'num_cpus': 1, 'resources': {'scheduler': 1, 'ps': 1}}}
        rm = ResourceManager(self.cluster_res, node_res)
        true = {'num_cpus': 1, 'resources': {'scheduler-0': 1, 'scheduler': 1}}
        res = rm.allocate('ps')
        self.assertDictEqual(res, true)
        res = rm.allocate('ps')
        self.assertDictEqual(res, true)

    def test_allocate_3(self):
        cluster_res = {
            'scheduler-0': Machine('scheduler', 'scheduler-0', 1840,
                                   {'replay': 16, 'BRAIN': 1}),
            'scheduler-1': Machine('scheduler', 'scheduler-1', 1840,
                                   {'replay': 16}),
            'scheduler-2': Machine('scheduler', 'scheduler-2', 1840,
                                   {'replay': 16}),
            'scheduler-3': Machine('scheduler', 'scheduler-3', 1840,
                                   {'replay': 16}),
            'server-0': Machine('server', 'server-0', 920,
                                {'learner': 4, 'ps': 1}),
            'worker-0': Machine('worker', 'worker-0', 1840, {}),
            'worker-1': Machine('worker', 'worker-1', 1840, {}),
            'worker-10': Machine('worker', 'worker-10', 1840, {}),
            'worker-11': Machine('worker', 'worker-11', 1840, {}),
            'worker-12': Machine('worker', 'worker-12', 1840, {}),
            'worker-13': Machine('worker', 'worker-13', 1840, {}),
            'worker-14': Machine('worker', 'worker-14', 1840, {}),
            'worker-15': Machine('worker', 'worker-15', 1840, {}),
            'worker-2': Machine('worker', 'worker-2', 1840, {}),
            'worker-3': Machine('worker', 'worker-3', 1840, {}),
            'worker-4': Machine('worker', 'worker-4', 1840, {}),
            'worker-5': Machine('worker', 'worker-5', 1840, {}),
            'worker-6': Machine('worker', 'worker-6', 1840, {}),
            'worker-7': Machine('worker', 'worker-7', 1840, {}),
            'worker-8': Machine('worker', 'worker-8', 1840, {}),
            'worker-9': Machine('worker', 'worker-9', 1840, {})}
        node_res = {
            'BRAIN': {'num_cpus': 1, 'resources': {'BRAIN': 1, 'scheduler': 1}},
            'head': {'num_cpus': 1, 'resources': {'scheduler': 1}},
            'learner': {'num_cpus': 1, 'num_gpus': 1,
                        'resources': {'server': 1}},
            'logger': {'num_cpus': 1, 'resources': {'scheduler': 1}},
            'manager': {'num_cpus': 1, 'resources': {'scheduler': 1}},
            'ps': {'num_cpus': 1, 'resources': {'server': 1}},
            'replay': {'num_cpus': 1,
                       'resources': {'replay': 1, 'scheduler': 1}},
            'runner': {'num_cpus': 1, 'resources': {'scheduler': 1}},
            'sampler': {'num_cpus': 1, 'resources': {'server': 1}},
            'storage': {'num_cpus': 1, 'resources': {'scheduler': 1}},
            'util': {'num_cpus': 1, 'resources': {'scheduler': 1}},
            'worker': {'num_cpus': 1, 'resources': {'worker': 1}}}
        rm = ResourceManager(cluster_res, node_res)
        res = rm.allocate('BRAIN', None)
        res = rm.allocate('logger', {'num_cpus': 1,
                                     'resources': {'scheduler-0': 1,
                                                   'scheduler': 1}})
        res = rm.allocate('storage', {'num_cpus': 1,
                                      'resources': {'scheduler-0': 1,
                                                    'scheduler': 1}})
        res = rm.allocate('manager', {'num_cpus': 1,
                                      'resources': {'scheduler-0': 1,
                                                    'scheduler': 1}})
        print('manager', res)
        res = rm.allocate('util', {'num_cpus': 1,
                                   'resources': {'scheduler-0': 1,
                                                 'scheduler': 1}})
        print('util', res)
        res = rm.allocate('runner', {'num_cpus': 1,
                                     'resources': {'scheduler-0': 1,
                                                   'scheduler': 1}})
        print('runner', res)
        res = rm.allocate('replay', None)
        [rm.allocate('head', res) for _ in range(1)]
        print('replay', res)
        res = rm.allocate('replay', None)
        [rm.allocate('head', res) for _ in range(1)]
        print('replay', res)
        res = rm.allocate('replay', None)
        [rm.allocate('head', res) for _ in range(1)]
        print('replay', res)
        res = rm.allocate('replay', None)
        [rm.allocate('head', res) for _ in range(1)]
        print('replay', res)
