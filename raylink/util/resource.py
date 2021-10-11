__all__ = ['Machine', 'ResourceManager', 'get_identity']

import copy
import sys


def get_identity(node_cfg):
    if node_cfg is None: return
    return max(list(node_cfg['resources'].keys()))


class OutOfResourceError(Exception):
    """Raised when out of resource"""
    MESSAGE = 'Error {}. No {} resource available in cluster. ' \
              'node resources: {}, cluster resources: {}'

    def __init__(self, code, node_type, cluster_cfg, node_cfg):
        super(OutOfResourceError, self).__init__(
            self.MESSAGE.format(code, node_type, node_cfg, cluster_cfg))


class CustomResourceNotFoundError(Exception):
    """Raised when can not find custom resource in a ray configuration"""
    MESSAGE = '{} ray configuration should contains custom resources setting'

    def __init__(self, node_type):
        super(CustomResourceNotFoundError, self).__init__(
            self.MESSAGE.format(node_type))


class Machine(object):
    def __init__(self, role, identity, basic, additional=None):
        """Define a machine in cluster

        Args:
            role (str): Role of machine
            identity (str): Identity of machine
            basic (int): Number of basic resource
            additional (dict): Dictionary of additional resources

        Examples::

            >>> Machine('scheduler', 'scheduler-0', 90, {'BRAIN': 1})
        """
        if additional is None:
            additional = {}
        self.role = role
        self.identity = identity
        self.basic = basic
        self.ori_additional = self.additional = copy.deepcopy(additional)
        self.resources = {role: basic, identity: basic}
        self.additional.update({'ps_officer': 1, 'telescreen': 1})
        self.resources.update(copy.deepcopy(self.additional))

    def __repr__(self):
        return f"Machine('{self.role}', '{self.identity}', " \
               f"{self.basic}, {self.ori_additional})"


class ResourceManager(object):
    def __init__(self, cluster_cfg, node_cfg):
        self.cluster_cfg = cluster_cfg
        self.node_cfg = node_cfg
        if cluster_cfg and node_cfg:
            self.cluster_mode = True
        else:
            self.cluster_mode = False
        self.ids = list(self.cluster_cfg.keys())
        self.update_resource_map()

    def update_resource_map(self):
        if not self.cluster_mode:
            return
        self.primary_resource_map = {}
        self.resource_map = {}
        # resource map = key -> resource type, value -> dict
        # key -> how many, value -> list of machines
        for _id, m in self.cluster_cfg.items():
            assert m.role in m.resources, \
                f'{m.role} not in node custom resource, {m.resources}'
            for t, v in m.additional.items():
                if t not in self.primary_resource_map:
                    self.primary_resource_map[t] = {}
                if v not in self.primary_resource_map[t]:
                    self.primary_resource_map[t][v] = []
                assert v > 0, f"{t} of {m.identity} must greater than 0"
                self.primary_resource_map[t][v].append(m)
            for t, v in m.resources.items():
                if t not in self.resource_map:
                    self.resource_map[t] = {}
                if v not in self.resource_map[t]:
                    self.resource_map[t][v] = []
                assert v > 0, f"{t} of {m.identity} must greater than 0"
                self.resource_map[t][v].append(m)

    @staticmethod
    def _modify(resource_map, res_type, machine, attr, value):
        old_v = getattr(machine, attr)[res_type]
        resource_map[res_type][old_v].remove(machine)
        if len(resource_map[res_type][old_v]) == 0:
            resource_map[res_type].pop(old_v)
        assert getattr(machine, attr)[res_type] >= value
        getattr(machine, attr)[res_type] -= value
        new_v = getattr(machine, attr)[res_type]
        if new_v == 0:
            getattr(machine, attr).pop(res_type)
            return
        if new_v not in resource_map[res_type]:
            resource_map[res_type][new_v] = []
        resource_map[res_type][new_v].append(machine)

    def modify(self, machine, resource):
        if machine.role in resource and machine.identity not in resource:
            resource[machine.identity] = resource[machine.role]
        for t, v in resource.items():
            if t not in machine.resources:
                raise Exception(f"No {t} in {machine.role}")
            if machine.resources[t] < v:
                raise Exception(f"No enough {t} in {machine.role}")
            self._modify(self.resource_map, t, machine, 'resources', v)
            if t not in self.primary_resource_map:
                continue
            self._modify(self.primary_resource_map, t, machine, 'additional', v)

    def validate(self, node_type):
        node_res = self.node_cfg[node_type]
        node_res = copy.deepcopy(node_res)
        custom_resources = node_res['resources']
        valid = None
        valids = {}
        primary = False
        for t, v in custom_resources.items():
            if t not in self.primary_resource_map:
                continue
            primary = True
            nums = sorted(list(self.primary_resource_map[t].keys()))
            if nums[-1] < v:
                return False, -2
            _valid = set(self.primary_resource_map[t][nums[-1]])
            valids[t] = _valid
            if valid is None:
                valid = _valid
            else:
                valid.intersection(_valid)
        if primary and len(valid) == 0:
            print('Validate resource fail, valids:', valids, file=sys.stderr)
            return False, -3
        if primary:
            return True, valid
        for t, v in custom_resources.items():
            if t not in self.resource_map:
                return False, -1
            nums = sorted(list(self.resource_map[t].keys()))
            if nums[-1] < v:
                return False, -2
            _valid = set(self.resource_map[t][nums[-1]])
            valids[t] = _valid
            if valid is None:
                valid = _valid
            else:
                valid.intersection(_valid)
        if len(valid) == 0:
            print('Validate resource fail, valids:', valids, file=sys.stderr)
            return False, -3
        return True, valid

    def allocate(self, node_type, parent_node_res=None):
        """Allocate node resources
        If parent_node_res is present,
        then it will allocate with respect to the parent node

        See test case `tests/test_resources.py` for more info.

        Args:
            node_type (str): node type
            parent_node_res (dict): resources that allocate by raylink

        Returns:
            dict: resource for the node
        """
        if not self.cluster_mode:
            return
        if node_type not in self.node_cfg:
            raise Exception(f"{node_type} not in node resources config.")
        node_res = self.node_cfg[node_type]
        assert 'resources' in node_res, CustomResourceNotFoundError(node_type)
        node_res = copy.deepcopy(node_res)
        custom_res = node_res['resources']
        assert isinstance(custom_res, dict)
        if parent_node_res is None:
            # allocate new cfg
            flag, valid = self.validate(node_type)
            if not flag:
                raise OutOfResourceError(
                    valid, node_type, self.cluster_cfg, node_res)
            sorted_v = sorted(list(valid), key=lambda m: m.identity)
            sel_m = sorted_v[0]
        else:
            # allocate same
            parent_custom_res = parent_node_res['resources']
            identity = ''
            for identity in self.cluster_cfg:
                if identity in parent_custom_res:
                    break
            sel_m = self.cluster_cfg[identity]
        self.modify(sel_m, custom_res)
        _resources = {
            sel_m.identity: custom_res[sel_m.role],
            sel_m.role: custom_res[sel_m.role]
        }
        node_res['resources'] = _resources
        return node_res
