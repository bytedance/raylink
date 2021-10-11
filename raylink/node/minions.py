__all__ = ['SuperVillain']

from raylink.util.resource import get_identity
from .outline import OutlineNode


class SuperVillain(OutlineNode):
    TYPE = 'supervillain'

    def setup(self, minion_class):
        """Wake up minions."""
        self._wakeup_minion(minion_class)

    def _wakeup_minion(self, minion_class):
        import raylink
        rm = self._BRAIN.get_res_mgr()
        if rm.cluster_mode:
            # cluster mode
            basic = 1
            num_node = 0
            ps_node_cfg = []
            for _id, m in rm.cluster_cfg.items():
                cfg = {
                    'num_cpus': basic,
                    'resources': {m.role: basic, m.identity: basic}
                }
                ps_node_cfg.append(cfg)
                num_node += 1
        else:
            num_node = 1
            ps_node_cfg = [None]
        self.minions = [raylink.ccon(minion_class, self, node_cfg=ps_node_cfg[i])
                        for i in range(num_node)]
        self.node_cfg_minions_nid_map = {}
        self.nid_minions_map = {}
        for m in self.minions:
            _node_cfg = get_identity(m.node_cfg_())
            self.node_cfg_minions_nid_map[_node_cfg] = m.nid_()
            self.nid_minions_map[m.nid_()] = m

    def get_minion(self, node_cfg):
        return self.node_cfg_minions_nid_map[get_identity(node_cfg)]
