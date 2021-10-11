from raylink import logtime
from .policy import Policy
import random
import raylink
import gym


class Worker(raylink.OutlineNode):
    TYPE = 'worker'

    @logtime.info('worker: {dur:.3f}s env: {self.config.env.name}', index=0)
    def setup(self):
        """Setup worker. Acquire storage, config, logger, replay, replay heads. Create env.
        """

        self.config = self._storage.get('config')
        self.replay = self.find_alias('replay')
        self.rhead = self.find_path(
            random.choice(self.replay.get_write_heads_path()))
        self.queue = self.find_alias('queue')
        self.env = gym.make(self.config.env.name)
        self._logger.info('Worker setup complete')

    def setup_policy(self, tag):
        """Initialize policy."""

        self.policy = Policy(self.config.env.obs_n, self.config.env.act_n)
        self.policy.setup()
        self.weights_id = ''
        self._tag = tag

    def update_params(self):
        """Update parameters from ps."""

        weights_id = self._ps.get_id(self._tag)
        if weights_id == self.weights_id:
            return
        weights, info = self._ps.pull_tag(self._tag)
        self.learn_step = info
        self.policy.model.set_params_bytes(weights)

    def rollout(self):
        """Collect rollouts until the replay is full."""

        flag = True
        while flag:
            self._llogger.debug('update params')
            self.update_params()
            self._llogger.debug('episode')
            self.episode()
            flag = self._storage.get(self.path_() + '/loop')
        self._logger.info('Worker loop end.')

    def episode(self):
        """Collect a simple trajectory and store it in the replay."""

        s = self.env.reset()
        while True:
            a = self.policy.sample(s)
            s_, r, done, info = self.env.step(a)
            t = 1 if done else 0
            # pre-set this structure in config.py
            sample = {'s': s, 'a': a, 'r': r, 't': t, 's_': s_}
            sample['ts'] = self.learn_step
            self.rhead.write_inc_t(sample)
            self.queue.put_t(sample)
            if done:
                break
