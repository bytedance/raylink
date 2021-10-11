from easydict import EasyDict
import time
import os

import raylink
from .policy import Policy


class Learner(raylink.OutlineNode):
    TYPE = 'learner'

    def setup(self):
        """Setup learner node. Acquire storage, config, logger and replay."""

        # config
        self.config = self._storage.get('config')

        # sample
        self.batch_size = self.config.sampler.batch_size
        self.sampled_batch = [None]

        self.replay = self.find_alias('replay')
        self.rheads = [self.find_path(path)
                       for path in self.replay.get_read_heads_path()]
        self.rkeys = self.replay.get_keys()
        self.queue = self.find_alias('queue')

        self._logger.info('Learner setup complete')

    def setup_policy(self, tag):
        """Initialize policy."""

        # create model and policies
        self.policy = Policy(self.config.env.obs_n, self.config.env.act_n)
        self.policy.setup()
        self.weights_id = ''
        self._tag = tag

    def sample(self):
        """Sample from the replay."""

        self.queue.get_t(self.batch_size)
        indices, _ = self.replay.acquire_safe_area(
            self.nid_(), self.batch_size, self.rkeys)
        assert len(indices) == self.batch_size
        # Only use the first readhead
        samples = self.rheads[0].read_t_async(
            self.rkeys, cursors={k: indices for k in self.rkeys}, count=True)
        self.sampled_batch = raylink.get(samples)

    def learn(self):
        """Update policy with the batch sampled"""

        # sample data from replay buffer
        data = self.sampled_batch
        # calculate loss
        self.policy.learn(EasyDict(data))

    def push_weights(self, wait=False):
        """Push policy parameters to PS captain."""

        params = self.policy.model.get_params_bytes()
        self._ps.push(self._tag, params, self.policy.learn_step, wait=wait)

    def update_weights(self):
        """Update policy parameters from PS caption"""
        weights_id = self._ps.get_ids([self._tag()])[0]
        if weights_id == self.weights_id:
            return
        weights, info = self._ps.pull_tags([self._tag])[0]
        self.learn_step = info
        self.set_weights(weights)
        self.weights_id = weights_id

    def set_weights(self, params):
        """Set policy parameters

        Args:
            params (numpy.ndarray): Parameters
        """
        self.policy.model.set_params_bytes(params)
