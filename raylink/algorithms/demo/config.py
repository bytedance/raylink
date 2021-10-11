import gym
import logging
import numpy as np

from raylink.util import config


class UserConfig(config.Config):
    """User defined config class."""

    # env config
    env = config.namespace()
    env.name = 'CartPole-v0'

    # replay config
    replay = config.namespace()
    replay.capacity = 100
    replay.structure = config.namespace()
    replay.num_write_head = 1
    replay.num_read_head = 1

    # learner
    learner = config.namespace()
    learner.backend = 'gloo'
    learner.master_port_range = (3000, 60000)

    sampler = config.namespace()
    sampler.batch_size = 16
    sampler.sample_batche_ratio = 5  # make sure replay has batch_factor * batch_size samples
    sampler.online_ratio = 0.3
    sampler.off_policy_step = 1
    sampler.cache_size = 1
    sampler.collect_idle_time = 0.01
    sampler.sample_idle_time = 0.1

    # runner
    runner = config.namespace()
    runner.num_workers_per_ip = 92
    runner.total_learn_step = int(1e5)
    runner.learn_per_step = 5
    runner.learn_time_scale = 5

    # worker
    worker = config.namespace()
    worker.rollout_num = 5
    worker.stack_size = 5

    policy = config.namespace()

    logger = config.namespace()
    logger.level = (logging.INFO, logging.DEBUG)
    logger.enable = (True, True)

    def setup(self):
        """Setup replay structure on CartPole game."""

        def ones(struct, dtype):
            return np.ones(struct, dtype=dtype) * -1

        _env = gym.make(self.env.name)
        obs_space = _env.observation_space
        act_space = _env.action_space
        self.env.obs_n = int(np.prod(obs_space.shape))
        self.env.act_n = int(np.prod(act_space.shape))
        self.replay.stack_size = 10
        stack_size = self.replay.stack_size
        self.replay.structure['s'] = [stack_size, self.env.obs_n], obs_space.dtype, None
        self.replay.structure['r'] = [stack_size, ], np.float32, None
        self.replay.structure['a'] = [stack_size, ], np.float32, None
        self.replay.structure['t'] = [stack_size, ], np.float32, None
        self.replay.structure['ts'] = (1,), np.float32, ones
        self.replay.structure['s_'] = [stack_size, self.env.obs_n], obs_space.dtype, None
        self.worker.dtype_replay = [
            {key: dtype for key, (_, dtype, _) in self.replay.structure.items()},
        ]
