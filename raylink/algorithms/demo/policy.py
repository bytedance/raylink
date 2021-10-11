from .model import Model
import numpy as np
import time


def softmax(x):
    r"""Compute softmax values for each sets of scores in $x$.

    Args:
        x (numpy.ndarray): Input vector to compute softmax

    Returns:
        numpy.ndarray: softmax(x)
    """
    e_x = np.exp(x - np.max(x))
    return e_x / e_x.sum()


class Policy(object):
    """A basic policy class"""

    TYPE = 'policy'

    def __init__(self, obs_n, act_n):
        self.obs_n = obs_n
        self.act_n = act_n
        self.learn_step = 0

    def setup(self):
        """Initialize policy model"""

        self.model = Model(self.obs_n, self.act_n)
        self.model.setup()

    def sample(self, obs):
        """Sample actions with respect to the observation

        Args:
            obs (numpy.ndarray): The observation.

        Returns:
            np.ndarray: Sampled actions.
        """

        logit = self.model.forward(obs)
        p = softmax(logit)[0]
        a = np.random.choice(self.act_n, 1, p=p)[0]
        return a

    def learn(self, sample):
        """Learn with sample

        Args:
            sample (numpy.ndarray): Samples containing 'r'
        """
        r = sample['r']
        r = np.transpose(r)[0]
        loss = r[0]
        params = self.model.get_params()
        grad = loss - np.random.randn(*params.shape)
        params += grad
        self.model.set_params(params)
        time.sleep(0.1)
