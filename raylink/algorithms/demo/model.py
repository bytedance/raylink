import numpy as np
import pickle


class Model(object):
    """This is a basic policy model."""

    def __init__(self, obs_n, act_n):
        self.obs_n = obs_n
        self.act_n = act_n

    def setup(self):
        """Initialize parameters"""

        self._params = np.random.randn(self.obs_n, self.act_n)

    def set_params_bytes(self, params):
        """Set parameters.

        Args:
            params (numpy.ndarray): Parameters
        """

        self._params = pickle.loads(params)

    def set_params(self, params):
        """Set parameters.

        Args:
            params (numpy.ndarray): Parameters
        """

        self._params = params

    def get_params_bytes(self):
        """Get parameters

        Returns:
            numpy.ndarray: Current parameters
        """
        return pickle.dumps(self._params)

    def get_params(self):
        """Get parameters

        Returns:
            numpy.ndarray: Current parameters
        """
        return self._params

    def forward(self, x):
        x = np.array(x)
        return np.matmul(x[np.newaxis, ...], self._params)
