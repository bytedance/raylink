Demonstration
-------------
In what follows, we introduce how to implement an algorithm part by part based on RayLink system.

Step #0: load arguments from console
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Load arguments from the console and combine them with :py:class:`~raylink.algorithms.demo.config.UserConfig`.

.. code-block:: python

    def parse_args():
        import argparse
        from raylink.algorithms.demo.config import UserConfig
        import raylink.util.config as c
        cfg = UserConfig()
        parser = argparse.ArgumentParser()
        c.add_arguments(cfg, parser)
        args = parser.parse_args()
        c.post_parse_config(cfg, args)
        return cfg

    cfg = parse_args()
    raylink.init(user_cfg=cfg)


Step #1: Create Manager
~~~~~~~~~~~~~~~~~~~~~~~
:py:class:`~raylink.algorithms.demo.manager.Manager` is the algorithm entrance.

.. code-block:: python

    class Manager(raylink.OutlineNode):
        TYPE = 'manager'

        def setup(self):
            """Setup manager node, create storage, ps captain, logger and runner, 
            and put the config into the storage.
            """
            self.ps_captain = raylink.create(PSCaptain, self)
            self._storage.enable_log()
            self._logger.warning('Step 2 complete')
            self.runner_0 = raylink.create(Runner, self)
            self._logger.warning('Step 3 complete')

        def run(self):
            """Run the demo."""
            self.runner_0.set_tag('demo')
            self.runner_0.run()

    mgr = raylink.create(Manager)


Step #2: Create Runner
~~~~~~~~~~~~~~~~~~~~~~
:py:class:`~raylink.algorithms.demo.runner.Runner` is the global controller of the algorithm, controlling sampling and learning.

.. code-block:: python

    class Runner(raylink.OutlineNode):
        TYPE = 'runner'

        def setup(self):
            """Setup runner. Acquire storage, configuration, and logger. 
            Create learner, worker and ps tunnels.
            """
            # get config
            self.config = self._storage.get('config')
            # create a shm replay buffer for each learner
            self.replay = raylink.create(ShmReplay, self, cfg=self.config.replay)
            self.replay.setup(self.config.replay)
            self.create_head_tunnels()
            # create learners
            self.learner = raylink.create(Learner, self)
            # create workers
            self._logger.info('create worker')
            self.worker = raylink.create(Worker, self)
            self.create_ps_tunnel()


Step #3: Create Worker
~~~~~~~~~~~~~~~~~~~~~~
:py:class:`~raylink.algorithms.demo.worker.Worker` collects rollouts and stores them into a replay buffer.

.. code-block:: python

    class Worker(raylink.OutlineNode):
        TYPE = 'worker'

        def setup(self):
            """Setup worker. Acquire storage, config, logger, replay, replay heads. Create env.
            """

            self.config = self._storage.get('config')
            self.replay = self.find_path(os.path.dirname(self.path()) + '/replay-0')
            self.replay_head = self.find_path(
                random.choice(self.replay.get_write_heads_path()))
            self.env = gym.make(self.config.env.name)
            self.single_rollout = self.single_rollout_normal
            self._logger.info('Worker setup complete')
    
        def setup_policy(self):
            """Initialize policy."""

            self.policy = Policy(self.config.env.obs_n, self.config.env.act_n)
            self.policy.setup()


Step #4: Create Learner
~~~~~~~~~~~~~~~~~~~~~~~
:py:class:`~raylink.algorithms.demo.learner.Learner` samples from the replayer buffer and updates the parameters of its model by a specific algorithm.

.. code-block:: python

    class Learner(raylink.OutlineNode):
        TYPE = 'learner'

        def setup(self):
            """Setup learner node. Acquire storage, config, logger and replay."""

            # config
            self.config = self._storage.get('config')

            # sample
            self.batch_size = self.config.sampler.batch_size
            self.sampled_batch = [None]

            self.replay = self.find_path(
                os.path.join(os.path.dirname(self.path()), 'replay-0'))

            self._logger.info('Learner setup complete')


Step #5: Learner initializes params, and push params to PS 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In runner, 

.. code-block:: python

    def run(self):
        self.learner.setup_policy()
        self.learner.push_weights()


Step #6: Get Worker running
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In runner.run(),

.. code-block:: python

    self._storage.put(self.get_path(self.worker.get_nid()) + '/loop', True)
    self.worker.rollout_async()


Step #7: Step Learner to update params, push to ps
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In runner.run(),

.. code-block:: python

    raylink.get(self.learner.sample_async())
    raylink.get(self.learner.learn_async())
    self.learner.push_weights()


Step #8: Stop Worker and terminate running
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In runner.run(),

.. code-block:: python

    # put terminate signal for the worker to stop
    self._storage.put(self.get_path(self.worker.get_nid()) + '/loop', False)  
    
    self.worker.join()
