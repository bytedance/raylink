from raylink import logtime
from .learner import Learner
from .worker import Worker
import raylink
import time


class Runner(raylink.OutlineNode):
    TYPE = 'runner'

    @logtime.info
    def setup(self):
        """Setup runner. Acquire storage, configuration, and logger. 
        Create learner, worker and ps tunnels.
        """
        # get config
        self.config = self._storage.get('config')

        # create replays
        self._logger.info('create replay')
        # create a shm replay buffer for each learner
        self.replay = raylink.create(
            raylink.ShmReplay, self, alias='replay', cfg=self.config.replay)
        self.queue = raylink.create(raylink.Queue, self, alias='queue', size=64)

        # create learners
        self._logger.info('create learner')
        self.learner = raylink.create(Learner, self)

        # create workers
        self._logger.info('create worker')
        self.workers = raylink.batch_create(Worker, self, num=2)

        self._logger.warning('Step 4 complete')

        self._logger.info('Runner setup complete')
        st_time = raylink.get_storage().get('time')
        self._logger.info(f'Setup takes {int(time.time() - st_time)} seconds')

    def run(self):
        """1. Learner push parameters to the ps captain. 
        
        2. Worker collects samples and put them to replay until the replay buffer is full.

        3. Learner samples from replay and learns from samples.

        4. Worker stops.
        """
        self._logger.info('runner can call offline method of manager, '
                          f'for example, runner_size {self.parent().runner_size_()}')
        self.learner.setup_policy('default')
        self._logger.info('push learner weights')
        self.learner.push_weights(wait=True)
        self._logger.info('all tags: {}'.format(self._ps.list_tags()))
        self._logger.info('Step 5 complete')
        for w in self.workers:
            w.setup_policy('default')
            # run worker async
            self._storage.put(w.path_() + '/loop', True)
            w.rollout_async()
        self._logger.info('Start rollout')
        # run learner async
        raylink.get(self.learner.sample_async())
        self._logger.info('Step 6 complete')
        raylink.get(self.learner.learn_async())
        self.learner.push_weights()
        self._logger.info('Step 7 complete')
        self._ps.duplicate('default', 'newbee')
        self._logger.info('all tags: {}'.format(self._ps.list_tags()))

        # stopping worker
        for w in self.workers:
            self._storage.put(w.path_() + '/loop', False)
        for w in self.workers:
            w.join()
        self._logger.info('Step 8 complete')
        self._logger.info('Make sure all workers stopped when deleting tag')
        self._ps.delete('default')
        self._logger.info('all tags: {}'.format(self._ps.list_tags()))
