from .runner import Runner
import raylink
import time


class Manager(raylink.OutlineNode):
    TYPE = 'manager'

    def setup(self):
        """Setup manager node, create storage, ps captain, logger and runner, 
        and put the config into the storage.
        """
        st_time = time.time()
        self._storage.put('time', {'t': st_time})
        time.sleep(2)

        def update_time(v):
            v['t'] = time.time()
            return v['t']

        assert self._storage.func('time', update_time)[1] - st_time > 2
        self._logger.warning('Step 2 complete')
        # example to add new offline attribute
        self._runner_size = 1
        self.add_offline_attr('_runner_size')
        self.update_offline()
        self.runner_0 = raylink.create(Runner, self)
        self._logger.warning('Step 3 complete')

    @raylink.offlinemethod
    def runner_size_(self):
        return self._runner_size

    def run(self):
        """Run the demo."""
        self.runner_0.run()
