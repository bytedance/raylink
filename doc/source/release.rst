Release Note
------------
0.2.9
======
Storage now support save and load variables.

0.2.8
======
PS now support duplicate and delete a certain tag.

0.2.7
======
Remove `create_tunnel`. `get_tunnel` now will auto create a tunnel by the tag.

0.2.6
======
Support logtime. Use `raylink.util.logger.logtime` to log function running time. Check `demo/worker.py` for example.
Support gpu staging. Use `raylink.GPUStage` to create GPU staging area. Copy data to the GPU using `GPUStage`, which is about 800% faster than normal. Check `tests/test_stage.py` for example.
Support queue as replay. This queue is designed to use Tunnel by default. Check worker and learner for example. Notice worker will stuck when queue is full.

0.2.5
======
Improve offline attribute. Use `self.update_offline()` to upload new attribute.
Implement alias. When create node, use `alias` keyword argument to assign an alias to this node. Later use `self.find_alias()` to get node.
Now you don't have to initialize ps or create ps tunnel, just use `self._ps`.
Remove batch create, use `num` keyword argument to assign num.
Remove `set_tag` API, `basic` argument in `create`.

0.2.4
======
Implement offline attributes and methods

0.2.3
======
Complete change BRAIN to named actor
API for write head to push multiple samples

0.2.2
======
PS use tree update
Add api to get current node, storage and logger

0.2.1
======
PS use shared memory

0.2.0
======
Initialize logger and storage as basic node functionality
Change init function

0.1.12
======
Add model save and load for ps

0.1.11
======
change resource allocation part
support create node bind with parent machine

0.1.10
======
support async call for tunnel

0.1.9
======
support self defined pickle method
optimize readhead and writehead pickler

0.1.8
======
separate same name logger

0.1.7
======
move some code to data

0.1.6
======
change tunnel API to enable create multiple tunnel
change ps to use tunnel

0.1.5
======
create parent path for logger

0.1.4
======
change lock and unlock to protected method

0.1.3
======
raylink.init now support ray_cfg, a dict for every type of node

0.1.2
======
Add tunnel for OutlineNode

0.1.0
======
Add PS captain and officer
