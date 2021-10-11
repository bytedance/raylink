Walkthrough
-----------
*RayLink* is a distributed system based on the actor schema of `Ray <https://github.com/ray-project/ray>`_. The idea of *RayLink* is inspired by StarCraft II: "For we are bound by the Khala, the sacred union of our every thought, and emotion!" In the *RayLink* system, all nodes are connected by a central node named *Brain*. *RayLink* focuses on how to access data but does not care about the algorithm implementation.

Installation
~~~~~~~~~~~~

To install *RayLink*, enter the raylink folder and run the following command

.. code-block:: bash

    pip install -e .

OutlineNode
~~~~~~~~~~~

Your algorithm will be built on a bunch of :py:class:`~raylink.node.OutlineNode`, each wraps a ray node running within an individual process. In particular, we have a central virtual outline node named :py:class:`~raylink.node.Brain`, which connects all the nodes and assigns each of them a unique path. There are some important class properties we list as below:

- :py:attr:`~raylink.node.OutlineNode._name`: the name of the current node, such as `BRAIN`, `learner-0`, etc. Use . :py:func:`~raylink.node.SelfAwareNode.name` to acquire.
- :py:attr:`~raylink.node.OutlineNode._path`: the absolute path of the current node assigned by :py:class:`~raylink.node.Brain`, such as `manager-0/runner-0/learner-0`. Use . :py:func:`~raylink.node.SelfAwareNode.path` to acquire.
- :py:attr:`~raylink.node.OutlineNode._ip`: the ip address of the current node. Use . :py:func:`~raylink.node.SelfAwareNode.get_ip` to acquire.
- :py:attr:`~raylink.node.OutlineNode._pid`: the process id of the current node. Use . :py:func:`~raylink.node.SelfAwareNode.get_pip` to acquire.

Also, we list the most commonly used member functions as follows.

- :py:func:`~raylink.node.OutlineNode.get_path`: get the path of a node by its nid.
- :py:func:`~raylink.node.OutlineNode.find_path`: get the node by its path.
- :py:func:`~raylink.node.OutlineNode.find_nid`: get the node by its nid.
- :py:func:`~raylink.node.OutlineNode.create_tunnel`: create a tunnel server.

As we mentioned above, each outline node can create a tunnel server. So, any other outline node attached to the client end can send information through the tunnel. Specifically, we enable the client node to call a function of the server node on the same or different machines by sending the function name and serealized arguments through the tunnel. The following example shows how to create a tunnel and call functions via it.

Example::

>>> import raylink
>>> raylink.init()
>>> node = raylink.create(raylink.OutlineNode)
>>> proxy = raylink.TunnelProxy(node.create_tunnel('example_tunnel'))
>>> proxy.name()  # call node.name() via the tunnel

We can build any type of nodes upon :py:class:`~raylink.node.OutlineNode`. RayLink will automatically wrap each OutlineNode function (e.g., `foo`) into two versions, a local function (e.g., `foo`) and a remote function (e.g., `foo_async`). The remote version will call `ray.remote` to execute this function asynchronously. We can use :py:func:`raylink.get` to acquire the return values.

For convenience, we have built several types of outline nodes useful for data processing and data transmission, such as :py:mod:`~raylink.data.tunnel`, :py:mod:`~raylink.data.replay.shm_replay`, :py:mod:`~raylink.data.ps`, and :py:mod:`~raylink.data.storage`. They all lie in :py:mod:`raylink.data` module. 

Replay Buffer
:::::::::::::
We provide a useful and high efficiency implementation of the shared-memory replay buffer as :py:class:`~raylink.data.replay.shm_replay.ShmReplay`. The  :py:class:`~raylink.data.replay.shm_replay.ShmReplay` node stores replay data in a shared memory, which is a new feature of *Python 3.8*. Reading from and writing to  the memory are through :py:class:`~raylink.data.replay.shm_replay.ReadHead` and :py:class:`~raylink.data.replay.shm_replay.WriteHead`, and :py:class:`~raylink.data.replay.shm_replay.ShmReplay` supports concurrent write/read operations and use *safe area* technique to avoid conflicts. You can create a :py:class:`~raylink.data.replay.shm_replay.ShmReplay` buffer instance with :py:func:`raylink.create` as the following example.

Example::

>>> import raylink
>>> raylink.init()
>>> from raylink.data.replay import ShmReplay
>>> replay = raylink.create(ShmReplay)  # TODO: specify the data structure.

Tunnel
::::::
In a nutshell, :py:mod:`~raylink.data.tunnel` builds information channels between any two nodes on the same or different machines. A tunnel consists of a :py:mod:`~raylink.data.tunnel.server` and a :py:mod:`~raylink.data.tunnel.client`. The node attached to the client end of the tunnel is able to call some function of the node on the server end by sending the name of the function and serialized arguments through the tunnel.

Parameter Server
::::::::::::::::
There are two kinds of nodes in :py:mod:`~raylink.data.ps`, one is the captain node (:py:class:`~raylink.data.ps.PSCaptain`), and the other is the officer node (:py:class:`~raylink.data.ps.PSOfficer`). After creating a captain node, it will generate a bunch of ps officers (according to configuration) and builds tunnels between itself and each officer. Note that the officers may not locate on the same machine with the captain. When pushing parameters to the captain node, it'll automatically push these parameters to all of its officers.

Example::

>>> import raylink
>>> raylink.init()
>>> from raylink.data.ps import PSCaptain
>>> ps = raylink.create(PSCaptain)


Storage
:::::::
:py:class:`~raylink.data.storage.Storage` is a node that stores information for global status and statistics. Each outline node can access the storage node via BRAIN. The following example shows how to leverage :py:class:`~raylink.data.storage.Storage`.

Example::

>>> import raylink
>>> raylink.init()
>>> node = raylink.create(raylink.OutlineNode)
>>> storage = node.find_path('storage-0')
>>> storage.put('current time step', 1)  # 'e75486aa-c7ff-4f39-99d6-25ff805c1a8a'
>>> storage.get('current time step')  # 1
>>> storage.incr('current time step', 2)  # 3
>>> storage.decr('current time step', 2)  # 1
