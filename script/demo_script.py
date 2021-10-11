import raylink
from raylink.algorithms.demo.manager import Manager

"""
Dummy script shows steps to build a simple training process.

Steps:
0. load arguments from console
1. create manager with arguments
2. inside manager, create ps
3. create runner
4. inside runner, get handlers and create replay, workers, learners
5. learner initialize params, push params to ps
6. get worker running, add transitions to replay buffer, until replay is full
7. step learner to update params, push to ps
8. stop worker, finish a single run
"""


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


def main():
    cfg = parse_args()
    raylink.init(user_cfg=cfg)
    mgr = raylink.create(Manager)
    print('Step 1 complete')
    mgr.run()


if __name__ == '__main__':
    main()
