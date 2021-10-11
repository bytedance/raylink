__all__ = ['Config', 'add_arguments', 'post_parse_config']

from easydict import EasyDict as namespace
import logging


def _parse_config(cfg, field=''):
    args = []
    for o in dir(cfg):
        _attr = getattr(cfg, o)
        if o.startswith('__') or callable(_attr):
            continue
        name = f'{field}.{o}' if field else o
        if isinstance(_attr, namespace):
            _dict = {'name': name, 'arg_type': 'group',
                     'value': _parse_config(_attr, name)}
            args.append(_dict)
        elif isinstance(_attr, list) and len(_attr) == 2 and \
                isinstance(_attr[1], dict) and 'help' in _attr[1]:
            _dict = {'name': name, 'arg_type': 'single',
                     'default': _attr[0], 'metavar': str(_attr[0])}
            _dict.update(_attr[1])
            args.append(_dict)
        else:
            _dict = {'name': name, 'arg_type': 'single',
                     'default': _attr, 'metavar': str(_attr)}
            args.append(_dict)
    return args


def add_arguments(cfg, parser):
    def try_cast(obj):
        try:
            return eval(obj)
        except:
            return obj

    def _add(args, _parser, _base_parser):
        for arg in args:
            name = arg.pop('name')
            arg_type = arg.pop('arg_type')
            if arg_type == 'group':
                __parser = _parser
                if name.count('.') > 0:
                    __parser = _base_parser
                _group = __parser.add_argument_group(title=name)
                _add(arg['value'], _group, _base_parser)
            elif arg_type == 'single':
                if 'type' not in arg:
                    arg['type'] = try_cast
                _parser.add_argument(f'--{name}', **arg)

    cfg_args = _parse_config(cfg)
    _add(cfg_args, parser, parser)


def _clean_config(cfg):
    """
    Remove help string from config

    Args:
        cfg: config object

    """
    for o in dir(cfg):
        _attr = getattr(cfg, o)
        if o.startswith('__') or callable(_attr):
            continue
        if isinstance(_attr, namespace):
            _clean_config(_attr)
        elif isinstance(_attr, list) and len(_attr) == 2 and \
                isinstance(_attr[1], dict) and 'help' in _attr[1]:
            setattr(cfg, o, _attr[0])
        else:
            setattr(cfg, o, _attr)


def _replace_config(cfg, args):
    """
    Replace value in default configuration.

    Args:
        cfg: config object
        args: args object from argparser

    Returns:

    """

    def _setattr(obj, name, value):
        if name.count('.') == 0:
            return setattr(obj, name, value)
        name = name.split('.')
        return _setattr(getattr(obj, name[0]), '.'.join(name[1:]), value)

    # get all set value from args object
    args_dict = vars(args)
    # value already cast using type or eval()
    for k, v in args_dict.items():
        _setattr(cfg, k, v)

    return cfg


def print_config(cfg, indent=''):
    for o in dir(cfg):
        _attr = getattr(cfg, o)
        if o.startswith('__') or callable(_attr):
            continue
        if isinstance(_attr, namespace):
            print(indent + o + ':')
            print_config(_attr, indent + ' ' * 4)
        elif isinstance(_attr, list) and len(_attr) == 2 and \
                isinstance(_attr[1], dict) and 'help' in _attr[1]:
            print(indent + o + "\t" * 2 + str(_attr[0]))
        else:
            print(indent + o + "\t" * 2 + str(_attr))


def post_parse_config(cfg, args):
    _clean_config(cfg)
    _replace_config(cfg, args)
    cfg.setup()


class Config(object):
    def setup(self):
        pass

    def __getstate__(self):
        state_dict = {}
        for o in dir(self):
            _attr = getattr(self, o)
            if o.startswith('__') or callable(_attr):
                continue
            state_dict[o] = _attr
        return state_dict

    def __setstate__(self, state):
        for o in dir(self):
            if o in state:
                setattr(self, o, state[o])

    @staticmethod
    def _str_config(cfg, indent=''):
        lines = []
        for o in dir(cfg):
            _attr = getattr(cfg, o)
            if o.startswith('__') or callable(_attr):
                continue
            if isinstance(_attr, namespace):
                lines.append(indent + o + ':')
                lines.append(Config._str_config(_attr, indent + ' ' * 4))
            elif isinstance(_attr, list) and len(_attr) == 2 and \
                    isinstance(_attr[1], dict) and 'help' in _attr[1]:
                lines.append(indent + o + "\t" * 2 + str(_attr[0]))
            else:
                lines.append(indent + o + "\t" * 2 + str(_attr))
        return '\n'.join(lines)

    def __str__(self):
        return Config._str_config(self)


class BasicConfig(Config):
    logger = namespace()
    logger.level = (logging.INFO, logging.DEBUG)
    logger.enable = (True, True)
