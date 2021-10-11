from datetime import datetime
import socket
import os

import raylink.constants as c


def get_path(name='log', abspath=None, relative_path=None,
             _file=None, parent=False):
    """Create path if path don't exist
    Args:
        name: folder name
        abspath: absolute path to be prefix
        relative_path: relative path that can be convert into absolute path
        _file: use directory based on _file
        parent: whether the path is in the parent folder
    Returns: Path of the folder
    """
    import os
    if name.startswith('~/'):
        name = os.path.expanduser('~') + name[1:]
    if abspath:
        directory = os.path.abspath(os.path.join(abspath, name))
    elif relative_path:
        directory = os.path.abspath(os.path.join(
            os.path.abspath(relative_path), name))
    else:
        import sys
        if getattr(sys, 'frozen', False):
            application_path = os.path.dirname(sys.executable)
            directory = os.path.abspath(
                os.path.join(application_path, name))
        elif _file:
            if parent:
                directory = os.path.abspath(
                    os.path.join(os.path.dirname(_file), os.pardir, name))
            else:
                directory = os.path.abspath(
                    os.path.join(os.path.dirname(_file), name))
        else:
            if parent:
                directory = os.path.abspath(
                    os.path.join(os.path.dirname(__file__), os.pardir, name))
            else:
                directory = os.path.abspath(
                    os.path.join(os.path.dirname(__file__), name))
    os.makedirs(directory, exist_ok=True)
    return directory


def get_run_rel_path(comment):
    current_time = datetime.now().strftime('%b%d_%H-%M-%S')
    if comment == '':
        folder = '_'.join([current_time, socket.gethostname()])
    else:
        folder = '_'.join([current_time, socket.gethostname(), comment])
    log_dir = os.path.join(c.RUN_PATH, folder)
    return log_dir
