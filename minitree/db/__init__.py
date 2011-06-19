__all__ = ['InvalidPathError']


class InvalidPathError(Exception):
    pass


class NodeNotFound(Exception):
    pass


class NodeCreationError(Exception):
    pass


class PathDuplicatedError(Exception):
    pass
