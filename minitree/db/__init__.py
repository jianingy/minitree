__all__ = ['InvalidPathError']


class PathError(Exception):
    pass


class ParentNotFound(Exception):
    pass


class NodeNotFound(Exception):
    pass


class NodeCreationError(Exception):
    pass


class PathDuplicatedError(Exception):
    pass


class DataTypeError(Exception):
    pass
