import builtins
from collections import namedtuple


class EventGraph:
    Empty = namedtuple("Empty", "")
    Atom = namedtuple("Atom", "value")
    Sequentially = namedtuple("Sequentially", "prefix suffix")
    Concurrently = namedtuple("Concurrently", "left right")

    @staticmethod
    def empty():
        return EventGraph.Empty()

    @staticmethod
    def atom(value):
        return EventGraph.Atom(value)

    @staticmethod
    def sequentially(prefix, suffix):
        if type(prefix) == EventGraph.Empty:
            return suffix
        elif type(suffix) == EventGraph.Empty:
            return prefix
        return EventGraph.Sequentially(prefix, suffix)

    @staticmethod
    def concurrently(left, right):
        if type(left) == EventGraph.Empty:
            return right
        elif type(right) == EventGraph.Empty:
            return left
        return EventGraph.Concurrently(left, right)

    @staticmethod
    def to_string(event_graph, parent=None, use_str=False):
        if type(event_graph) == EventGraph.Empty:
            return ""
        if type(event_graph) == EventGraph.Atom:
            if use_str:
                return builtins.str(event_graph.value)
            else:
                return f"{event_graph.value.topic}={event_graph.value.value}"
        if type(event_graph) == EventGraph.Sequentially:
            res = f"{EventGraph.to_string(event_graph.prefix, parent=type(event_graph), use_str=use_str)};{EventGraph.to_string(event_graph.suffix, parent=type(event_graph), use_str=use_str)}"
            if parent == EventGraph.Concurrently:
                return f"({res})"
            else:
                return res
        if type(event_graph) == EventGraph.Concurrently:
            left_str = EventGraph.to_string(event_graph.left, parent=type(event_graph), use_str=use_str)
            right_str = EventGraph.to_string(event_graph.right, parent=type(event_graph), use_str=use_str)
            left_str, right_str = sorted([left_str, right_str])
            res = f"{left_str}|{right_str}"
            if parent == EventGraph.Sequentially:
                return f"({res})"
            else:
                return res
        return use_str(event_graph)

    @staticmethod
    def iter(event_graph):
        rest = event_graph
        while True:
            if type(rest) is EventGraph.Empty:
                return
            elif type(rest) is EventGraph.Atom:
                yield rest.value
                return
            elif type(rest) is EventGraph.Sequentially:
                if type(rest.prefix) is EventGraph.Sequentially:
                    rest = EventGraph.sequentially(
                        rest.prefix.prefix, EventGraph.sequentially(rest.prefix.suffix, rest.suffix)
                    )
                    continue
                elif type(rest.prefix) is EventGraph.Atom:
                    yield rest.prefix.value
                    rest = rest.suffix
                    continue
            elif type(rest) is EventGraph.Concurrently:
                raise ValueError("Cannot iterate across a concurrent node: " + EventGraph.to_string(rest))
            else:
                raise ValueError("Wat. " + str(rest))

    @staticmethod
    def filter(event_graph, topics):
        if type(event_graph) == EventGraph.Empty:
            return event_graph
        if type(event_graph) == EventGraph.Atom:
            if event_graph.value.topic in topics:
                return EventGraph.atom(event_graph.value)
            else:
                return EventGraph.empty()
        if type(event_graph) == EventGraph.Sequentially:
            return EventGraph.sequentially(EventGraph.filter(event_graph.prefix, topics), EventGraph.filter(event_graph.suffix, topics))
        if type(event_graph) == EventGraph.Concurrently:
            return EventGraph.concurrently(EventGraph.filter(event_graph.left, topics), EventGraph.filter(event_graph.right, topics))
        raise ValueError("Not an event_graph: " + str(event_graph))

    @staticmethod
    def filter_p(event_graph, predicate):
        if type(event_graph) == EventGraph.Empty:
            return event_graph
        if type(event_graph) == EventGraph.Atom:
            if predicate(event_graph.value):
                return EventGraph.atom(event_graph.value)
            else:
                return EventGraph.empty()
        if type(event_graph) == EventGraph.Sequentially:
            return EventGraph.sequentially(EventGraph.filter_p(event_graph.prefix, predicate), EventGraph.filter_p(event_graph.suffix, predicate))
        if type(event_graph) == EventGraph.Concurrently:
            return EventGraph.concurrently(EventGraph.filter_p(event_graph.left, predicate), EventGraph.filter_p(event_graph.right, predicate))
        raise ValueError("Not an event_graph: " + str(event_graph))

    @staticmethod
    def map(event_graph, f):
        if type(event_graph) == EventGraph.Empty:
            return event_graph
        if type(event_graph) == EventGraph.Atom:
            return EventGraph.atom(f(event_graph.value))
        if type(event_graph) == EventGraph.Sequentially:
            return EventGraph.sequentially(EventGraph.map(event_graph.prefix, f), EventGraph.map(event_graph.suffix, f))
        if type(event_graph) == EventGraph.Concurrently:
            return EventGraph.concurrently(EventGraph.map(event_graph.left, f), EventGraph.map(event_graph.right, f))
        raise ValueError("Not an event_graph: " + str(event_graph))

    @staticmethod
    def to_set(event_graph, f=lambda x: x):
        if type(event_graph) == EventGraph.Empty:
            return event_graph
        if type(event_graph) == EventGraph.Atom:
            return {f(event_graph.value)}
        if type(event_graph) == EventGraph.Sequentially:
            return EventGraph.to_set(event_graph.prefix, f).union(EventGraph.to_set(event_graph.suffix, f))
        if type(event_graph) == EventGraph.Concurrently:
            return EventGraph.to_set(event_graph.left, f).union(EventGraph.to_set(event_graph.right, f))
        raise ValueError("Not an event_graph: " + str(event_graph))

    @staticmethod
    def is_event_graph(event_graph):
        if type(event_graph) == EventGraph.Empty:
            return True
        if type(event_graph) == EventGraph.Atom:
            return True
        if type(event_graph) == EventGraph.Sequentially:
            return EventGraph.is_event_graph(event_graph.prefix) and EventGraph.is_event_graph(event_graph.suffix)
        if type(event_graph) == EventGraph.Concurrently:
            return EventGraph.is_event_graph(event_graph.left) and EventGraph.is_event_graph(event_graph.right)
        return False

    @staticmethod
    def is_empty(event_graph):
        if type(event_graph) == EventGraph.Empty:
            return True
        if type(event_graph) == EventGraph.Atom:
            return False
        if type(event_graph) == EventGraph.Sequentially:
            return False
        if type(event_graph) == EventGraph.Concurrently:
            return False
        raise ValueError("Not an event_graph: " + str(event_graph))