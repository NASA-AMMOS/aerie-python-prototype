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
    def to_string(event_graph, parent=None):
        if type(event_graph) == EventGraph.Empty:
            return ""
        if type(event_graph) == EventGraph.Atom:
            return f"{event_graph.value.topic}={event_graph.value.value}"
        if type(event_graph) == EventGraph.Sequentially:
            res = f"{EventGraph.to_string(event_graph.prefix, parent=type(event_graph))};{EventGraph.to_string(event_graph.suffix, parent=type(event_graph))}"
            if parent == EventGraph.Concurrently:
                return f"({res})"
            else:
                return res
        if type(event_graph) == EventGraph.Concurrently:
            res = f"{EventGraph.to_string(event_graph.left, parent=type(event_graph))}|{EventGraph.to_string(event_graph.right, parent=type(event_graph))}"
            if parent == EventGraph.Sequentially:
                return f"({res})"
            else:
                return res
        return str(event_graph)

    @staticmethod
    def iter(event_graph):
        rest = event_graph
        while True:
            if type(rest) is EventGraph.Empty:
                return
            if type(rest) is EventGraph.Atom:
                yield rest.value
                return
            if type(rest) is EventGraph.Sequentially:
                if type(rest.prefix) is EventGraph.Sequentially:
                    rest = EventGraph.sequentially(
                        rest.prefix.prefix, EventGraph.sequentially(rest.prefix.suffix, rest.suffix)
                    )
                    continue
                elif type(rest.prefix) is EventGraph.Atom:
                    yield rest.prefix.value
                    rest = rest.suffix
                    continue
            if type(rest) is EventGraph.Concurrently:
                raise ValueError("Cannot iterate across a concurrent node: " + EventGraph.to_string(rest))

    @staticmethod
    def filter(event_graph, topic):
        if type(event_graph) == EventGraph.Empty:
            return event_graph
        if type(event_graph) == EventGraph.Atom:
            if event_graph.value.topic == topic:
                return event_graph
            else:
                return EventGraph.empty()
        if type(event_graph) == EventGraph.Sequentially:
            return EventGraph.sequentially(EventGraph.filter(event_graph.prefix, topic), EventGraph.filter(event_graph.suffix, topic))
        if type(event_graph) == EventGraph.Concurrently:
            return EventGraph.concurrently(EventGraph.filter(event_graph.left, topic), EventGraph.filter(event_graph.right, topic))
        raise ValueError("Not an event_graph: " + str(event_graph))