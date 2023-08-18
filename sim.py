# This is a simplified Aerie for prototyping purposes
from collections import namedtuple
from typing import List
import inspect

Dataset = namedtuple("Dataset", "spans profiles events")
Telemetry = namedtuple("Telemetry", "evrs eha")
Directive = namedtuple("Directive", "type start_time args")
Span = namedtuple("Span", "start_time duration")
Profile = namedtuple("Profile", "name segments")
Segment = namedtuple("Segment", "start_time value")
Event = namedtuple("Event", "topic value")

Delay = namedtuple("Delay", "duration")
AwaitCondition = namedtuple("AwaitCondition", "condition")
Call = namedtuple("Call", "child_task")
Completed = namedtuple("Completed", "")


class Globals:
    def __init__(self):
        self.elapsed_time = 0
        self.events = []  # list of tuples (start_offset, event_graph)
        self.current_task_frame = TaskFrame()
        self.schedule = []
        self.model = None  # Filled in by register_model
        self.activity_types_by_name = None  # Filled in by register_model
        self.task_start_times = {}
        self.task_directives = {}
        self.task_inputs = {}


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
    def to_string(event_graph):
        if type(event_graph) == EventGraph.Empty:
            return ""
        if type(event_graph) == EventGraph.Atom:
            return f"{event_graph.value.topic}={event_graph.value.value}"
        if type(event_graph) == EventGraph.Sequentially:
            return f"{EventGraph.to_string(event_graph.prefix)};{EventGraph.to_string(event_graph.suffix)}"
        if type(event_graph) == EventGraph.Concurrently:
            return f"{EventGraph.to_string(event_graph.prefix)}|{EventGraph.to_string(event_graph.suffix)}"
class TaskFrame:
    """
    TODO: This currently is limited to linear chains of Sequentially
    """
    def __init__(self):
        self.event_graph = EventGraph.empty()

    def append(self, value):
        self.event_graph = EventGraph.sequentially(self.event_graph, EventGraph.atom(value))

    def clear(self):
        self.event_graph = EventGraph.empty()

    def __iter__(self):
        rest = self.event_graph
        while True:
            if type(rest) is EventGraph.Empty:
                return
            if type(rest) is EventGraph.Atom:
                yield rest.value
                return
            if type(rest) is EventGraph.Sequentially:
                if type(rest.prefix) is EventGraph.Sequentially:
                    rest = EventGraph.sequentially(rest.prefix.prefix, EventGraph.sequentially(rest.prefix.suffix, rest.suffix))
                    continue
                elif type(rest.prefix) is EventGraph.Atom:
                    yield rest.prefix.value
                    rest = rest.suffix
                    continue
            raise Exception("Unhandled: " + str(rest))



def initialize_mutable_globals():
    global globals_
    globals_ = Globals()


def register_model(cls):
    global globals_
    globals_.model = cls()
    globals_.activity_types_by_name = globals_.model.get_activity_types()
    return globals_.model


def emit(event):
    # TODO: globals_.elapsed_time,
    globals_.current_task_frame.append(event)


def spawn(directive_type, arguments):
    defer(directive_type, 0, arguments)


def defer(directive_type, duration, arguments):
    if inspect.isgeneratorfunction(directive_type):
        task = directive_type.__call__(globals_.model, **arguments)
        globals_.schedule.append(
            (globals_.elapsed_time + duration, task)
        )
    else:
        task = make_generator(directive_type, dict(**arguments, model=globals_.model))
        globals_.schedule.append(
            (globals_.elapsed_time + duration, task)
        )
    # NOTE: this is the only place that task start time is defined - assumption is that all tasks are started via defer
    globals_.task_start_times[task] = globals_.elapsed_time + duration
    globals_.task_inputs[task] = (directive_type, arguments)
    return task


class RegisterCell:
    def __init__(self, topic, initial_value):
        self._topic = topic
        self._initial_value = initial_value

    def get(self):
        res = self._initial_value
        for event in linearize(globals_.events, globals_.current_task_frame):
            if event.topic == self._topic:
                res = event.value
        return res

    def set(self, new_value):
        emit(Event(self._topic, new_value))

    def __add__(self, other):
        return self.get() + other

    def __sub__(self, other):
        return self.get() - other

    def __gt__(self, other):
        def condition():
            return self.get() > other

        return condition

    def is_equal_to(self, other):
        def condition():
            return self.get() == other

        return condition

    def __lt__(self, other):
        def condition():
            return self.get() < other

        return condition


class Accumulator:
    SetValue = namedtuple("Set", "new_value")
    SetRate = namedtuple("Set", "new_rate")

    def __init__(self, topic, value, rate):
        self._topic = topic
        self._initial_value = value
        self._initial_rate = rate

    def get(self):
        value = self._initial_value
        rate = self._initial_rate
        previous_event_time = 0
        for event in linearize(globals_.events, globals_.current_task_frame):
            if event.topic != self._topic:
                continue
            value += rate * (event.start_time - previous_event_time)
            previous_event_time = event.start_time
            if type(event.value) == Accumulator.SetValue:
                value = event.value.new_value
            if type(event.value) == Accumulator.SetRate:
                rate = event.value.new_rate
        value += rate * (globals_.elapsed_time - previous_event_time)
        return value

    def __repr__(self):
        return f"{self._topic}, {self.get()}"


class Plan:
    def __init__(self, directives):
        self.directives: List[Directive] = list(directives)


def simulate(plan):
    for directive in sorted(plan.directives, key=lambda directive: directive.start_time):
        directive_type = globals_.activity_types_by_name[directive.type]
        task = defer(directive_type, directive.start_time, directive.args)
        globals_.task_directives[task] = directive

    awaiting_conditions = []
    spans = []

    while globals_.schedule:
        globals_.schedule = sorted(globals_.schedule, key=lambda x: x[0])
        resume_time, task = globals_.schedule.pop(0)
        if resume_time > globals_.elapsed_time and not type(
                globals_.current_task_frame.event_graph) == EventGraph.Empty:
            globals_.events.append((globals_.elapsed_time, globals_.current_task_frame.event_graph))
            globals_.current_task_frame.clear()

        globals_.elapsed_time = resume_time

        task_status = step(task)
        if type(task_status) == Delay:
            globals_.schedule.append((resume_time + task_status.duration, task))
        elif type(task_status) == AwaitCondition:
            awaiting_conditions.append((task_status.condition, task))
        elif type(task_status) == Completed:
            spans.append((globals_.task_directives.get(task, (globals_.task_inputs[task][0].__name__, globals_.task_inputs[task][1])), globals_.task_start_times[task], globals_.elapsed_time))

        old_awaiting_conditions = list(awaiting_conditions)
        awaiting_conditions.clear()
        while old_awaiting_conditions:
            condition, task = old_awaiting_conditions.pop()
            if condition():
                globals_.schedule.insert(0, (globals_.elapsed_time, task))
            else:
                awaiting_conditions.append((condition, task))
    if not type(globals_.current_task_frame.event_graph) == EventGraph.Empty:
        globals_.events.append((globals_.elapsed_time, globals_.current_task_frame.event_graph))
        globals_.current_task_frame.clear()
    return sorted(spans, key=lambda x: (x[1], x[2])), list(globals_.events)


def linearize(events, current_task_frame):
    print("linearize")
    for _, eg in events:
        tf = TaskFrame()
        tf.event_graph = eg
        for x in tf:
            yield x
    for x in current_task_frame:
        yield x


def execute(commands):
    return Telemetry([], [])


def step(task):
    try:
        return next(task)
    except StopIteration:
        return Completed()


def make_generator(f, arguments):
    yield f(**arguments)
