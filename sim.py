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
        self.schedule = JobSchedule()
        self.model = None  # Filled in by register_model
        self.activity_types_by_name = None  # Filled in by register_model
        self.task_start_times = {}
        self.task_directives = {}
        self.task_inputs = {}
        self.awaiting_conditions = []
        self.spans = []


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
            return f"({EventGraph.to_string(event_graph.prefix)};{EventGraph.to_string(event_graph.suffix)})"
        if type(event_graph) == EventGraph.Concurrently:
            return f"({EventGraph.to_string(event_graph.left)}|{EventGraph.to_string(event_graph.right)})"
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


class TaskFrame:
    Branch = namedtuple("Branch", "base event_graph")

    def __init__(self, history=EventGraph.empty()):
        self.tip = EventGraph.empty()
        if type(history) == list:
            self.history = EventGraph.empty()
            for _, commit in history:
                self.history = EventGraph.sequentially(self.history, commit)
        else:
            self.history = history
        self.branches = []

    def emit(self, value):
        self.tip = EventGraph.sequentially(self.tip, EventGraph.atom(value))

    def spawn(self, event_graph):
        self.branches.append((self.tip, event_graph))
        self.tip = EventGraph.empty()

    def get_current_history(self):
        res = self.history
        for base, _ in self.branches:
            res = EventGraph.sequentially(res, base)
        res = EventGraph.sequentially(res, self.tip)
        return res

    def collect(self):
        res = self.tip
        for base, event_graph in reversed(self.branches):
            res = EventGraph.sequentially(base, EventGraph.concurrently(event_graph, res))
        return res


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
    globals_.current_task_frame.emit(event)


def spawn(directive_type, arguments):
    task = make_task(directive_type, arguments)
    globals_.task_start_times[task] = globals_.elapsed_time
    globals_.task_inputs[task] = (directive_type, arguments)

    parent_task_frame = globals_.current_task_frame

    task_status, events = step(task)
    if type(task_status) == Delay:
        globals_.schedule.schedule(globals_.elapsed_time + task_status.duration, task)
    elif type(task_status) == AwaitCondition:
        globals_.awaiting_conditions.append((task_status.condition, task))
    elif type(task_status) == Completed:
        globals_.spans.append(
            (
                globals_.task_directives.get(
                    task, (globals_.task_inputs[task][0].__name__, globals_.task_inputs[task][1])
                ),
                globals_.task_start_times[task],
                globals_.elapsed_time,
            )
        )

    parent_task_frame.spawn(events)
    globals_.current_task_frame = parent_task_frame


def defer(directive_type, duration, arguments):
    task = make_task(directive_type, arguments)
    globals_.schedule.schedule(globals_.elapsed_time + duration, task)
    globals_.task_start_times[task] = globals_.elapsed_time + duration
    globals_.task_inputs[task] = (directive_type, arguments)
    return task


def make_task(directive_type, arguments):
    if inspect.isgeneratorfunction(directive_type):
        return directive_type.__call__(globals_.model, **arguments)
    else:
        return make_generator(directive_type, dict(**arguments, model=globals_.model))


class RegisterCell:
    def __init__(self, topic, initial_value):
        self._topic = topic
        self._initial_value = initial_value

    def get(self):
        res = self._initial_value
        for event in EventGraph.iter(EventGraph.filter(globals_.current_task_frame.get_current_history(), self._topic)):
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


class JobSchedule:
    def __init__(self):
        self._schedule = {}

    def schedule(self, start_offset, task):
        if not start_offset in self._schedule:
            self._schedule[start_offset] = []
        for _, batch in self._schedule.items():
            if task in batch:
                raise Exception("Double scheduling task: " + str(task))
        self._schedule[start_offset].append(task)

    def peek_next_time(self):
        return min(self._schedule)

    def get_next_batch(self):
        next_time = self.peek_next_time()
        res = self._schedule[next_time]
        del self._schedule[next_time]
        return res

    def is_empty(self):
        return len(self._schedule) == 0


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
        for event in EventGraph.iter(EventGraph.filter(globals_.current_task_frame.get_current_history(), self._topic)):
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

    while not globals_.schedule.is_empty():
        resume_time = globals_.schedule.peek_next_time()
        globals_.elapsed_time = resume_time
        batch_event_graph = EventGraph.empty()
        for task in globals_.schedule.get_next_batch():
            task_status, event_graph = step(task)
            if type(task_status) == Delay:
                globals_.schedule.schedule(resume_time + task_status.duration, task)
            elif type(task_status) == AwaitCondition:
                globals_.awaiting_conditions.append((task_status.condition, task))
            elif type(task_status) == Completed:
                globals_.spans.append(
                    (
                        globals_.task_directives.get(
                            task, (globals_.task_inputs[task][0].__name__, globals_.task_inputs[task][1])
                        ),
                        globals_.task_start_times[task],
                        globals_.elapsed_time,
                    )
                )
            batch_event_graph = EventGraph.concurrently(batch_event_graph, event_graph)
        if type(batch_event_graph) != EventGraph.Empty:
            if globals_.events and globals_.events[-1][0] == globals_.elapsed_time:
                globals_.events[-1] = (globals_.elapsed_time, EventGraph.sequentially(globals_.events[-1][1], batch_event_graph))
            else:
                globals_.events.append((globals_.elapsed_time, batch_event_graph))
        old_awaiting_conditions = list(globals_.awaiting_conditions)
        globals_.awaiting_conditions.clear()
        while old_awaiting_conditions:
            condition, task = old_awaiting_conditions.pop()
            if condition():
                globals_.schedule.schedule(globals_.elapsed_time, task)
            else:
                globals_.awaiting_conditions.append((condition, task))
    return sorted(globals_.spans, key=lambda x: (x[1], x[2])), list(globals_.events)


def linearize(events, current_task_frame):
    for x in EventGraph.iter(current_task_frame.get_current_history()):
        yield x


def execute(commands):
    return Telemetry([], [])


def step(task):
    globals_.current_task_frame = TaskFrame(history=globals_.current_task_frame.get_current_history())
    try:
        task_status = next(task)
    except StopIteration:
        task_status = Completed()
    return task_status, globals_.current_task_frame.collect()


def make_generator(f, arguments):
    yield f(**arguments)
