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
Call = namedtuple("Call", "child_task args")
Spawn = namedtuple("Spawn", "child_task")
Completed = namedtuple("Completed", "")


class SimulationEngine:
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
        self.awaiting_tasks = {}  # map from blocking task to blocked task
        self.spans = []

    def register_model(self, cls):
        self.model = cls()
        self.activity_types_by_name = self.model.get_activity_types()
        return self.model

    def spawn(self, directive_type, arguments):
        task = make_task(self.model, directive_type, arguments)
        self.task_inputs[task] = (directive_type, arguments)
        self.spawn_task(task)

    def spawn_task(self, task):
        self.task_start_times[task] = self.elapsed_time
        parent_task_frame = self.current_task_frame
        task_status, events = self.step(task)
        parent_task_frame.spawn(events)
        self.current_task_frame = parent_task_frame

    def defer(self, directive_type, duration, arguments):
        task = make_task(self.model, directive_type, arguments)
        self.schedule.schedule(self.elapsed_time + duration, task)
        self.task_start_times[task] = self.elapsed_time + duration
        self.task_inputs[task] = (directive_type, arguments)
        return task

    def step(self, task):
        self.current_task_frame = TaskFrame(history=self.current_task_frame.get_current_history())
        try:
            task_status = next(task)
        except StopIteration:
            task_status = Completed()
        if type(task_status) == Delay:
            self.schedule.schedule(self.elapsed_time + task_status.duration, task)
        elif type(task_status) == AwaitCondition:
            self.awaiting_conditions.append((task_status.condition, task))
        elif type(task_status) == Completed:
            self.spans.append(
                (
                    self.task_directives.get(
                        task, (self.task_inputs[task][0].__name__, self.task_inputs[task][1])
                    ),
                    self.task_start_times[task],
                    self.elapsed_time,
                )
            )
            if task in self.awaiting_tasks:
                self.schedule.schedule(self.elapsed_time, self.awaiting_tasks[task])
                del self.awaiting_tasks[task]
        elif type(task_status) == Call:
            child_task = make_task(self.model, task_status.child_task, task_status.args)
            self.awaiting_tasks[child_task] = task
            self.task_inputs[child_task] = (task_status.child_task, task_status.args)
            self.spawn_task(child_task)
        else:
            raise ValueError("Unhandled task status: " + str(task_status))
        return task_status, self.current_task_frame.collect()


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


def make_task(model, directive_type, arguments):
    if inspect.isgeneratorfunction(directive_type):
        return directive_type.__call__(model, **arguments)
    else:
        return make_generator(directive_type, dict(**arguments, model=model))


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


class Plan:
    def __init__(self, directives):
        self.directives: List[Directive] = list(directives)


def simulate(register_engine, model_class, plan):
    engine = SimulationEngine()
    register_engine(engine)
    engine.register_model(model_class)
    for directive in plan.directives:
        directive_type = engine.activity_types_by_name[directive.type]
        task = engine.defer(directive_type, directive.start_time, directive.args)
        engine.task_directives[task] = directive

    while not engine.schedule.is_empty():
        resume_time = engine.schedule.peek_next_time()
        engine.elapsed_time = resume_time
        batch_event_graph = EventGraph.empty()
        for task in engine.schedule.get_next_batch():
            task_status, event_graph = engine.step(task)
            batch_event_graph = EventGraph.concurrently(batch_event_graph, event_graph)
        if type(batch_event_graph) != EventGraph.Empty:
            if engine.events and engine.events[-1][0] == engine.elapsed_time:
                engine.events[-1] = (engine.elapsed_time, EventGraph.sequentially(engine.events[-1][1], batch_event_graph))
            else:
                engine.events.append((engine.elapsed_time, batch_event_graph))
        old_awaiting_conditions = list(engine.awaiting_conditions)
        engine.awaiting_conditions.clear()
        while old_awaiting_conditions:
            condition, task = old_awaiting_conditions.pop()
            if condition():
                engine.schedule.schedule(engine.elapsed_time, task)
            else:
                engine.awaiting_conditions.append((condition, task))
    return sorted(engine.spans, key=lambda x: (x[1], x[2])), list(engine.events)


def linearize(events, current_task_frame):
    for x in EventGraph.iter(current_task_frame.get_current_history()):
        yield x


def execute(commands):
    return Telemetry([], [])


def make_generator(f, arguments):
    if False:
        yield Completed()
    f(**arguments)
