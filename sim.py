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
Event = namedtuple("Event", "topic start_time value")

Delay = namedtuple("Delay", "duration")
AwaitCondition = namedtuple("AwaitCondition", "condition")
Call = namedtuple("Call", "child_task")
Completed = namedtuple("Completed", "")


class Globals:
    def __init__(self):
        self.elapsed_time = 0
        self.events = []
        self.schedule = []
        self.model = None  # Filled in by register_model
        self.activity_types_by_name = None  # Filled in by register_model
        self.task_start_times = {}
        self.task_directives = {}


def initialize_mutable_globals():
    global globals_
    globals_ = Globals()


def emit(event):
    globals_.events.append(event)


def spawn(directive_type, arguments):
    defer(directive_type, 0, arguments)


def defer(directive_type, duration, arguments):
    if inspect.isgeneratorfunction(directive_type):
        task = directive_type.__call__(globals_.model, **arguments)
        globals_.schedule.append(
            (globals_.elapsed_time + duration, task)
        )
    else:
        task = make_generator(directive_type, [globals_.model] + arguments)
        globals_.schedule.append(
            (globals_.elapsed_time + duration, task)
        )
    # NOTE: this is the only place that task start time is defined - assumption is that all tasks are started via defer
    globals_.task_start_times[task] = globals_.elapsed_time + duration
    return task


class RegisterCell:
    def __init__(self, topic, initial_value):
        self._topic = topic
        self._initial_value = initial_value

    def get(self):
        for event in reversed(globals_.events):
            if event.topic == self._topic:
                return event.value
        return self._initial_value

    def set(self, new_value):
        emit(Event(self._topic, globals_.elapsed_time, new_value))

    def __add__(self, other):
        return self.get() + other

    def __sub__(self, other):
        return self.get() - other

    def __gt__(self, other):
        def condition():
            return self.get() > other

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
        for event in globals_.events:
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


def register_model(cls):
    global globals_
    globals_.model = cls()
    globals_.activity_types_by_name = globals_.model.get_activity_types()
    return globals_.model


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
        globals_.elapsed_time = resume_time

        task_status = step(task)
        if type(task_status) == Delay:
            globals_.schedule.append((resume_time + task_status.duration, task))
        elif type(task_status) == AwaitCondition:
            awaiting_conditions.append((task_status.condition, task))
        elif type(task_status) == Completed:
            spans.append((globals_.task_directives.get(task, None), globals_.task_start_times[task], globals_.elapsed_time))

        old_awaiting_conditions = list(awaiting_conditions)
        awaiting_conditions.clear()
        while old_awaiting_conditions:
            condition, task = old_awaiting_conditions.pop()
            if condition():
                globals_.schedule.insert(0, (globals_.elapsed_time, task))
            else:
                awaiting_conditions.append((condition, task))
    return sorted(spans), list(globals_.events)


def execute(commands):
    return Telemetry([], [])


def step(task):
    try:
        return next(task)
    except StopIteration:
        return Completed()


def make_generator(f, arguments):
    yield f(**arguments)
