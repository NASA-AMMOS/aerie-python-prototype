# This is a simplified Aerie for prototyping purposes
from collections import namedtuple
import inspect

from protocol import Completed, Delay, AwaitCondition, Call, Directive, make_generator
from event_graph import EventGraph

Event = namedtuple("Event", "topic value")

class SimulationEngine:
    def __init__(self):
        self.elapsed_time = 0
        self.events = []  # list of tuples (start_offset, event_graph)
        self.current_task_frame = TaskFrame(self.elapsed_time)
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
        self.task_directives[task] = Directive(directive_type, self.elapsed_time, arguments)
        self.spawn_task(task)

    def spawn_anonymous(self, task_factory):
        task = task_factory()
        self.task_inputs[task] = ("unknown", {})
        self.task_directives[task] = Directive("unknown", self.elapsed_time, {})
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
        self.task_directives[task] = Directive(directive_type, self.elapsed_time + duration, arguments)
        return task

    def step(self, task):
        self.current_task_frame = TaskFrame(self.elapsed_time, history=self.current_task_frame.get_visible_history())
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
                        task, (self.task_inputs[task][0], self.task_inputs[task][1])
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
            self.task_directives[child_task] = Directive(task_status.child_task, self.elapsed_time, task_status.args)
            self.spawn_task(child_task)
        else:
            raise ValueError("Unhandled task status: " + str(task_status))
        return task_status, self.current_task_frame.collect()


class TaskFrame:
    Branch = namedtuple("Branch", "base event_graph")

    def __init__(self, elapsed_time, history=None):
        if history is None:
            history = []
        self.tip = EventGraph.empty()
        self.history = history
        self.branches = []
        self.elapsed_time = elapsed_time

    def emit(self, topic, value):
        self.tip = EventGraph.sequentially(self.tip, EventGraph.Atom(Event(topic, value)))

    def read(self, topic_or_topics, function):
        topics = [topic_or_topics] if type(topic_or_topics) != list else topic_or_topics
        res = []
        for start_offset, x in self.get_visible_history():
            filtered = EventGraph.filter(x, topics)
            if type(filtered) != EventGraph.Empty:
                res.append((start_offset, filtered))
        return function(res)

    def spawn(self, event_graph):
        self.branches.append((self.tip, event_graph))
        self.tip = EventGraph.empty()

    def get_visible_history(self):
        res = EventGraph.empty()
        for base, _ in self.branches:
            res = EventGraph.sequentially(res, base)
        res = EventGraph.sequentially(res, self.tip)
        return self.history + [(self.elapsed_time, res)]

    def collect(self):
        res = self.tip
        for base, event_graph in reversed(self.branches):
            res = EventGraph.sequentially(base, EventGraph.concurrently(event_graph, res))
        return res


def make_task(model, directive_type, arguments):
    try:
        func = model.get_activity_types()[directive_type]
    except:
        print()
    if inspect.isgeneratorfunction(func):
        return func.__call__(model, **arguments)
    else:
        return make_generator(func, dict(**arguments, model=model))


class JobSchedule:
    def __init__(self):
        self._schedule = {}

    def schedule(self, start_offset, task):
        if type(start_offset) is not int:
            raise ValueError("start_offset must be an int. Received: " + start_offset)
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


def simulate(register_engine, model_class, plan):
    engine = SimulationEngine()
    engine.register_model(model_class)
    register_engine(engine)

    for directive in plan.directives:
        engine.defer(directive.type, directive.start_time, directive.args)

    while not engine.schedule.is_empty():
        resume_time = engine.schedule.peek_next_time()
        engine.elapsed_time = resume_time
        batch_event_graph = EventGraph.empty()
        saved_task_frame = engine.current_task_frame
        for task in engine.schedule.get_next_batch():
            engine.current_task_frame = saved_task_frame
            task_status, event_graph = engine.step(task)
            batch_event_graph = EventGraph.concurrently(batch_event_graph, event_graph)
        engine.current_task_frame = TaskFrame(engine.elapsed_time, history=engine.events)
        if type(batch_event_graph) != EventGraph.Empty:
            if engine.events and engine.events[-1][0] == engine.elapsed_time:
                engine.events[-1] = (engine.elapsed_time, EventGraph.sequentially(engine.events[-1][1], batch_event_graph))
            else:
                engine.events.append((engine.elapsed_time, batch_event_graph))
        old_awaiting_conditions = list(engine.awaiting_conditions)
        engine.awaiting_conditions.clear()
        while old_awaiting_conditions:
            condition, task = old_awaiting_conditions.pop()
            engine.current_task_frame = TaskFrame(engine.elapsed_time, history=engine.events)
            time_to_wake_task = condition(True, 0, 9999)
            if time_to_wake_task is not None:
                engine.schedule.schedule(engine.elapsed_time + time_to_wake_task, task)  # All our times are integers here
            else:
                engine.awaiting_conditions.append((condition, task))
    return sorted(engine.spans, key=lambda x: (x[1], x[2])), list(engine.events), None  # Third item is "payload", unused.

