# This is a simplified Aerie for prototyping purposes
from collections import namedtuple
import inspect
from typing import List, Tuple

from protocol import Completed, Delay, AwaitCondition, Directive, Call, Plan, tuple_args, hashable_directive, \
    restore_directive
from event_graph import EventGraph

Event = namedtuple("Event", "topic value progeny")

# Event.__repr__ = lambda evt: f"{evt.topic}:{evt.value}{{{evt.progeny.__name__}}}"

EventHistory = List[Tuple[int, EventGraph]]

SPECIAL_READ_TOPIC = "READ"


class SimulationEngine:
    def __init__(self):
        self.task_children = {}
        self.elapsed_time = 0
        self.events: EventHistory = []  # list of tuples (start_offset, event_graph)
        self.current_task_frame = None  # created by step
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
        task_status, events = self.step(task, TaskFrame(self.elapsed_time, task=task, history=self.events))
        if parent_task_frame.task is not None:
            if parent_task_frame.task not in self.task_children:
                self.task_children[parent_task_frame.task] = []
            self.task_children[parent_task_frame.task].append(task)
        parent_task_frame.spawn(events)
        self.current_task_frame = parent_task_frame

    def defer(self, directive_type, duration, arguments):
        task = make_task(self.model, directive_type, arguments)
        self.schedule.schedule(self.elapsed_time + duration, task)
        self.task_start_times[task] = self.elapsed_time + duration
        self.task_inputs[task] = (directive_type, arguments)
        return task

    def step(self, task, task_frame):
        restore = self.current_task_frame
        self.current_task_frame = task_frame
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
                        task, (self.task_inputs[task][0].__name__, self.task_inputs[task][1], task)
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
        self.current_task_frame = restore
        return task_status, task_frame.collect()


class TaskFrame:
    Branch = namedtuple("Branch", "base event_graph")

    def __init__(self, elapsed_time, history=None, task=None):
        if history is None:
            history = []
        self.elapsed_time = elapsed_time
        self.task = task
        self.tip = EventGraph.empty()
        self.history = history
        self.branches = []

    def emit(self, topic, value):
        if self.task is None:
            raise ValueError("Cannot emit events when task is None")
        self.tip = EventGraph.sequentially(self.tip, EventGraph.Atom(Event(topic, value, self.task)))

    def read(self, topic_or_topics):
        """
        Returns the visible event history, filtered to the given topic
        """
        topics = [topic_or_topics] if type(topic_or_topics) != list else topic_or_topics
        # Track reads as Events
        self.tip = EventGraph.sequentially(self.tip, EventGraph.Atom(Event(SPECIAL_READ_TOPIC, topics, self.task)))
        res = []
        for start_offset, x in self._get_visible_history():
            filtered = EventGraph.filter(x, topics)
            if type(filtered) != EventGraph.Empty:
                res.append((start_offset, filtered))
        return res

    def spawn(self, event_graph):
        self.branches.append((self.tip, event_graph))
        self.tip = EventGraph.empty()

    def _get_visible_history(self):
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


def simulate(register_engine, model_class, plan, old_events=None, deleted_tasks=None, old_task_directives=None):
    if old_events is None:
        old_events = []
    if deleted_tasks is None:
        deleted_tasks = set()
    if old_task_directives is None:
        old_task_directives = {}
    engine = SimulationEngine()
    engine.register_model(model_class)
    register_engine(engine)
    for directive in plan.directives:
        directive_type = engine.activity_types_by_name[directive.type]
        task = engine.defer(directive_type, directive.start_time, directive.args)
        engine.task_directives[task] = directive

    while not engine.schedule.is_empty():
        resume_time = engine.schedule.peek_next_time()
        engine.elapsed_time = resume_time

        while old_events and old_events[0][0] < resume_time:
            engine.events.append(old_events.pop(0))

        batch_event_graph = EventGraph.empty()

        for task in engine.schedule.get_next_batch():
            task_status, event_graph = engine.step(
                task, TaskFrame(engine.elapsed_time, task=task, history=engine.events)
            )
            batch_event_graph = EventGraph.concurrently(batch_event_graph, event_graph)

        newly_invalidated_topics = EventGraph.to_set(batch_event_graph, lambda evt: evt.topic)

        if old_events and old_events[0][0] == resume_time:
            batch_event_graph = EventGraph.concurrently(batch_event_graph, old_events.pop(0)[1])

        if old_events and old_events[0][0] == resume_time:
            raise ValueError("Duplicate resume time in old_events:", resume_time)

        if type(batch_event_graph) != EventGraph.Empty:
            if engine.events and engine.events[-1][0] == engine.elapsed_time:
                engine.events[-1] = (
                    engine.elapsed_time,
                    EventGraph.sequentially(engine.events[-1][1], batch_event_graph),
                )
            else:
                engine.events.append((engine.elapsed_time, batch_event_graph))

        # TODO re-simulate stale reads
        newly_stale_readers = set()
        for start_offset, event_graph in old_events:
            filtered = EventGraph.filter_p(event_graph, lambda evt: evt.topic == SPECIAL_READ_TOPIC and evt.progeny not in deleted_tasks and set(evt.value).intersection(newly_invalidated_topics))
            newly_stale_readers.update(EventGraph.to_set(filtered, lambda evt: evt.progeny))

        if newly_stale_readers:
            deleted_tasks.update(newly_stale_readers)
            # Filter out all events from this task in the future
            for i in range(len(old_events)):
                start_offset, event_graph = old_events[i]
                old_events[i] = (start_offset, EventGraph.filter_p(event_graph, lambda evt: evt.progeny not in newly_stale_readers))
            # Rewind time
            old_events = engine.events + old_events
            engine.events = []
            engine.elapsed_time = 0

            # Re-schedule the tasks
            for reader_task in newly_stale_readers:
                directive = old_task_directives[reader_task]
                directive_type = engine.activity_types_by_name[directive.type]
                task = engine.defer(directive_type, directive.start_time, directive.args)
                engine.task_directives[task] = directive

        # stale_tasks = {x.progeny for x in stale_reads if x.progeny not in deleted_tasks}
        # task_to_directive = {task: directive for directive, task in payload["plan_directive_to_task"].items()}
        # stale_directives = [restore_directive(task_to_directive[task]) for task in stale_tasks]

        old_awaiting_conditions = list(engine.awaiting_conditions)
        engine.awaiting_conditions.clear()
        condition_reads = EventGraph.empty()
        while old_awaiting_conditions:
            condition, task = old_awaiting_conditions.pop()
            engine.current_task_frame = TaskFrame(engine.elapsed_time, history=engine.events, task=task)
            if condition():
                engine.schedule.schedule(engine.elapsed_time, task)
            else:
                engine.awaiting_conditions.append((condition, task))
            condition_reads = EventGraph.concurrently(condition_reads, engine.current_task_frame.collect())
        if type(condition_reads) != EventGraph.Empty:
            if engine.events and engine.events[-1][0] == engine.elapsed_time:
                engine.events[-1] = (
                    engine.elapsed_time,
                    EventGraph.sequentially(engine.events[-1][1], condition_reads),
                )
            else:
                engine.events.append((engine.elapsed_time, condition_reads))

    engine.events.extend(old_events)

    deleted_directives = [old_task_directives[task] for task in deleted_tasks if task in old_task_directives]

    spans = sorted(filter(lambda span: span[0] not in deleted_directives, engine.spans), key=lambda x: (x[1], x[2]))
    payload = {
        "events": list(engine.events),
        "spans": spans,
        "plan_directive_to_task": {hashable_directive(y): x for x, y in engine.task_directives.items()},
        "task_directives": engine.task_directives,
        "task_children": engine.task_children,
    }
    filtered_spans = remove_task_from_spans(spans)
    return filtered_spans, without_read_events(engine.events), payload


def remove_task_from_spans(spans):
    filtered_spans = []
    for span in spans:
        if type(span[0]) == Directive:
            filtered_spans.append(span)
        else:
            filtered_spans.append(((span[0][0], span[0][1]), span[1], span[2]))
    return filtered_spans


def without_read_events(events):
    non_read_events = []
    for x, y in events:
        filtered = EventGraph.filter_p(y, lambda evt: evt.topic != SPECIAL_READ_TOPIC)
        if type(filtered) != EventGraph.Empty:
            non_read_events.append((x, filtered))
    return non_read_events


def simulate_incremental(register_engine, model_class, new_plan, old_plan, payload):
    unchanged_directives, deleted_directives, added_directives = diff(old_plan.directives, new_plan.directives)
    deleted_tasks = [payload["plan_directive_to_task"][hashable_directive(x)] for x in deleted_directives]

    worklist = list(deleted_tasks)
    while worklist:
        task = worklist.pop()
        if task in payload["task_children"]:
            deleted_tasks.extend(payload["task_children"][task])
            worklist.extend(payload["task_children"][task])

    deleted_events = []
    for start_offset, event_graph in payload["events"]:
        deleted = EventGraph.filter_p(event_graph, lambda evt: evt.progeny in deleted_tasks)
        if type(deleted) != EventGraph.Empty:
            deleted_events.append((start_offset, deleted))

    # A read is stale if it contains a deleted event or a new event to one of its topics in its history

    # TODO re-simulate stale reads
    reads_and_deleted_events = EventGraph.empty()
    for start_offset, event_graph in payload["events"]:
        filtered = EventGraph.filter_p(event_graph, lambda evt: evt.topic == SPECIAL_READ_TOPIC or evt.progeny in deleted_tasks)
        reads_and_deleted_events = EventGraph.sequentially(reads_and_deleted_events, filtered)

    stale_reads = get_stale_reads(reads_and_deleted_events)
    stale_tasks = {x.progeny for x in stale_reads if x.progeny not in deleted_tasks}
    task_to_directive = {task: directive for directive, task in payload["plan_directive_to_task"].items()}
    stale_directives = [restore_directive(task_to_directive[task]) for task in stale_tasks]

    directives_to_simulate = added_directives + stale_directives

    old_events_without_deleted_tasks = []
    for start_offset, event_graph in payload["events"]:
        filtered = EventGraph.filter_p(event_graph, lambda evt: evt.progeny not in deleted_tasks and evt.progeny not in stale_tasks)
        if type(filtered) != EventGraph.Empty:
            old_events_without_deleted_tasks.append((start_offset, filtered))

    new_spans, new_events, _ = simulate(
        register_engine,
        model_class,
        Plan(directives_to_simulate), old_events=old_events_without_deleted_tasks, deleted_tasks=set(deleted_tasks), old_task_directives=payload["task_directives"])

    old_spans = list(payload["spans"])

    for deleted_directive in deleted_directives:
        old_spans = [x for x in old_spans if x[0] != deleted_directive]
    old_spans = [x for x in old_spans if x[0][2] not in deleted_tasks]
    old_spans = [x for x in old_spans if x[0] not in stale_directives]
    return (
        sorted(remove_task_from_spans(old_spans) + new_spans, key=lambda x: (x[1], x[2])),
        without_read_events(collapse_simultaneous(new_events, EventGraph.sequentially)),
        None,
    )

def get_stale_reads(event_graph):
    stale_reads, stale_topics = get_stale_reads_helper(event_graph, set())
    return stale_reads

def get_stale_reads_helper(event_graph, stale_topics):
    if type(event_graph) == EventGraph.Empty:
        return [], stale_topics
    if type(event_graph) == EventGraph.Atom:
        if event_graph.value.topic == SPECIAL_READ_TOPIC and set(event_graph.value.value).intersection(stale_topics):
            return [event_graph.value], stale_topics
        elif event_graph.value.topic != SPECIAL_READ_TOPIC:
            return [], stale_topics.union({event_graph.value.topic})
        else:
            return [], stale_topics
    if type(event_graph) == EventGraph.Sequentially:
        prefix_stale_reads, prefix_stale_topics = get_stale_reads_helper(event_graph.prefix, stale_topics)
        suffix_stale_reads, suffix_stale_topics = get_stale_reads_helper(event_graph.suffix, stale_topics.union(prefix_stale_topics))
        return prefix_stale_reads + suffix_stale_reads, prefix_stale_topics.union(suffix_stale_topics)
    if type(event_graph) == EventGraph.Concurrently:
        left_stale_reads, left_stale_topics = get_stale_reads_helper(event_graph.left, stale_topics)
        right_stale_reads, right_stale_topics = get_stale_reads_helper(event_graph.right, stale_topics)
        return left_stale_reads + right_stale_reads, left_stale_topics.union(right_stale_topics)
    raise ValueError("Not an event_graph: " + str(event_graph))


def collapse_simultaneous(history, combiner):
    sorted_history = sorted(history, key=lambda x: x[0])
    res = []
    for start_offset, event_graph in sorted_history:
        if not res:
            res.append([start_offset, event_graph])
        else:
            if start_offset == res[-1][0]:
                res[-1][1] = combiner(res[-1][1], event_graph)
            else:
                res.append([start_offset, event_graph])
    return [tuple(x) for x in res]


def diff(old_directives, new_directives):
    old_directives = list(old_directives)
    new_directives = list(new_directives)
    unchanged_directives = []

    any_matched = True
    while any_matched:
        # TODO use a more efficient diff algorithm
        any_matched = False
        for old in old_directives:
            for new in new_directives:
                if old == new:
                    unchanged_directives.append(old)
                    new_directives.remove(new)
                    old_directives.remove(old)
                    any_matched = True
    deleted_directives = old_directives
    added_directives = new_directives
    return unchanged_directives, deleted_directives, added_directives


def make_generator(f, arguments):
    if False:
        yield
    f(**arguments)