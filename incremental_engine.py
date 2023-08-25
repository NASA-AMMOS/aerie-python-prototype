# This is a simplified Aerie for prototyping purposes
from collections import namedtuple
import inspect
from typing import List, Tuple, Optional

from protocol import (
    Completed,
    Delay,
    AwaitCondition,
    Directive,
    Call,
    Plan,
    tuple_args,
    hashable_directive,
    restore_directive,
)
from event_graph import EventGraph

Event = namedtuple("Event", "topic value progeny")

# Event.__repr__ = lambda evt: f"{evt.topic}:{evt.value}{{{evt.progeny.__name__}}}"

EventHistory = List[Tuple[int, EventGraph]]

SPECIAL_READ_TOPIC = "READ"
SPECIAL_SPAWN_TOPIC = "SPAWN"
make_finish_topic = lambda x: ("FINISH", x)


class SimulationEngine:
    def __init__(self):
        self.task_children_spawned = {}
        self.task_children_called = {}
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
        self.task_directives[task] = Directive(directive_type, self.elapsed_time, arguments)
        self.spawn_task(task)

    def spawn_task(self, task, is_call=False):
        self.current_task_frame.emit(SPECIAL_SPAWN_TOPIC, task)
        self.task_start_times[task] = self.elapsed_time
        parent_task_frame = self.current_task_frame
        task_frame = TaskFrame(self.elapsed_time, task=task, history=parent_task_frame._get_visible_history())
        task_status, events = self.step(task, task_frame)
        if parent_task_frame.task is not None:
            if is_call:
                if parent_task_frame.task not in self.task_children_called:
                    self.task_children_called[parent_task_frame.task] = []
                self.task_children_called[parent_task_frame.task].append(task)
            else:
                if parent_task_frame.task not in self.task_children_spawned:
                    self.task_children_spawned[parent_task_frame.task] = []
                self.task_children_spawned[parent_task_frame.task].append(task)
        parent_task_frame.spawn(events)
        self.current_task_frame = parent_task_frame

    def defer(self, directive_type, duration, arguments):
        task = make_task(self.model, directive_type, arguments)
        self.schedule.schedule(self.elapsed_time + duration, task)
        self.task_start_times[task] = self.elapsed_time + duration
        self.task_inputs[task] = (directive_type, arguments)
        self.task_directives[task] = Directive(directive_type, self.elapsed_time + duration, arguments)
        return task

    def step(self, task, task_frame):
        restore = self.current_task_frame
        self.current_task_frame = task_frame
        resuming_caller_task = None
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
                    self.task_directives.get(task, (self.task_inputs[task][0], self.task_inputs[task][1], task)),
                    self.task_start_times[task],
                    self.elapsed_time,
                )
            )
            if task in self.awaiting_tasks:
                self.schedule.schedule(self.elapsed_time, self.awaiting_tasks[task])
                resuming_caller_task = self.awaiting_tasks[task]
                task_frame.emit(make_finish_topic(task), "FINISHED")
                del self.awaiting_tasks[task]
        elif type(task_status) == Call:
            child_task = make_task(self.model, task_status.child_task, task_status.args)
            self.awaiting_tasks[child_task] = task
            self.task_inputs[child_task] = (task_status.child_task, task_status.args)
            self.task_directives[child_task] = Directive(task_status.child_task, self.elapsed_time, task_status.args)
            self.spawn_task(child_task, is_call=True)
        else:
            raise ValueError("Unhandled task status: " + str(task_status))
        self.current_task_frame = restore
        if resuming_caller_task is None:
            return task_status, task_frame.collect()
        else:
            return task_status, EventGraph.sequentially(
                task_frame.collect(),
                EventGraph.Atom(Event(SPECIAL_READ_TOPIC, (make_finish_topic(task),), resuming_caller_task)),
            )


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
        topics = (topic_or_topics,) if type(topic_or_topics) != list else tuple(topic_or_topics)
        # Track reads as Events
        self.tip = EventGraph.sequentially(self.tip, EventGraph.Atom(Event(SPECIAL_READ_TOPIC, topics, self.task)))
        res = []
        for start_offset, x in self._get_visible_history():
            filtered = EventGraph.filter(x, topics)
            if not EventGraph.is_empty(filtered):
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
    func = model.get_activity_types()[directive_type]
    if inspect.isgeneratorfunction(func):
        return func.__call__(model, **arguments)
    else:
        return make_generator(func, dict(**arguments, model=model))


class JobSchedule:
    def __init__(self):
        self._schedule = {}

    def schedule(self, start_offset, task):
        if not start_offset in self._schedule:
            self._schedule[start_offset] = []
        for _, batch in self._schedule.items():
            if task in batch and not EventGraph.is_event_graph(task):
                raise Exception("Double scheduling task: " + str(task))
        self._schedule[start_offset].append(task)

    def unschedule(self, task):
        for _, batch in self._schedule.items():
            if task in batch and not EventGraph.is_event_graph(task):
                batch.remove(task)

    def peek_next_time(self):
        return min(self._schedule)

    def get_next_batch(self):
        next_time = self.peek_next_time()
        res = self._schedule[next_time]
        del self._schedule[next_time]
        return res

    def is_empty(self):
        return len(self._schedule) == 0


def simulate(
    register_engine,
    model_class,
    plan,
    stop_time=None,
    old_events=None,
    deleted_tasks=None,
    old_task_directives=None,
    old_task_parent_spawned=None,
    old_task_parent_called=None,
    old_task_children_spawned=None,
    old_task_children_called=None,
    tasks_to_restart=None
):
    if old_events is None:
        old_events = []
    if deleted_tasks is None:
        deleted_tasks = set()
    if old_task_directives is None:
        old_task_directives = {}
    if old_task_parent_spawned is None:
        old_task_parent_spawned = {}
    if old_task_parent_called is None:
        old_task_parent_called = {}
    if old_task_children_spawned is None:
        old_task_children_spawned = {}
    if old_task_children_called is None:
        old_task_children_called = {}
    if tasks_to_restart is None:
        tasks_to_restart = set()
    engine = SimulationEngine()
    engine.register_model(model_class)
    register_engine(engine)
    for directive in plan.directives:
        engine.defer(directive.type, directive.start_time, directive.args)

    for task in tasks_to_restart:
        directive = old_task_directives[task]
        deleted_tasks.add(task)

        # TODO splice the task into its spawn point
        if task in old_task_parent_spawned:
            pass
        if task in old_task_parent_called:
            pass

        engine.defer(directive.type, directive.start_time, directive.args)

    for start_offset, event_graph in old_events:
        reads = EventGraph.filter(event_graph, [SPECIAL_READ_TOPIC])
        if not EventGraph.is_empty(reads):
            engine.schedule.schedule(start_offset, reads)

    # TODO: rather than start these off as empty, seed them from the caller... somehow?
    # The hope being to avoid the convoluted logic in simulated_incremental
    # And set up the possibility for marking reads as "not stale" if their computed values are the same
    restarted_tasks = set()
    stale_topics = set()

    old_task_to_new_task = {}
    new_task_to_events = {}

    while not engine.schedule.is_empty():
        resume_time = engine.schedule.peek_next_time()
        if stop_time is not None and resume_time >= stop_time:
            break
        engine.elapsed_time = resume_time

        while old_events and old_events[0][0] < resume_time:
            engine.events.append(old_events.pop(0))

        batch = engine.schedule.get_next_batch()
        batch_reads = [x for x in batch if EventGraph.is_event_graph(x)]
        batch_tasks = [x for x in batch if not EventGraph.is_event_graph(x)]

        # TODO do something with batch_reads
        tasks_to_restart = set()
        for read_graph in batch_reads:
            for read in EventGraph.to_set(read_graph):
                if read.progeny in restarted_tasks:
                    continue
                # TODO check if it's stale
                if set(read.value).intersection(stale_topics):
                    tasks_to_restart.add(read.progeny)

        if tasks_to_restart:
            old_task_to_new_task.update(restart_stale_tasks(
                register_engine, model_class, engine, tasks_to_restart, old_task_directives, old_task_parent_called, old_task_parent_spawned, list(engine.events), list(old_events)
            ))

            restarted_tasks.update(tasks_to_restart)

            for i in range(len(old_events)):
                start_offset, event_graph = old_events[i]
                old_events[i] = (
                    start_offset,
                    EventGraph.filter_p(event_graph, lambda evt: evt.progeny not in restarted_tasks),
                )
            old_events = [x for x in old_events if not EventGraph.is_empty(x[1])]
            for task in batch_tasks:
                engine.schedule.schedule(engine.elapsed_time, task)
            continue  # In order to process all tasks at this time in parallel, we re-schedule the batch tasks and start the loop over

        batch_event_graph = EventGraph.empty()

        for task in batch_tasks:
            if task in old_task_to_new_task.values():
                old_task = [x for x, y in old_task_to_new_task.items() if y == task][0]
                if old_task in old_task_parent_spawned and engine.task_start_times[task] == engine.elapsed_time:
                    history = find_spawn_event(engine.events + old_events, old_task)
                else:
                    history = engine.events
            else:
                history = engine.events
            task_status, event_graph = engine.step(
                task, TaskFrame(engine.elapsed_time, task=task, history=history)
            )
            # if task in old_task_to_new_task.values():
            #     new_task_to_events[task] = event_graph
            # else:
            batch_event_graph = EventGraph.concurrently(batch_event_graph, event_graph)

        newly_invalidated_topics = EventGraph.to_set(batch_event_graph, lambda evt: evt.topic)
        stale_topics.update(newly_invalidated_topics)

        if old_events and old_events[0][0] == resume_time:
            batch_event_graph = EventGraph.concurrently(batch_event_graph, old_events.pop(0)[1])

        if old_events and old_events[0][0] == resume_time:
            raise ValueError("Duplicate resume time in old_events:", resume_time)

        if not EventGraph.is_empty(batch_event_graph):
            if engine.events and engine.events[-1][0] == engine.elapsed_time:
                engine.events[-1] = (
                    engine.elapsed_time,
                    EventGraph.sequentially(engine.events[-1][1], batch_event_graph),
                )
            else:
                engine.events.append((engine.elapsed_time, batch_event_graph))

        # for old_task, new_task in list(old_task_to_new_task.items()):
        #     engine.events, success = graft(engine.events, new_task_to_events[new_task], old_task, new_task)
        #     if success:
        #         del old_task_to_new_task[old_task]
        #         del new_task_to_events[new_task]

        newly_stale_readers = set()
        for start_offset, event_graph in old_events:
            filtered = EventGraph.filter_p(
                event_graph,
                lambda evt: evt.topic == SPECIAL_READ_TOPIC
                and evt.progeny not in deleted_tasks
                and set(evt.value).intersection(newly_invalidated_topics),
            )
            newly_stale_readers.update(EventGraph.to_set(filtered, lambda evt: evt.progeny))

        if newly_stale_readers:
            # while newly_stale_readers:
            worklist = list(newly_stale_readers)
            while worklist:
                reader = worklist.pop(0)
                if reader in old_task_parent_called:
                    # If the stale read is in a child activity, restart the parent???
                    # Can we do better??? Can we use the spans?
                    newly_stale_readers.add(old_task_parent_called[reader])
                    worklist.append(old_task_parent_called[reader])
            deleted_tasks.update(newly_stale_readers)
            # Filter out all events from these tasks in the future
            for i in range(len(old_events)):
                start_offset, event_graph = old_events[i]
                old_events[i] = (
                    start_offset,
                    EventGraph.filter_p(event_graph, lambda evt: evt.progeny not in newly_stale_readers),
                )
            old_events = [x for x in old_events if not EventGraph.is_empty(x[1])]

            # TODO What about events emitted by children?

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
        if not EventGraph.is_empty(condition_reads):
            if engine.events and engine.events[-1][0] == engine.elapsed_time:
                engine.events[-1] = (
                    engine.elapsed_time,
                    EventGraph.sequentially(engine.events[-1][1], condition_reads),
                )
            else:
                engine.events.append((engine.elapsed_time, condition_reads))

    engine.events.extend(old_events)

    spans = sorted(engine.spans, key=lambda x: (x[1], x[2]))

    payload = {
        "events": list(engine.events),
        "spans": spans,
        "plan_directive_to_task": {hashable_directive(y): x for x, y in engine.task_directives.items()},
        "task_directives": engine.task_directives,
        "task_children_called": engine.task_children_called,
        "task_children_spawned": engine.task_children_spawned,
        "task_parent_called": {
            child: parent for parent, children in engine.task_children_called.items() for child in children
        },
        "task_parent_spawned": {
            child: parent for parent, children in engine.task_children_spawned.items() for child in children
        },
        "deleted_tasks": deleted_tasks,
    }
    filtered_spans = remove_task_from_spans(spans)
    return filtered_spans, without_special_events(engine.events), payload


def restart_stale_tasks(
    register_engine, model_class, engine, tasks_to_restart, old_task_directives, old_task_parent_called, old_task_parent_spawned, current_events, old_events
):
    temp_engine: Optional[SimulationEngine] = None

    def local_register_engine(engine):
        nonlocal temp_engine
        temp_engine = engine
        register_engine(engine)

    worklist = list(tasks_to_restart)
    tasks_to_restart = set()
    while worklist:
        task = worklist.pop()
        if task in old_task_parent_called:
            worklist.append(old_task_parent_called[task])
        else:
            tasks_to_restart.add(task)

    old_task_to_new_task = {}

    for reader_task in [x for x in tasks_to_restart if x not in old_task_parent_called]:
        engine.schedule.unschedule(reader_task)
        # TODO: Figure out what should be in the past of this task
        if reader_task in old_task_parent_spawned:
            spawn_event_prefix = find_spawn_event(current_events + old_events, reader_task)
        else:
            spawn_event_prefix = list(current_events)
        # Maybe slap it in old_events?
        _, _, payload = simulate(local_register_engine, model_class, Plan([old_task_directives[reader_task]]), stop_time=engine.elapsed_time, old_events=spawn_event_prefix)
        for task in temp_engine.task_start_times:
            if task not in payload["task_parent_called"] and task not in payload["task_parent_spawned"]:
                old_task_to_new_task[reader_task] = task
        register_engine(engine)  # restore the main engine
        engine.task_children_called.update(temp_engine.task_children_called)  # = {}
        engine.task_children_spawned.update(temp_engine.task_children_spawned)  # = {}
        while not temp_engine.schedule.is_empty():
            start_offset = temp_engine.schedule.peek_next_time()
            for task in temp_engine.schedule.get_next_batch():
                engine.schedule.schedule(start_offset, task)
        engine.task_start_times.update(temp_engine.task_start_times)  # = {}
        engine.task_directives.update(temp_engine.task_directives)  # = {}
        engine.task_inputs.update(temp_engine.task_inputs)  # = {}
        engine.awaiting_conditions.extend(temp_engine.awaiting_conditions)  # = []
        engine.awaiting_tasks.update(temp_engine.awaiting_tasks)  # = {}  # map from blocking task to blocked task
        engine.spans.extend(temp_engine.spans)  # = []
    return old_task_to_new_task


def graft(history, new_events, old_task, new_task):
    res = []
    any_success = False
    for x, eg in history:
        dominated, not_dominated, _ = find_all_dominated_by(eg, old_task)
        eg, success = graft_helper(not_dominated, EventGraph.concurrently(dominated, new_events), old_task, new_task)
        res.append((x, eg))
        if success:
            any_success = True
    return res, any_success


def graft_helper(event_graph, new_events, old_task, new_task, suffix=EventGraph.empty()):
    if type(event_graph) == EventGraph.Empty:
        return EventGraph.empty(), False
    if type(event_graph) == EventGraph.Atom:
        evt = event_graph.value
        if evt.topic == SPECIAL_SPAWN_TOPIC and evt.value == old_task:
            return EventGraph.sequentially(EventGraph.atom(Event(SPECIAL_SPAWN_TOPIC, new_task, event_graph.value.progeny)), EventGraph.concurrently(new_events, suffix)), True
        else:
            return event_graph, False
    if type(event_graph) == EventGraph.Sequentially:
        prefix, prefix_found = graft_helper(event_graph.prefix, new_events, old_task, new_task)
        suffix, suffix_found = graft_helper(event_graph.suffix, new_events, old_task, new_task)
        return EventGraph.sequentially(prefix, suffix), prefix_found or suffix_found
    if type(event_graph) == EventGraph.Concurrently:
        left, left_found = graft_helper(event_graph.left, new_events, old_task, new_task)
        right, right_found = graft_helper(event_graph.right, new_events, old_task, new_task)
        return EventGraph.concurrently(left, right), left_found or right_found
    raise ValueError("Not an event_graph: " + str(event_graph))


def find_all_dominated_by(event_graph, old_task):
    if type(event_graph) == EventGraph.Empty:
        return EventGraph.empty(), EventGraph.empty(), False
    if type(event_graph) == EventGraph.Atom:
        evt = event_graph.value
        if evt.topic == SPECIAL_SPAWN_TOPIC and evt.value == old_task:
            return EventGraph.empty(), event_graph, True
        else:
            return event_graph, EventGraph.empty(), False
    if type(event_graph) == EventGraph.Sequentially:
        prefix_dominated, prefix_not_dominated, prefix_found = find_all_dominated_by(event_graph.prefix, old_task)
        suffix_dominated, suffix_not_dominated, suffix_found = find_all_dominated_by(event_graph.suffix, old_task)
        if prefix_found:
            return EventGraph.sequentially(prefix_dominated, suffix_dominated), EventGraph.sequentially(prefix_not_dominated, suffix_not_dominated), True
        if suffix_found:
            return suffix_dominated, EventGraph.sequentially(event_graph.prefix, suffix_not_dominated), True
        return EventGraph.empty(), event_graph, False
    if type(event_graph) == EventGraph.Concurrently:
        left_dominated, left_not_dominated, left_found = find_all_dominated_by(event_graph.left, old_task)
        right_dominated, right_not_dominated, right_found = find_all_dominated_by(event_graph.right, old_task)
        if left_found:
            return left_dominated, EventGraph.concurrently(left_not_dominated, event_graph.right), True
        if right_found:
            return right_dominated, EventGraph.concurrently(event_graph.left, right_not_dominated), True
        return EventGraph.empty(), event_graph, False
    raise ValueError("Not an event_graph: " + str(event_graph))


def find_spawn_event(history, task):
    res = []
    for x, eg in history:
        prefix = get_prefix(eg, lambda evt: evt.topic == SPECIAL_SPAWN_TOPIC and evt.value == task)
        if prefix is None:
            res.append((x, eg))
        else:
            res.append((x, prefix))
            return res
    raise ValueError("Could not find " + repr(task) + " in " + repr(history))


def get_prefix(event_graph, f):
    """
    Returns the prefix up to and not including f, if f is found
    Otherwise, returns None.
    """
    if type(event_graph) == EventGraph.Empty:
        return None
    if type(event_graph) == EventGraph.Atom:
        if f(event_graph.value):
            return EventGraph.empty()
        else:
            return None
    if type(event_graph) == EventGraph.Sequentially:
        res_p = get_prefix(event_graph.prefix, f)
        res_s = get_prefix(event_graph.suffix, f)
        if res_p is None and res_s is None:
            return None
        if not res_p is None:
            return res_p
        return EventGraph.sequentially(event_graph.prefix, res_s)
    if type(event_graph) == EventGraph.Concurrently:
        res_l = get_prefix(event_graph.left, f)
        res_r = get_prefix(event_graph.right, f)
        if res_l is not None:
            return res_l
        elif res_r is not None:
            return res_r
        else:
            return None

    raise ValueError("Not an event_graph: " + str(event_graph))



def remove_task_from_spans(spans):
    filtered_spans = []
    for span in spans:
        if type(span[0]) == Directive:
            filtered_spans.append(span)
        else:
            filtered_spans.append(((span[0][0], span[0][1]), span[1], span[2]))
    return filtered_spans


def without_special_events(events):
    non_read_events = []
    for x, y in events:
        filtered = EventGraph.filter_p(
            y,
            lambda evt: evt.topic not in (SPECIAL_READ_TOPIC, SPECIAL_SPAWN_TOPIC)
            and not (type(evt.topic) == tuple and evt.topic[0] == "FINISH"),
        )
        if not EventGraph.is_empty(filtered):
            non_read_events.append((x, filtered))
    return non_read_events


def simulate_incremental(register_engine, model_class, new_plan, old_plan, payload):
    unchanged_directives, deleted_directives, added_directives = diff(old_plan.directives, new_plan.directives)
    deleted_tasks = [payload["plan_directive_to_task"][hashable_directive(x)] for x in deleted_directives]

    worklist = list(deleted_tasks)
    while worklist:
        task = worklist.pop()
        if task in payload["task_children_spawned"]:
            deleted_tasks.extend(payload["task_children_spawned"][task])
            worklist.extend(payload["task_children_spawned"][task])
        if task in payload["task_children_called"]:
            deleted_tasks.extend(payload["task_children_called"][task])
            worklist.extend(payload["task_children_called"][task])

    first = True

    stale_tasks = set()

    new_stale_tasks = set()

    deleted_events = []
    while first or new_stale_tasks:
        first = False

        for start_offset, event_graph in payload["events"]:
            deleted = EventGraph.filter_p(
                event_graph, lambda evt: evt.progeny in deleted_tasks or evt.progeny in stale_tasks
            )
            if not EventGraph.is_empty(deleted):
                deleted_events.append((start_offset, deleted))

        # A read is stale if it contains a deleted event or a new event to one of its topics in its history

        # TODO re-simulate stale reads
        reads_and_deleted_events = EventGraph.empty()
        for start_offset, event_graph in payload["events"]:
            filtered = EventGraph.filter_p(
                event_graph,
                lambda evt: evt.topic == SPECIAL_READ_TOPIC
                or evt.progeny in deleted_tasks
                or evt.progeny in stale_tasks,
            )
            reads_and_deleted_events = EventGraph.sequentially(reads_and_deleted_events, filtered)

        stale_reads = get_stale_reads(reads_and_deleted_events)
        new_stale_tasks = {
            x.progeny for x in stale_reads if x.progeny not in deleted_tasks and x.progeny not in stale_tasks
        }

        worklist = list(new_stale_tasks)
        while worklist:
            task = worklist.pop()
            if task in payload["task_children_spawned"]:
                new_stale_tasks.update(payload["task_children_spawned"][task])
                worklist.extend(payload["task_children_spawned"][task])
            if task in payload["task_children_called"]:
                new_stale_tasks.update(payload["task_children_called"][task])
                worklist.extend(payload["task_children_called"][task])
        stale_tasks.update(new_stale_tasks)

    task_to_directive = {task: directive for directive, task in payload["plan_directive_to_task"].items()}

    stale_tasks_bk = set(stale_tasks)
    worklist = list(stale_tasks)
    stale_tasks = set()
    while worklist:
        task = worklist.pop()
        parent_task = payload["task_parent_spawned"].get(task, payload["task_parent_called"].get(task, None))
        if parent_task is None or parent_task not in stale_tasks_bk:
            stale_tasks.add(task)
        else:
            deleted_tasks.append(task)

    # stale_directives = [restore_directive(task_to_directive[task]) for task in stale_tasks]

    directives_to_simulate = added_directives  # + stale_directives

    old_events_without_deleted_tasks = []
    for start_offset, event_graph in payload["events"]:
        filtered = EventGraph.filter_p(
            event_graph, lambda evt: evt.progeny not in deleted_tasks and evt.progeny not in stale_tasks
        )
        if not EventGraph.is_empty(filtered):
            old_events_without_deleted_tasks.append((start_offset, filtered))

    new_spans, new_events, new_payload = simulate(
        register_engine,
        model_class,
        Plan(directives_to_simulate),
        old_events=old_events_without_deleted_tasks,
        deleted_tasks=set(deleted_tasks),
        old_task_directives=payload["task_directives"],
        old_task_parent_called=payload["task_parent_called"],
        old_task_parent_spawned=payload["task_parent_spawned"],
        old_task_children_called=payload["task_children_called"],
        old_task_children_spawned=payload["task_children_spawned"],
        tasks_to_restart=stale_tasks
    )

    old_spans = list(payload["spans"])

    deleted_tasks.extend(new_payload["deleted_tasks"])
    for task in deleted_tasks:
        if task in payload["task_directives"]:
            deleted_directives.append(payload["task_directives"][task])

    for deleted_directive in deleted_directives:
        old_spans = [x for x in old_spans if x[0] != deleted_directive]
    old_spans = [x for x in old_spans if x[0][2] not in deleted_tasks]
    # old_spans = [x for x in old_spans if x[0] not in stale_directives]
    return (
        sorted(remove_task_from_spans(old_spans) + new_spans, key=lambda x: (x[1], x[2])),
        without_special_events(collapse_simultaneous(new_events, EventGraph.sequentially)),
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
        suffix_stale_reads, suffix_stale_topics = get_stale_reads_helper(
            event_graph.suffix, stale_topics.union(prefix_stale_topics)
        )
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
