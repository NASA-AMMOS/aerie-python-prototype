# This is a simplified Aerie for prototyping purposes
from collections import namedtuple
import inspect
from typing import List, Tuple

from protocol import (
    Completed,
    Delay,
    AwaitCondition,
    Directive,
    Call,
    Plan,
    hashable_directive,
)
from event_graph import EventGraph

Event = namedtuple("Event", "topic value progeny")

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
        self.awaiting_conditions = []  # tuple (condition, task)
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
        task_frame = TaskFrame(self.elapsed_time, parent_task_frame._get_visible_history(), task=task)
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
            task_frame.emit(make_finish_topic(task), "FINISHED")
            if task in self.awaiting_tasks:
                self.schedule.schedule(self.elapsed_time, self.awaiting_tasks[task])
                resuming_caller_task = self.awaiting_tasks[task]
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

    def __init__(self, elapsed_time, history, task=None):
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

    def unschedule(self, task, allow_event_graph=False):
        for _, batch in self._schedule.items():
            if task in batch and (allow_event_graph or not EventGraph.is_event_graph(task)):
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
    old_task_parent_called=None
):
    """
    Initialization of mutable keyword arguments, because python is... python
    """
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

    all_events_from_previous_sim = old_events  # this variable name is to differentiate from future_events_from_previous_sim, introduced further down

    """
    Prepare for simulation
    """
    old_task_parent = dict_union(old_task_parent_spawned, old_task_parent_called)  # useful when you don't care if it was spawned or called
    engine = SimulationEngine()  # keeps track of what's next to do
    engine.register_model(model_class)  # creates a new instance of the model class
    register_engine(engine)  # this is a hook for the called to be able to hold a reference to the engine. Called here to give a chance to override activities for testing.
    for directive in plan.directives:  # Add all plan directives to the schedule
        engine.defer(directive.type, directive.start_time, directive.args)
    for start_offset, event_graph in all_events_from_previous_sim:  # Add READ events from the previous sim to the schedule, so we remember to check whether they're stale
        if engine.task_start_times and start_offset <= min(engine.task_start_times.values()):
            continue
        reads = EventGraph.filter(event_graph, [SPECIAL_READ_TOPIC])
        if not EventGraph.is_empty(reads):
            engine.schedule.schedule(start_offset, reads)
    restarted_tasks = {}  # Every restarted task has an old task (key) and a corresponding new task (value)
    stale_topics = {}  # map of topic to time at which it became stale
    restarted_tasks_not_yet_grafted = set()  # to-do list of tasks that have been restarted, but not stepped yet. If they are newly spawned, they will need to be grafted
    new_task_to_events = {}  # tracks events emitted by restarted tasks.
    future_events_from_previous_sim = list(old_events)  # tracks events from the previous simulation that have not yet been added to engine.events
    while not engine.schedule.is_empty():
        resume_time = engine.schedule.peek_next_time()
        if stop_time is not None and resume_time >= stop_time:
            break
        engine.elapsed_time = resume_time
        while future_events_from_previous_sim and future_events_from_previous_sim[0][0] < resume_time:
            next_commit = future_events_from_previous_sim.pop(0)
            engine.events.append((next_commit[0], EventGraph.filter_p(next_commit[1], lambda evt: evt.progeny not in set(restarted_tasks).union(deleted_tasks))))
        batch = engine.schedule.get_next_batch()
        batch_reads = [x for x in batch if EventGraph.is_event_graph(x)]
        batch_tasks = [x for x in batch if not EventGraph.is_event_graph(x)]

        # Iterate to a fixed point, marking topics stale as more tasks are added to tasks_to_restart
        prev_tasks_to_restart = None
        tasks_to_restart = set()
        while tasks_to_restart != prev_tasks_to_restart:
            prev_tasks_to_restart = set(tasks_to_restart)
            affected_topics = get_topics_affected_by_tasks(all_events_from_previous_sim, set(restarted_tasks).union(deleted_tasks).union(tasks_to_restart))
            stale_topics = dict_union(stale_topics, affected_topics, lambda a, b: min(a, b))
            for read_graph in batch_reads:
                for read in EventGraph.to_set(read_graph):
                    if read.progeny in restarted_tasks or read.progeny in deleted_tasks:
                        continue
                    if set(read.value).intersection(set(topic for topic, start_offset in stale_topics.items() if start_offset <= engine.elapsed_time)):
                        tasks_to_restart.add(read.progeny)
        """
        Remove child tasks from tasks_to_restart when their parent is also in tasks_to_restart.
        Add those child tasks to deleted_tasks
        """
        tasks_to_restart = remove_children_whose_parents_are_present(tasks_to_restart, old_task_parent, deleted_tasks)
        if tasks_to_restart:
            """
            Restart the tasks. At the end, all tasks have been stepped up to a time prior to engine.elapsed time,
            and scheduled to resume at engine.elapsed time.
            """
            newly_restarted_tasks = restart_stale_tasks(register_engine, model_class, engine, tasks_to_restart, old_task_directives,
                                                        old_task_parent_called, old_task_parent_spawned, list(engine.events),
                                                        all_events_from_previous_sim)
            # At this point, the restarted tasks have been scheduled to execute in the next time step
            restarted_tasks_not_yet_grafted.update(newly_restarted_tasks)
            restarted_tasks.update(newly_restarted_tasks)

            for task in batch_tasks:
                engine.schedule.schedule(engine.elapsed_time, task)
        else:  # If there are no tasks to restart
            batch_event_graph = EventGraph.empty()
            for task in batch_tasks:
                if task in [restarted_tasks[x] for x in restarted_tasks_not_yet_grafted]:
                    old_task = [x for x in restarted_tasks_not_yet_grafted if restarted_tasks[x] == task][0]
                    if old_task in old_task_parent and engine.task_start_times[task] == engine.elapsed_time:
                        history = engine.events + [find_spawn_event(all_events_from_previous_sim, old_task)[-1]]
                    else:
                        history = engine.events
                else:
                    history = engine.events
                task_status, event_graph = engine.step(
                    task, TaskFrame(engine.elapsed_time, history, task=task)
                )
                if type(task_status) == Completed:
                    """
                    Move future reads of the ("FINISH", restarted_task) topic to now.
                    """
                    for t, batch in list(engine.schedule._schedule.items()):
                        for eg in batch:
                            if EventGraph.is_event_graph(eg):
                                caller = EventGraph.to_set(
                                    EventGraph.filter_p(
                                        eg,
                                        lambda evt: (evt.topic == "READ" and
                                                     type(evt.value[0]) == tuple and
                                                     len(evt.value[0]) == 2 and
                                                     evt.value[0][0] == "FINISH" and
                                                     evt.value[0][1] in restarted_tasks and
                                                     restarted_tasks[evt.value[0][1]] == task)),
                                    lambda evt: evt.progeny)
                                if caller:
                                    caller, = caller  # unpack a single-item set. The comma is crucial.
                                    engine.schedule.unschedule(EventGraph.atom(Event("READ", (make_finish_topic(task),), caller)))
                                    engine.schedule.schedule(engine.elapsed_time, EventGraph.atom(Event("READ", (make_finish_topic(task),), caller)))
                if task in [restarted_tasks[x] for x in restarted_tasks_not_yet_grafted] and engine.task_start_times[task] == engine.elapsed_time:
                    new_task_to_events[task] = event_graph
                else:
                    batch_event_graph = EventGraph.concurrently(batch_event_graph, event_graph)
            newly_invalidated_topics = EventGraph.to_set(batch_event_graph, lambda evt: evt.topic)
            for new_events in new_task_to_events.values():
                newly_invalidated_topics.update(EventGraph.to_set(new_events, lambda evt: evt.topic))
            for topic in newly_invalidated_topics:
                if topic in stale_topics:
                    stale_topics[topic] = min(engine.elapsed_time, stale_topics[topic])
                else:
                    stale_topics[topic] = engine.elapsed_time

            """
            Include old events in the batch_event graph
            """
            previous_eg = EventGraph.empty()
            while future_events_from_previous_sim and future_events_from_previous_sim[0][0] == resume_time:
                previous_eg = EventGraph.sequentially(previous_eg, EventGraph.filter_p(future_events_from_previous_sim.pop(0)[1], lambda evt: evt.progeny not in set(restarted_tasks).union(deleted_tasks)))
            batch_event_graph = EventGraph.concurrently(batch_event_graph, previous_eg)
            if future_events_from_previous_sim and future_events_from_previous_sim[0][0] == resume_time:
                raise ValueError("Duplicate resume time in old_events:", resume_time)

            extend_history(engine.events, engine.elapsed_time, batch_event_graph)

            """
            Graft events from new_task_to_events into engine.events
            """
            for old_task, new_task in [(x, restarted_tasks[x]) for x in restarted_tasks_not_yet_grafted]:
                if engine.task_start_times[new_task] == engine.elapsed_time:
                    engine.events, success = graft(engine.events, new_task_to_events[new_task], old_task, new_task)
                    if engine.events and engine.events[-1][0] < engine.elapsed_time:
                        engine.events.append((engine.elapsed_time, new_task_to_events[new_task]))
                    if success:
                        restarted_tasks_not_yet_grafted.remove(old_task)
                if engine.task_start_times[new_task] < engine.elapsed_time:
                    restarted_tasks_not_yet_grafted.remove(old_task)

            """
            Identify future stale reads, and remove all events from stale readers from the future, since we expect them to be restarted.
            
            TODO: Does this even make sense?
            """
            newly_stale_readers = set()
            for start_offset, event_graph in future_events_from_previous_sim:
                filtered = EventGraph.filter_p(
                    event_graph,
                    lambda evt: evt.topic == SPECIAL_READ_TOPIC
                    and evt.progeny not in deleted_tasks
                    and set(evt.value).intersection(newly_invalidated_topics),
                )
                newly_stale_readers.update(EventGraph.to_set(filtered, lambda evt: evt.progeny))
            if newly_stale_readers:
                # Filter out all events from these tasks in the future
                for i in range(len(future_events_from_previous_sim)):
                    start_offset, event_graph = future_events_from_previous_sim[i]
                    future_events_from_previous_sim[i] = (
                        start_offset,
                        EventGraph.filter_p(event_graph, lambda evt: evt.progeny not in newly_stale_readers),
                    )
                future_events_from_previous_sim = [x for x in future_events_from_previous_sim if not EventGraph.is_empty(x[1])]
                # TODO What about events emitted by children?

            """
            Check conditions
            """
            old_awaiting_conditions = list(engine.awaiting_conditions)
            engine.awaiting_conditions.clear()
            condition_reads = EventGraph.empty()
            while old_awaiting_conditions:
                condition, task = old_awaiting_conditions.pop()
                engine.current_task_frame = TaskFrame(engine.elapsed_time, engine.events, task=task)
                if condition():
                    engine.schedule.schedule(engine.elapsed_time, task)
                else:
                    engine.awaiting_conditions.append((condition, task))
                condition_reads = EventGraph.concurrently(condition_reads, engine.current_task_frame.collect())
            extend_history(engine.events, engine.elapsed_time, condition_reads)
    """
    Close out the simulation
    """
    engine.events.extend(future_events_from_previous_sim)
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
        "deleted_tasks": deleted_tasks.union(restarted_tasks),
    }
    filtered_spans = remove_task_from_spans(spans)
    return filtered_spans, collapse_simultaneous(without_special_events(engine.events), EventGraph.sequentially), payload


def remove_children_whose_parents_are_present(tasks_to_restart, task_parent, deleted_tasks):
    tasks_to_restart_bk = tasks_to_restart
    worklist = list(tasks_to_restart)
    tasks_to_restart = set()
    while worklist:
        task = worklist.pop()
        parent_task = task_parent.get(task, None)
        if parent_task not in tasks_to_restart_bk:
            tasks_to_restart.add(task)
        else:
            deleted_tasks.add(task)
    return tasks_to_restart


def extend_history(history, elapsed_time, events):
    if history and history[-1][0] > elapsed_time:
        raise ValueError("Time in history must be monotonically increasing: " + str(history) + ", " + str(elapsed_time) + ", " + str(events))
    if EventGraph.is_empty(events):
        return
    # if history and history[-1][0] == elapsed_time:
    #     history[-1] = (
    #         elapsed_time,
    #         EventGraph.sequentially(history[-1][1], events),
    #     )
    # else:
    history.append((elapsed_time, events))


def raise_error(message):
    def f(a, b):
        raise ValueError(message)
    return f


def dict_union(a, b, on_conflict=raise_error("No conflict resolution method was defined")):
    """
    Returns the union of dictionaries a and b.

    Where both contain the same key, the value will be the result of on_conflict called on the two values.
    """
    res = {}
    for k, v in a.items():
        if k in b:
            res[k] = on_conflict(v, b[k])
        else:
            res[k] = v
    for k, v in b.items():
        if k not in res:
            res[k] = v
    return res

def get_topics_affected_by_tasks(history, tasks):
    """
    Finds all topics to which the given tasks have ever emitted events.

    :param history: An event history to search
    :param tasks: The tasks whose events to search for
    :return: A dictionary from topic to time at which it was affected
    """
    res = {}
    for start_offset, event_graph in history:
        filtered = EventGraph.filter_p(
            event_graph,
            lambda evt: evt.progeny in tasks
        )
        for topic in EventGraph.to_set(filtered, lambda evt: evt.topic):
            res[topic] = start_offset
    return res


def filter_history(history, deleted_tasks):
    res = []
    for x, eg in history:
        filtered = EventGraph.filter_p(eg, lambda evt: evt.progeny not in deleted_tasks)
        if not EventGraph.is_empty(filtered):
            res.append((x, filtered))
    return res


def restart_stale_tasks(
    register_engine, model_class, engine, tasks_to_restart, old_task_directives, old_task_parent_called, old_task_parent_spawned, current_events, all_old_events
):
    old_task_to_new_task = {}

    for reader_task in tasks_to_restart:
        engine.schedule.unschedule(reader_task)
        if reader_task in old_task_parent_spawned or reader_task in old_task_parent_called:
            spawn_event_prefix = find_spawn_event(all_old_events, reader_task)[-1]
        else:
            spawn_event_prefix = (old_task_directives[reader_task].start_time, EventGraph.empty())
        task, status = step_up_single_task(register_engine, model_class, old_task_directives[reader_task].type,
                            old_task_directives[reader_task].start_time, old_task_directives[reader_task].args,
                            engine.elapsed_time, all_old_events, spawn_event_prefix)
        register_engine(engine)
        engine.task_start_times[task] = old_task_directives[reader_task].start_time
        engine.task_directives[task] = old_task_directives[reader_task]
        engine.task_inputs[task] = (old_task_directives[reader_task].type, old_task_directives[reader_task].args)
        if type(status) == AwaitCondition:
            engine.awaiting_conditions.append((status.condition, task))
        if type(status) == Delay:
            engine.schedule.schedule(engine.elapsed_time, task)
        if type(status) == Call:
            # Assume that the reason we're restarting this task is because the child has just finished
            engine.schedule.schedule(engine.elapsed_time, task)
        old_task_to_new_task[reader_task] = task

    assert set(old_task_to_new_task) == tasks_to_restart, f"{set(old_task_to_new_task)} != {tasks_to_restart}"
    return old_task_to_new_task


def step_up_single_task(register_engine, model_class, directive_type, start_offset, arguments, stop_time, history, spawn_prefix):
    """
    Steps up the given task using old events, ignoring spawns and calls, up to the given time
    """
    engine = SimulationEngine()  # keeps track of what's next to do
    engine.register_model(model_class)  # creates a new instance of the model class
    register_engine(engine)  # this is a hook for the called to be able to hold a reference to the engine. Called here to give a chance to override activities for testing.
    task = engine.defer(directive_type, start_offset, arguments)
    elapsed_time = start_offset
    future_history = list(history)
    past_history = list()
    status = Delay(0)
    first_step = True
    while elapsed_time < stop_time:
        while future_history and future_history[0][0] < elapsed_time:
            past_history.append(future_history.pop(0))
        if first_step:
            task_frame = TaskFrame(elapsed_time, past_history + [spawn_prefix], task=task)
            first_step = False
        else:
            task_frame = TaskFrame(engine.elapsed_time, past_history, task=task)
        engine.current_task_frame = task_frame
        try:
            status = next(task)
        except StopIteration:
            status = Completed()
        if type(status) == Delay:
            elapsed_time += status.duration
        elif type(status) == AwaitCondition:
            task_frame = TaskFrame(engine.elapsed_time, past_history, task=task)
            engine.current_task_frame = task_frame
            condition_satisfied = False
            if status.condition():
                condition_satisfied = True
            while future_history and not condition_satisfied:
                past_history.append(future_history.pop(0))
                elapsed_time = past_history[-1][0]
                if elapsed_time >= stop_time:
                    return task, status
                task_frame = TaskFrame(engine.elapsed_time, past_history, task=task)
                engine.current_task_frame = task_frame
                if status.condition():
                    condition_satisfied = True
                    break
            if condition_satisfied:
                continue
            else:
                break
        elif type(status) == Completed:
            raise ValueError("Restarted task finished before reaching stop time")
        elif type(status) == Call:
            # TODO: What if the task calls multiple children?
            break
        else:
            raise ValueError("Unhandled task status: " + str(status))
    return task, status

def graft(history, new_events, old_task, new_task):
    res = []
    any_success = False
    for x, eg in history:
        dominated, not_dominated, _ = find_all_dominated_by(eg, old_task)
        new_eg, success = graft_helper(not_dominated, EventGraph.concurrently(dominated, new_events), old_task, new_task)
        if success:
            res.append((x, new_eg))
            any_success = True
        else:
            res.append((x, eg))
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
    """
    x is dominated if all paths from origin to x lead through spawn(old_task)
    not dominated is the original graph without the dominated nodes
    """
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
            return EventGraph.sequentially(prefix_dominated, event_graph.suffix), prefix_not_dominated, True
        if suffix_found:
            return suffix_dominated, EventGraph.sequentially(event_graph.prefix, suffix_not_dominated), True
        return EventGraph.empty(), event_graph, False
    if type(event_graph) == EventGraph.Concurrently:
        left_dominated, left_not_dominated, left_found = find_all_dominated_by(event_graph.left, old_task)
        right_dominated, right_not_dominated, right_found = find_all_dominated_by(event_graph.right, old_task)
        if left_found:
            return left_dominated, EventGraph.concurrently(left_not_dominated, event_graph.right), True  # TODO: Untested
        if right_found:
            return right_dominated, EventGraph.concurrently(event_graph.left, right_not_dominated), True  # TODO: Untested
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
    if type(event_graph) == EventGraph.Concurrently:  # TODO: Untested
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
            # TODO: Untested
            deleted_tasks.extend(payload["task_children_spawned"][task])
            worklist.extend(payload["task_children_spawned"][task])
        if task in payload["task_children_called"]:
            deleted_tasks.extend(payload["task_children_called"][task])
            worklist.extend(payload["task_children_called"][task])

    new_spans, new_events, new_payload = simulate(
        register_engine,
        model_class,
        Plan(added_directives),
        old_events=list(payload["events"]),
        deleted_tasks=set(deleted_tasks),
        old_task_directives=payload["task_directives"],
        old_task_parent_called=payload["task_parent_called"],
        old_task_parent_spawned=payload["task_parent_spawned"]
    )

    old_spans = list(payload["spans"])

    deleted_tasks.extend(new_payload["deleted_tasks"])
    for task in deleted_tasks:
        if task in payload["task_directives"]:
            deleted_directives.append(payload["task_directives"][task])

    for deleted_directive in deleted_directives:
        old_spans = [x for x in old_spans if x[0] != deleted_directive]
    old_spans = [x for x in old_spans if x[0][2] not in deleted_tasks]
    return (
        sorted(remove_task_from_spans(old_spans) + new_spans, key=lambda x: (x[1], x[2])),
        without_special_events(collapse_simultaneous(new_events, EventGraph.sequentially)),
        payload,
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
    return OneShotTask(f, arguments)


class OneShotTask:
    """
    This class exists just so that __repr__ returns a useful string, and not just `make_generator`.
    """
    def __init__(self, f, arguments):
        self.f = f
        self.arguments = arguments
        self.stepped = False

    def __iter__(self):
        return self

    def __next__(self):
        if self.stepped:
            raise StopIteration()
        self.stepped = True
        self.f(**self.arguments)
        return Completed()

    def __repr__(self):
        return repr(self.f)
