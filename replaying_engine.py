"""
This approach records the actions taken by each task, and replays them as appropriate.

TO-DO:
- daemon tasks
- anonymous tasks
- sim config
- try not to re-run called tasks
- be able to find un-stale reads (i.e. take into account the cell function, not just the history)
- perhaps avoid re-running tasks within the same simulation (e.g. if same directive repeated)
"""
from collections import namedtuple
import inspect

from plan_diff import diff
from protocol import Completed, Delay, AwaitCondition, Call, Directive, hashable_directive_without_time, tuple_args
from event_graph import EventGraph

Event = namedtuple("Event", "topic value")
TaskId = namedtuple("TaskId", "id label")

__task_id_counter = 0
def fresh_task_id(label=""):
    global __task_id_counter
    __task_id_counter += 1
    return TaskId(__task_id_counter, label)

class SimulationEngine:
    def __init__(self, register_engine, rbt=None):
        self.action_log = []
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
        self.tasks = {}

        if rbt is None:
            rbt = {}
        self.rbt = rbt
        self.register_engine = register_engine

    def register_model(self, cls):
        self.model = cls()
        self.activity_types_by_name = self.model.get_activity_types()
        return self.model

    def spawn(self, directive_type, arguments):
        task_id = fresh_task_id(str(directive_type) + " " + str(arguments))
        self.make_task(task_id, directive_type, arguments)
        task = make_task(self.model, directive_type, arguments)
        self.current_task_frame.action_log.append((self.current_task_frame.task_id, "spawn", directive_type, arguments))
        self.tasks[task_id] = task
        self.task_inputs[task_id] = (directive_type, arguments)
        self.task_directives[task_id] = Directive(directive_type, self.elapsed_time, arguments)
        self.spawn_task(task_id, task)
        return task_id

    def spawn_task(self, task_id, task):
        self.task_start_times[task_id] = self.elapsed_time
        parent_task_frame = self.current_task_frame
        task_status, events = self.step(task_id, task)
        parent_task_frame.spawn(events)
        self.current_task_frame = parent_task_frame

    def defer(self, directive_type, duration, arguments):
        task = make_task(self.model, directive_type, arguments)
        task_id = fresh_task_id(repr(task))
        self.schedule.schedule(self.elapsed_time + duration, task_id)
        self.task_start_times[task_id] = self.elapsed_time + duration
        self.task_inputs[task_id] = (directive_type, arguments)
        self.task_directives[task_id] = Directive(directive_type, self.elapsed_time + duration, arguments)
        self.tasks[task_id] = task
        return task_id, task

    def step(self, task_id, task):
        self.current_task_frame = TaskFrame(self.elapsed_time, task_id=task_id, history=self.current_task_frame.get_visible_history())
        try:
            task_status = next(task)
        except StopIteration:
            task_status = Completed()
        if type(task_status) == Delay:
            self.schedule.schedule(self.elapsed_time + task_status.duration, task_id)
        elif type(task_status) == AwaitCondition:
            self.awaiting_conditions.append((task_status.condition, task_id))
        elif type(task_status) == Completed:
            self.spans.append(
                (
                    self.task_directives.get(
                        task_id, (self.task_inputs[task_id][0], self.task_inputs[task_id][1])
                    ),
                    self.task_start_times[task_id],
                    self.elapsed_time,
                )
            )
            if task_id in self.awaiting_tasks:
                self.schedule.schedule(self.elapsed_time, self.awaiting_tasks[task_id])
                del self.awaiting_tasks[task_id]
        elif type(task_status) == Call:
            child_task_id = fresh_task_id(repr((task_status.child_task, task_status.args)))
            child_task = self.make_task(child_task_id, task_status.child_task, task_status.args)
            self.tasks[child_task_id] = child_task
            self.awaiting_tasks[child_task_id] = task_id
            self.task_inputs[child_task_id] = (task_status.child_task, task_status.args)
            self.task_directives[child_task_id] = Directive(task_status.child_task, self.elapsed_time, task_status.args)
            self.spawn_task(child_task_id, child_task)
        else:
            raise ValueError("Unhandled task status: " + str(task_status))
        self.action_log.extend(self.current_task_frame.action_log)
        self.action_log.append((task_id, "yield", task_status))
        return task_status, self.current_task_frame.collect()

    def make_task(self, task_id, directive_type, args):
        key = (directive_type, tuple_args(args))
        if key in self.rbt:
            return task_replayer(self, task_id, Directive(directive_type, 0, args), self.rbt[key], Imitator(self.rbt, self.register_engine), self.register_engine)
        else:
            return make_task(self.model, directive_type, args)


class TaskFrame:
    Branch = namedtuple("Branch", "base event_graph")

    def __init__(self, elapsed_time, task_id=None, history=None):
        if history is None:
            history = []
        self.task_id = task_id
        self.tip = EventGraph.empty()
        self.history = history
        self.branches = []
        self.elapsed_time = elapsed_time
        self.action_log = []

    def emit(self, topic, value):
        self.action_log.append((self.task_id, "emit", topic, value))
        self.tip = EventGraph.sequentially(self.tip, EventGraph.Atom(Event(topic, value)))

    def read(self, topic_or_topics):
        topics = [topic_or_topics] if type(topic_or_topics) != list else topic_or_topics
        res = []
        for start_offset, x in self.get_visible_history():
            filtered = EventGraph.filter(x, topics)
            if type(filtered) != EventGraph.Empty:
                res.append((start_offset, filtered))
        self.action_log.append((self.task_id, "read", topics, res))
        return res

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
    func = model.get_activity_types()[directive_type]
    if inspect.isgeneratorfunction(func):
        return func.__call__(model, **arguments)
    else:
        return make_generator(func, dict(**arguments, model=model))


class JobSchedule:
    def __init__(self):
        self._schedule = {}

    def schedule(self, start_offset, task_id):
        if start_offset not in self._schedule:
            self._schedule[start_offset] = []
        for _, batch in self._schedule.items():
            if task_id in batch:
                raise Exception("Double scheduling task: " + str(task_id))
        self._schedule[start_offset].append(task_id)

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
    engine = SimulationEngine(register_engine)
    engine.register_model(model_class)
    register_engine(engine)

    directive_to_task_id = {}

    for directive in plan.directives:
        task_id, task = engine.defer(directive.type, directive.start_time, directive.args)
        directive_to_task_id[hashable_directive_without_time(directive)] = task_id

    while not engine.schedule.is_empty():
        resume_time = engine.schedule.peek_next_time()
        engine.elapsed_time = resume_time
        batch_event_graph = EventGraph.empty()
        for task_id in engine.schedule.get_next_batch():
            task_status, event_graph = engine.step(task_id, engine.tasks[task_id])
            batch_event_graph = EventGraph.concurrently(batch_event_graph, event_graph)
        if type(batch_event_graph) != EventGraph.Empty:
            if engine.events and engine.events[-1][0] == engine.elapsed_time:
                engine.events[-1] = (engine.elapsed_time, EventGraph.sequentially(engine.events[-1][1], batch_event_graph))
            else:
                engine.events.append((engine.elapsed_time, batch_event_graph))
        old_awaiting_conditions = list(engine.awaiting_conditions)
        engine.awaiting_conditions.clear()
        engine.current_task_frame = TaskFrame(engine.elapsed_time, history=engine.events)
        while old_awaiting_conditions:
            condition, task_id = old_awaiting_conditions.pop()
            if condition():
                engine.schedule.schedule(engine.elapsed_time, task_id)
            else:
                engine.awaiting_conditions.append((condition, task_id))

    action_log_by_id = {}
    for x in engine.action_log:
        if x[0] not in action_log_by_id:
            action_log_by_id[x[0]] = []
        action_log_by_id[x[0]].append(x[1:])

    # action log structure:
    # key: hashable(directive name, args)
    # value: a read-branching tree. It is one of:
    # - NonRead(payload, next)
    # - Read(topic, Map[Value, read-branching tree])

    action_log = {}
    for task_id, actions in action_log_by_id.items():
        directive = engine.task_directives[task_id]
        key = (directive.type, tuple_args(directive.args))
        if key in action_log:
            action_log[key] = extend(action_log[key], actions)
        else:
            action_log[key] = make_rbt(actions)

    return sorted(engine.spans, key=lambda x: (x[1], x[2])), list(engine.events), {
        "action_log": action_log
    }


RBT_Empty = namedtuple("RBT_Empty", "")
RBT_NonRead = namedtuple("RBT_NonRead", "payload rest")
RBT_Read = namedtuple("RBT_Read", "topics branches")


def make_rbt(actions):
    if not actions:
        return RBT_Empty()
    first, rest = actions[0], actions[1:]
    action, action_args = first[0], first[1:]
    if action == "read":
        topics, res = action_args
        return RBT_Read(topics, ((res, make_rbt(rest)),))
    else:
        return RBT_NonRead(first, make_rbt(rest))


def extend(rbt, actions):
    if not actions:
        return rbt
    first, rest = actions[0], actions[1:]
    action, action_args = first[0], first[1:]
    if action == "read":
        assert type(rbt) == RBT_Read
        topics, res = action_args
        res = tuple(res)
        assert rbt.topics == topics
        new_branches = list()
        found_match = False
        for old_res, old_rbt in rbt.branches:
            old_res = tuple(old_res)
            if old_res == res:
                found_match = True
                new_branches.append((old_res, extend(old_rbt, rest)))
            else:
                new_branches.append((old_res, old_rbt))
        if not found_match:
            new_branches.append((res, make_rbt(rest)))
        return RBT_Read(topics, tuple(new_branches))
    else:
        assert type(rbt) == RBT_NonRead
        return RBT_NonRead(first, extend(rbt.rest, rest))


def simulate_incremental(register_engine, model_class, new_plan, old_plan, payload):
    engine = SimulationEngine(register_engine, rbt=payload["action_log"])
    engine.register_model(model_class)
    register_engine(engine)
    unchanged_directives, deleted_directives, added_directives = diff(old_plan.directives, new_plan.directives)
    action_log = payload["action_log"]
    replayer = Imitator(action_log, register_engine)
    for directive in unchanged_directives:
        replayer.imitate(engine, directive)

    for directive in added_directives:
        engine.defer(directive.type, directive.start_time, directive.args)

    # COPY START
    while not engine.schedule.is_empty():
        resume_time = engine.schedule.peek_next_time()
        engine.elapsed_time = resume_time
        batch_event_graph = EventGraph.empty()
        for task_id in engine.schedule.get_next_batch():
            task_status, event_graph = engine.step(task_id, engine.tasks[task_id])
            batch_event_graph = EventGraph.concurrently(batch_event_graph, event_graph)
        if type(batch_event_graph) != EventGraph.Empty:
            if engine.events and engine.events[-1][0] == engine.elapsed_time:
                engine.events[-1] = (engine.elapsed_time, EventGraph.sequentially(engine.events[-1][1], batch_event_graph))
            else:
                engine.events.append((engine.elapsed_time, batch_event_graph))
        old_awaiting_conditions = list(engine.awaiting_conditions)
        engine.awaiting_conditions.clear()
        engine.current_task_frame = TaskFrame(engine.elapsed_time, history=engine.events)
        while old_awaiting_conditions:
            condition, task_id = old_awaiting_conditions.pop()
            if condition():
                engine.schedule.schedule(engine.elapsed_time, task_id)
            else:
                engine.awaiting_conditions.append((condition, task_id))
    # COPY END

    return sorted(engine.spans, key=lambda x: (x[1], x[2])), list(engine.events), {
        "action_log": {}
    }

class Imitator:
    def __init__(self, action_log, register_engine):
        self.action_log = action_log
        self.register_engine = register_engine

    def imitate(self, engine, directive):
        action_log = self.action_log.get(hashable_directive_without_time(directive), None)
        if action_log is not None:
            task_id = fresh_task_id(repr(directive))
            task = task_replayer(engine, task_id, directive, action_log, self, self.register_engine)
            engine.schedule.schedule(engine.elapsed_time + directive.start_time, task_id)
            engine.task_start_times[task_id] = engine.elapsed_time + directive.start_time
            engine.task_inputs[task_id] = (directive.type, directive.args)
            engine.task_directives[task_id] = directive
            engine.tasks[task_id] = task
            return task_id
        else:
            return engine.defer(directive.type, directive.start_time, directive.args)

    def imitate_spawn(self, engine, directive_type, arguments):
        action_log = self.action_log.get((directive_type, tuple_args(arguments)), None)
        if action_log is not None:
            task_id = fresh_task_id(repr((directive_type, arguments)))
            task = task_replayer(engine, task_id, Directive(directive_type, 0, arguments), action_log, self, self.register_engine)
            engine.current_task_frame.action_log.append((engine.current_task_frame.task_id, "spawn", directive_type, arguments))
            engine.tasks[task_id] = task
            engine.task_inputs[task_id] = (directive_type, arguments)
            engine.task_directives[task_id] = Directive(directive_type, engine.elapsed_time, arguments)
            engine.task_start_times[task_id] = engine.elapsed_time
            parent_task_frame = engine.current_task_frame
            task_status, events = engine.step(task_id, task)
            parent_task_frame.spawn(events)
            engine.current_task_frame = parent_task_frame
            return task_id
        else:
            return engine.spawn(directive_type, arguments)

def task_replayer(engine: SimulationEngine, task_id, directive, action_log, imitator, register_engine):
    processed_reads = []
    while type(action_log) != RBT_Empty:
        if type(action_log) == RBT_NonRead:
            entry = action_log.payload
            action = entry[0]
            action_args = entry[1:]
            if action == "emit":
                topic, value = action_args
                engine.current_task_frame.emit(topic, value)
            elif action == "yield":
                task_status, = action_args
                yield task_status  # TODO if task status is call - make sure the engine tries to look up known tasks before running
            elif action == "spawn":
                directive_type, arguments = action_args
                imitator.imitate_spawn(engine, directive_type, arguments)
                # engine.spawn(directive_type, arguments)
            else:
                raise RuntimeError("Unhandled action type: " + action)
            action_log = action_log.rest
        elif type(action_log) == RBT_Read:
            topics = action_log.topics
            branches = action_log.branches
            new_res = engine.current_task_frame.read(topics)
            for old_res, rbt in branches:
                if tuple(old_res) == tuple(new_res):
                    processed_reads.append(new_res)
                    action_log = rbt
                    break
            else:
                yield step_up_task(register_engine, engine, task_id, directive.type, directive.args, processed_reads, new_res)
        else:
            raise RuntimeError("Unhandled action log type: " + type(action_log))


def step_up_task(register_engine, engine, task_id, directive_type, arguments, reads, last_read):
    temp_engine = ReplayingSimulationEngine(engine)
    register_engine(temp_engine)
    task = make_task(engine.model, directive_type, arguments)
    engine.tasks[task_id] = task
    task_status = temp_engine.step_up(task, reads, last_read)
    register_engine(engine)  # restore main engine
    return task_status


class ReplayingSimulationEngine:
    def __init__(self, main_engine):
        self.model = main_engine.model
        self.main_engine = main_engine
        self.current_task_frame = TaskFrame(main_engine.elapsed_time)
        self.is_stale = False

    def step_up(self, task, reads, last_read):
        self.current_task_frame = ReplayingTaskFrame(reads, last_read, self.main_engine.current_task_frame, self)
        task_status = None
        while not self.is_stale:
            try:
                task_status = next(task)
            except StopIteration:
                self.is_stale = True
                task_status = Completed()
        return task_status

    def spawn(self, directive_type, args):
        if self.is_stale:
            self.main_engine.spawn(directive_type, args)
        else:
            pass  # ignore

class ReplayingTaskFrame:
    def __init__(self, reads, last_read, main_task_frame, replaying_engine):
        self.reads = list(reads)
        self.last_read = last_read
        self.main_task_frame = main_task_frame
        self.replaying_engine = replaying_engine

    def read(self, topic_or_topics):
        if self.replaying_engine.is_stale:
            return self.main_task_frame.read(topic_or_topics)
        if self.reads:
            return self.reads.pop(0)
        else:
            self.replaying_engine.is_stale = True
            return self.last_read

    def emit(self, topic, value):
        if self.replaying_engine.is_stale:
            self.main_task_frame.emit(topic, value)
        else:
            pass # ignore


def make_generator(f, arguments):
    if False:
        yield
    f(**arguments)
