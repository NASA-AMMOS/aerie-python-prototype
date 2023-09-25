"""
This approach records the actions taken by each task, and replays them as appropriate.

TO-DO:
- daemon tasks
- anonymous tasks
- sim config
- optimize
"""
from collections import namedtuple
import inspect

from protocol import Completed, Delay, AwaitCondition, Call, Directive, hashable_directive_without_time, tuple_args, \
    make_generator
from event_graph import EventGraph

Event = namedtuple("Event", "topic value")
TaskId = namedtuple("TaskId", "id label")

__task_id_counter = 0
def fresh_task_id(label=""):
    global __task_id_counter
    __task_id_counter += 1
    return TaskId(__task_id_counter, label)

def fresh_anonymous_task_name(label=""):
    global __task_id_counter
    __task_id_counter += 1
    return f"ANONYMOUS({__task_id_counter}){' ' + label if label != '' else ''}"

class SimulationEngine:
    def __init__(self, register_engine, action_log=None, anonymous_tasks=None):
        self.action_log = ActionLog(self, action_log)
        self.elapsed_time = 0
        self.events = []  # list of tuples (start_offset, event_graph)
        self.current_task_frame = TaskFrame(self.elapsed_time, self.action_log)
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
        self.register_engine = register_engine
        if anonymous_tasks is None:
            anonymous_tasks = {}
        self.anonymous_tasks = anonymous_tasks

    def register_model(self, cls):
        self.model = cls()
        self.activity_types_by_name = self.model.get_activity_types()
        return self.model

    def spawn(self, directive_type, arguments):
        task_id = fresh_task_id(str(directive_type) + " " + str(arguments))
        task = self.make_task(task_id, directive_type, arguments)
        self.current_task_frame.action_log.spawn(self.current_task_frame.task_id, directive_type, arguments)
        self.tasks[task_id] = task
        self.task_inputs[task_id] = (directive_type, arguments)
        self.task_directives[task_id] = Directive(directive_type, self.elapsed_time, arguments)
        self.task_start_times[task_id] = self.elapsed_time
        self.spawn_task(task_id, task)
        return task_id

    def spawn_anonymous(self, task_factory):
        task = task_factory()
        task_id = fresh_task_id(repr(task))
        task_name = fresh_anonymous_task_name()
        self.current_task_frame.action_log.spawn(self.current_task_frame.task_id, task_name, {})
        self.anonymous_tasks[task_name] = task_factory
        self.tasks[task_id] = task
        self.task_inputs[task_id] = (task_name, {})
        self.task_directives[task_id] = Directive(task_name, self.elapsed_time, {})
        self.task_start_times[task_id] = self.elapsed_time
        self.spawn_task(task_id, task)
        return task_id

    def spawn_task(self, task_id, task):
        parent_task_frame = self.current_task_frame
        task_status, events = self.step(task_id, task)
        parent_task_frame.record_spawned_child(events)
        self.current_task_frame = parent_task_frame

    def defer(self, directive_type, duration, arguments):
        task_id = fresh_task_id(repr((directive_type, arguments)))
        task = self.make_task(task_id, directive_type, arguments)
        self.schedule.schedule(self.elapsed_time + duration, task_id)
        self.task_start_times[task_id] = self.elapsed_time + duration
        self.task_inputs[task_id] = (directive_type, arguments)
        self.task_directives[task_id] = Directive(directive_type, self.elapsed_time + duration, arguments)
        self.tasks[task_id] = task
        return task_id, task

    def step(self, task_id, task):
        self.current_task_frame = TaskFrame(self.elapsed_time, self.action_log, task_id=task_id, history=self.current_task_frame.get_visible_history())
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
            self.task_start_times[child_task_id] = self.elapsed_time
            self.spawn_task(child_task_id, child_task)
        else:
            raise ValueError("Unhandled task status: " + str(task_status))
        self.action_log.yield_(task_id, task_status)
        return task_status, self.current_task_frame.collect()

    def make_task(self, task_id, directive_type, args):
        if self.action_log.contains_key(directive_type, args):
            return task_replayer(self, directive_type, args, self.action_log.get(directive_type, args), Doppelganger(self.action_log, self.register_engine), self.register_engine)
        elif directive_type in self.anonymous_tasks:  # oxymoron
            return self.anonymous_tasks[directive_type]()
        else:
            return make_task(self.model, directive_type, args)

class ActionLog:
    def __init__(self, engine, old_action_log):
        self.action_log = {}
        if old_action_log is None:
            self.rbt = {}
        else:
            self.rbt = old_action_log.to_rbt()
        self.engine = engine

    def ensure_key_present(self, key):
        if key not in self.action_log:
            self.action_log[key] = []

    def emit(self, task_id, topic, value):
        self.ensure_key_present(task_id)
        self.action_log[task_id].append(("emit", topic, value))

    def read(self, task_id, topics, function, result):
        if task_id is None:
            return
        self.ensure_key_present(task_id)
        self.action_log[task_id].append(("read", topics, function, result))

    def spawn(self, task_id, directive_type, arguments):
        self.ensure_key_present(task_id)
        self.action_log[task_id].append(("spawn", directive_type, arguments))

    def yield_(self, task_id, task_status):
        self.ensure_key_present(task_id)
        self.action_log[task_id].append(("yield", task_status))

    def contains_key(self, directive_type, args):
        return (directive_type, tuple_args(args)) in self.to_rbt()

    def get(self, directive_type, args):
        return self.to_rbt().get((directive_type, tuple_args(args)), None)

    def to_rbt(self):
        # action log structure:
        # key: hashable(directive name, args)
        # value: a read-branching tree. It is one of:
        # - NonRead(payload, next)
        # - Read(topic, Map[Value, read-branching tree])

        action_log = dict(self.rbt)
        for task_id, actions in self.action_log.items():
            directive = self.engine.task_directives[task_id]
            key = (directive.type, tuple_args(directive.args))
            if key in action_log:
                action_log[key] = extend(action_log[key], actions)
            else:
                action_log[key] = make_rbt(actions)
        return action_log


class TaskFrame:
    Branch = namedtuple("Branch", "base event_graph")

    def __init__(self, elapsed_time, action_log, task_id=None, history=None):
        if history is None:
            history = []
        self.task_id = task_id
        self.tip = EventGraph.empty()
        self.history = history
        self.branches = []
        self.elapsed_time = elapsed_time
        self.action_log = action_log

    def emit(self, topic, value):
        self.action_log.emit(self.task_id, topic, value)
        self.tip = EventGraph.sequentially(self.tip, EventGraph.Atom(Event(topic, value)))

    def read(self, topic_or_topics, function):
        topics = [topic_or_topics] if type(topic_or_topics) != list else topic_or_topics
        res = []
        for start_offset, x in self.get_visible_history():
            filtered = EventGraph.filter(x, topics)
            if type(filtered) != EventGraph.Empty:
                res.append((start_offset, filtered))
        res = function(res)
        self.action_log.read(self.task_id, topics, function, res)
        return res

    def record_spawned_child(self, event_graph):
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


def simulate(register_engine, model_class, plan, action_log=None, anonymous_tasks=None):
    engine = SimulationEngine(register_engine, action_log=action_log, anonymous_tasks=anonymous_tasks)
    engine.register_model(model_class)
    register_engine(engine)

    for directive in plan.directives:
        engine.defer(directive.type, directive.start_time, directive.args)

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
        old_awaiting_conditions = engine.awaiting_conditions
        engine.awaiting_conditions = []
        engine.current_task_frame = TaskFrame(engine.elapsed_time, engine.action_log, history=engine.events)
        for condition, task_id in old_awaiting_conditions:
            if condition():
                engine.schedule.schedule(engine.elapsed_time, task_id)
            else:
                engine.awaiting_conditions.append((condition, task_id))



    return sorted(engine.spans, key=lambda x: (x[1], x[2])), list(engine.events), {
        "action_log": engine.action_log,
        "anonymous_tasks": engine.anonymous_tasks
    }


RBT_Empty = namedtuple("RBT_Empty", "")
RBT_NonRead = namedtuple("RBT_NonRead", "payload rest")
RBT_Read = namedtuple("RBT_Read", "topics function branches")


def make_rbt(actions):
    if not actions:
        return RBT_Empty()
    first, rest = actions[0], actions[1:]
    action, action_args = first[0], first[1:]
    if action == "read":
        topics, function, res = action_args
        return RBT_Read(topics, function, ((res, make_rbt(rest)),))
    else:
        return RBT_NonRead(first, make_rbt(rest))


def extend(rbt, actions):
    if not actions:
        return rbt
    first, rest = actions[0], actions[1:]
    action, action_args = first[0], first[1:]
    if action == "read":
        assert type(rbt) == RBT_Read
        topics, function, res = action_args
        assert rbt.topics == topics
        new_branches = list()
        found_match = False
        for old_res, old_rbt in rbt.branches:
            if old_res == res:
                found_match = True
                new_branches.append((old_res, extend(old_rbt, rest)))
            else:
                new_branches.append((old_res, old_rbt))
        if not found_match:
            new_branches.append((res, make_rbt(rest)))
        return RBT_Read(topics, function, tuple(new_branches))
    else:
        assert type(rbt) == RBT_NonRead
        return RBT_NonRead(first, extend(rbt.rest, rest))


def simulate_incremental(register_engine, model_class, new_plan, old_plan, payload):
    return simulate(register_engine, model_class, new_plan, action_log=payload["action_log"], anonymous_tasks=payload["anonymous_tasks"])


class Doppelganger:
    def __init__(self, action_log, register_engine):
        self.action_log = action_log
        self.register_engine = register_engine

    def imitate(self, engine, directive):
        action_log = self.action_log.get(directive.type, directive.args)
        if action_log is not None:
            task_id = fresh_task_id(repr(directive))
            task = task_replayer(engine, directive, action_log, self, self.register_engine)
            engine.schedule.schedule(engine.elapsed_time + directive.start_time, task_id)
            engine.task_start_times[task_id] = engine.elapsed_time + directive.start_time
            engine.task_inputs[task_id] = (directive.type, directive.args)
            engine.task_directives[task_id] = directive
            engine.tasks[task_id] = task
            return task_id
        else:
            return engine.defer(directive.type, directive.start_time, directive.args)

    def imitate_spawn(self, engine, directive_type, arguments):
        action_log = self.action_log.get(directive_type, arguments)
        if action_log is not None:
            task_id = fresh_task_id(repr((directive_type, arguments)))
            task = task_replayer(engine, directive_type, arguments, action_log, self, self.register_engine)
            engine.current_task_frame.action_log.spawn(engine.current_task_frame.task_id, directive_type, arguments)
            engine.tasks[task_id] = task
            engine.task_inputs[task_id] = (directive_type, arguments)
            engine.task_directives[task_id] = Directive(directive_type, engine.elapsed_time, arguments)
            engine.task_start_times[task_id] = engine.elapsed_time
            parent_task_frame = engine.current_task_frame
            task_status, events = engine.step(task_id, task)
            parent_task_frame.record_spawned_child(events)
            engine.current_task_frame = parent_task_frame
            return task_id
        else:
            return engine.spawn(directive_type, arguments)

def task_replayer(engine: SimulationEngine, directive_type, args, action_log, imitator, register_engine):
    processed_reads = []
    # TODO use python's newfangled match
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
                yield task_status
            elif action == "spawn":
                child_directive_type, arguments = action_args
                imitator.imitate_spawn(engine, child_directive_type, arguments)
            else:
                raise RuntimeError("Unhandled action type: " + action)
            action_log = action_log.rest
        elif type(action_log) == RBT_Read:
            topics = action_log.topics
            function = action_log.function
            branches = action_log.branches
            new_res = engine.current_task_frame.read(topics, function)
            for old_res, branch in branches:
                if old_res != new_res: continue
                processed_reads.append(new_res)
                action_log = branch
                break
            else:
                # TODO: yield from instead of yield without resume
                yield from step_up_task(register_engine, engine, directive_type, args, processed_reads, new_res)
                return
        else:
            raise RuntimeError("Unhandled action log type: " + type(action_log))


def step_up_task(register_engine, engine, directive_type, arguments, reads, last_read):
    if directive_type in engine.anonymous_tasks:
        task = engine.anonymous_tasks[directive_type]()
    else:
        task = make_task(engine.model, directive_type, arguments)
    return ReplayingSimulationEngine(engine).step_up(register_engine, engine, task, reads, last_read)


class ReplayingSimulationEngine:
    def __init__(self, main_engine):
        self.model = main_engine.model
        self.main_engine = main_engine
        self.current_task_frame = TaskFrame(main_engine.elapsed_time, main_engine.action_log)
        self.is_replaying = True

    def step_up(self, register_engine, old_engine, task, reads, last_read):
        register_engine(self)
        self.current_task_frame = ReplayingTaskFrame(reads, last_read, self.main_engine.current_task_frame, self)
        task_status = None
        while self.is_replaying:
            try:
                task_status = next(task)
            except StopIteration:
                task_status = Completed()
        register_engine(old_engine)
        yield task_status
        yield from task

    def spawn(self, directive_type, args):
        if self.is_replaying:
            pass  # ignore
        else:
            self.main_engine.spawn(directive_type, args)


class ReplayingTaskFrame:
    def __init__(self, reads, last_read, main_task_frame, replaying_engine):
        self.reads = list(reads)
        self.last_read = last_read
        self.main_task_frame = main_task_frame
        self.replaying_engine = replaying_engine

    def read(self, topic_or_topics, function):
        if self.replaying_engine.is_replaying:
            if self.reads:
                return self.reads.pop(0)
            else:
                self.replaying_engine.is_replaying = False
                return self.last_read
        else:
            return self.main_task_frame.read(topic_or_topics, function)

    def emit(self, topic, value):
        if self.replaying_engine.is_replaying:
            pass  # ignore
        else:
            self.main_task_frame.emit(topic, value)
