"""
This approach records the actions taken by each task, and replays them as appropriate.

TO-DO:
- daemon tasks
- sim config
- optimize
- avoid imitating things that we don't need to

Want to know: historical record of queries, and who made them

Q: Given a `change`, what queries will return different results, and what conditions will be triggered at different times?

- Maybe we don't need to check all reads
  - Track "potentially" changed reads (i.e. invalidated)

Given a write, which reads does it intersect with?

AwaitCondition(x > 10)
Call(foo)

def foo():
   while not (x > 10):
      delay(epsilon)
"""
from collections import namedtuple
import inspect

from protocol import Completed, Delay, AwaitCondition, Call, Directive, tuple_args, make_generator
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

#############################################
#           Simulation Engine               #
#############################################

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
        self.awaiting_conditions = Subscriptions()
        self.signaled_conditions = []
        self.awaiting_tasks = {}  # map from blocking task to blocked task
        self.spans = []
        self.tasks = {}
        self.register_engine = register_engine
        if anonymous_tasks is None:
            anonymous_tasks = {}
        self.anonymous_tasks = anonymous_tasks
        self.imitating_tasks = {}

    def register_model(self, cls):
        """
        Instantiate the model, save, and return that instance
        """
        self.model = cls()
        self.activity_types_by_name = self.model.get_activity_types()
        return self.model

    def spawn(self, directive_type, arguments):
        """
        Spawn a task, and step it once, recording its actions.
        From then on, that task is just like any other task - only its first step gets special treatment, since it
        needs to be grafted into the current event graph
        """
        self.current_task_frame.action_log.spawn(self.current_task_frame.task_id, directive_type, arguments)
        task_id = fresh_task_id(str(directive_type) + " " + str(arguments))
        self.task_start_times[task_id] = self.elapsed_time
        self.task_directives[task_id] = Directive(directive_type, self.elapsed_time, arguments)
        self.make_task(task_id, directive_type, arguments)

        # make_task will put this task in either self.tasks or self.imitating_tasks, depending on whether we've seen this task before
        if task_id in self.tasks:
            self.step_child_task(task_id, self.tasks[task_id])
        else:
            self.step_imitating_task(task_id)
        return task_id

    def spawn_anonymous(self, task_factory):
        """
        Spawn a task
        """
        task = task_factory()
        task_id = fresh_task_id(repr(task))
        task_name = fresh_anonymous_task_name()
        self.current_task_frame.action_log.spawn(self.current_task_frame.task_id, task_name, {})
        self.anonymous_tasks[task_name] = task_factory
        self.tasks[task_id] = task
        self.task_inputs[task_id] = (task_name, {})
        self.task_directives[task_id] = Directive(task_name, self.elapsed_time, {})
        self.task_start_times[task_id] = self.elapsed_time
        self.step_child_task(task_id, task)
        return task_id

    def step_child_task(self, task_id, task):
        """
        callee saves current_task_frame
        """
        parent_task_frame = self.current_task_frame
        task_status, emitted_topics, events = self.step(task_id, task)
        parent_task_frame.record_spawned_child(events)
        self.current_task_frame = parent_task_frame
        return emitted_topics

    def step_imitating_task(self, task_id):
        return self.step_child_task(task_id, self.imitating_tasks[task_id])

    def defer(self, directive_type, duration, arguments):
        task_id = fresh_task_id(repr((directive_type, arguments)))
        self.schedule.schedule(self.elapsed_time + duration, task_id)
        self.task_directives[task_id] = Directive(directive_type, self.elapsed_time + duration, arguments)
        self.task_start_times[task_id] = self.elapsed_time + duration
        self.make_task(task_id, directive_type, arguments)

    def step(self, task_id, task):
        """
        caller saves current_task_frame
        """
        self.current_task_frame = TaskFrame(self.elapsed_time, self.action_log, task_id=task_id, history=self.current_task_frame.get_visible_history())

        try:
            task_status = next(task)
        except StopIteration:
            task_status = Completed()

        self.action_log.yield_(task_id, task_status)

        match task_status:
            case Delay(duration):
                self.schedule.schedule(self.elapsed_time + duration, task_id)
            case AwaitCondition(condition):
                self.signaled_conditions.append((condition, task_id))
            case Completed():
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
            case Call(child_task, args):
                child_task_id = fresh_task_id(repr((child_task, args)))
                self.awaiting_tasks[child_task_id] = task_id
                self.task_directives[child_task_id] = Directive(child_task, self.elapsed_time, args)
                self.task_start_times[child_task_id] = self.elapsed_time

                self.make_task(child_task_id, child_task, args)
                if child_task_id in self.tasks:
                    self.step_child_task(child_task_id, self.tasks[child_task_id])
                else:
                    self.step_imitating_task(child_task_id)
            case _:
                raise ValueError("Unhandled task status: " + str(task_status))

        return task_status, set(self.current_task_frame.emitted_topics), self.current_task_frame.collect()

    def make_task(self, task_id, directive_type, args):
        """
        Adds task_id to either self.tasks or self.imitating_tasks, depending on whether we know anything about this task
        """
        self.task_inputs[task_id] = (directive_type, args)
        if self.action_log.contains_key(directive_type, args):
            self.imitating_tasks[task_id] = task_replayer(self, task_id, directive_type, args, self.action_log.get(directive_type, args), self.register_engine)
        elif directive_type in self.anonymous_tasks:  # oxymoron
            self.tasks[task_id] = self.anonymous_tasks[directive_type]()
        else:
            self.tasks[task_id] = make_task(self.model, directive_type, args)

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
        self.read_topics = set()
        self.emitted_topics = set()

    def emit(self, topic, value):
        self.emitted_topics.add(topic)
        self.action_log.emit(self.task_id, topic, value)
        self.tip = EventGraph.sequentially(self.tip, EventGraph.Atom(Event(topic, value)))

    def read(self, topic_or_topics, function):
        topics = [topic_or_topics] if type(topic_or_topics) != list else topic_or_topics
        self.read_topics.update(topics)
        res = []
        for start_offset, x in self.get_visible_history():
            filtered = EventGraph.filter(x, topics)
            if not EventGraph.is_empty(filtered):
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


class Subscriptions:
    def __init__(self):
        self.subcribers_by_topics = {}

    def subscribe(self, topics, new_subscriber):
        for topic in topics:
            if topic not in self.subcribers_by_topics:
                self.subcribers_by_topics[topic] = set()
            self.subcribers_by_topics[topic].add(new_subscriber)

    def unsubscribe(self, subscriber):
        for subscribers in self.subcribers_by_topics.values():
            subscribers.remove(subscriber)

    def invalidate(self, topics):
        result = set()
        for topic in topics:
            if not topic in self.subcribers_by_topics: continue
            result.update(self.subcribers_by_topics[topic])
            del self.subcribers_by_topics[topic]
        return result



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
        saved_task_frame = engine.current_task_frame
        emitted_topics = set()
        for task_id in engine.schedule.get_next_batch():
            engine.current_task_frame = saved_task_frame
            if task_id in engine.tasks:
                task_status, newly_emitted_topics, event_graph = engine.step(task_id, engine.tasks[task_id])
                emitted_topics.update(newly_emitted_topics)
            else:
                emitted_topics.update(engine.step_imitating_task(task_id))
                event_graph = engine.current_task_frame.collect()
            batch_event_graph = EventGraph.concurrently(batch_event_graph, event_graph)
        engine.current_task_frame = saved_task_frame

        # At this point, emitted_topics contains all invalidated topics
        if emitted_topics and engine.awaiting_conditions.subcribers_by_topics:
            engine.signaled_conditions.extend(engine.awaiting_conditions.invalidate(emitted_topics))

        if not EventGraph.is_empty(batch_event_graph):
            if engine.events and engine.events[-1][0] == engine.elapsed_time:
                engine.events[-1] = (engine.elapsed_time, EventGraph.sequentially(engine.events[-1][1], batch_event_graph))
            else:
                engine.events.append((engine.elapsed_time, batch_event_graph))
        engine.current_task_frame = TaskFrame(engine.elapsed_time, engine.action_log, history=engine.events)
        for condition, task_id in engine.signaled_conditions:
            engine.current_task_frame = TaskFrame(engine.elapsed_time, engine.action_log, history=engine.events)
            time_to_wake_task = condition(True, 0, 9999)
            if time_to_wake_task is not None:
                engine.schedule.schedule(engine.elapsed_time + time_to_wake_task, task_id)
            else:
                engine.awaiting_conditions.subscribe(engine.current_task_frame.read_topics, (condition, task_id))
        engine.signaled_conditions.clear()

    return sorted(engine.spans, key=lambda x: (x[1], x[2])), list(engine.events), {
        "action_log": engine.action_log,
        "anonymous_tasks": engine.anonymous_tasks
    }


RBT_Empty = namedtuple("RBT_Empty", "")
RBT_NonRead = namedtuple("RBT_NonRead", "actions event_graph rest")
RBT_Read = namedtuple("RBT_Read", "topics function branches")


def make_rbt(actions, accumulator=None):
    if accumulator is None: accumulator = []

    # Handle base case
    if not actions:
        if accumulator:
            return RBT_NonRead(accumulator, make_event_graph(accumulator), RBT_Empty())
        else:
            return RBT_Empty()

    # We have at least one action
    (action, *action_args), *rest = actions

    if action == "read":
        topics, function, res = action_args
        if accumulator:
            return RBT_NonRead(accumulator, make_event_graph(accumulator), RBT_Read(topics, function, ((res, make_rbt(rest)),)))
        else:
            return RBT_Read(topics, function, ((res, make_rbt(rest)),))
    else:
        accumulator = accumulator + [(action, *action_args)]
        if action == "yield":
            return RBT_NonRead(accumulator, make_event_graph(accumulator), make_rbt(rest))
        else:
            return make_rbt(rest, accumulator)

"""
Given an existing rbt, and a set of compatible actions, propagate the rbt using the given actions

Precondition: The rbt must be compatible with the given actions. Let RD be the first differing read in the
list of actions. Every action prior to RD must already be represented in the rbt.
"""
def extend(rbt, actions):
    # If the set of actions is empty, it is trivially compatible with this rbt. No propagation is necessary.
    if not actions:
        return rbt

    # If the rbt is empty, the set of actions is also trivially compatible.
    # Delegate to make_rbt to create a brand new rbt.
    match rbt:
        case RBT_Empty():
            return make_rbt(actions)

        case RBT_NonRead(rbt_actions, rbt_event_graph, rbt_rest):
            expected_nonreads = list(rbt_actions)

            # If neither the rbt nor the actions are empty, traverse both up until the next read or yield, or the actions are exhausted
            nonreads = []
            actions = list(actions)
            while actions:
                if actions[0][0] == "read":
                    break
                elif actions[0][0] == "yield":
                    nonreads.append(actions.pop(0))
                    return RBT_NonRead(nonreads, make_event_graph(nonreads), extend(rbt_rest, actions))
                else:
                    assert actions[0] == expected_nonreads.pop(0)
                    nonreads.append(actions.pop(0))

            # We reach here if either a read was found, or the list of actions was exhausted
            if expected_nonreads and actions:
                raise RuntimeError("Not all nonreads were exhausted! " + str(rbt.actions) + ":::" + str(actions))
            if nonreads:
                return RBT_NonRead(nonreads, make_event_graph(nonreads), extend(rbt.rest, actions))
            raise RuntimeError("Does this ever happen?")

        case RBT_Read(_, _, _):
            first, *rest = actions
            action, topics, function, res = first
            assert rbt.topics == topics  # The read query must be identical. The response may or may not be.

            branches = []
            found_match = False

            for old_res, old_rbt in rbt.branches:
                if old_res == res:
                    found_match = True
                    branches.append((old_res, extend(old_rbt, rest)))
                else:
                    branches.append((old_res, old_rbt))

            if not found_match:
                # We've never seen this read before! We're in uncharted territory; mint a new rbt with make_rbt.
                branches.append((res, make_rbt(rest)))
            return RBT_Read(topics, function, tuple(branches))

        case _:
            raise RuntimeError("Non-exhaustive match: " + rbt)

def make_event_graph(actions):
    event_graph = EventGraph.empty()
    seen_yield = False
    for action, *action_args in reversed(actions):
        if action == "yield":
            if seen_yield: raise RuntimeError("Double yield in actions " + str(actions))
            seen_yield = True
        elif action == "emit":
            topic, value = action_args
            event_graph = EventGraph.sequentially(EventGraph.atom(Event(topic, value)), event_graph)
        elif action == "spawn":
            child_directive_type, arguments = action_args
            event_graph = EventGraph.concurrently(event_graph, "spawned " + child_directive_type + " with args " + str(arguments))#make_event_graph(action_log[child_directive_type], action_log))
        else:
            raise RuntimeError("Unhandled action type: " + action)
    return event_graph


def simulate_incremental(register_engine, model_class, new_plan, old_plan, payload):
    return simulate(register_engine, model_class, new_plan, action_log=payload["action_log"], anonymous_tasks=payload["anonymous_tasks"])


def task_replayer(engine: SimulationEngine, task_id, directive_type, args, action_log, register_engine):
    processed_reads = []
    while True:
        match action_log:
            case RBT_Empty():
                break
            case RBT_NonRead(entries, event_graph, rest):
                # engine.current_task_frame.emit_all(event_graph)
                for entry in entries:
                    action, *action_args = entry
                    if action == "emit":
                        topic, value = action_args
                        engine.current_task_frame.emit(topic, value)
                    elif action == "yield":
                        task_status, = action_args
                        yield task_status
                    elif action == "spawn":
                        child_directive_type, arguments = action_args
                        engine.spawn(child_directive_type, arguments)
                    else:
                        raise RuntimeError("Unhandled action type: " + action)
                    action_log = rest
            case RBT_Read(topics, function, branches):
                new_res = engine.current_task_frame.read(topics, function)
                for old_res, branch in branches:
                    if old_res != new_res: continue
                    processed_reads.append(new_res)
                    action_log = branch
                    break
                else:
                    # Cache miss! Time to step up the task, patch it into the engine, and then yield its next status.
                    engine.tasks[task_id] = step_up_task(register_engine, engine, directive_type, args, processed_reads, new_res)
                    yield next(engine.tasks[task_id])
                    return
            case _:
                raise RuntimeError("Unhandled action log type: " + str(type(action_log)))


def step_up_task(register_engine, engine, directive_type, arguments, reads, last_read):
    """
    Conjures a given task in the requested state. Calling `next()` will yield its next task status.

    In detail, calling next will do the following:
    1. Register a dummy simulation engine globally
    2. Run the given directive_type and args, ignoring any emits or spawns. Reads are fed from the `reads` parameter
    3. When the reads have been exhausted, re-register the real simulation engine, and yield the next task status

    During step 3, any emits and spawns are processed by the real simulation engine
    """
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