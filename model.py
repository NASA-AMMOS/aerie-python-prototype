from event_graph import EventGraph
from sim import Register, Accumulator, Delay, AwaitCondition, spawn, Call


def my_activity(model: "Model", param1):
    model.x.set(model.x.get() - param1)
    yield Delay(5)
    model.x.set(model.x.get() + param1)
    yield Delay(5)
    model.x.set(model.x.get() + param1)
    yield Delay(5)
    model.x.set(model.x.get() - param1)


def my_other_activity(model: "Model"):
    yield AwaitCondition(model.x > 56)
    model.y.set(10)
    yield AwaitCondition(model.x < 56)
    model.y.set(9)
    model.y.set(model.y.get() / 3)


def my_decomposing_activity(model: "Model"):
    model.x.set(55)
    spawn("my_child_activity", {})
    model.x.set(57)
    yield Delay(1)
    model.x.set(55)
    yield AwaitCondition(model.y.is_equal_to(10))


def my_child_activity(model: "Model"):
    model.y.set(13)
    yield Delay(1)
    model.y.set(10)


def caller_activity(model: "Model"):
    model.x.set(100)
    yield Call("callee_activity", {"value": 99})
    model.x.set(98)


def callee_activity(model: "Model", value):
    model.x.set(value)


def emit_event(model, topic, value, _):
    import sim as facade
    facade.sim_engine.current_task_frame.emit(topic, value)
    if False:
        yield


def read_topic(model, topic, _):
    import sim as facade
    res = []
    for start_offset, events in facade.sim_engine.current_task_frame.read(topic):
        res.append((start_offset, EventGraph.to_string(events)))
    facade.sim_engine.current_task_frame.emit("history", res)


def emit_then_read(model, read_topic, emit_topic, delay, value, _):
    import sim as facade
    facade.sim_engine.current_task_frame.emit(emit_topic, value)
    yield Delay(delay)
    res = []
    for start_offset, events in facade.sim_engine.current_task_frame.read(read_topic):
        res.append((start_offset, EventGraph.to_string(events)))
    facade.sim_engine.current_task_frame.emit("history", res)


def read_emit_three_times(model, read_topic, emit_topic, delay, _):
    import sim as facade
    for x in range(3):
        res = []
        for start_offset, events in facade.sim_engine.current_task_frame.read(read_topic):
            res.append((start_offset, EventGraph.to_string(events)))
        facade.sim_engine.current_task_frame.emit(emit_topic, res)
        if x < 2:
            yield Delay(delay)


def parent_of_reading_child(model):
    import sim as facade
    facade.sim_engine.current_task_frame.emit("y", 1)
    yield Call("reading_child", {})
    facade.sim_engine.current_task_frame.emit("y", 2)


def reading_child(model):
    import sim as facade
    res = []
    for start_offset, events in facade.sim_engine.current_task_frame.read("x"):
        res.append((start_offset, EventGraph.to_string(events)))
    facade.sim_engine.current_task_frame.emit("history", res)
    yield Delay(model.x.get())


def spawns_reading_child(model):
    import sim as facade
    facade.sim_engine.current_task_frame.emit("y", 1)
    spawn("reading_child", {})
    facade.sim_engine.current_task_frame.emit("y", 2)


def emit_if_x_equal(model, x_value, topic, value_to_emit, _):
    import sim as facade
    if model.x.get() == x_value:
        facade.sim_engine.current_task_frame.emit(topic, value_to_emit)
    if False:
        yield

def conditional_decomposition(model, _):
    if model.x.get() == 1:
        spawn("reading_child", {})
    else:
        spawn("emit_event", {"topic": "u", "value": 2, "_": "_"})
    if False:
        yield

def parent_of_conditional_decomposition(model, _):
    import sim as facade
    facade.sim_engine.current_task_frame.emit("q", 1)
    yield Call("conditional_decomposition", {"_": _})

def parent_of_parent_of_conditional_decomposition(model, _):
    import sim as facade
    facade.sim_engine.current_task_frame.emit("q", -1)
    spawn("parent_of_conditional_decomposition", {"_": _})


class Model:
    def __init__(self):
        self.x = Register("x", 55)
        self.y = Register("y", 0)
        self.z = Accumulator("z", 0, 1)

    def attributes(self):
        return [x for x in self.__dict__ if not x.startswith("_")]

    def get_activity_types(self):
        activity_types = [
            my_activity,
            my_other_activity,
            my_child_activity,
            my_decomposing_activity,
            caller_activity,
            callee_activity,
            emit_event,
            read_topic,
            emit_then_read,
            read_emit_three_times,
            parent_of_reading_child,
            reading_child,
            spawns_reading_child,
            emit_if_x_equal,
            conditional_decomposition,
            parent_of_conditional_decomposition,
            parent_of_parent_of_conditional_decomposition
        ]
        return {x.__name__: x for x in activity_types}
