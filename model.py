from event_graph import EventGraph
from sim import Register, Accumulator, Delay, AwaitCondition, spawn, Call, spawn_anonymous


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


def read_topic(model, topic, _):
    import sim as facade
    res = []
    for start_offset, events in facade.sim_engine.current_task_frame.read(topic, identity):
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
        for start_offset, events in facade.sim_engine.current_task_frame.read(read_topic, identity):
            res.append((start_offset, EventGraph.to_string(events)))
        facade.sim_engine.current_task_frame.emit(emit_topic, res)
        if x < 2:
            yield Delay(delay)


def read_emit_three_times_and_whoopee(model, read_topic, emit_topic, delay, _):
    if model.x.get() == -1:
        should_emit_whoopee = True
    else:
        should_emit_whoopee = False
    import sim as facade
    for x in range(3):
        res = []
        for start_offset, events in facade.sim_engine.current_task_frame.read(read_topic, identity):
            res.append((start_offset, EventGraph.to_string(events)))
        facade.sim_engine.current_task_frame.emit(emit_topic, res)
        if x < 2:
            yield Delay(delay)
    if should_emit_whoopee:
        facade.sim_engine.current_task_frame.emit(emit_topic, "whoopee")


def parent_of_read_emit_three_times_and_whoopee(model, **kwargs):
    spawn("read_emit_three_times_and_whoopee", kwargs)


def parent_of_reading_child(model):
    import sim as facade
    facade.sim_engine.current_task_frame.emit("y", 1)
    yield Call("reading_child", {})
    facade.sim_engine.current_task_frame.emit("y", 2)


def reading_child(model):
    import sim as facade
    res = []
    for start_offset, events in facade.sim_engine.current_task_frame.read("x", identity):
        res.append((start_offset, EventGraph.to_string(events)))
    facade.sim_engine.current_task_frame.emit("history", res)
    res = []
    for start_offset, events in facade.sim_engine.current_task_frame.read("y", identity):
        res.append((start_offset, EventGraph.to_string(events)))
    facade.sim_engine.current_task_frame.emit("history", res)
    yield Delay(model.x.get())


def identity(x):
    return x


def spawns_reading_child(model):
    import sim as facade
    facade.sim_engine.current_task_frame.emit("y", 1)
    spawn("reading_child", {})
    facade.sim_engine.current_task_frame.emit("y", 2)


def emit_if_x_equal(model, x_value, topic, value_to_emit, _):
    import sim as facade
    if model.x.get() == x_value:
        facade.sim_engine.current_task_frame.emit(topic, value_to_emit)

def conditional_decomposition(model, _):
    if model.x.get() == 1:
        spawn("reading_child", {})
    else:
        spawn("emit_event", {"topic": "u", "value": 2, "_": _})

def parent_of_conditional_decomposition(model, _):
    import sim as facade
    facade.sim_engine.current_task_frame.emit("q", 1)
    yield Call("conditional_decomposition", {"_": _})

def parent_of_parent_of_conditional_decomposition(model, _):
    import sim as facade
    facade.sim_engine.current_task_frame.emit("q", -1)
    spawn("parent_of_conditional_decomposition", {"_": _})


def delay_zero_between_spawns(model, _):
    spawn("conditional_decomposition", {"_": 1})
    model.y.set(800)
    yield Delay(0)
    spawn("conditional_decomposition", {"_": 2})


def await_condition_set_by_child(model, _):
    model.x.set(9)
    spawn("maybe_delay_then_emit", {"_": _})
    yield AwaitCondition(model.x > 9)
    model.x.set(11)
    yield Delay(5)


def maybe_delay_then_emit(model, _):
    if model.y.get() == 1:
        yield Delay(20)
    model.x.set(10)
    yield Delay(3)


def call_multiple(model, _):
    yield Call("emit_and_delay", {"delay": 1, "topic": "u", "value": 2, "_": _})

    yield Call("emit_and_delay", {"delay": model.x.get(), "topic": "u", "value": 3, "_": _})

    yield Call("emit_and_delay", {"delay": 1, "topic": "u", "value": 4, "_": _})


def emit_and_delay(model, topic, value, delay, _):
    import sim as facade
    facade.sim_engine.current_task_frame.emit(topic, value)
    yield Delay(delay)


def await_x_greater_than(model, value):
    import sim as facade
    facade.sim_engine.current_task_frame.emit("u", 1)
    yield AwaitCondition(model.x > value)
    facade.sim_engine.current_task_frame.emit("u", 2)


def await_y_greater_than(model, value):
    import sim as facade
    facade.sim_engine.current_task_frame.emit("u", 1)
    yield AwaitCondition(model.y > value)
    facade.sim_engine.current_task_frame.emit("u", 2)


def call_then_read(model):
    import sim as facade
    facade.sim_engine.current_task_frame.emit("y", 7)
    yield Call("reading_child", {})
    facade.sim_engine.current_task_frame.emit("y", model.x.get())


def spawns_anonymous_task(model):
    yield Delay(1)
    spawn_anonymous(my_anonymous_task(model, 1))
    yield Delay(2)
    x = model.x.get()
    res = x * 100
    model.y.set(res)


def my_anonymous_task(model, delay):
    def task():
        yield Delay(delay)
        x = model.x.get()
        model.x.set(55)
        model.y.set(x + 1)
    return task


def no_op(model):
    pass


def condition_becomes_true_with_no_steps(model):
    model.x.set(model.z.get())
    yield AwaitCondition(model.z >= 200)
    model.x.set(model.z.get())


def set_z(model, value, rate):
    import sim as facade
    facade.sim_engine.current_task_frame.emit("z", Accumulator.SetValue(value))
    facade.sim_engine.current_task_frame.emit("z", Accumulator.SetRate(rate))


def read_and_await_condition(model):
    z = model.z.get()
    yield AwaitCondition(model.z >= 100)
    model.x.set(-2)


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
            parent_of_parent_of_conditional_decomposition,
            read_emit_three_times_and_whoopee,
            parent_of_read_emit_three_times_and_whoopee,
            delay_zero_between_spawns,
            await_condition_set_by_child,
            maybe_delay_then_emit,
            call_multiple,
            emit_and_delay,
            await_x_greater_than,
            await_y_greater_than,
            call_then_read,
            spawns_anonymous_task,
            no_op,
            condition_becomes_true_with_no_steps,
            set_z,
            read_and_await_condition
        ]
        return {x.__name__: x for x in activity_types}
