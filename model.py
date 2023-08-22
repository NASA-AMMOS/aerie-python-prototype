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
    spawn(my_child_activity, {})
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
    yield Call(callee_activity, {"value": 99})
    model.x.set(98)


def callee_activity(model: "Model", value):
    model.x.set(value)


def emit_event(model, topic, value, _):
    import sim as facade
    facade.sim_engine.current_task_frame.emit(topic, value)


def read_topic(model, topic, _):
    import sim as facade
    res = []
    for start_offset, events in facade.sim_engine.current_task_frame.read(topic):
        res.append((start_offset, EventGraph.to_string(events)))
    facade.sim_engine.current_task_frame.emit("history", res)


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
            my_decomposing_activity,
            caller_activity,
            callee_activity,
            emit_event,
            read_topic
        ]
        return {x.__name__: x for x in activity_types}
