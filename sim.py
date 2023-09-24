from collections import namedtuple

from protocol import Delay, AwaitCondition, EventGraph, Call, make_generator

sim_engine = None

def spawn(*args, **kwargs):
    sim_engine.spawn(*args, **kwargs)


def spawn_anonymous(task):
    sim_engine.spawn_anonymous(task)



class Register:
    def __init__(self, topic, initial_value):
        self._topic = topic
        self._initial_value = initial_value

    def get(self):
        read = sim_engine.current_task_frame.read(self._topic, self.function)
        if type(read) != int:
            print()
        return read

    def function(self, history):
        res = self._initial_value
        for _, event_graph in history:
            for event in EventGraph.iter(event_graph):
                res = event.value
        return res

    def set(self, new_value):
        sim_engine.current_task_frame.emit(self._topic, new_value)

    def __add__(self, other):
        return self.get() + other

    def __sub__(self, other):
        return self.get() - other

    def __gt__(self, other):
        def condition():
            return self.get() > other

        return condition

    def is_equal_to(self, other):
        def condition():
            return self.get() == other

        return condition

    def __lt__(self, other):
        def condition():
            return self.get() < other

        return condition


class Accumulator:
    SetValue = namedtuple("Set", "new_value")
    SetRate = namedtuple("Set", "new_rate")

    def __init__(self, topic, value, rate):
        self._topic = topic
        self._initial_value = value
        self._initial_rate = rate

    def get(self):
        return sim_engine.current_task_frame.read(self._topic, self.function)

    def function(self, history):
        value = self._initial_value
        rate = self._initial_rate
        previous_event_time = 0
        for start_offset, event_graph in history:
            for event in EventGraph.iter(event_graph):
                value += rate * (event.start_time - previous_event_time)
                previous_event_time = event.start_time
                if type(event.value) == Accumulator.SetValue:
                    value = event.value.new_value
                if type(event.value) == Accumulator.SetRate:
                    rate = event.value.new_rate
        value += rate * (sim_engine.elapsed_time - previous_event_time)
        return value

    def __repr__(self):
        return f"{self._topic}, {self.get()}"