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
        return sim_engine.current_task_frame.read(self._topic, self.function)

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
        def condition(positive, at_earliest, at_latest):
            if (self.get() > other) == positive:
                return at_earliest
            return None

        return condition

    def is_equal_to(self, other):
        def condition(positive, at_earliest, at_latest):
            if (self.get() == other) == positive:
                return at_earliest
            return None

        return condition

    def __lt__(self, other):
        def condition(positive, at_earliest, at_latest):
            if (self.get() < other) == positive:
                return at_earliest
            return None

        return condition


class Accumulator:
    SetValue = namedtuple("Set", "new_value")
    SetRate = namedtuple("Set", "new_rate")

    def __init__(self, topic, value, rate):
        self._topic = topic
        self._initial_value = value
        self._initial_rate = rate

    def get(self):
        return sim_engine.current_task_frame.read(self._topic, self.get_value)

    def get_value_and_rate(self, history):
        value = self._initial_value
        rate = self._initial_rate
        previous_event_time = 0
        for start_offset, event_graph in history:
            for event in EventGraph.iter(event_graph):
                value += rate * (start_offset - previous_event_time)
                previous_event_time = start_offset
                if type(event.value) == Accumulator.SetValue:
                    value = event.value.new_value
                if type(event.value) == Accumulator.SetRate:
                    rate = event.value.new_rate
        value += rate * (sim_engine.elapsed_time - previous_event_time)
        return value, rate

    def get_value(self, history):
        value, rate = self.get_value_and_rate(history)
        return value

    def __ge__(self, other):
        def condition(positive, at_earliest, at_latest):
            value, rate = sim_engine.current_task_frame.read(self._topic, self.get_value_and_rate)
            time_until_intersection = int((other - value) / rate)
            if time_until_intersection > at_latest:
                return None
            if time_until_intersection < at_earliest:
                return at_earliest
            return time_until_intersection
        return condition

    def __repr__(self):
        return f"{self._topic}, {self.get()}"