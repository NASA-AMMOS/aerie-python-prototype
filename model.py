from sim import RegisterCell, Accumulator, Delay, AwaitCondition, spawn


def my_activity(model: "Model", param1):
    model.x -= param1
    print(model.z)
    yield Delay(5)
    model.x += param1
    print(model.z)
    yield Delay(5)
    model.x += param1
    print(model.z)
    yield Delay(5)
    print(model.z)
    model.x -= param1


def my_other_activity(model: "Model"):
    print(model.x.get())
    yield AwaitCondition(model.x > 56)
    print(model.x.get())
    model.y = 10
    yield AwaitCondition(model.x < 56)
    print(model.x.get())
    model.y = 9
    model.y = model.y.get() / 3


def my_decomposing_activity(model: "Model"):
    if (model.x > 56):
        model.x = 55
    spawn(my_other_activity, {})
    model.x = 57
    yield Delay(1)
    model.x = 55
    yield AwaitCondition(model.y.is_equal_to(10))


class Model:
    def __init__(self):
        self.x = RegisterCell("x", 55)
        self.y = RegisterCell("y", 0)
        self.z = Accumulator("z", 0, 1)

        def __setattr__(self, key, value):
            getattr(self, key).set(value)

        Model.__setattr__ = __setattr__

    def attributes(self):
        return [x for x in self.__dict__ if not x.startswith("_")]

    def get_activity_types(self):
        activity_types = [
            my_activity,
            my_other_activity,
            my_decomposing_activity
        ]
        return {x.__name__: x for x in activity_types}
