import sim
import model


def test_something():
    sim.initialize_mutable_globals()
    model.model = sim.register_model(model.Model)
    run(model.model)


def run(model):
    print("Init:", model.x, model.y)
    spans, sim_events = sim.simulate(
        sim.Plan(
            [
                sim.Directive("my_other_activity", 10, {}),
                sim.Directive("my_activity", 20, {"param1": 5}),
                sim.Directive("my_decomposing_activity", 40, {}),
            ]
        )
    )

    assert [(x, sim.EventGraph.to_string(y)) for x, y in sim_events] == [
        (20, "x=50"),
        (25, "x=55"),
        (30, "(x=60;y=10)"),
        (35, "(x=55;(y=9;y=3.0))"),
        (40, "(x=55;(y=13|x=57))"),
        (41, "(y=10|x=55)"),
    ]

    sim.globals_.events = []
    sim.globals_.current_task_frame = sim.TaskFrame(history=sim.globals_.events)
    profiles = {}
    for attribute in model.attributes():
        profiles[attribute] = []
    for attribute in model.attributes():
        profiles[attribute].append((0, getattr(model, attribute).get()))
    for start_offset, event_graph in sim_events:
        sim.globals_.events.append((start_offset, event_graph))
        sim.globals_.current_task_frame = sim.TaskFrame(history=sim.globals_.events)
        for attribute in model.attributes():
            profiles[attribute].append((start_offset, getattr(model, attribute).get()))

    assert spans == [
        (sim.Directive(type="my_other_activity", start_time=10, args={}), 10, 35),
        (sim.Directive(type="my_activity", start_time=20, args={"param1": 5}), 20, 35),
        (("my_child_activity", {}), 40, 41),
        (sim.Directive(type="my_decomposing_activity", start_time=40, args={}), 40, 41),
    ]

    assert profiles == {
        "x": [(0, 55), (20, 50), (25, 55), (30, 60), (35, 55), (40, 57), (41, 55)],
        "y": [(0, 0), (20, 0), (25, 0), (30, 10), (35, 3.0), (40, 13), (41, 10)],
        "z": [(0, 41), (20, 41), (25, 41), (30, 41), (35, 41), (40, 41), (41, 41)],
    }


if __name__ == "__main__":
    test_something()
