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
    all_events = list(sim_events)
    sim.globals_.events.clear()
    profiles = {}
    for attribute in model.attributes():
        profiles[attribute] = []
    for event in all_events:
        for attribute in model.attributes():
            profiles[attribute].append((event.start_time, getattr(model, attribute).get()))
        sim.globals_.events.append(event)

    assert spans == [
        (sim.Directive(type="my_other_activity", start_time=10, args={}), 10, 35),
        (sim.Directive(type="my_activity", start_time=20, args={"param1": 5}), 20, 35),
        (("my_other_activity", {}), 40, 41),
        (sim.Directive(type="my_decomposing_activity", start_time=40, args={}), 40, 41),
    ]

    assert profiles == {
        "x": [
            (20, 55),
            (25, 50),
            (30, 55),
            (30, 60),
            (35, 60),
            (35, 55),
            (35, 55),
            (40, 55),
            (40, 55),
            (40, 57),
            (41, 57),
            (41, 55),
            (41, 55),
        ],
        "y": [
            (20, 0),
            (25, 0),
            (30, 0),
            (30, 0),
            (35, 10),
            (35, 10),
            (35, 9),
            (40, 3.0),
            (40, 3.0),
            (40, 3.0),
            (41, 10),
            (41, 10),
            (41, 9),
        ],
        "z": [
            (20, 41),
            (25, 41),
            (30, 41),
            (30, 41),
            (35, 41),
            (35, 41),
            (35, 41),
            (40, 41),
            (40, 41),
            (40, 41),
            (41, 41),
            (41, 41),
            (41, 41),
        ],
    }


if __name__ == '__main__':
    test_something()