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
        (sim.Directive(type="my_activity", start_time=20, args={"param1": 5}), 20, 35),
        (sim.Directive(type="my_other_activity", start_time=10, args={}), 10, 35),
    ]

    assert profiles == {
        "x": [(20, 55), (25, 50), (30, 55), (30, 60), (35, 60), (35, 55), (35, 55)],
        "y": [(20, 0), (25, 0), (30, 0), (30, 0), (35, 10), (35, 10), (35, 9)],
        "z": [(20, 35), (25, 35), (30, 35), (30, 35), (35, 35), (35, 35), (35, 35)],
    }
