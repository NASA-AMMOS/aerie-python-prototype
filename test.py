import engine as sim
import model
import sim as facade


def test_something():
    run()

def run():
    def register_engine(engine):
        facade.sim_engine = engine
    spans, sim_events = sim.simulate(
        register_engine,
        model.Model,
        sim.Plan(
            [
                sim.Directive("my_other_activity", 10, {}),
                sim.Directive("my_activity", 20, {"param1": 5}),
                sim.Directive("my_decomposing_activity", 40, {}),
                sim.Directive("caller_activity", 50, {}),
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
        (50, "((x=100;x=99);x=98)"),
    ]

    assert spans == [
        (sim.Directive(type="my_other_activity", start_time=10, args={}), 10, 35),
        (sim.Directive(type="my_activity", start_time=20, args={"param1": 5}), 20, 35),
        (("my_child_activity", {}), 40, 41),
        (sim.Directive(type="my_decomposing_activity", start_time=40, args={}), 40, 41),
        (("callee_activity", {}), 50, 50),
        (sim.Directive(type="caller_activity", start_time=50, args={}), 50, 50),
    ]

    assert compute_profiles(model.Model(), sim_events) == {
        "x": [(0, 55), (20, 50), (25, 55), (30, 60), (35, 55), (40, 57), (41, 55), (50, 98)],
        "y": [(0, 0), (20, 0), (25, 0), (30, 10), (35, 3.0), (40, 13), (41, 10), (50, 10)],
        "z": [(0, 0), (20, 0), (25, 0), (30, 0), (35, 0), (40, 0), (41, 0), (50, 0)],
    }


def compute_profiles(model, sim_events):
    engine = sim.SimulationEngine()
    facade.sim_engine = engine
    engine.events = []
    engine.current_task_frame = sim.TaskFrame(history=engine.events)
    profiles = {}
    for attribute in model.attributes():
        profiles[attribute] = []
    for attribute in model.attributes():
        profiles[attribute].append((0, getattr(model, attribute).get()))
    for start_offset, event_graph in sim_events:
        engine.events.append((start_offset, event_graph))
        engine.current_task_frame = sim.TaskFrame(history=engine.events)
        for attribute in model.attributes():
            profiles[attribute].append((start_offset, getattr(model, attribute).get()))
    return profiles


if __name__ == "__main__":
    test_something()
