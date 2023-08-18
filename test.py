import sim
import model


Event = sim.Event
Empty = sim.EventGraph.Empty
Atom = sim.EventGraph.Atom
Sequentially = sim.EventGraph.Sequentially
Concurrently = sim.EventGraph.Concurrently


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

    assert sim_events == [
        (20, Atom(value=Event(topic="x", value=50))),
        (
            25,
            Sequentially(prefix=Atom(value=Event(topic="x", value=50)), suffix=Atom(value=Event(topic="x", value=55))),
        ),
        (
            30,
            Sequentially(
                prefix=Sequentially(
                    prefix=Sequentially(
                        prefix=Atom(value=Event(topic="x", value=50)), suffix=Atom(value=Event(topic="x", value=55))
                    ),
                    suffix=Atom(value=Event(topic="x", value=60)),
                ),
                suffix=Atom(value=Event(topic="y", value=10)),
            ),
        ),
        (
            35,
            Sequentially(
                prefix=Sequentially(
                    prefix=Sequentially(
                        prefix=Sequentially(
                            prefix=Sequentially(
                                prefix=Sequentially(
                                    prefix=Atom(value=Event(topic="x", value=50)),
                                    suffix=Atom(value=Event(topic="x", value=55)),
                                ),
                                suffix=Atom(value=Event(topic="x", value=60)),
                            ),
                            suffix=Atom(value=Event(topic="y", value=10)),
                        ),
                        suffix=Atom(value=Event(topic="x", value=55)),
                    ),
                    suffix=Atom(value=Event(topic="y", value=9)),
                ),
                suffix=Atom(value=Event(topic="y", value=3.0)),
            ),
        ),
        (
            40,
            Sequentially(
                prefix=Sequentially(
                    prefix=Sequentially(
                        prefix=Sequentially(
                            prefix=Sequentially(
                                prefix=Sequentially(
                                    prefix=Sequentially(
                                        prefix=Sequentially(
                                            prefix=Atom(value=Event(topic="x", value=50)),
                                            suffix=Atom(value=Event(topic="x", value=55)),
                                        ),
                                        suffix=Atom(value=Event(topic="x", value=60)),
                                    ),
                                    suffix=Atom(value=Event(topic="y", value=10)),
                                ),
                                suffix=Atom(value=Event(topic="x", value=55)),
                            ),
                            suffix=Atom(value=Event(topic="y", value=9)),
                        ),
                        suffix=Atom(value=Event(topic="y", value=3.0)),
                    ),
                    suffix=Atom(value=Event(topic="x", value=57)),
                ),
                suffix=Atom(value=Event(topic="y", value=10)),
            ),
        ),
        (
            41,
            Sequentially(
                prefix=Sequentially(
                    prefix=Sequentially(
                        prefix=Sequentially(
                            prefix=Sequentially(
                                prefix=Sequentially(
                                    prefix=Sequentially(
                                        prefix=Sequentially(
                                            prefix=Sequentially(
                                                prefix=Sequentially(
                                                    prefix=Sequentially(
                                                        prefix=Atom(value=Event(topic="x", value=50)),
                                                        suffix=Atom(value=Event(topic="x", value=55)),
                                                    ),
                                                    suffix=Atom(value=Event(topic="x", value=60)),
                                                ),
                                                suffix=Atom(value=Event(topic="y", value=10)),
                                            ),
                                            suffix=Atom(value=Event(topic="x", value=55)),
                                        ),
                                        suffix=Atom(value=Event(topic="y", value=9)),
                                    ),
                                    suffix=Atom(value=Event(topic="y", value=3.0)),
                                ),
                                suffix=Atom(value=Event(topic="x", value=57)),
                            ),
                            suffix=Atom(value=Event(topic="y", value=10)),
                        ),
                        suffix=Atom(value=Event(topic="x", value=55)),
                    ),
                    suffix=Atom(value=Event(topic="y", value=9)),
                ),
                suffix=Atom(value=Event(topic="y", value=3.0)),
            ),
        ),
    ]

    sim.globals_.events.clear()
    sim.globals_.current_task_frame.clear()
    profiles = {}
    for attribute in model.attributes():
        profiles[attribute] = []
    for start_offset, event_graph in sim_events:
        sim.globals_.current_task_frame.clear()
        tf = sim.TaskFrame()
        tf.event_graph = event_graph
        for event in tf:
            for attribute in model.attributes():
                # TODO: get the start time
                profiles[attribute].append((start_offset, getattr(model, attribute).get()))
            sim.globals_.current_task_frame.append(event)
        sim.globals_.events.append((None, event_graph))

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
            (25, 50),
            (30, 55),
            (30, 50),
            (30, 55),
            (30, 60),
            (35, 60),
            (35, 50),
            (35, 55),
            (35, 60),
            (35, 60),
            (35, 55),
            (35, 55),
            (40, 55),
            (40, 50),
            (40, 55),
            (40, 60),
            (40, 60),
            (40, 55),
            (40, 55),
            (40, 55),
            (40, 57),
            (41, 57),
            (41, 50),
            (41, 55),
            (41, 60),
            (41, 60),
            (41, 55),
            (41, 55),
            (41, 55),
            (41, 57),
            (41, 57),
            (41, 55),
            (41, 55),
        ],
        "y": [
            (20, 0),
            (25, 0),
            (25, 0),
            (30, 0),
            (30, 0),
            (30, 0),
            (30, 0),
            (35, 10),
            (35, 10),
            (35, 10),
            (35, 10),
            (35, 10),
            (35, 10),
            (35, 9),
            (40, 3.0),
            (40, 3.0),
            (40, 3.0),
            (40, 3.0),
            (40, 10),
            (40, 10),
            (40, 9),
            (40, 3.0),
            (40, 3.0),
            (41, 10),
            (41, 10),
            (41, 10),
            (41, 10),
            (41, 10),
            (41, 10),
            (41, 9),
            (41, 3.0),
            (41, 3.0),
            (41, 10),
            (41, 10),
            (41, 9),
        ],
        "z": [
            (20, 41),
            (25, 41),
            (25, 41),
            (30, 41),
            (30, 41),
            (30, 41),
            (30, 41),
            (35, 41),
            (35, 41),
            (35, 41),
            (35, 41),
            (35, 41),
            (35, 41),
            (35, 41),
            (40, 41),
            (40, 41),
            (40, 41),
            (40, 41),
            (40, 41),
            (40, 41),
            (40, 41),
            (40, 41),
            (40, 41),
            (41, 41),
            (41, 41),
            (41, 41),
            (41, 41),
            (41, 41),
            (41, 41),
            (41, 41),
            (41, 41),
            (41, 41),
            (41, 41),
            (41, 41),
            (41, 41),
        ],
    }

    # assert profiles == {
    #     "x": [
    #         (20, 55),
    #         (25, 50),
    #         (30, 55),
    #         (30, 60),
    #         (35, 60),
    #         (35, 55),
    #         (35, 55),
    #         (40, 55),
    #         (40, 55),
    #         (40, 57),
    #         (41, 57),
    #         (41, 55),
    #         (41, 55),
    #     ],
    #     "y": [
    #         (20, 0),
    #         (25, 0),
    #         (30, 0),
    #         (30, 0),
    #         (35, 10),
    #         (35, 10),
    #         (35, 9),
    #         (40, 3.0),
    #         (40, 3.0),
    #         (40, 3.0),
    #         (41, 10),
    #         (41, 10),
    #         (41, 9),
    #     ],
    #     "z": [
    #         (20, 41),
    #         (25, 41),
    #         (30, 41),
    #         (30, 41),
    #         (35, 41),
    #         (35, 41),
    #         (35, 41),
    #         (40, 41),
    #         (40, 41),
    #         (40, 41),
    #         (41, 41),
    #         (41, 41),
    #         (41, 41),
    #     ],
    # }


if __name__ == "__main__":
    test_something()
