import threading
import time

import engine as sim
import incremental_engine as incremental_sim
import model
import sim as facade
from protocol import Plan, Directive, hashable_directive

model_ = model


def test_baseline():
    run_baseline(sim)


def run_baseline(sim):
    def register_engine(engine):
        facade.sim_engine = engine

    spans, sim_events, _ = sim.simulate(
        register_engine,
        model.Model,
        Plan(
            [
                Directive("my_other_activity", 10, {}),
                Directive("my_activity", 20, {"param1": 5}),
                Directive("my_decomposing_activity", 40, {}),
                Directive("caller_activity", 50, {}),
            ]
        ),
    )

    assert [(x, sim.EventGraph.to_string(y)) for x, y in sim_events] == [
        (20, "x=50"),
        (25, "x=55"),
        (30, "x=60;y=10"),
        (35, "x=55;y=9;y=3.0"),
        (40, "x=55;(x=57|y=13)"),
        (41, "x=55|y=10"),
        (50, "x=100;x=99;x=98"),
    ]

    assert spans == [
        (Directive(type="my_other_activity", start_time=10, args={}), 10, 35),
        (Directive(type="my_activity", start_time=20, args={"param1": 5}), 20, 35),
        (Directive(type="my_child_activity", start_time=40, args={}), 40, 41),
        (Directive(type="my_decomposing_activity", start_time=40, args={}), 40, 41),
        (Directive(type="callee_activity", start_time=50, args={"value": 99}), 50, 50),
        (Directive(type="caller_activity", start_time=50, args={}), 50, 50),
    ]

    assert compute_profiles(model.Model(), sim_events) == {
        "x": [(0, 55), (20, 50), (25, 55), (30, 60), (35, 55), (40, 57), (41, 55), (50, 98)],
        "y": [(0, 0), (20, 0), (25, 0), (30, 10), (35, 3.0), (40, 13), (41, 10), (50, 10)],
        "z": [(0, 0), (20, 20), (25, 25), (30, 30), (35, 35), (40, 40), (41, 41), (50, 50)],
    }


def compute_profiles(model, sim_events):
    engine = sim.SimulationEngine()
    facade.sim_engine = engine
    engine.events = []
    engine.current_task_frame = sim.TaskFrame(engine.elapsed_time, history=engine.events)
    profiles = {}
    for attribute in model.attributes():
        profiles[attribute] = []
    for attribute in model.attributes():
        profiles[attribute].append((0, getattr(model, attribute).get()))
    for start_offset, event_graph in sim_events:
        engine.elapsed_time = start_offset
        engine.events.append((start_offset, event_graph))
        engine.current_task_frame = sim.TaskFrame(start_offset, history=engine.events)
        for attribute in model.attributes():
            profiles[attribute].append((start_offset, getattr(model, attribute).get()))
    return profiles


def test_incremental_baseline():
    run_baseline(incremental_sim)


def test_incremental():
    incremental_sim_test_case(
        Plan(
            [
                Directive("callee_activity", 10, {"value": 1}),
                Directive("callee_activity", 15, {"value": 2}),
            ]
        ),
        Plan(
            [
                Directive("callee_activity", 10, {"value": 1}),
                Directive("callee_activity", 15, {"value": 3}),  # Changed value only
            ]
        ),
        {"callee_activity": error_on_rerun("callee_activity", lambda args: args["value"] == 1)},
    )


def error_on_rerun(name, predicate=lambda kwargs: True):
    def foo(model, **kwargs):
        if predicate(kwargs):
            raise ValueError("Reran " + str(name))
        for x in incremental_sim.make_task(model_.Model(), name, kwargs):
            yield x

    return foo


def test_more_complex_add_only():
    incremental_sim_test_case(
        Plan(
            [
                Directive("my_other_activity", 10, {}),
                Directive("my_activity", 20, {"param1": 5}),
                Directive("caller_activity", 50, {}),
            ]
        ),
        Plan(
            [
                Directive("my_other_activity", 10, {}),
                Directive("my_activity", 20, {"param1": 5}),
                Directive("caller_activity", 50, {}),
                Directive("my_decomposing_activity", 60, {}),
            ]
        ),
        {x: error_on_rerun(x) for x in ("my_other_activity", "my_activity", "caller_activity")},
    )


def test_more_complex_remove_only():
    incremental_sim_test_case(
        Plan(
            [
                Directive("my_other_activity", 10, {}),
                Directive("my_activity", 20, {"param1": 5}),
                Directive("caller_activity", 50, {}),
            ]
        ),
        Plan(
            [
                Directive("my_other_activity", 10, {}),
                Directive("my_activity", 20, {"param1": 5}),
            ]
        ),
        {x: error_on_rerun(x) for x in ("my_other_activity", "my_activity", "caller_activity")},
    )


def test_with_reads():
    incremental_sim_test_case(
        Plan(
            [
                Directive("my_other_activity", 10, {}),
                Directive("my_activity", 20, {"param1": 4}),
                Directive("my_other_activity", 110, {}),
                Directive("my_activity", 120, {"param1": 5}),
            ]
        ),
        Plan(
            [
                Directive("my_other_activity", 10, {}),
                Directive("my_activity", 20, {"param1": 4}),
                Directive("my_other_activity", 110, {}),
                Directive("my_activity", 119, {"param1": 5}),
            ]
        ),
        {"my_activity": error_on_rerun("my_activity", lambda kwargs: kwargs["param1"] == 4)},
    )


def test_with_new_reads_of_old_topics():
    incremental_sim_test_case(
        Plan(
            [
                Directive("emit_event", 10, {"topic": "x", "value": 1, "_": 1}),
                Directive("read_topic", 15, {"topic": "x", "_": 1}),
            ]
        ),
        Plan(
            [
                Directive("emit_event", 10, {"topic": "x", "value": 1, "_": 1}),
                Directive("read_topic", 15, {"topic": "x", "_": 1}),
                Directive("read_topic", 16, {"topic": "x", "_": 2}),
            ]
        ),
        {"my_activity": error_on_rerun("my_activity", lambda kwargs: kwargs["param1"] == 4)},
    )


def test_with_reads_made_stale_dynamically():
    incremental_sim_test_case(
        Plan(
            [
                Directive("emit_event", 10, {"topic": "x", "value": 1, "_": 1}),
                Directive("read_topic", 15, {"topic": "x", "_": 1}),
            ]
        ),
        Plan(
            [
                Directive("emit_event", 10, {"topic": "x", "value": 1, "_": 1}),
                Directive("emit_event", 11, {"topic": "x", "value": 2, "_": 2}),
                Directive("read_topic", 15, {"topic": "x", "_": 1}),
            ]
        ),
        {},
    )


def test_with_reads_made_stale_dynamically_with_durative_activities():
    incremental_sim_test_case(
        Plan(
            [
                Directive("read_emit_three_times", 10, {"read_topic": "x", "emit_topic": "y", "delay": 5, "_": 1}),
                Directive("emit_event", 12, {"topic": "x", "value": 1, "_": 1}),
                Directive("read_topic", 30, {"topic": "y", "_": 1}),
            ]
        ),
        Plan(
            [
                Directive("read_emit_three_times", 10, {"read_topic": "x", "emit_topic": "y", "delay": 5, "_": 1}),
                Directive("emit_event", 12, {"topic": "x", "value": 1, "_": 1}),
                Directive("emit_event", 13, {"topic": "x", "value": 2, "_": 2}),
                Directive("read_topic", 30, {"topic": "y", "_": 1}),
            ]
        ),
        {"emit_event": error_on_rerun("emit_event", lambda args: args["_"] == 1)},
    )


def test_called_activity():
    """
    Parent -> child
    child reads x and delays for x

    Change plan with a new write to x

    Parent should emit its second event later
    """
    incremental_sim_test_case(
        Plan(
            [
                Directive("emit_event", 2, {"topic": "z", "value": 1, "_": 10}),
                Directive("parent_of_reading_child", 10, {}),
            ]
        ),
        Plan(
            [
                Directive("emit_event", 2, {"topic": "z", "value": 1, "_": 10}),
                Directive("emit_event", 5, {"topic": "x", "value": 1, "_": 1}),
                Directive("parent_of_reading_child", 10, {}),
            ]
        ),
        {"emit_event": error_on_rerun("emit_event", lambda kwargs: kwargs["_"] == 10)},
    )


def test_deleted_read():
    incremental_sim_test_case(
        Plan(
            [
                Directive("emit_event", 2, {"topic": "x", "value": 1, "_": 10}),
                Directive("emit_if_x_equal", 3, {"x_value": 1, "topic": "z", "value_to_emit": 1, "_": 10}),
                Directive("read_topic", 5, {"topic": "z", "_": 1}),
            ]
        ),
        Plan(
            [
                Directive("emit_event", 2, {"topic": "x", "value": 2, "_": 10}),
                Directive("emit_if_x_equal", 3, {"x_value": 1, "topic": "z", "value_to_emit": 1, "_": 10}),
                Directive("read_topic", 5, {"topic": "z", "_": 1}),
            ]
        ),
        {}
    )


def test_conditional_decomposition():
    incremental_sim_test_case(
        Plan(
            [
                Directive("emit_event", 2, {"topic": "x", "value": 1, "_": 1}),
                Directive("conditional_decomposition", 3, {"_": 2}),
            ]
        ),
        Plan(
            [
                Directive("emit_event", 2, {"topic": "x", "value": 2, "_": 1}),
                Directive("conditional_decomposition", 3, {"_": 2}),
            ]
        ),
        {"reading_child": error_on_rerun("reading_child")},
    )


def test_conditional_decomposition_2():
    incremental_sim_test_case(
        Plan(
            [
                Directive("emit_event", 2, {"topic": "x", "value": 1, "_": 1}),
                Directive("parent_of_conditional_decomposition", 3, {"_": 2}),
            ]
        ),
        Plan(
            [
                Directive("emit_event", 2, {"topic": "x", "value": 2, "_": 1}),
                Directive("parent_of_conditional_decomposition", 3, {"_": 2}),
            ]
        ),
        {},
    )


def test_conditional_decomposition_3():
    incremental_sim_test_case(
        Plan(
            [
                Directive("emit_event", 2, {"topic": "x", "value": 1, "_": 1}),
                Directive("parent_of_parent_of_conditional_decomposition", 3, {"_": 2}),
            ]
        ),
        Plan(
            [
                Directive("emit_event", 2, {"topic": "x", "value": 2, "_": 1}),
                Directive("parent_of_parent_of_conditional_decomposition", 3, {"_": 2}),
            ]
        ),
        {},
    )


def test_read_becomes_unstale():
    incremental_sim_test_case(
        Plan(
            [
                Directive("emit_event", 2, {"topic": "x", "value": 1, "_": 1}),
                Directive("emit_if_x_equal", 5, {"x_value": 1, "topic": "z", "value_to_emit": 1, "_": 3}),
            ]
        ),
        Plan(
            [
                Directive("emit_event", 2, {"topic": "x", "value": 1, "_": 1}),
                Directive("emit_event", 3, {"topic": "x", "value": 1, "_": 2}),
                Directive("emit_if_x_equal", 5, {"x_value": 1, "topic": "z", "value_to_emit": 1, "_": 3}),
            ]
        ),
        {"emit_if_x_equal": error_on_rerun("emit_if_x_equal")},
    )


def test_spawned_activity():
    """
    Parent -> child
    child reads x and delays for x

    Change plan with a new write to x

    Parent should emit its second event later
    """
    incremental_sim_test_case(
        Plan(
            [
                Directive("emit_event", 2, {"topic": "z", "value": 1, "_": 10}),
                Directive("spawns_reading_child", 10, {}),
            ]
        ),
        Plan(
            [
                Directive("emit_event", 2, {"topic": "z", "value": 1, "_": 10}),
                Directive("emit_event", 5, {"topic": "x", "value": 1, "_": 1}),
                Directive("spawns_reading_child", 10, {}),
            ]
        ),
        {
            "emit_event": error_on_rerun("emit_event", lambda kwargs: kwargs["_"] == 10),
            "spawns_reading_child": error_on_rerun("spawns_reading_child"),
        },
    )


def test_restart_task_with_earler_non_stale_read():
    incremental_sim_test_case(
        Plan(
            [
                Directive("emit_event", 7, {"topic": "x", "value": -1, "_": 3}),
                Directive(
                    "parent_of_read_emit_three_times_and_whoopee",
                    8,
                    {"read_topic": "x", "emit_topic": "history", "delay": 5, "_": 1},
                ),
            ]
        ),
        Plan(
            [
                Directive("emit_event", 7, {"topic": "x", "value": -1, "_": 3}),
                Directive(
                    "parent_of_read_emit_three_times_and_whoopee",
                    8,
                    {"read_topic": "x", "emit_topic": "history", "delay": 5, "_": 1},
                ),
                Directive("emit_event", 9, {"topic": "x", "value": 11, "_": 2}),
            ]
        ),
        {},
    )


# TODO: I suspect some edge cases with Delay(0) - e.g. spawn(); Delay(0); emit(). The emit should come causally after the first step of the spawn.
def test_baseline_delay_zero_between_spawns():
    def register_engine(engine):
        facade.sim_engine = engine

    spans, sim_events, _ = sim.simulate(
        register_engine,
        model.Model,
        Plan(
            [
                Directive("emit_event", 2, {"topic": "x", "value": 1, "_": 1}),
                Directive("delay_zero_between_spawns", 3, {"_": 2}),
            ]
        ),
    )

    assert [(x, sim.EventGraph.to_string(y)) for x, y in sim_events] == [
        (2, "x=1"),
        (3, "history=[(2, 'x=1')];history=[];history=[(2, 'x=1')];history=[]"),
    ]


def test_baseline_delay_zero_between_spawns_2():
    def register_engine(engine):
        facade.sim_engine = engine

    spans, sim_events, _ = sim.simulate(
        register_engine,
        model.Model,
        Plan(
            [
                Directive("emit_event", 2, {"topic": "x", "value": 2, "_": 1}),
                Directive("delay_zero_between_spawns", 3, {"_": 2}),
            ]
        ),
    )

    assert [(x, sim.EventGraph.to_string(y)) for x, y in sim_events] == [(2, "x=2"), (3, "u=2;u=2")]


# TODO: I suspect some edge cases with Delay(0) - e.g. spawn(); Delay(0); emit(). The emit should come causally after the first step of the spawn.
def test_delay_zero_between_spawns():
    incremental_sim_test_case(
        Plan(
            [
                Directive("emit_event", 2, {"topic": "x", "value": 1, "_": 1}),
                Directive("delay_zero_between_spawns", 3, {"_": 2}),
            ]
        ),
        Plan(
            [
                Directive("emit_event", 2, {"topic": "x", "value": 2, "_": 1}),
                Directive("delay_zero_between_spawns", 3, {"_": 2}),
            ]
        ),
        {},
    )


def incremental_sim_test_case(old_plan, new_plan, overrides):
    def register_engine(engine):
        facade.sim_engine = engine

    _, _, payload = incremental_sim.simulate(register_engine, model.Model, old_plan)

    expected_spans, expected_sim_events, _ = sim.simulate(register_engine, model.Model, new_plan)

    def register_engine_with_error_activity(engine):
        register_engine(engine)

        def spoofed_get_activity_types():
            activity_types = dict(model_.Model().get_activity_types())
            activity_types.update(overrides)
            return activity_types

        engine.model.get_activity_types = spoofed_get_activity_types

    actual_spans, actual_sim_events, _ = incremental_sim.simulate_incremental(
        register_engine_with_error_activity, model.Model, new_plan, old_plan, payload
    )

    actual = [(x, sim.EventGraph.to_string(y)) for x, y in actual_sim_events]
    expected = [(x, sim.EventGraph.to_string(y)) for x, y in expected_sim_events]
    print()
    print("Expected :", expected)
    print("Actual   :", actual)

    assert actual == expected
    assert set((hashable_directive(x), y, z) for x, y, z in actual_spans) == set((hashable_directive(x), y, z) for x, y, z in expected_spans)


if __name__ == "__main__":
    test_baseline()
