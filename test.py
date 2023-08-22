import pytest

import engine as sim
import incremental_engine as incremental_sim
import model
import sim as facade
from protocol import Plan, Directive

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
        (40, "x=55;(y=13|x=57)"),
        (41, "y=10|x=55"),
        (50, "x=100;x=99;x=98"),
    ]

    assert spans == [
        (Directive(type="my_other_activity", start_time=10, args={}), 10, 35),
        (Directive(type="my_activity", start_time=20, args={"param1": 5}), 20, 35),
        (("my_child_activity", {}), 40, 41),
        (Directive(type="my_decomposing_activity", start_time=40, args={}), 40, 41),
        (("callee_activity", {"value": 99}), 50, 50),
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
    def error_on_rerun_callee_activity(model: "Model", value):
        if value == 1:
            raise Exception("Incremental simulation reran callee_activity with value " + str(value))
        return model_.callee_activity(model, value)

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
        {"callee_activity": error_on_rerun_callee_activity},
    )


def error_on_rerun(name):
    def foo(*args, **kwargs):
        raise ValueError("Reran " + str(name))

    return foo


def test_incremental_more_complex_add_only():
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


def test_incremental_more_complex_remove_only():
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


def test_incremental_with_reads():
    real_my_activity = model_.my_activity

    def fake_my_activity(model, param1):
        if param1 == 4:
            raise ValueError("Resimulated unchanged activity!")
        for task_status in real_my_activity(model, param1):
            yield task_status

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
        {"my_activity": fake_my_activity},
    )


def test_incremental_with_new_reads_of_old_topics():
    real_my_activity = model_.my_activity

    def fake_my_activity(model, param1):
        if param1 == 4:
            raise ValueError("Resimulated unchanged activity!")
        for task_status in real_my_activity(model, param1):
            yield task_status

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
        {"my_activity": fake_my_activity},
    )


def test_incremental_with_reads_made_stale_dynamically():
    real_my_activity = model_.my_activity

    def fake_my_activity(model, param1):
        if param1 == 4:
            raise ValueError("Resimulated unchanged activity!")
        for task_status in real_my_activity(model, param1):
            yield task_status

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
        {"my_activity": fake_my_activity},
    )


def test_incremental_with_reads_made_stale_dynamically_with_durative_activities():
    overrides = {}

    def fake_emit_event(model, **kwargs):
        if kwargs["_"] == 1:
            raise ValueError("Resimulated unchanged activity!")
        return model_.emit_event(model, **kwargs)

    overrides["emit_event"] = fake_emit_event

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
        overrides,
    )

def test_incremental_child_activity():
    """
    Parent -> child
    child reads x and delays for x

    Change plan with a new write to x

    Parent should emit its second event later
    """
    incremental_sim_test_case(
        Plan(
            [
                Directive("parent_of_reading_child", 10, {}),
            ]
        ),
        Plan(
            [
                Directive("emit_event", 5, {"topic": "x", "value": 1, "_": 1}),
                Directive("parent_of_reading_child", 10, {}),
            ]
        ),
        {}
    )


def incremental_sim_test_case(old_plan, new_plan, overrides):
    def register_engine(engine):
        facade.sim_engine = engine

    _, _, payload = incremental_sim.simulate(register_engine, model.Model, old_plan)

    expected_spans, expected_sim_events, _ = sim.simulate(register_engine, model.Model, new_plan)

    def register_engine_with_error_activity(engine):
        register_engine(engine)
        for name, impl in overrides.items():
            engine.activity_types_by_name[name] = impl

    actual_spans, actual_sim_events, _ = incremental_sim.simulate_incremental(
        register_engine_with_error_activity, model.Model, new_plan, old_plan, payload
    )

    assert [(x, sim.EventGraph.to_string(y)) for x, y in actual_sim_events] == [
        (x, sim.EventGraph.to_string(y)) for x, y in expected_sim_events
    ]
    assert actual_spans == expected_spans


if __name__ == "__main__":
    test_baseline()
