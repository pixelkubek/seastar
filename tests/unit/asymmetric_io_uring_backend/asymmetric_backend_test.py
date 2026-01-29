import pytest
from yaml import safe_load
from pathlib import Path
from syscalls import parse_cpu_set
from io_uring_assertions import (
    EXPECTED_IO_URING_SYSCALLS,
    assert_expected_io_uring_register_calls,
)


class SeastarCommandBuilder:
    smp: str | None
    async_workers_cpuset: str | None
    cpuset: str | None
    overprovisioned: bool

    def __init__(self):
        self.smp = None
        self.async_workers_cpuset = None
        self.cpuset = None
        self.overprovisioned = False

    def with_smp(self, smp: str) -> "SeastarCommandBuilder":
        self.smp = smp
        return self

    def with_async_workers_cpuset(self, cpuset: str) -> "SeastarCommandBuilder":
        self.async_workers_cpuset = cpuset
        return self

    def with_cpuset(self, cpuset: str) -> "SeastarCommandBuilder":
        self.cpuset = cpuset
        return self

    def with_overprovisioned(self) -> "SeastarCommandBuilder":
        self.overprovisioned = True
        return self

    def build(self, seastar_path: Path) -> list[str]:
        args = [str(seastar_path)]
        if self.smp is not None:
            args += ["--smp", self.smp]
        if self.async_workers_cpuset is not None:
            args += ["--async-workers-cpuset", self.async_workers_cpuset]
        if self.cpuset is not None:
            args += ["--cpuset", self.cpuset]
        if self.overprovisioned:
            args += ["--overprovisioned"]
        return args


def assert_shared_cpus_warning(cpuset: str, async_workers_cpuset: str, std_out: str):
    cpus_for_shards = parse_cpu_set(cpuset)
    cpus_for_async_workers = parse_cpu_set(async_workers_cpuset)
    shared_cpus = cpus_for_shards.intersection(cpus_for_async_workers)
    if not shared_cpus:
        raise AssertionError(
            "No shared CPUs found between cpuset and async_workers_cpuset. Test setup error."
        )

    expected_warning = f"The following CPUs assigned to shards overlap with the async workers cpuset: {','.join(str(cpu) for cpu in shared_cpus)}. This may lead to performance degradation. It is recommended to keep the main cpuset and async workers cpuset disjoint."
    assert expected_warning in std_out, (
        f"Expected warning about shared CPUs not found in output:\n{std_out}"
    )


def assert_shard_cpu_distribution(
    expected_cpuset: dict[int, int], mapping: list[dict[str, int]]
):
    available_cpus = expected_cpuset.copy()
    for shard_info in mapping:
        shard_cpu = shard_info["cpu_id"]
        assert shard_cpu in expected_cpuset, (
            f"Shard assigned to CPU {shard_cpu}, which is not in allowed cpuset {expected_cpuset}."
        )

        if available_cpus[shard_cpu] == 0:
            raise AssertionError(
                f"CPU {shard_cpu} assigned more than {expected_cpuset[shard_cpu]} times to shards."
            )
        available_cpus[shard_cpu] -= 1

    for cpu_id, remaining in available_cpus.items():
        assert (
            remaining == 0
        ), f"CPU {cpu_id} was expected to be assigned {expected_cpuset[cpu_id]} times, but was assigned only {expected_cpuset[cpu_id] - remaining} times."


def test_on_no_async_workers_cpuset_should_exit(run_process, seastar_path: Path):
    _, std_err, rc = run_process(
        SeastarCommandBuilder().with_smp("2").build(seastar_path)
    )

    assert rc == 1, f"Expected return code 1, got {rc}."
    assert (
        "No CPUs specified for asymmetric_io_uring workers. Please see --async-workers-cpuset option."
        in std_err
    ), f"Unexpected seastar output:\n{std_err}"


@pytest.mark.parametrize(
    "cpuset,async_workers_cpuset,expected_shard_cpus,expected_workers_cpus",
    [
        ("0,1", "0", {0: 1, 1: 1}, "0"),
        ("0,1", "1", {0: 1, 1: 1}, "1"),
        ("0,1", "0,1", {0: 1, 1: 1}, "0,1"),
    ],
)
def test_on_overlapping_cpuset_and_async_workers_cpuset_should_work_fine_but_warn(
    run_process_with_strace,
    seastar_path: Path,
    parse_syscalls,
    cpuset: str,
    async_workers_cpuset: str,
    expected_shard_cpus: dict[int, int],
    expected_workers_cpus: str,
):
    std_out, std_err, rc, files = run_process_with_strace(
        SeastarCommandBuilder()
        .with_cpuset(cpuset)
        .with_async_workers_cpuset(async_workers_cpuset)
        .build(seastar_path),
        syscalls=EXPECTED_IO_URING_SYSCALLS,
    )

    assert rc == 0, f"Expected return code 0, got {rc}."
    assert_shared_cpus_warning(cpuset, async_workers_cpuset, std_err)

    mapping = safe_load(std_out)
    assert_shard_cpu_distribution(expected_shard_cpus, mapping)
    assert_expected_io_uring_register_calls(
        parse_syscalls, mapping, files, parse_cpu_set(expected_workers_cpus)
    )


@pytest.mark.parametrize(
    "cpuset,smp,async_workers_cpuset,expected_shard_cpus,expected_workers_cpus",
    [
        ("0,1", "1", "1", {0: 1}, "1"),
        ("0,1", "1", "1", {0: 1}, "1"),
        ("0,1", "1", "0", {1: 1}, "0"),
        ("0,1", "1", "0", {1: 1}, "0"),
        # TODO: what if task_set==async_workers_cpuset? then cpuset for the app will be empty?
        # TODO: what if smp> task_set - async_workers_cpuset? then cpuset for the app will be empty?
    ],
)
def test_on_without_cpuset_and_with_async_workers_cpuset_should_remove_cpus_from_cpuset(
    run_process_with_strace,
    seastar_path: Path,
    parse_syscalls,
    cpuset: str,
    smp: str,
    async_workers_cpuset: str,
    expected_shard_cpus: dict[int, int],
    expected_workers_cpus: str,
):
    seastar_args = (
        SeastarCommandBuilder()
        .with_smp(smp)
        .with_async_workers_cpuset(async_workers_cpuset)
        .build(seastar_path)
    )
    taskset_args = ["taskset", "-c", cpuset] + seastar_args
    std_out, _, rc, files = run_process_with_strace(
        taskset_args, syscalls=EXPECTED_IO_URING_SYSCALLS
    )

    assert rc == 0, f"Expected return code 0, got {rc}."

    mapping = safe_load(std_out)
    assert_shard_cpu_distribution(expected_shard_cpus, mapping)
    assert_expected_io_uring_register_calls(
        parse_syscalls, mapping, files, parse_cpu_set(expected_workers_cpus)
    )


@pytest.mark.parametrize(
    "cpuset,smp,async_workers_cpuset,expected_shard_cpus,expected_workers_cpus",
    [
        ("0,1", "1", "0", {1: 1}, "0"),
        ("0,1", "2", "0", {1: 2}, "0"),
        ("0,1", "3", "0", {1: 3}, "0"),
        ("0,1", "4", "0", {1: 4}, "0"),
        ("0,1", "1", "1", {0: 1}, "1"),
        ("0,1", "2", "1", {0: 2}, "1"),
        ("0,1", "3", "1", {0: 3}, "1"),
        ("0,1", "4", "1", {0: 4}, "1"),
    ],
)
def test_on_overprovisioned_and_smp_should_exclude_cpus_from_current_cpuset(
    run_process_with_strace,
    seastar_path: Path,
    parse_syscalls,
    cpuset: str,
    smp: str,
    async_workers_cpuset: str,
    expected_shard_cpus: dict[int, int],
    expected_workers_cpus: str,
):
    seastar_args = (
        SeastarCommandBuilder()
        .with_smp(smp)
        .with_async_workers_cpuset(async_workers_cpuset)
        .with_overprovisioned()
        .build(seastar_path)
    )
    taskset_args = ["taskset", "-c", cpuset] + seastar_args
    std_out, _, rc, files = run_process_with_strace(
        taskset_args, syscalls=EXPECTED_IO_URING_SYSCALLS
    )

    assert rc == 0, f"Expected return code 0, got {rc}."

    mapping = safe_load(std_out)
    assert_shard_cpu_distribution(expected_shard_cpus, mapping)
    assert_expected_io_uring_register_calls(
        parse_syscalls, mapping, files, parse_cpu_set(expected_workers_cpus)
    )
