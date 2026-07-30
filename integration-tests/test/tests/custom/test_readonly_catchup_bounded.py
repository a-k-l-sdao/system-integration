import threading
import time

import pytest
import requests

from ...infra.config import ShardConfig
from ...infra.keys import VALIDATOR1_ID, VALIDATOR2_ID, VALIDATOR3_ID
from ...infra.polling import (
    poll_until,
    wait_for_block_visible,
    wait_for_lfb_at_least,
    wait_for_node_running,
)
from ...infra.shard import Shard

pytestmark = pytest.mark.xdist_group("custom")

_MIN_DAG_DEPTH = 40
_OBSERVER_MEMORY_CEILING_MB = 1500


@pytest.fixture(scope="module")
def deep_shard(provider, timeouts):
    config = ShardConfig(
        bonds=[
            (VALIDATOR1_ID, 100),
            (VALIDATOR2_ID, 100),
            (VALIDATOR3_ID, 100),
        ],
        heartbeat=True,
        global_cli_options={
            "--heartbeat-check-interval": "1second",
            "--heartbeat-max-lfb-age": "1second",
        },
    )
    shard = Shard.create(provider, config, timeouts)
    yield shard
    shard.destroy()


def test_readonly_catchup_parallelism_keeps_api_responsive(deep_shard, timeouts) -> None:
    source = deep_shard.node("validator1")
    poll_until(
        lambda: (
            blocks if len(blocks := source.get_blocks(_MIN_DAG_DEPTH)) >= _MIN_DAG_DEPTH else None
        ),
        timeout=timeouts.finalization * 4,
        interval=2,
        description=f"source DAG reaches {_MIN_DAG_DEPTH} blocks",
    )
    target = source.last_finalized_block().blockInfo

    with deep_shard.add_observer(wait_running=False) as observer:
        stop = threading.Event()
        latencies = []
        memory_samples = []

        def probe_status() -> None:
            url = f"{observer.http_url}/api/status"
            while not stop.is_set():
                started = time.monotonic()
                try:
                    response = requests.get(url, timeout=2)
                    if response.status_code == 200:
                        latencies.append(time.monotonic() - started)
                except requests.RequestException:
                    pass
                stop.wait(0.1)

        def sample_memory() -> None:
            while not stop.is_set():
                usage = observer.resource_usage()
                memory = float(usage.get("memory_mb", 0) or 0)
                if memory > 0:
                    memory_samples.append(memory)
                stop.wait(0.5)

        threads = [
            threading.Thread(target=probe_status, daemon=True),
            threading.Thread(target=sample_memory, daemon=True),
        ]
        for thread in threads:
            thread.start()

        try:
            wait_for_node_running(
                get_logs=observer.logs,
                is_running=observer.is_running,
                node_name=observer.name,
                timeout=timeouts.node_startup,
                status_url=f"{observer.http_url}/api/status",
            )
            wait_for_lfb_at_least(
                observer,
                target.blockNumber,
                timeout=timeouts.node_startup,
            )
            wait_for_block_visible(
                observer,
                target.blockHash,
                timeout=timeouts.deploy_inclusion * 3,
            )
        finally:
            stop.set()
            for thread in threads:
                thread.join(timeout=5)

        assert len(latencies) >= 5, f"only {len(latencies)} status probes succeeded"
        assert max(latencies) < 2
        assert memory_samples, "observer resource usage was never available"
        assert max(memory_samples) < _OBSERVER_MEMORY_CEILING_MB

        observer_view = observer.get_block(target.blockHash)
        assert observer_view.blockInfo.postStateHash == target.postStateHash
        response = observer.api_post(
            "/explore-deploy",
            {"term": "new x in { x!(1) }"},
            timeout=15,
        )
        assert response.status_code == 200
