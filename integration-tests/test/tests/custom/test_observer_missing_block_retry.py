import pytest

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


@pytest.fixture(scope="module")
def active_shard(provider, timeouts):
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


def test_observer_retries_missing_block_after_peer_returns(active_shard, timeouts) -> None:
    v1 = active_shard.node("validator1")
    poll_until(
        lambda: (count if (count := len(v1.get_blocks(15))) >= 10 else None),
        timeout=timeouts.finalization * 3,
        interval=2,
        description="source shard builds a non-trivial finalized history",
    )
    target = v1.last_finalized_block().blockInfo
    sources = list(active_shard.all_nodes)

    with active_shard.add_observer(
        cli_options={"--network-timeout": "2seconds"},
        wait_running=False,
    ) as observer:
        poll_until(
            lambda: (True if "LFS Block Requester stream initialized" in observer.logs() else None),
            timeout=timeouts.node_startup,
            interval=0.05,
            description="observer starts initial block retrieval",
        )

        try:
            for source in sources:
                source.pause()
            poll_until(
                lambda: (True if "request remains scheduled" in observer.logs() else None),
                timeout=15,
                interval=0.25,
                description="failed initial block request remains scheduled",
            )
        finally:
            for source in sources:
                source.unpause()

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
        observer_view = observer.get_block(target.blockHash)
        assert observer_view.blockInfo.postStateHash == target.postStateHash
