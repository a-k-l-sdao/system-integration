from concurrent.futures import ThreadPoolExecutor

import pytest

from ...infra.assertions import assert_all_deploys_finalized_on_all_nodes
from ...infra.config import ShardConfig
from ...infra.keys import VALIDATOR1_ID, VALIDATOR2_ID, VALIDATOR3_ID
from ...infra.polling import (
    wait_for_lfb_at_least,
    wait_for_lfb_converged,
    wait_for_node_quiet,
)
from ...infra.shard import Shard

pytestmark = pytest.mark.xdist_group("custom")

_DEPLOY_COUNT = 16


@pytest.fixture(scope="module")
def notification_shard(provider, timeouts):
    config = ShardConfig(
        bonds=[
            (VALIDATOR1_ID, 100),
            (VALIDATOR2_ID, 100),
            (VALIDATOR3_ID, 100),
        ],
        ftt=0.1,
        heartbeat=True,
        global_cli_options={"--network-timeout": "15seconds"},
    )
    shard = Shard.create(provider, config, timeouts)
    yield shard
    shard.destroy()


def test_slow_peer_does_not_block_block_processing(notification_shard, timeouts) -> None:
    v1 = notification_shard.node("validator1")
    v2 = notification_shard.node("validator2")
    slow_peer = notification_shard.node("validator3")
    active = [v1, v2]
    baseline = v1.last_finalized_block().blockInfo.blockNumber

    slow_peer.pause()
    try:
        wait_for_node_quiet(slow_peer, timeout=10)

        def submit(index: int) -> str:
            node = active[index % len(active)]
            key = VALIDATOR1_ID.private_key() if node is v1 else VALIDATOR2_ID.private_key()
            return node.deploy_string(
                f'@"slow-notification-{index}"!({index})',
                key,
            )

        with ThreadPoolExecutor(max_workers=8) as executor:
            deploy_ids = list(executor.map(submit, range(_DEPLOY_COUNT)))

        assert_all_deploys_finalized_on_all_nodes(
            active,
            deploy_ids,
            timeouts.finalization * 3,
            label="slow-peer-notification",
        )
        wait_for_lfb_at_least(
            v1,
            baseline + 3,
            timeout=timeouts.finalization * 3,
        )
        wait_for_lfb_at_least(
            v2,
            baseline + 3,
            timeout=timeouts.finalization * 3,
        )
        assert v1.api_get("/status")["isReady"] is True
        assert v2.api_get("/status")["isReady"] is True
    finally:
        slow_peer.unpause()

    wait_for_lfb_converged(
        notification_shard.all_nodes,
        timeout=timeouts.finalization * 4,
        min_height=baseline + 3,
        max_spread=5,
        description="slow peer catches up after block-notification interruption",
    )
