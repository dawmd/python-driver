"""
End-to-end tests for TABLETS_ROUTING_V2 against a V2-capable Scylla build.

Unlike the unit tests in tests/unit/test_tablets.py and tests/unit/test_policies.py,
these tests cross the driver<->server boundary: they validate that the driver
negotiates the extension, parses the server's `tablets-routing-v2` payload with
the correct field layout, and that the tablet_version_block it sends actually
matches the server's encoding.

The whole module is opt-in: the server only advertises the extension when started
with the `strongly-consistent-tables` experimental feature, and it is exchanged on
the wire under the name `TABLETS_ROUTING_V2_EXPERIMENTAL`. When run against a
server that does not advertise it (e.g. a released Scylla), every test skips.
"""

import pytest

from cassandra.cluster import Cluster
from cassandra.policies import ConstantReconnectionPolicy, RoundRobinPolicy, TokenAwarePolicy

from tests.integration import PROTOCOL_VERSION, use_cluster


def setup_module(module):
    try:
        use_cluster('tablets_routing_v2', [3], start=True, set_keyspace=False,
                    configuration_options={
                        # `strongly-consistent-tables` is what gates the server's
                        # advertisement of TABLETS_ROUTING_V2_EXPERIMENTAL
                        # (see scylladb transport/controller.cc).
                        'experimental_features': ['lwt', 'udf', 'strongly-consistent-tables'],
                    })
    except Exception as exc:
        pytest.skip("Could not start a Scylla cluster with the "
                    "'strongly-consistent-tables' experimental feature: {}".format(exc),
                    allow_module_level=True)


class TestTabletsRoutingV2Integration:
    @classmethod
    def setup_class(cls):
        cls.cluster = Cluster(contact_points=["127.0.0.1", "127.0.0.2", "127.0.0.3"],
                              protocol_version=PROTOCOL_VERSION,
                              load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()),
                              reconnection_policy=ConstantReconnectionPolicy(1))
        cls.session = cls.cluster.connect()
        cls._create_schema(cls.session)

    @classmethod
    def teardown_class(cls):
        cls.cluster.shutdown()

    @classmethod
    def _create_schema(cls, session):
        session.execute("DROP KEYSPACE IF EXISTS test_v2")
        session.execute(
            """
            CREATE KEYSPACE test_v2
            WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}
            AND tablets = {'initial': 8}
            """)
        session.execute("CREATE TABLE test_v2.t (pk int PRIMARY KEY, v int)")
        prepared = session.prepare("INSERT INTO test_v2.t (pk, v) VALUES (?, ?)")
        for i in range(50):
            session.execute(prepared.bind((i, i)))

    # -- helpers ----------------------------------------------------------------

    def _v2_negotiated(self):
        return bool(self.session.cluster.control_connection._tablets_routing_v2)

    def _skip_if_no_v2(self):
        if not self._v2_negotiated():
            pytest.skip("Server did not advertise TABLETS_ROUTING_V2_EXPERIMENTAL; "
                        "needs a build started with the 'strongly-consistent-tables' feature")

    def _cached_tablet(self, bound):
        md = self.session.cluster.metadata
        token = md.token_map.token_class.from_key(bound.routing_key)
        tablet = md._tablets.get_tablet_for_key(bound.keyspace, bound.table, token)
        return tablet, token

    def _ensure_cached(self, bound, attempts=30):
        """
        Drive requests until the V2 routing cache is populated for `bound`.

        On a cold start the driver sends a *random* tablet_version_block, which
        only matches the server ~1/16 of the time; on a mismatch the server
        returns routing info and the cache is filled. We retry until that happens.
        """
        for _ in range(attempts):
            self.session.execute(bound)
            tablet, _token = self._cached_tablet(bound)
            if tablet is not None and tablet.tablet_version is not None:
                return tablet
        raise AssertionError("V2 routing cache was never populated; the server "
                             "never returned a 'tablets-routing-v2' payload")

    # -- tests ------------------------------------------------------------------

    def test_v2_is_negotiated(self):
        self._skip_if_no_v2()
        # Every per-host pool must also have negotiated V2 (it gates the request byte).
        for pool in self.session._pools.values():
            assert getattr(pool, 'tablets_routing_v2', False) is True

    def test_v2_payload_populates_cache_with_valid_fields(self):
        """Regression guard for the payload tuple field order (bug #1)."""
        self._skip_if_no_v2()

        select = self.session.prepare("SELECT v FROM test_v2.t WHERE pk = ?")
        bound = select.bind([2])

        tablet = self._ensure_cached(bound)
        _, token = self._cached_tablet(bound)

        # If the tuple were decoded in the wrong order, first_token/last_token
        # would actually carry the version / replica list and these invariants
        # would not hold.
        assert tablet.tablet_version is not None
        assert tablet.first_token <= tablet.last_token
        # get_tablet_for_key matches first_token < token <= last_token.
        assert tablet.first_token < token.value <= tablet.last_token

        # Replicas must be real hosts known to the cluster with sane shard ids.
        known_host_ids = {h.host_id for h in self.session.cluster.metadata.all_hosts()}
        assert tablet.replicas, "tablet has no replicas"
        for host_id, shard in tablet.replicas:
            assert host_id in known_host_ids, \
                "replica host_id {} is not a known host (corrupt payload?)".format(host_id)
            assert isinstance(shard, int) and shard >= 0

    def test_matching_block_yields_no_payload(self):
        """Regression guard for the tablet_version_block encoding (bug #2)."""
        self._skip_if_no_v2()

        select = self.session.prepare("SELECT v FROM test_v2.t WHERE pk = ?")
        bound = select.bind([7])

        # Populate the cache so the driver knows the current tablet_version.
        self._ensure_cached(bound)

        # The next request carries a block derived from the cached version. If the
        # driver's encoding agrees with the server, the versions match and NO
        # routing payload is returned. A wrong shift would mismatch and the server
        # would keep returning routing info.
        result = self.session.execute(bound)
        assert result.one() is not None
        payload = result.response_future.custom_payload
        assert not (payload and 'tablets-routing-v2' in payload), (
            "Server returned routing info despite a cached, up-to-date "
            "tablet_version; the driver's tablet_version_block encoding likely "
            "disagrees with the server (locator::compare_tablet_version_block)")
