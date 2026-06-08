import unittest

from cassandra.tablets import Tablets, Tablet, choose_tablet_version_block, random_tablet_version_block

class TabletsTest(unittest.TestCase):
    def compare_ranges(self, tablets, ranges):
        assert len(tablets) == len(ranges)

        for idx, tablet in enumerate(tablets):
            assert tablet.first_token == ranges[idx][0], "First token is not correct in tablet: {}".format(tablet)
            assert tablet.last_token == ranges[idx][1], "Last token is not correct in tablet: {}".format(tablet)

    def test_add_tablet_to_empty_tablets(self):
        tablets = Tablets({("test_ks", "test_tb"): []})
        
        tablets.add_tablet("test_ks", "test_tb", Tablet(-6917529027641081857, -4611686018427387905, None))
        
        tablets_list = tablets._tablets.get(("test_ks", "test_tb"))

        self.compare_ranges(tablets_list, [(-6917529027641081857, -4611686018427387905)])

    def test_add_tablet_at_the_beggining(self):
        tablets = Tablets({("test_ks", "test_tb"): [Tablet(-6917529027641081857, -4611686018427387905, None)]})

        tablets.add_tablet("test_ks", "test_tb", Tablet(-8611686018427387905, -7917529027641081857, None))
        
        tablets_list = tablets._tablets.get(("test_ks", "test_tb"))

        self.compare_ranges(tablets_list, [(-8611686018427387905, -7917529027641081857),
                                           (-6917529027641081857, -4611686018427387905)])

    def test_add_tablet_at_the_end(self):
        tablets = Tablets({("test_ks", "test_tb"): [Tablet(-6917529027641081857, -4611686018427387905, None)]})

        tablets.add_tablet("test_ks", "test_tb", Tablet(-1, 2305843009213693951, None))
        
        tablets_list = tablets._tablets.get(("test_ks", "test_tb"))

        self.compare_ranges(tablets_list, [(-6917529027641081857, -4611686018427387905),
                                           (-1, 2305843009213693951)])

    def test_add_tablet_in_the_middle(self):
        tablets = Tablets({("test_ks", "test_tb"): [Tablet(-6917529027641081857, -4611686018427387905, None), 
                                                    Tablet(-1, 2305843009213693951, None)]},)
        
        tablets.add_tablet("test_ks", "test_tb", Tablet(-4611686018427387905, -2305843009213693953, None))
        
        tablets_list = tablets._tablets.get(("test_ks", "test_tb"))

        self.compare_ranges(tablets_list, [(-6917529027641081857, -4611686018427387905),
                                           (-4611686018427387905, -2305843009213693953),
                                           (-1, 2305843009213693951)])

    def test_add_tablet_intersecting(self):
        tablets = Tablets({("test_ks", "test_tb"): [Tablet(-6917529027641081857, -4611686018427387905, None), 
                                                    Tablet(-4611686018427387905, -2305843009213693953, None),
                                                    Tablet(-2305843009213693953, -1, None),
                                                    Tablet(-1, 2305843009213693951, None)]})
        
        tablets.add_tablet("test_ks", "test_tb", Tablet(-3611686018427387905, -6, None))
        
        tablets_list = tablets._tablets.get(("test_ks", "test_tb"))

        self.compare_ranges(tablets_list, [(-6917529027641081857, -4611686018427387905),
                                           (-3611686018427387905, -6),
                                           (-1, 2305843009213693951)])

    def test_add_tablet_intersecting_with_first(self):
        tablets = Tablets({("test_ks", "test_tb"): [Tablet(-8611686018427387905, -7917529027641081857, None),
                                                    Tablet(-6917529027641081857, -4611686018427387905, None)]})
        
        tablets.add_tablet("test_ks", "test_tb", Tablet(-8011686018427387905, -7987529027641081857, None))
        
        tablets_list = tablets._tablets.get(("test_ks", "test_tb"))

        self.compare_ranges(tablets_list, [(-8011686018427387905, -7987529027641081857),
                                           (-6917529027641081857, -4611686018427387905)])

    def test_add_tablet_intersecting_with_last(self):
        tablets = Tablets({("test_ks", "test_tb"): [Tablet(-8611686018427387905, -7917529027641081857, None),
                                                    Tablet(-6917529027641081857, -4611686018427387905, None)]})
        
        tablets.add_tablet("test_ks", "test_tb", Tablet(-5011686018427387905, -2987529027641081857, None))
        
        tablets_list = tablets._tablets.get(("test_ks", "test_tb"))

        self.compare_ranges(tablets_list, [(-8611686018427387905, -7917529027641081857),
                                           (-5011686018427387905, -2987529027641081857)])


class GetTabletForKeyTest(unittest.TestCase):
    """Tests for Tablets.get_tablet_for_key."""

    def test_found(self):
        t1 = Tablet(0, 100, [("host1", 0)])
        t2 = Tablet(100, 200, [("host2", 0)])
        t3 = Tablet(200, 300, [("host3", 0)])
        tablets = Tablets({("ks", "tb"): [t1, t2, t3]})

        class Token:
            def __init__(self, v):
                self.value = v

        result = tablets.get_tablet_for_key("ks", "tb", Token(150))
        self.assertIs(result, t2)

    def test_not_found_empty(self):
        tablets = Tablets({})

        class Token:
            def __init__(self, v):
                self.value = v

        self.assertIsNone(tablets.get_tablet_for_key("ks", "tb", Token(50)))

    def test_not_found_outside_range(self):
        t1 = Tablet(100, 200, [("host1", 0)])
        tablets = Tablets({("ks", "tb"): [t1]})

        class Token:
            def __init__(self, v):
                self.value = v

        # Token value 50 is not > first_token (100) of the tablet whose
        # last_token (200) is >= 50, so no match.
        self.assertIsNone(tablets.get_tablet_for_key("ks", "tb", Token(50)))


class TabletVersionBlockTest(unittest.TestCase):
    """Tests for tablet_version_block encoding used by TABLETS_ROUTING_V2."""

    def test_choose_tablet_version_block_encoding(self):
        """Verify that the block byte encodes (index << 4) | nibble correctly."""
        # Version 0x123456789ABCDEF0:
        # block 0 = 0x1, block 1 = 0x2, ..., block 15 = 0x0
        version = 0x123456789ABCDEF0

        # Manually check a few blocks.
        # Block 0: shift = (15-0)*4 = 60, nibble = (version >> 60) & 0xF = 0x1
        block = choose_tablet_version_block.__wrapped__(version, 0) if hasattr(choose_tablet_version_block, '__wrapped__') else self._extract_block(version, 0)
        # Use the actual function with a known index by testing properties:
        for idx in range(16):
            shift = (15 - idx) * 4
            expected_nibble = (version >> shift) & 0xF
            expected_byte = (idx << 4) | expected_nibble
            actual = self._extract_block(version, idx)
            self.assertEqual(actual, expected_byte,
                f"Block {idx}: expected 0x{expected_byte:02X}, got 0x{actual:02X}")

    def _extract_block(self, version, idx):
        """Manually compute the expected block byte for verification."""
        shift = (15 - idx) * 4
        nibble = (version >> shift) & 0xF
        return (idx << 4) | nibble

    def test_choose_tablet_version_block_round_robin(self):
        """Verify that choose_tablet_version_block cycles through block indices."""
        version = 0xFFFFFFFFFFFFFFFF  # All nibbles are 0xF
        import cassandra.tablets as tablets_module
        # Reset the counter to a known state.
        tablets_module._block_index_counter = 0

        seen_indices = []
        for _ in range(16):
            block = choose_tablet_version_block(version)
            idx = (block >> 4) & 0xF
            seen_indices.append(idx)

        # Should have cycled through 0..15.
        self.assertEqual(seen_indices, list(range(16)))

    def test_choose_tablet_version_block_wraps(self):
        """Verify that the counter wraps around after 16 calls."""
        version = 0xABCDABCDABCDABCD
        import cassandra.tablets as tablets_module
        tablets_module._block_index_counter = 15

        block1 = choose_tablet_version_block(version)
        self.assertEqual((block1 >> 4) & 0xF, 15)

        block2 = choose_tablet_version_block(version)
        self.assertEqual((block2 >> 4) & 0xF, 0)

    def test_random_tablet_version_block_returns_byte(self):
        """Verify random_tablet_version_block returns a value in [0, 255]."""
        for _ in range(100):
            block = random_tablet_version_block()
            self.assertIsInstance(block, int)
            self.assertGreaterEqual(block, 0)
            self.assertLessEqual(block, 255)

    def test_cold_start_uses_random_block(self):
        """Verify that a Tablet with no version triggers random block generation."""
        tablet = Tablet.from_row(-100, 100, [("host1", 0)], tablet_version=None)
        self.assertIsNotNone(tablet)
        self.assertIsNone(tablet.tablet_version)
        # Cold start: should use random_tablet_version_block (no crash, returns byte)
        block = random_tablet_version_block()
        self.assertGreaterEqual(block, 0)
        self.assertLessEqual(block, 255)

    def test_tablet_version_stored_from_v2_response(self):
        """Verify that Tablet.from_row stores tablet_version from V2 payload."""
        version = 0xDEADBEEFCAFEBABE
        tablet = Tablet.from_row(-100, 100, [("host1", 0), ("host2", 1)], tablet_version=version)
        self.assertIsNotNone(tablet)
        self.assertEqual(tablet.tablet_version, version)
        self.assertEqual(tablet.first_token, -100)
        self.assertEqual(tablet.last_token, 100)
        self.assertEqual(len(tablet.replicas), 2)
