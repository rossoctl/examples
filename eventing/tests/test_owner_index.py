"""The global `correlationid -> userkey` index. DESIGN_PHASE3.md §2.6, §6.2.

The uniqueness constraint is the load-bearing part, and the test that matters most is
`test_the_constraint_is_on_correlationid_alone`: `(userkey, correlationid)` is the right
primary key *inside* a tenant's store and exactly the wrong one here, because two tenants
minting one id would produce two distinct tuples, never conflict, and the collision would
pass through silently — which is the failure the whole guarantee exists to prevent.
"""
from __future__ import annotations

import threading

import pytest

from eventbridge.correlation import Minter, mint_for
from eventbridge.owner_index import Collision, OwnerIndex

A = "gh-alice-a1b2c3d4"
B = "gh-bob-9e8f7a6b"
CORR = "brave-otter-4718"


@pytest.fixture
def idx(tmp_path):
    return OwnerIndex(tmp_path)


# ---- claim / exists -------------------------------------------------------

def test_claim_then_exists(idx):
    assert not idx.exists(CORR)
    idx.claim(CORR, A)
    assert idx.exists(CORR)


def test_claim_is_idempotent_for_the_same_owner(idx):
    """A retried submit must not fail."""
    idx.claim(CORR, A)
    idx.claim(CORR, A)
    assert idx.count() == 1


def test_the_constraint_is_on_correlationid_alone(idx):
    """Two tenants cannot share one correlation id.

    With `(userkey, correlationid)` as the key this would silently succeed and the two
    tenants would each believe they owned it — and `owner_of` would return whichever
    row the query happened to find.
    """
    idx.claim(CORR, A)
    with pytest.raises(Collision) as e:
        idx.claim(CORR, B)
    assert A in str(e.value) and B in str(e.value)
    assert idx.count() == 1


def test_a_null_owner_and_a_real_owner_are_different_owners(idx):
    """A correlation minted in single-tenant mode is not free for a tenant to claim:
    that is a genuine ambiguity, not a no-op."""
    idx.claim(CORR, None)
    with pytest.raises(Collision):
        idx.claim(CORR, A)


def test_a_null_owner_claim_is_idempotent(idx):
    idx.claim(CORR, None)
    idx.claim(CORR, None)
    assert idx.count() == 1


# ---- owner_of ------------------------------------------------------------

def test_owner_of_distinguishes_unknown_from_unowned(idx):
    """The two-value return exists for exactly this: a bare `None` would conflate
    "the shared tier owns it" with "never seen"."""
    assert idx.owner_of(CORR) == (None, False)
    idx.claim(CORR, None)
    assert idx.owner_of(CORR) == (None, True)
    idx.claim("brave-otter-0002", A)
    assert idx.owner_of("brave-otter-0002") == (A, True)


# ---- listing -------------------------------------------------------------

def test_correlations_for_a_tenant(idx):
    idx.claim("brave-otter-0001", A)
    idx.claim("brave-otter-0002", A)
    idx.claim("brave-otter-0003", B)
    assert set(idx.correlations_for(A)) == {"brave-otter-0001", "brave-otter-0002"}
    assert idx.correlations_for(B) == ["brave-otter-0003"]


def test_correlations_for_the_null_owner_does_not_leak_tenants(idx):
    """`WHERE userkey IS NULL` rather than `= NULL`, which matches nothing in SQL."""
    idx.claim("brave-otter-0001", None)
    idx.claim("brave-otter-0002", A)
    assert idx.correlations_for(None) == ["brave-otter-0001"]


def test_userkeys_excludes_the_null_owner(idx):
    idx.claim("brave-otter-0001", None)
    idx.claim("brave-otter-0002", A)
    idx.claim("brave-otter-0003", B)
    assert set(idx.userkeys()) == {A, B}


# ---- deletion (§6.5 groundwork) ------------------------------------------

def test_forget_one(idx):
    idx.claim(CORR, A)
    idx.forget(CORR)
    assert not idx.exists(CORR)


def test_forget_tenant_returns_a_count_and_leaves_others(idx):
    idx.claim("brave-otter-0001", A)
    idx.claim("brave-otter-0002", A)
    idx.claim("brave-otter-0003", B)
    assert idx.forget_tenant(A) == 2
    assert idx.correlations_for(B) == ["brave-otter-0003"]


# ---- persistence ---------------------------------------------------------

def test_the_index_survives_a_restart(tmp_path):
    """It names topics and stores that outlive the process; an in-memory set would
    lose every ownership fact on restart."""
    one = OwnerIndex(tmp_path)
    one.claim(CORR, A)
    one.close()
    two = OwnerIndex(tmp_path)
    assert two.owner_of(CORR) == (A, True)


# ---- concurrency ---------------------------------------------------------

def test_concurrent_claims_of_one_id_produce_exactly_one_winner(idx):
    """EventBridge is single-replica but multi-threaded: the HTTP worker pool and the
    responses consumer both reach this, so check-then-insert has to be atomic."""
    results: list[object] = []
    lock = threading.Lock()

    def claim(uk):
        try:
            idx.claim(CORR, uk)
            with lock:
                results.append(("ok", uk))
        except Collision:
            with lock:
                results.append(("collision", uk))

    threads = [threading.Thread(target=claim, args=(f"gh-u{i}-0000000{i}",))
               for i in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sum(1 for r, _ in results if r == "ok") == 1
    assert idx.count() == 1


# ---- the Minter integration ----------------------------------------------

def test_the_minter_claims_what_it_mints(idx):
    m = Minter(index=idx)
    corr = m.mint(userkey=A)
    assert idx.owner_of(corr) == (A, True)


def test_the_minter_never_returns_a_claimed_id(tmp_path):
    """The uniqueness guarantee §2.6 relies on to keep `sessionuuid` unsalted.

    A 1-word corpus would be ideal but the real word lists are large, so instead mint
    many and assert no repeats — with the index as the only thing preventing them.
    """
    idx = OwnerIndex(tmp_path)
    m = Minter(seed=1, index=idx)
    minted = [m.mint(userkey=A) for _ in range(200)]
    assert len(set(minted)) == 200
    assert idx.count() == 200


def test_two_minters_sharing_an_index_do_not_collide(tmp_path):
    """Two Minters with the SAME rng seed would produce identical sequences; the index
    is what keeps them apart. This is the cross-tenant case in miniature."""
    idx = OwnerIndex(tmp_path)
    a = Minter(seed=7, index=idx)
    b = Minter(seed=7, index=idx)
    first = [a.mint(userkey=A) for _ in range(20)]
    second = [b.mint(userkey=B) for _ in range(20)]
    assert not (set(first) & set(second))


def test_the_minter_without_an_index_keeps_phase2_behaviour(tmp_path):
    """No index configured: the in-memory `seen` set, exactly as before."""
    m = Minter(seed=3)
    corr = m.mint()
    assert corr
    m.remember("brave-otter-0001")
    # `remember` is a no-op with an index, and meaningful without one. Minting enough
    # ids to hit the remembered one is impractical, so assert the set directly.
    assert "brave-otter-0001" in m._seen


def test_remember_is_a_noop_once_an_index_is_configured(idx):
    m = Minter(index=idx)
    m.remember("brave-otter-0001")
    assert m._seen == set()


# ---- mint_for's compatibility shim ---------------------------------------

def test_mint_for_passes_the_userkey_when_supported(idx):
    m = Minter(index=idx)
    corr = mint_for(m, A)
    assert idx.owner_of(corr) == (A, True)


def test_mint_for_falls_back_for_a_substitute_without_userkey():
    """Several tests supply deterministic fakes that predate tenancy; they must keep
    working rather than failing at the call site."""
    class OldMinter:
        def mint(self):
            return "brave-otter-0001"

    assert mint_for(OldMinter(), A) == "brave-otter-0001"


def test_mint_for_does_not_swallow_a_typeerror_from_inside_mint():
    """A working `mint()` that itself raises TypeError must NOT be silently retried
    without its ownership claim — that would turn a real bug into a missing index
    entry, which is far harder to notice."""
    class Exploding:
        def mint(self, *, userkey=None):
            raise TypeError("something inside went wrong")

    with pytest.raises(TypeError, match="something inside"):
        mint_for(Exploding(), A)


def test_mint_for_passes_the_userkey_to_a_kwargs_substitute():
    seen = {}

    class KwargsMinter:
        def mint(self, **kw):
            seen.update(kw)
            return "brave-otter-0001"

    mint_for(KwargsMinter(), A)
    assert seen == {"userkey": A}
