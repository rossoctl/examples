"""Detached JWS over the CloudEvent envelope. DESIGN_PHASE1.md §11.

Feature-flagged and **off by default**, so the e2e path is unaffected. Both services
use this module: EventBridge signs the requests and group events it publishes,
EventRunner signs terminal responses, and each verifies what the other produced
against `shared.keyset` — which is the authorization list, not merely a key lookup.

Two design constraints shape this:

* **Canonicalization must be byte-identical between signer and verifier.** That
  is the part that bites. Rather than depend on JCS (a new dependency, which §1.1
  forbids), the canonical form is sorted **length-prefixed** `key=value` fields
  over a fixed signed attribute set, plus `sha256(data-bytes)`. The length
  prefixes are load-bearing, not decoration: with plain `key=value` lines a value
  containing a newline can synthesize an extra attribute line, so two
  structurally different events encode to identical bytes and one signature
  validates both (see `canonical`). Netstring-style prefixes make the encoding
  unambiguous, and it stays trivially reimplementable in another language and
  diffable when it disagrees.
* **Pure Python.** `cryptography` is a C extension and is banned. Ed25519 is
  implemented here from RFC 8032 using only `hashlib` — about 70 lines, and
  verified against the RFC's own test vectors in `tests/test_signing.py`.

Key handling: an Ed25519 seed (32 bytes) in a read-only Secret, hex or base64
encoded. `ER_REQUIRE_SIGNATURE=true` makes EventRunner refuse unsigned or
badly-signed requests — logging and committing the offset rather than retrying
forever, because a bad signature will still be bad on redelivery.
"""
from __future__ import annotations

import base64
import binascii
import hashlib
import json
import pathlib
import sys
from typing import Any

from shared import ce

# ---- Ed25519 (RFC 8032), pure Python ---------------------------------------

_P = 2 ** 255 - 19
_L = 2 ** 252 + 27742317777372353535851937790883648493
_D = -121665 * pow(121666, _P - 2, _P) % _P
_I = pow(2, (_P - 1) // 4, _P)


def _sha512(b: bytes) -> bytes:
    return hashlib.sha512(b).digest()


def _sha512_int(b: bytes) -> int:
    return int.from_bytes(_sha512(b), "little")


def _x_recover(y: int) -> int:
    xx = (y * y - 1) * pow(_D * y * y + 1, _P - 2, _P)
    x = pow(xx, (_P + 3) // 8, _P)
    if (x * x - xx) % _P != 0:
        x = (x * _I) % _P
    if (x * x - xx) % _P != 0:
        raise ValueError("point is not on the curve")
    if x % 2 != 0:
        x = _P - x
    return x


def _edwards_add(p: tuple[int, int], q: tuple[int, int]) -> tuple[int, int]:
    x1, y1 = p
    x2, y2 = q
    k = _D * x1 * x2 * y1 * y2
    x3 = (x1 * y2 + x2 * y1) * pow(1 + k, _P - 2, _P)
    y3 = (y1 * y2 + x1 * x2) * pow(1 - k, _P - 2, _P)
    return x3 % _P, y3 % _P


def _scalar_mult(p: tuple[int, int], e: int) -> tuple[int, int]:
    """Double-and-add. **Not side-channel resistant — do not promote this to a
    trust boundary that assumes it is.**

    `if e & 1` branches on secret bits when `e` is the secret scalar from
    `_secret_scalar`, and `_edwards_add` does a modular inversion per addition, so
    wall-clock time varies with the scalar's Hamming weight. Verification uses only
    public inputs, so it is not the sensitive direction.

    **This justification weakened when signing got production callers.** It used to
    rest on "nothing exposes a remote timing oracle over `sign()`", which is no longer
    strictly true: with `EB_SIGNING_KEY_PATH` set, EventBridge signs on the HTTP
    request path, so a caller who can time `POST /v0/agents` observes something
    correlated with the scalar. What still makes it acceptable is that the signal is
    buried under a Kafka round trip and a ~150 ms pure-Python operation whose variance
    dwarfs the leak, the seed never leaves the pod, and signing remains opt-in. It is
    a real if impractical weakness rather than a non-issue — do not promote this to a
    trust boundary that assumes constant time.

    If the pure-Python constraint (§1.1) is ever relaxed, `cryptography`'s Ed25519
    is the better trade than hardening this by hand.

    Iterative rather than recursive: the recursive form used one stack frame per
    exponent bit, ~253 deep for a clamped Ed25519 scalar — under CPython's default
    limit, but needless stack for a loop that unrolls cleanly.
    """
    q = (0, 1)
    for bit in reversed(range(max(e.bit_length(), 1))):
        q = _edwards_add(q, q)
        if (e >> bit) & 1:
            q = _edwards_add(q, p)
    return q


# The standard base point: y = 4/5 mod p, x recovered from it.
_BASE_Y = 4 * pow(5, _P - 2, _P) % _P
_BASE = (_x_recover(_BASE_Y), _BASE_Y)


def _encode_point(p: tuple[int, int]) -> bytes:
    x, y = p
    return ((y | ((x & 1) << 255)).to_bytes(32, "little"))


def _decode_point(b: bytes) -> tuple[int, int]:
    if len(b) != 32:
        raise ValueError("an Ed25519 point is 32 bytes")
    i = int.from_bytes(b, "little")
    y = i & ((1 << 255) - 1)
    sign = i >> 255
    x = _x_recover(y)
    if x & 1 != sign:
        x = _P - x
    return (x, y)


def _secret_scalar(seed: bytes) -> tuple[int, bytes]:
    h = _sha512(seed)
    a = int.from_bytes(h[:32], "little")
    a &= (1 << 254) - 8       # clamp per RFC 8032 §5.1.5
    a |= (1 << 254)
    return a, h[32:]


def public_key(seed: bytes) -> bytes:
    """Derive the 32-byte Ed25519 public key from a 32-byte seed."""
    if len(seed) != 32:
        raise ValueError(f"an Ed25519 seed is 32 bytes, got {len(seed)}")
    a, _ = _secret_scalar(seed)
    return _encode_point(_scalar_mult(_BASE, a))


def sign(message: bytes, seed: bytes) -> bytes:
    """64-byte Ed25519 signature over `message`."""
    a, prefix = _secret_scalar(seed)
    pub = _encode_point(_scalar_mult(_BASE, a))
    r = _sha512_int(prefix + message) % _L
    big_r = _scalar_mult(_BASE, r)
    enc_r = _encode_point(big_r)
    k = _sha512_int(enc_r + pub + message) % _L
    s = (r + k * a) % _L
    return enc_r + s.to_bytes(32, "little")


def verify(message: bytes, signature: bytes, pub: bytes) -> bool:
    """Constant-time-ish Ed25519 verification. False rather than raising."""
    try:
        if len(signature) != 64 or len(pub) != 32:
            return False
        big_r = _decode_point(signature[:32])
        a_point = _decode_point(pub)
        s = int.from_bytes(signature[32:], "little")
        if s >= _L:
            return False
        k = _sha512_int(signature[:32] + pub + message) % _L
        lhs = _scalar_mult(_BASE, s)
        rhs = _edwards_add(big_r, _scalar_mult(a_point, k))
        return lhs == rhs
    except (ValueError, OverflowError):
        return False


# ---- canonicalization -------------------------------------------------------

# The attribute set covered by the signature. Fixed and explicit: an attacker
# must not be able to shrink the signed set by omitting an attribute, and a
# verifier must not accept a signature that covered less than it thinks.
SIGNED_ATTRS = ("specversion", "type", "source", "id", "time", "subject",
                "datacontenttype", "correlationid", "sessionuuid", "sequence",
                "phase", "final", "mode", "causationid",
                # Added once signing had real callers. `submitter`/`submitteriss`
                # were held back deliberately: covering them before anything signed
                # would have invalidated canonicalisation twice for no benefit.
                # `groupid` is required by DESIGN_PHASE1.md §21.9.9 — without it a
                # signature says nothing about which batch an event belongs to, so a
                # forged `groupid` could move a response into another batch and
                # corrupt its fan-in counts.
                "submitter", "submitteriss", "groupid",
                # Phase 3 §2.6 and §7.7, added together in ONE change — §8.3's rule.
                # Adding an attribute changes canonicalisation, so a signer and a
                # verifier on different versions disagree about every signature;
                # splitting this into two commits would invalidate canonicalisation
                # twice for no benefit. Deployments with signing already enabled must
                # upgrade both services together. With signing off (the default) there
                # is nothing to coordinate, which is most deployments.
                #
                # All three have to be here rather than merely present on the event:
                # `userkey` decides which store a response is written into and which
                # ntfy topic announces it, so a mutable one lets anything with topic
                # write access file events into another user's history. `depth` is the
                # hop limit, and a resettable hop limit does not limit hops.
                #
                # `agent` selects the AgentSpec, and §5.1 calls the tool policy "the
                # sandbox": the spec supplies `--permission-mode`, `--allowedTools`,
                # `--disallowedTools`, `--settings` and `--mcp-config`. Outside this set,
                # anything with write access to a requests topic could rewrite `ce_agent`
                # to name a baked spec with `permission_mode = "bypassPermissions"` and no
                # `disallowed_tools` — and THE SIGNATURE WOULD STILL VERIFY, so the runner
                # executes the forged policy as an approved request. That makes the signed
                # configuration worse than the unsigned one, because an operator believes
                # it is attested. Added in the same change as the other two, per §8.3's
                # one-canonicalisation-break rule.
                "userkey", "depth", "agent")


def data_bytes(data: Any) -> bytes:
    """The exact bytes that ride the Kafka value, so signer and verifier hash the
    same thing. Mirrors `shared.ce.to_kafka_binary`."""
    if data is None:
        return b""
    if isinstance(data, (bytes, bytearray)):
        return bytes(data)
    return json.dumps(data, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _field(key: str, value: Any) -> str:
    """One netstring-style field: `len(key):key=len(value):value`.

    The length prefixes are what make the whole encoding injective. See
    `canonical` for the collision they prevent.
    """
    k, v = str(key), str(value)
    return f"{len(k)}:{k}={len(v)}:{v}"


def canonical(attrs: dict[str, Any], data: Any) -> bytes:
    """Sorted length-prefixed fields over SIGNED_ATTRS, plus a digest of the payload.

    Absent attributes are omitted rather than written empty, and the field set is
    sorted by attribute name, so the encoding is independent of dict order.
    `datadigest` binds the payload without embedding it, which keeps the signed blob
    small — the point of pairing signing with `ER_INCLUDE_RAW=false` (§8.9).

    **Why the lengths.** An earlier form joined bare `key=value` lines with `\\n`
    and escaped nothing, which is not injective: a value containing a newline
    synthesizes an additional attribute line. These two attribute sets encoded to
    byte-identical output, so one signature validated both —

        {"type": "x", "source": "s", "id": "1", "phase": "a\\nsequence=999"}
        {"type": "x", "source": "s", "id": "1", "phase": "a", "sequence": 999}

    Recomputing the canonical form on the verifier does not help, because both
    sides compute the same ambiguous encoding. Prefixing each key and value with
    its length removes the ambiguity without rejecting any value. This was latent
    rather than exploitable — every signed attribute reaching here is either
    regex-validated (`correlationid`) or a server-set literal — but the design
    rests on this encoding being unambiguous, so it has to actually be so.
    `tests/test_signing.py` asserts the pair above now diverges.
    """
    present = sorted(k for k in SIGNED_ATTRS if attrs.get(k) not in (None, ""))
    fields = [_field(k, attrs[k]) for k in present]
    fields.append(_field("datadigest", "sha256:" + hashlib.sha256(data_bytes(data)).hexdigest()))
    return "\n".join(fields).encode("utf-8")


def _b64u(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode().rstrip("=")


def _b64u_dec(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def sign_event(event, seed: bytes, kid: str | None = None) -> str:
    """Return the detached-JWS value for `ce_signature`.

    Detached: the payload is not carried inside the JWS (it is the event itself),
    so the compact serialization has an empty middle segment —
    `<protected>..<signature>`, as RFC 7515 Appendix F describes.

    `kid` names the key that signed this, so a verifier holding several approved
    public keys can select the right one instead of trying each. It is part of the
    protected header and therefore covered by the signature, so it cannot be
    swapped to point at a different key. Omitting it stays valid: a verifier with
    exactly one key does not need it.
    """
    header: dict[str, Any] = {"alg": "EdDSA", "typ": "ce+jws"}
    if kid:
        header["kid"] = kid
    protected = _b64u(json.dumps(header, separators=(",", ":"),
                                 sort_keys=True).encode())
    payload = _b64u(canonical(event.attrs, event.data))
    sig = sign(f"{protected}.{payload}".encode(), seed)
    return f"{protected}..{_b64u(sig)}"


def sign_into(event, seed: bytes | None, kid: str | None = None) -> bool:
    """Assign `event.attrs["signature"]` when a seed is configured. Returns whether
    it signed.

    `sign_event` does not mutate, so every producer would otherwise repeat the same
    three lines; this is the one place that decides what "signing is enabled" means.

    **Never raises.** A signing failure here would turn a key-configuration mistake
    into a total publish outage — every request rejected because one seed file has a
    stray byte. Degrading to unsigned is the lesser harm, and it is safe only because
    the verifying side is what enforces: an unsigned event is refused there when
    enforcement is on. The loud failure belongs at startup instead, where the seed is
    loaded once and a bad path stops the process.
    """
    if seed is None:
        return False
    try:
        # `sign()` does not check the seed length — only `public_key()` does — so a
        # truncated key file would produce a well-formed signature that no verifier
        # can ever match, reported to the operator as success. Check it here, where
        # production signing enters, so the failure names the key instead.
        if len(seed) != 32:
            raise ValueError(f"an Ed25519 seed is 32 bytes, got {len(seed)}")
        event.attrs["signature"] = sign_event(event, seed, kid)
        return True
    except Exception as e:  # noqa: BLE001 - see above: publishing unsigned beats not publishing
        print(f"[signing] could not sign {event.get('id')!r}, publishing unsigned: {e!r}",
              file=sys.stderr, flush=True)
        return False


def token_kid(token: str) -> str | None:
    """The `kid` from a detached-JWS token's protected header, or None.

    Read BEFORE verification, to choose which key to verify with — so treat it as
    a hint, not a fact. It only becomes trustworthy once `verify_signature`
    succeeds with the key it named, because the header is part of the signed
    input. A token naming an unapproved key is refused for that reason, not
    because the `kid` itself was disbelieved.
    """
    parts = token.split(".")
    if len(parts) != 3:
        return None
    try:
        header = json.loads(_b64u_dec(parts[0]))
    except Exception:  # noqa: BLE001
        return None
    kid = header.get("kid") if isinstance(header, dict) else None
    return kid if isinstance(kid, str) and kid else None


def verify_signature(event, pub: bytes) -> tuple[bool, str]:
    """(ok, reason). Recomputes the canonical form locally — never trusts one
    supplied in the event."""
    token = event.get("signature")
    if not token:
        return False, "no ce_signature attribute"
    parts = token.split(".")
    if len(parts) != 3 or parts[1] != "":
        return False, f"not a detached JWS ({len(parts)} segments)"
    protected, _, sig_b64 = parts
    try:
        header = json.loads(_b64u_dec(protected))
    except (ValueError, binascii.Error):
        return False, "undecodable JWS header"
    if header.get("alg") != "EdDSA":
        return False, f"unexpected alg {header.get('alg')!r} (only EdDSA is accepted)"
    payload = _b64u(canonical(event.attrs, event.data))
    try:
        sig = _b64u_dec(sig_b64)
    except binascii.Error:
        return False, "undecodable signature segment"
    if not verify(f"{protected}.{payload}".encode(), sig, pub):
        return False, "signature does not verify over the canonical attributes"
    return True, "ok"


# ---- authorization: the keyset as an allowlist -------------------------------

def verify_with_keyset(event, ks, *, expect_kid: str | None = None) -> tuple[bool, str]:
    """(ok, reason) for one event against an approved-key set.

    The composition `tests/test_keyset.py` previously had to hand-wire: read the
    `kid`, select the key it names, then let the signature decide. The `kid` is only
    a hint until that last step succeeds, because the header is signed input — so a
    token naming an unapproved key is refused for *naming* it, not because the `kid`
    itself was disbelieved.

    `expect_kid` pins the signer to one identity. Group lifecycle events use it: the
    keyset is otherwise flat, so any approved runner could forge a `group.completed`
    and end a batch early. Passing it restricts a class of event to one key while
    leaving the rest of the set alone.

    **Never raises**, so a caller inside a consumer loop needs no guard of its own to
    stay alive. Every exit returns a distinct reason, because a rejection nobody can
    explain gets diagnosed as "signing is broken" and switched off.
    """
    token = event.get("signature")
    if not token:
        return False, "no ce_signature attribute"
    kid = token_kid(token)
    if expect_kid and kid != expect_kid:
        return False, (f"expected a signature from kid {expect_kid!r}, "
                       f"got {kid!r}")
    pub = ks.select(kid)
    if pub is None:
        if kid is None:
            return False, (f"the token names no kid and the approved set holds "
                           f"{len(ks)} keys, so it is ambiguous")
        return False, f"kid {kid!r} is not in the approved key set"
    return verify_signature(event, pub)


def verify_request(event, cfg, ks=None) -> tuple[bool, str]:
    """(ok, reason) for a request event, using whichever key source is configured.

    Prefers the keyset, which is an allowlist of many approved agents. Falls back to
    `verify_event`'s single-key path so `ER_REQUIRE_SIGNATURE=true` with only
    `ER_VERIFY_KEY_PATH` set keeps behaving as it did — that combination predates the
    keyset and is still the simplest useful deployment.
    """
    if ks is not None:
        return verify_with_keyset(event, ks)
    return verify_event(event, cfg)


def _is_terminal(event) -> bool:
    """Whether a signature is expected on this event.

    Terminal responses are signed (`emit()` signs on `final`), and so is every group
    lifecycle event. A group event carries no `final` attribute at all, so testing
    `final` alone would classify it as an unsigned intermediate frame and wave it
    through — which is exactly the forged `group.completed` this is meant to catch.
    """
    if ce.is_group_event(event):
        return True
    return str(event.get("final", "")).lower() == "true"


def response_decision(event, ks, *, require: bool,
                      bridge_kid: str | None = None) -> tuple[bool, str]:
    """(accept_as_is, reason) for one event off the responses topic.

    Pure: no I/O, no logging, and **it does not touch `event`** — the caller owns the
    rewrite, which is what makes this testable without a Kafka consumer.

    Collapsing three questions into one answer is deliberate; it leaves the caller no
    policy to get wrong:

    * `ks is None` — verification is not configured. Accept, as today.
    * verified — accept.
    * not verified and `require` false — **audit mode**: accept, but hand back the
      reason so the caller can log it. Enforcement rewrites persisted rows and pages
      a phone, so there has to be a way to watch the reject rate first.
    * not verified and `require` true — reject; the caller stores it as `phase=error`.

    `bridge_kid` pins group lifecycle events to EventBridge's own key. It is only
    applied when set, so a single-key deployment — where `KeySet.select(None)` returns
    the sole key and nothing needs to name a kid — keeps working untouched.

    **Unsigned non-terminal frames are accepted, and that is not a loophole being
    left open — it is the signing policy on the other side.** `emit()` signs terminal
    events only, because it runs for every `stdout` frame and a signature costs
    ~150-200 ms; verifying all-or-nothing would rewrite every streamed frame of every
    genuine run to `phase=error`. So an event that carries no signature AND is not
    terminal is passed through, while an unsigned **terminal** event is still refused —
    that is the one the transcript presents as the answer, and refusing it is the whole
    control. A forged intermediate frame therefore still renders (see `emit.py`: this
    proves who *finished* a run, not what it said along the way), but it can no longer
    masquerade as the result.
    """
    if ks is None:
        return True, "verification not enabled"
    expect = bridge_kid if (bridge_kid and ce.is_group_event(event)) else None
    ok, why = verify_with_keyset(event, ks, expect_kid=expect)
    if ok:
        return True, why
    if not event.get("signature") and not _is_terminal(event):
        return True, "unsigned non-terminal frame (signing covers terminal events)"
    return (not require), why


# ---- key loading ------------------------------------------------------------

def load_seed(path: str | pathlib.Path) -> bytes:
    """Read a 32-byte Ed25519 seed from a Secret-mounted file (hex, base64 or raw)."""
    # Check the unstripped length first. A raw 32-byte key can legitimately begin
    # or end with a byte that is ASCII whitespace (0x09 0x0a 0x0b 0x0c 0x0d 0x20),
    # and stripping before the length test truncates it to 31 bytes — which then
    # fails `decode(errors="strict")` on arbitrary binary and reports the key as the
    # wrong length when it is not. That is ~4.6% of random keys. Stripping still
    # happens below, where it is wanted, for hex/base64 written with a trailing
    # newline.
    data = pathlib.Path(path).read_bytes()
    if len(data) == 32:
        return bytes(data)
    raw = data.strip()
    if len(raw) == 32:
        return bytes(raw)
    text = raw.decode(errors="strict").strip()
    for decode in (bytes.fromhex, lambda s: base64.b64decode(s, validate=True),
                   _b64u_dec):
        try:
            out = decode(text)
        except Exception:  # noqa: BLE001
            continue
        if len(out) == 32:
            return out
    raise ValueError(f"{path}: not a 32-byte Ed25519 seed (hex, base64 or raw)")


def verify_event(event, cfg) -> tuple[bool, str]:
    """Verify using the key configured on the runner. Used by consume.py when
    ER_REQUIRE_SIGNATURE=true."""
    key_path = cfg.verify_key_path or cfg.signing_key_path
    if not key_path:
        return False, ("ER_REQUIRE_SIGNATURE=true but neither ER_VERIFY_KEY_PATH "
                       "nor ER_SIGNING_KEY_PATH is set")
    try:
        # Same rule as load_seed: test the unstripped length first, so a raw key
        # with a whitespace edge byte is not truncated out of the 32-byte path.
        data = pathlib.Path(key_path).read_bytes()
        seed_or_pub = data if len(data) == 32 else data.strip()
        # 32 bytes on disk is ambiguous between a seed and a public key, and so is a
        # hex/base64 file that decodes to 32 — `load_seed` decodes either and cannot
        # tell them apart. So collect both readings and try each: the encoded-public-key
        # case used to be missed entirely, because only `public_key(load_seed(...))` was
        # tried and that derives the wrong key from a public one.
        if len(seed_or_pub) == 32:
            candidates = [bytes(seed_or_pub), public_key(bytes(seed_or_pub))]
        else:
            decoded = load_seed(key_path)
            candidates = [decoded, public_key(decoded)]
    except Exception as e:  # noqa: BLE001
        return False, f"cannot load verification key from {key_path}: {e}"
    why = "no candidate key verified the signature"
    for pub in candidates:
        ok, why = verify_signature(event, pub)
        if ok:
            return True, why
    return False, why
