#!/usr/bin/env python3
"""Generate the Ed25519 seeds and the approved-key set that §11 signing needs.

Signing was wired in both directions (EventBridge signs requests and group events,
EventRunner signs terminal responses) but there was no way to produce keys for it, so
turning it on meant hand-rolling `os.urandom(32).hex()` and deriving public keys in a
REPL. This is that, with the parts that are easy to get wrong done once:

  * **A seed is never printed.** Only kids and public keys go to stdout, so pasting a
    terminal transcript into an issue cannot leak a private key. The same reason
    `k8s_deploy.py` logs a 4-char prefix and a length instead of a token.
  * **Seeds land at mode 0600, in a directory created 0700.** A seed written
    world-readable is the whole control gone, and it is not obvious from the outside.
  * **Public keys are derived, never typed.** `agents.json` is produced from the seeds
    by `shared.signing.public_key`, so the keyset cannot disagree with the keys it is
    supposed to authorize — the failure mode that otherwise shows up as "signing is
    broken" with nothing pointing at the key set.

What this does NOT do is attest anything. The set records which keys an operator
approved, not which workloads a platform vouched for; `shared/keyset.py` says the same
thing at more length, and it is worth repeating out loud when demoing.

Asymmetry is the point: EventBridge gets its own seed and EventRunner gets another, so
neither can produce the other's signatures. A single shared seed would be HMAC with
extra steps, which `DESIGN_PHASE2.md` §4.3 rejected for exactly that reason.

  python3 scripts/gen_signing_keys.py --out ~/.eventing-keys
  python3 scripts/gen_signing_keys.py --out ~/.eventing-keys --force
  python3 scripts/gen_signing_keys.py --print-keyset          # JSON only, no files
  python3 scripts/gen_signing_keys.py --out d --runners 3     # runner-01..runner-03
"""
from __future__ import annotations

import argparse
import json
import pathlib
import secrets
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from shared import signing  # noqa: E402

PREFIX = "keygen"
BRIDGE_KID = "eb-01"
SEED_FILE = "seed.hex"           # the key inside the Secret, per README_PHASE1.md


def _log(msg: str) -> None:
    print(f"[{PREFIX}] {msg}")


def generate(n_runners: int = 1, bridge_kid: str = BRIDGE_KID) -> dict[str, bytes]:
    """`kid` -> 32-byte seed. One for the bridge, `n_runners` for runners.

    `secrets.token_bytes`, not `random`: the latter is seeded predictably enough that a
    key from it is not a key.
    """
    keys = {bridge_kid: secrets.token_bytes(32)}
    for i in range(1, n_runners + 1):
        keys[f"runner-{i:02d}"] = secrets.token_bytes(32)
    return keys


def keyset_json(seeds: dict[str, bytes]) -> str:
    """The approved-key set: `kid` -> public key hex, derived from each seed.

    Public keys only. A 32-byte seed and a 32-byte public key are indistinguishable by
    length, so `keyset.load()` cannot catch a pasted private key — deriving them here is
    what makes that mistake impossible rather than merely discouraged.
    """
    return json.dumps({kid: signing.public_key(seed).hex()
                       for kid, seed in sorted(seeds.items())}, indent=2) + "\n"


def write_seeds(out: pathlib.Path, seeds: dict[str, bytes], *,
                force: bool = False) -> list[pathlib.Path]:
    """One `<kid>/seed.hex` per key, 0600, under a 0700 directory.

    A directory per kid rather than `<kid>.hex` files side by side: that is the shape
    `kubectl create secret generic --from-file=<dir>` wants, and it keeps the Secret's
    data key `seed.hex` — the name the Deployment's mount path and
    `ER_SIGNING_KEY_PATH` both already assume.
    """
    out.mkdir(parents=True, exist_ok=True)
    out.chmod(0o700)

    # Check EVERY target before writing ANY of them. Raising mid-loop would leave a new
    # seed on disk for one kid while `agents.json` — written by the caller, after this
    # returns — still holds the old public key for it. That is precisely the state this
    # module's docstring promises cannot happen ("the keyset cannot disagree with the
    # keys it is supposed to authorize"), and until k8s_deploy.py grew its
    # seed-derives-the-approved-key check, nothing downstream could detect it.
    #
    # Reachable whenever the FIRST kid in sorted order is missing and a later one is
    # present — a partial rotation, or a restored backup.
    if not force:
        clashes = [out / kid / SEED_FILE for kid in sorted(seeds)
                   if (out / kid / SEED_FILE).exists()]
        if clashes:
            raise SystemExit(
                f"[{PREFIX}] {', '.join(str(p) for p in clashes)} already exist — "
                f"refusing to overwrite keys that may already be deployed and approved. "
                f"Nothing was written. Pass --force if you mean to rotate, and remember "
                f"rotation means redeploying the keyset and restarting both services "
                f"(shared/keyset.py: no live reload, on purpose).")

    written = []
    for kid, seed in sorted(seeds.items()):
        d = out / kid
        d.mkdir(exist_ok=True)
        d.chmod(0o700)
        p = d / SEED_FILE
        # Create at 0600 before writing, not after: a chmod afterwards leaves a window
        # in which the seed exists world-readable.
        p.touch(mode=0o600, exist_ok=True)
        p.chmod(0o600)
        p.write_text(seed.hex() + "\n")
        written.append(p)
    return written


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Generate Ed25519 seeds and the approved-key set for §11 signing.")
    ap.add_argument("--out", type=pathlib.Path,
                    help="directory to write <kid>/seed.hex into (0600). "
                         "Omit with --print-keyset to generate nothing on disk.")
    ap.add_argument("--runners", type=int, default=1,
                    help="how many runner keys to generate (default 1)")
    ap.add_argument("--bridge-kid", default=BRIDGE_KID,
                    help=f"kid for EventBridge's own key (default {BRIDGE_KID})")
    ap.add_argument("--force", action="store_true",
                    help="overwrite existing seeds (rotation)")
    ap.add_argument("--print-keyset", action="store_true",
                    help="print agents.json to stdout and write no seed files")
    args = ap.parse_args(argv)

    if args.runners < 1:
        ap.error("--runners must be at least 1")
    if not args.out and not args.print_keyset:
        ap.error("give --out DIR, or --print-keyset to see a keyset without writing keys")

    seeds = generate(args.runners, args.bridge_kid)
    ks = keyset_json(seeds)

    if args.print_keyset and not args.out:
        # Seeds are discarded unread: this mode exists to show the shape of a keyset,
        # not to produce one whose private keys were thrown away.
        print(ks, end="")
        print(f"[{PREFIX}] NOTE: no seeds written, so these public keys are useless — "
              f"rerun with --out to keep the matching private keys.", file=sys.stderr)
        return 0

    paths = write_seeds(args.out, seeds, force=args.force)
    (args.out / "agents.json").write_text(ks)

    _log(f"wrote {len(paths)} seed(s) at mode 0600 under {args.out}")
    for kid, seed in sorted(seeds.items()):
        # Public key only. Never the seed, not even truncated.
        _log(f"  {kid:<12} pub={signing.public_key(seed).hex()}")
    _log(f"approved-key set -> {args.out / 'agents.json'}")
    _log("next: python3 scripts/k8s_deploy.py --yes --overlay kind-signed "
         f"--signing-keys {args.out}")
    if args.print_keyset:
        print(ks, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
