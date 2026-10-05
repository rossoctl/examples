"""EventBridge entrypoint: HTTP server + Kafka consumer/producer."""
from __future__ import annotations

import json
import pathlib
import signal
import sys
import threading

from eventbridge import registry
from eventbridge.config import load
from eventbridge.correlation import Minter
from eventbridge.group_service import GroupService
from eventbridge.handlers import Handlers
from eventbridge.http_server import make
from eventbridge.kafka_group_mirror import GroupMirror
from eventbridge.kafka_in import Consumer
from eventbridge.kafka_out import Producer
from eventbridge.kafka_requests_mirror import RequestsMirror
from eventbridge.netcands import enumerate_candidates, format_startup_hint
from eventbridge.ntfy import NtfyPublisher
from eventbridge.openapi import spec
from eventbridge.owner_index import OwnerIndex
from eventbridge.router import Dispatcher
from eventbridge.store_registry import StoreRegistry
from shared import keyset, signing
from shared.pidfile import PidFile


def main() -> int:
    cfg = load()
    pidfile = PidFile("eventbridge")
    pidfile.__enter__()
    print(f"[eventbridge] bootstrap={cfg.kafka_bootstrap} http={cfg.http_addr} ntfy={'on' if cfg.ntfy.enabled and cfg.ntfy.topic else 'off'}")
    try:
        _host, _port_s = cfg.http_addr.rsplit(":", 1)
        cands = enumerate_candidates()
        print(format_startup_hint(cands, int(_port_s), cfg.public_base_url))
    except Exception as e:  # noqa: BLE001
        print(f"[eventbridge] (candidate enumeration failed: {e})")

    # snapshot the OAS
    oas_path = pathlib.Path(cfg.tmpdir) / "eventbridge" / "openapi.json"
    oas_path.parent.mkdir(parents=True, exist_ok=True)
    oas_path.write_text(json.dumps(spec(), indent=2))

    eb_root = pathlib.Path(cfg.tmpdir) / "eventbridge"
    # §2.6 — the global `correlationid -> userkey` index. Always built, including in
    # single-tenant mode: the uniqueness guarantee it provides is worth having for every
    # deployment, and it is what replaces the startup seeding loop below.
    owners = OwnerIndex(eb_root)
    minter = Minter(index=owners)

    # §11 — key material is loaded ONCE, here, and deliberately not caught. A bridge
    # that believes it is signing but is not fails silently; one that will not start
    # says so in `kubectl logs` before it accepts a request.
    seed = None
    if cfg.signing_key_path:
        seed = signing.load_seed(cfg.signing_key_path)
        print(f"[eventbridge] request signing ON as kid={cfg.signing_kid or '(unnamed)'} "
              f"(seed {cfg.signing_key_path})")
    else:
        print("[eventbridge] request signing OFF (set EB_SIGNING_KEY_PATH to enable)")

    # Loaded once, not per record: live reload is deliberately absent (keyset.py), so a
    # mid-run edit must not silently widen the set of agents this bridge trusts.
    ks = keyset.load_if_set(cfg.verify_keyset_path)
    if ks is not None:
        mode = "ENFORCING" if cfg.require_response_signature else "audit only"
        print(f"[eventbridge] response verification ON ({mode}) — {len(ks)} kid(s): "
              f"{', '.join(ks.kids)}")
        if not cfg.signing_kid:
            # Without a bridge kid there is nothing to compare a group event against,
            # so any approved runner could forge one. Refuse rather than accept-any.
            raise SystemExit(
                "[eventbridge] EB_VERIFY_KEYSET_PATH is set but EB_SIGNING_KID is not: "
                "group lifecycle events could not be attributed to this bridge. "
                "Set EB_SIGNING_KID to the kid naming EventBridge's key.")
    else:
        print("[eventbridge] response verification OFF "
              "(set EB_VERIFY_KEYSET_PATH to enable)")

    # Phase 3 §3.2: one TopicSet, built once here and handed to everything that names a
    # topic, so §3.1's layout appears exactly once in the codebase. In `single` mode
    # every method returns the configured request/response topic, which is what makes
    # this step a pure refactor — the existing suite is the check.
    topics = cfg.topics
    print(f"[eventbridge] tenancy={cfg.tenancy_mode}"
          + (f" prefix={cfg.topic_prefix}" if topics.multi else
             f" requests={cfg.request_topic} responses={cfg.response_topic}"))

    # §2.5 — a bad registry must stop startup, but as a readable refusal rather than a
    # traceback: this is an operator-facing configuration error, and the message (which
    # names both the recorded and the derived userkey) is the whole point. A traceback
    # buries it under frames nobody reading `kubectl logs` needs.
    try:
        users = registry.load(cfg.user_registry_path)
    except registry.RegistryError as e:
        raise SystemExit(f"[eventbridge] user registry: {e}") from None
    if topics.multi:
        if not cfg.user_registry_path:
            raise SystemExit(
                "[eventbridge] EB_TENANCY_MODE=multi requires EB_USER_REGISTRY_PATH. "
                "Per-user isolation has no meaning without a list of users to isolate. "
                "See DESIGN_PHASE3.md §2.5.")
        if not len(users):
            # Phase 2 §2.4's reading, for Phase 2's reason: the other one turns a
            # missing file into an open door.
            raise SystemExit(
                f"[eventbridge] the user registry at {cfg.user_registry_path} approves "
                f"nobody. An empty registry denies everyone; add a user or run with "
                f"EB_TENANCY_MODE=single.")
        print(f"[eventbridge] registry: {len(users)} user(s) — "
              f"{', '.join(users.userkeys)}")
    elif len(users):
        # Harmless, but worth saying: a registry that is loaded and ignored is usually
        # somebody who set the path and forgot the mode.
        print(f"[eventbridge] registry has {len(users)} user(s) but "
              f"EB_TENANCY_MODE=single, so it is not consulted")

    # §6.1 — per-user stores. Built after `topics` because the layout depends on the
    # mode: single-tenant keeps `responses.sqlite`/`sessions.sqlite` directly in the
    # bridge root, so a Phase 2 deployment's existing sessions and transcripts stay
    # exactly where they are and keep showing up in the UI.
    stores = StoreRegistry(eb_root, max_open=cfg.max_open_stores,
                           multi=topics.multi)
    # The `shared` store: single-tenant mode's only store, and multi-tenant mode's home
    # for anything that arrives without a `userkey` (§6.1's `unattributed` path).
    store = stores.for_userkey(None)

    # Seed the ownership index from what is already on disk. REQUIRED for correctness on
    # the first start after an upgrade, not an optimisation: the index is new and empty
    # while `sessions.sqlite` still holds every pre-Phase-3 correlation, so without this
    # `exists()` reports live ids as free and `Minter` can reissue one — overwriting a
    # session and appending a new prompt to somebody's existing conversation. Phase 2
    # seeded its `seen` set from the store for exactly this reason, and dropping that loop
    # without replacing it was a regression in the DEFAULT configuration.
    #
    # Cheap after the first run: `INSERT OR IGNORE` over ids the index already holds, and
    # `all_correlations` is one indexed query per store. The `limit` matches the 10,000
    # Phase 2 applied to its own seed.
    # `needs_seeding` is checked FIRST so an already-seeded store is never opened and
    # never scanned. Without that gate this loop is the per-start N-store seeding §2.6
    # rejects: `seed_from` is `INSERT OR IGNORE` so repeating it is harmless to
    # correctness, but the cost §2.6's whole argument is about — two SQLite connections
    # and an indexed scan per tenant, before the socket binds — was being paid on every
    # boot. Measured at ~210 ms per restart for 100 tenants x 200 correlations, and §2.6's
    # own worked example (100 x 10,000) extrapolates to ~6 s added to every restart.
    #
    # Skipping is sound because nothing can add an UNINDEXED id to a store after it is
    # seeded: every mint claims as it mints, so a seeded store cannot later acquire one.
    seeded = 0
    if owners.needs_seeding():
        seeded += owners.seed_from(store.all_correlations(limit=10000))
    if topics.multi:
        # Every tenant's store too, otherwise a tenant's existing ids stay invisible to
        # the uniqueness check. `known_userkeys` reads the filesystem rather than the LRU,
        # so a tenant that is merely closed is still seeded.
        for uk in stores.known_userkeys():
            if not owners.needs_seeding(uk):
                continue
            seeded += owners.seed_from(
                stores.for_userkey(uk).all_correlations(limit=10000), uk)
    if seeded:
        print(f"[eventbridge] seeded {seeded} pre-existing correlation(s) into the "
              f"ownership index")
    if topics.multi:
        print(f"[eventbridge] per-user stores under {eb_root}/users "
              f"(max_open={cfg.max_open_stores}), "
              f"{owners.count()} correlation(s) in the ownership index")

    # The producer needs the RESPONSES topic too: group lifecycle events go there,
    # not on requests, because EventRunner would try to execute anything on requests.
    producer = Producer(cfg.kafka_bootstrap, cfg.request_topic, cfg.source_uri,
                        response_topic=cfg.response_topic,
                        seed=seed, kid=cfg.signing_kid or None,
                        topics=topics)
    groups = GroupService(cfg, store, producer, minter, stores=stores)

    ntfy = NtfyPublisher(cfg.ntfy, cfg.public_base_url, store=store,
                         stores=stores if topics.multi else None)
    if cfg.ntfy.enabled and cfg.ntfy.topic:
        ntfy.start()

    consumer = Consumer(cfg.kafka_bootstrap, cfg.response_topic, store,
                        on_event=ntfy.submit,
                        on_group_event=groups.on_group_event,
                        on_member_event=groups.on_member_event,
                        keyset=ks,
                        require_signature=cfg.require_response_signature,
                        bridge_kid=cfg.signing_kid or None,
                        topics=topics,
                        stores=stores if topics.multi else None)
    consumer.start()

    # Back-fill prompts from the requests topic — also gives us prompt visibility
    # for correlations we didn't originate ourselves.
    requests_mirror = RequestsMirror(cfg.kafka_bootstrap, cfg.request_topic, store,
                                     topics=topics,
                                     stores=stores if topics.multi else None)
    requests_mirror.start()

    # §21.2: rebuild group history from the responses topic. The live consumer above
    # commits offsets and so never re-reads, which meant a restarted pod (with an
    # emptyDir /data) served 404 for every earlier group even though its notifications
    # had already gone out. One-shot, never publishes, never notifies.
    group_mirror = GroupMirror(cfg.kafka_bootstrap, cfg.response_topic, groups,
                               topics=topics,
                               userkeys=users.userkeys if topics.multi else (),
                               owners=owners if topics.multi else None)
    group_mirror.start()

    h = Handlers(cfg, store, producer, minter, groups=groups,
                 registry=users if topics.multi else None,
                 stores=stores if topics.multi else None,
                 owners=owners if topics.multi else None)
    dsp = Dispatcher()
    dsp.add("POST", r"/v0/agents",                                          h.start_agent)
    dsp.add("POST", r"/v0/agents/(?P<correlationid>[a-z0-9-]+)/continue",   h.continue_agent)
    dsp.add("POST", r"/v0/agents/(?P<correlationid>[a-z0-9-]+)/continue-html", h.continue_agent_html)
    dsp.add("GET",  r"/v0/agents/(?P<correlationid>[a-z0-9-]+)",            h.get_html)
    dsp.add("GET",  r"/v0/agents/(?P<correlationid>[a-z0-9-]+)/turns",      h.get_turns)
    dsp.add("GET",  r"/v0/agents/(?P<correlationid>[a-z0-9-]+)/events",     h.get_events)
    dsp.add("GET",  r"/v0/agents/(?P<correlationid>[a-z0-9-]+)/events\.jsonl", h.get_events_jsonl)
    dsp.add("GET",  r"/v0/agents/(?P<correlationid>[a-z0-9-]+)/events\.sse",   h.get_events_sse)
    # §16 Gap B: per-correlation claude transcript checkpoint. PUT by EventRunner
    # after each turn, GET before a mode=continue turn on a cold pod.
    dsp.add("PUT",  r"/v0/agents/(?P<correlationid>[a-z0-9-]+)/transcript",   h.put_transcript)
    dsp.add("GET",  r"/v0/agents/(?P<correlationid>[a-z0-9-]+)/transcript",   h.get_transcript)
    # §21 agent groups. Registered before the agent routes purely for readability;
    # the path prefixes are disjoint so order does not matter.
    dsp.add("POST", r"/v0/groups",                                          h.create_group)
    dsp.add("GET",  r"/v0/groups",                                          h.list_groups)
    dsp.add("GET",  r"/v0/groups/(?P<groupid>[a-z0-9-]+)",                  h.group_html)
    dsp.add("GET",  r"/v0/groups/(?P<groupid>[a-z0-9-]+)/status",           h.group_status)
    dsp.add("POST", r"/v0/groups/(?P<groupid>[a-z0-9-]+)/close",            h.close_group)
    dsp.add("POST", r"/v0/groups/(?P<groupid>[a-z0-9-]+)/cancel",           h.cancel_group)
    dsp.add("GET",  r"/openapi\.json", h.openapi_json)
    dsp.add("GET",  r"/docs",          h.docs)
    dsp.add("GET",  r"/healthz",       h.healthz)
    dsp.add("GET",  r"/v0/selftest",   h.selftest)

    def app(environ, start_response):
        return dsp.dispatch(environ.get("REQUEST_METHOD", "GET"),
                            environ.get("PATH_INFO", "/"),
                            environ, start_response)

    host, port_s = cfg.http_addr.rsplit(":", 1)
    server = make((host, int(port_s)), app)
    print(f"[eventbridge] listening on http://{cfg.http_addr}  docs=/docs (Ctrl-C to stop)")

    # serve_forever() must run OFF the main thread — shutdown() blocks if it is
    # called from the same thread that is inside serve_forever().
    serve_thread = threading.Thread(target=server.serve_forever, name="wsgi-serve", daemon=True)
    serve_thread.start()

    # Run the URL self-test once the socket has bound so its verdicts reflect
    # reality (whether each candidate address actually answers).
    def _startup_selftest():
        import time

        from eventbridge.selftest import format_report, probe_candidates
        time.sleep(0.4)
        try:
            _h, _p = cfg.http_addr.rsplit(":", 1)
            results = probe_candidates(int(_p), current=cfg.public_base_url)
            print(format_report(results))
        except Exception as e:  # noqa: BLE001
            print(f"[selftest] failed: {e}")
    threading.Thread(target=_startup_selftest, daemon=True, name="startup-selftest").start()

    # §21.9.2: sweep group deadlines. A group short one member would otherwise never
    # complete and never notify, and silence is the worst failure mode for a batch the
    # operator has walked away from.
    def _deadline_sweeper():
        while not stop_evt.wait(timeout=15.0):
            try:
                n = groups.sweep_deadlines()
                if n:
                    print(f"[groups] completed {n} group(s) on deadline")
            except Exception as e:  # noqa: BLE001
                print(f"[groups] deadline sweep failed: {e!r}")

    stop_evt = threading.Event()
    threading.Thread(target=_deadline_sweeper, daemon=True, name="group-deadlines").start()
    signal_count = {"n": 0}

    def _sig(signum, _frame):
        signal_count["n"] += 1
        if signal_count["n"] == 1:
            print(f"\n[eventbridge] caught signal {signum}, shutting down (Ctrl-C again to force)")
            stop_evt.set()
        else:
            print("\n[eventbridge] second signal — forcing exit")
            import os as _os
            _os._exit(130)

    signal.signal(signal.SIGINT,  _sig)
    signal.signal(signal.SIGTERM, _sig)

    try:
        while not stop_evt.wait(timeout=1.0):
            pass
    finally:
        try: server.shutdown()
        except Exception as e: print(f"[eventbridge] server.shutdown error: {e}")
        try: server.server_close()
        except Exception: pass
        serve_thread.join(timeout=5.0)
        consumer.stop(); consumer.join(timeout=5.0)
        requests_mirror.stop(); requests_mirror.join(timeout=5.0)
        group_mirror.stop(); group_mirror.join(timeout=5.0)
        ntfy.stop()
        if ntfy.is_alive(): ntfy.join(timeout=5.0)
        producer.close()
        pidfile.__exit__(None, None, None)
        print("[eventbridge] shut down")
    return 0


if __name__ == "__main__":
    sys.exit(main())
