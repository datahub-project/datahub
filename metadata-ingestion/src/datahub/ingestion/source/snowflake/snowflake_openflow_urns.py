# Shared by the config module and the source. It lives on its own because the
# source imports the config, so the config cannot import the source -- and an
# earlier revision resolved that by duplicating the encoder into both, which
# meant a fix to either could make recipe validation disagree with what the
# emitter actually produces.

# java.net.URLEncoder leaves only these unencoded; everything else becomes %XX
# per UTF-8 byte and a space becomes "+". Python's quote_plus does NOT agree:
# it passes "~" through where Java writes %7E, and encodes "*" where Java does
# not. The "~" direction under-measures, so quote_plus would pass urns that GMS
# then rejects.
_URLENCODER_SAFE = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-*_"
)

# GMS rejects an aspect whose URL-encoded urn exceeds this; see metadata-utils
# UrnValidationUtil.URN_NUM_BYTES_LIMIT.
MAX_URN_BYTES = 512

# A DataJob urn nests its DataFlow urn whole, so the flow must leave room for a
# job to exist beneath it: "urn:li:dataJob:(" + <flow urn> + "," + <job id> + ")".
# Encoded, that wrapper is 24 + 3 + 3 bytes and the shortest job id the fitter
# can produce is a 16-character digest.
NESTED_JOB_HEADROOM = 48
FLOW_URN_BUDGET = MAX_URN_BYTES - NESTED_JOB_HEADROOM

# What a flow urn costs before its platform_instance: the "urn:li:dataFlow:("
# wrapper, the orchestrator and env components, the "." joining instance to
# flow id, and the shortest flow id the fitter can produce. Derived rather than
# guessed, and pinned by a test that binary-searches the real builders -- an
# earlier revision used a round 200 here and rejected recipes that would have
# worked perfectly, which is its own kind of wrong.
_SHORTEST_FLOW_ID_OVERHEAD = 61
MAX_PLATFORM_INSTANCE_BYTES = FLOW_URN_BUDGET - _SHORTEST_FLOW_ID_OVERHEAD


def encoded_urn_len(urn: object) -> int:
    """The length GMS measures: java.net.URLEncoder.encode(urn).length()."""
    return sum(
        1 if char in _URLENCODER_SAFE or char == " " else 3 * len(char.encode())
        for char in str(urn)
    )
