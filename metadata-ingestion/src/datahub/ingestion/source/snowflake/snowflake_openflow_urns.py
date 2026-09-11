# EVERY urn this connector emits, and what bounds its length. Enumerated from
# the golden file's aspects rather than from memory, because seven separate
# length defects were found here one at a time, each fix guarding the case it
# had just been shown while leaving a sibling unmeasured. If a new aspect or
# urn-typed field is added, add its row here and say what bounds it.
#
#   dataFlow entity urn            _fitted ladder + _urn_is_emittable
#   dataJob entity urn             _fitted (flow reserves NESTED_JOB_HEADROOM)
#   dataJobInputOutput in/out      _edge_within_urn_limits (foreign; skipped,
#                                  never shortened -- they must match what the
#                                  warehouse ingestion wrote)
#   ownership owners[].owner       _owner_group_urn returns None if it will not
#                                  fit; Owner.owner is a Urn field, so
#                                  UrnAnnotationValidator length-checks it
#   dataPlatformInstance.instance  MAX_PLATFORM_INSTANCE_BYTES (480 bytes at
#                                  the limit, measured)
#   container entity urns          GUID: ContainerKey hashes its parts, so the
#                                  urn is 55 bytes whatever the input
#   browsePathsV2 path[].urn/.id   container urns, so GUID-bounded
#   container.container            parent container urn, GUID-bounded
#   dataJobInfo.flowUrn            the flow urn, already bounded above
#   dataPlatformInstance.platform  the constant "openflow"
#   ownership lastModified.actor   a constant corpuser urn
#
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
