from typing import Iterable

from datahub.emitter.mce_builder import make_data_job_urn, make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import get_default_graph
from datahub.metadata.schema_classes import (
    DataJobInputOutputClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    List,
    StatusClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.utilities.urns.dataset_urn import DatasetUrn


def lineage_mcp_generator(
    urn: str, upstreams: List[str]
) -> Iterable[MetadataChangeProposalWrapper]:
    yield MetadataChangeProposalWrapper(
        entityUrn=urn,
        aspect=UpstreamLineageClass(
            upstreams=[
                UpstreamClass(
                    dataset=upstream,
                    type=DatasetLineageTypeClass.TRANSFORMED,
                )
                for upstream in upstreams
            ]
        ),
    )
    for upstream in upstreams:
        yield MetadataChangeProposalWrapper(
            entityUrn=upstream, aspect=StatusClass(removed=False)
        )
    for urn_itr in [urn, *upstreams]:
        yield MetadataChangeProposalWrapper(
            entityUrn=urn_itr,
            aspect=DatasetPropertiesClass(name=DatasetUrn.from_string(urn_itr).name),
        )


def datajob_lineage_mcp_generator(
    urn: str, upstreams: List[str], downstreams: List[str]
) -> Iterable[MetadataChangeProposalWrapper]:
    yield MetadataChangeProposalWrapper(
        entityUrn=urn,
        aspect=DataJobInputOutputClass(
            inputDatasets=upstreams,
            outputDatasets=downstreams,
        ),
    )
    for upstream in upstreams:
        yield MetadataChangeProposalWrapper(
            entityUrn=upstream, aspect=StatusClass(removed=False)
        )
    for downstream in downstreams:
        yield MetadataChangeProposalWrapper(
            entityUrn=downstream, aspect=StatusClass(removed=False)
        )


def scenario_truncate_basic():
    """searchAcrossLineage(root, depth=n, breadth=3, skip=None)
    All 21 urns.
    """

    path = "truncate.basic"
    root_urn = make_dataset_urn("snowflake", f"{path}.root")

    yield from lineage_mcp_generator(
        root_urn,
        [make_dataset_urn("snowflake", f"{path}.u_{i}") for i in range(10)],
    )

    for i in range(10):
        yield from lineage_mcp_generator(
            make_dataset_urn("snowflake", f"{path}.d_{i}"), [root_urn]
        )


def scenario_truncate_intermediate():
    """searchAcrossLineage(root, depth=3, skip=None)
    1 root urn, all 3 direct upstreams and downstreams, and then 4 urns for each 'expanded' urn.
    Total 1 + 3 + 4*3 = 16 urns.
    """

    path = "truncate.intermediate"
    root_urn = make_dataset_urn("snowflake", f"{path}.root")

    yield from lineage_mcp_generator(
        root_urn, [make_dataset_urn("snowflake", f"{path}.u_{i}") for i in range(10)]
    )

    for i in range(3):
        yield from lineage_mcp_generator(
            make_dataset_urn("snowflake", f"{path}.u_{i}"),
            [make_dataset_urn("snowflake", f"{path}.u_{i}_u_{j}") for j in range(3)],
        )

    for i in range(3):
        yield from lineage_mcp_generator(
            make_dataset_urn("snowflake", f"{path}.d_{i}"), [root_urn]
        )
        for j in range(3):
            yield from lineage_mcp_generator(
                make_dataset_urn("snowflake", f"{path}.d_{i}d_{j}"),
                [make_dataset_urn("snowflake", f"{path}.d_{i}")],
            )


def scenario_truncate_complex():
    """searchAcrossLineage(root, depth=n, breadth=3, skip=None)
    1 root urn,
    direct (lvl a) upstream,
    its two (lvl b) upstreams,
    each of their 3 (lvl c) upstreams,
    each of their 4 (lvl d) upstreams,
    then, for three of the lvl d nodes, 5 (lvl e) upstreams each,
    then, for two of the lvl e nodes, 6 (lvl f) upstreams, and for the other lvl e node, 1 (lvl f) upstream.
    Total 1 + 1 + 2 + (2 * 3) + (2 * 3 * 4) + (2 * 3 * 3 * 5) + (2 * 3 * 3 * 2 * 6) + (2 * 3 * 3 * 1 * 1) = 358 urns.
    """

    path = "truncate.complex"
    root_urn = make_dataset_urn("snowflake", f"{path}.root")
    lvl_a = make_dataset_urn("snowflake", f"{path}.u_0")
    lvl_b = {i: make_dataset_urn("snowflake", f"{path}.u_0_u_{i}") for i in range(2)}
    lvl_c = {
        (a, b): make_dataset_urn("snowflake", f"{path}.u_0_u_{a}_u_{b}")
        for a in range(2)
        for b in range(3)
    }
    lvl_d = {
        (a, b, c): make_dataset_urn("snowflake", f"{path}.u_0_u_{a}_u_{b}_u_{c}")
        for a in range(2)
        for b in range(3)
        for c in range(4)
    }
    lvl_e = {
        (a, b, c, d): make_dataset_urn(
            "snowflake", f"{path}.u_0_u_{a}_u_{b}_u_{c}_u_{d}"
        )
        for a in range(2)
        for b in range(3)
        for c in range(4)
        for d in range(5)
    }
    lvl_f = {
        (a, b, c, d, e): make_dataset_urn(
            "snowflake", f"{path}.u_0_u_{a}_u_{b}_u_{c}_u_{d}_u_{e}"
        )
        for a in range(2)
        for b in range(3)
        for c in range(4)
        for d in range(5)
        for e in range(6 if d % 2 == 0 else 1)
    }

    yield from lineage_mcp_generator(root_urn, [lvl_a])
    yield from lineage_mcp_generator(lvl_a, list(lvl_b.values()))
    for a, urn in lvl_b.items():
        yield from lineage_mcp_generator(urn, [lvl_c[(a, b)] for b in range(3)])
    for (a, b), urn in lvl_c.items():
        yield from lineage_mcp_generator(urn, [lvl_d[(a, b, c)] for c in range(4)])
    for (a, b, c), urn in lvl_d.items():
        yield from lineage_mcp_generator(urn, [lvl_e[(a, b, c, d)] for d in range(5)])
    for (a, b, c, d), urn in lvl_e.items():
        yield from lineage_mcp_generator(
            urn, [lvl_f[(a, b, c, d, e)] for e in range(6 if d % 2 == 0 else 1)]
        )


def scenario_skip_basic():
    """searchAcrossLineage(root, depth=1, breadth=10, skip=[{type: "dataJob"}, {type: "dataset", platform: "urn:li:dataPlatform:dbt"}])
    1 root urn, both airflow nodes, both dbt nodes, and all 6 snowflake neighbors.
    Total 1 + 2 + 2 + 6 = 11 urns.
    """
    path = "skip.basic"
    root_urn = make_dataset_urn("snowflake", f"{path}.root")
    upstream_dbt_urn = make_dataset_urn("dbt", f"{path}.u_0")
    upstream_airflow_urn = make_data_job_urn("airflow", f"{path}.flow", f"{path}.u_0")

    yield from lineage_mcp_generator(
        root_urn,
        [
            make_dataset_urn("snowflake", f"{path}.u_direct"),
            upstream_dbt_urn,
        ],
    )
    yield from lineage_mcp_generator(
        upstream_dbt_urn,
        [make_dataset_urn("snowflake", f"{path}.u_through_dbt")],
    )
    yield from datajob_lineage_mcp_generator(
        upstream_airflow_urn,
        [make_dataset_urn("snowflake", f"{path}.u_through_airflow")],
        [root_urn],
    )

    downstream_dbt_urn = make_dataset_urn("dbt", f"{path}.d_0")
    downstream_airflow_urn = make_data_job_urn("airflow", f"{path}.flow", f"{path}.d_0")
    yield from lineage_mcp_generator(
        make_dataset_urn("snowflake", f"{path}.d_direct"),
        [root_urn],
    )
    yield from lineage_mcp_generator(
        downstream_dbt_urn,
        [root_urn],
    )
    yield from lineage_mcp_generator(
        make_dataset_urn("snowflake", f"{path}.d_through_dbt"),
        [downstream_dbt_urn],
    )
    yield from datajob_lineage_mcp_generator(
        downstream_airflow_urn,
        [root_urn],
        [make_dataset_urn("snowflake", f"{path}.d_through_airflow")],
    )


def scenario_skip_intermediate():
    """searchAcrossLineage(root, depth=1, breadth=10, skip=[{type: "dataJob"}, {type: "dataset", platform: "urn:li:dataPlatform:dbt"}])
    1 root urn and all nodes aside from those upstream of `skip.intermediate.u_indirect_1`.
    Total 11 urns.
    searchAcrossLineage(root, depth=2, breadth=10, skip=[{type: "dataJob"}, {type: "dataset", platform: "urn:li:dataPlatform:dbt"}])
    All 14 urns.
    """
    path = "skip.intermediate"
    root_urn = make_dataset_urn("snowflake", f"{path}.root")
    upstream_dbt_urns = [make_dataset_urn("dbt", f"{path}.u_{i}") for i in range(6)]
    upstream_airflow_urn = make_data_job_urn("airflow", f"{path}.flow", f"{path}.u_0")

    yield from lineage_mcp_generator(
        root_urn,
        [
            make_dataset_urn("snowflake", f"{path}.u_direct"),
            upstream_dbt_urns[0],
        ],
    )
    yield from datajob_lineage_mcp_generator(
        upstream_airflow_urn, [upstream_dbt_urns[1]], [upstream_dbt_urns[0]]
    )
    yield from lineage_mcp_generator(
        upstream_dbt_urns[1],
        [
            upstream_dbt_urns[2],
        ],
    )
    yield from lineage_mcp_generator(
        upstream_dbt_urns[2],
        [
            upstream_dbt_urns[3],
            upstream_dbt_urns[4],
        ],
    )
    yield from lineage_mcp_generator(
        upstream_dbt_urns[3],
        [make_dataset_urn("snowflake", f"{path}.u_indirect_0")],
    )
    yield from lineage_mcp_generator(
        upstream_dbt_urns[4],
        [
            make_dataset_urn("snowflake", f"{path}.u_indirect_1"),
            make_dataset_urn("snowflake", f"{path}.u_indirect_2"),
        ],
    )
    yield from lineage_mcp_generator(
        make_dataset_urn("snowflake", f"{path}.u_indirect_1"),
        [make_dataset_urn("snowflake", f"{path}.u_depth_2"), upstream_dbt_urns[5]],
    )
    yield from lineage_mcp_generator(
        upstream_dbt_urns[5],
        [
            make_dataset_urn("snowflake", f"{path}.u_depth_2_indirect"),
        ],
    )


def scenario_skip_complex():
    """searchAcrossLineage(root, depth=1, breadth=1, skip=[{type: "dataJob"}, {type: "dataset", platform: "urn:li:dataPlatform:dbt"}])
    The 11 urns from scenario_skip_intermediate, plus 2 snowflake urns and 1 dbt node from the single expanded upstream.
    Total 14 urns.
    """
    path = "skip.complex"
    root_urn = make_dataset_urn("snowflake", f"{path}.root")
    upstream_dbt_urns = [make_dataset_urn("dbt", f"{path}.u_{i}") for i in range(5)]
    upstream_airflow_urn = make_data_job_urn("airflow", f"{path}.flow", f"{path}.u_0")
    depth_one_snowflake_urns = {
        "direct": make_dataset_urn("snowflake", f"{path}.u_direct"),
        "indirect_0": make_dataset_urn("snowflake", f"{path}.u_indirect_0"),
        "indirect_1": make_dataset_urn("snowflake", f"{path}.u_indirect_1"),
        "indirect_2": make_dataset_urn("snowflake", f"{path}.u_indirect_2"),
    }

    yield from lineage_mcp_generator(
        root_urn,
        [
            depth_one_snowflake_urns["direct"],
            upstream_dbt_urns[0],
        ],
    )
    yield from datajob_lineage_mcp_generator(
        upstream_airflow_urn, [upstream_dbt_urns[1]], [upstream_dbt_urns[0]]
    )
    yield from lineage_mcp_generator(
        upstream_dbt_urns[1],
        [
            upstream_dbt_urns[2],
        ],
    )
    yield from lineage_mcp_generator(
        upstream_dbt_urns[2],
        [
            upstream_dbt_urns[3],
            upstream_dbt_urns[4],
        ],
    )
    yield from lineage_mcp_generator(
        upstream_dbt_urns[3],
        [depth_one_snowflake_urns["indirect_0"]],
    )
    yield from lineage_mcp_generator(
        upstream_dbt_urns[4],
        [
            depth_one_snowflake_urns["indirect_1"],
            depth_one_snowflake_urns["indirect_2"],
        ],
    )

    for name, urn in depth_one_snowflake_urns.items():
        dbt_urn = make_dataset_urn("dbt", f"{path}.u_{name}")
        yield from lineage_mcp_generator(
            urn,
            [make_dataset_urn("snowflake", f"{path}.direct_u_{name}"), dbt_urn],
        )
        yield from lineage_mcp_generator(
            dbt_urn,
            [make_dataset_urn("snowflake", f"{path}.indirect_u_{name}")],
        )


def scenario_perf():
    """searchAcrossLineage(root, depth=n, breadth=3, skip=None)
    1 root urn,
    direct (lvl a) upstream,
    its 100 (lvl b) upstreams,
    each of their 30 (lvl c) upstreams,
    each of their 40 (lvl d) upstreams,
    then, 50 (lvl e) upstreams each,
    then, half of lvl e nodes, 6 (lvl f) upstreams, and for the other lvl e node, 1 (lvl f) upstream.
    Total 1 + 1 + 100 + (100 * 30) + (100 * 30 * 40) + (100 * 30 * 40 * 5) = 723,102 urns.
    Disabled by default to avoid overloading
    """

    path = "lineage.perf"
    root_urn = make_dataset_urn("snowflake", f"{path}.root")
    lvl_a = make_dataset_urn("snowflake", f"{path}.u_0")
    lvl_b = {i: make_dataset_urn("snowflake", f"{path}.u_0_u_{i}") for i in range(100)}
    lvl_c = {
        (a, b): make_dataset_urn("snowflake", f"{path}.u_0_u_{a}_u_{b}")
        for a in range(100)
        for b in range(30)
    }
    lvl_d = {
        (a, b, c): make_dataset_urn("snowflake", f"{path}.u_0_u_{a}_u_{b}_u_{c}")
        for a in range(100)
        for b in range(30)
        for c in range(40)
    }
    lvl_e = {
        (a, b, c, d): make_dataset_urn(
            "snowflake", f"{path}.u_0_u_{a}_u_{b}_u_{c}_u_{d}"
        )
        for a in range(100)
        for b in range(30)
        for c in range(40)
        for d in range(5)
    }

    yield from lineage_mcp_generator(root_urn, [lvl_a])
    yield from lineage_mcp_generator(lvl_a, list(lvl_b.values()))
    for a, urn in lvl_b.items():
        yield from lineage_mcp_generator(urn, [lvl_c[(a, b)] for b in range(30)])
    for (a, b), urn in lvl_c.items():
        yield from lineage_mcp_generator(urn, [lvl_d[(a, b, c)] for c in range(40)])
    for (a, b, c), urn in lvl_d.items():
        yield from lineage_mcp_generator(urn, [lvl_e[(a, b, c, d)] for d in range(5)])


if __name__ == "__main__":
    graph = get_default_graph()
    for mcp in [
        *scenario_truncate_basic(),
        *scenario_truncate_intermediate(),
        *scenario_truncate_complex(),
        *scenario_skip_basic(),
        *scenario_skip_intermediate(),
        *scenario_skip_complex(),
        # *scenario_perf(),
    ]:
        graph.emit_mcp(mcp)
