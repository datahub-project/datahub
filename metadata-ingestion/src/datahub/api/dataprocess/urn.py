from dataclasses import dataclass

import datahub.emitter.mce_builder as builder


@dataclass(frozen=True, eq=True)
class UrnBase:
    id: str

    @property
    def urn(self) -> str:
        raise NotImplementedError("Urn needs to be implemented")


@dataclass(frozen=True, eq=True)
class DataFlowUrn(UrnBase):
    cluster: str
    orchestrator: str

    @property
    def urn(self) -> str:
        return builder.make_data_flow_urn(
            orchestrator=self.orchestrator, flow_id=self.id, cluster=self.cluster
        )


@dataclass(frozen=True, eq=True)
class DataJobUrn(UrnBase):
    flow_urn: DataFlowUrn

    @property
    def urn(self) -> str:
        return builder.make_data_job_urn_with_flow(self.flow_urn.urn, self.id)
