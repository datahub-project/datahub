from datahub.ingestion.agent.verdicts import ProbeArgumentError, ProbeSoftError


def test_probe_argument_error_is_a_value_error_but_not_a_soft_error() -> None:
    err = ProbeArgumentError("no project titled 'x'")
    assert isinstance(err, ValueError)
    assert not isinstance(err, ProbeSoftError)
