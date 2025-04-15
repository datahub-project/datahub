# pylint: skip-file
# mypy: ignore-errors
# fmt: off
# isort: skip_file
# flake8: noqa
import json
import unittest
from typing import Any
from unittest.mock import MagicMock, patch

from datahub.emitter.mcp import MetadataChangeProposalClass, _make_generic_aspect
from datahub.emitter.serialization_helper import post_json_transform
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeTypeClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    MetadataChangeLogClass,
    SystemMetadataClass,
    TagAssociationClass,
    TestDefinitionClass,
    TestDefinitionTypeClass,
    TestInfoClass,
    TestResultClass,
    TestResultsClass,
    TestResultTypeClass,
)
from datahub_actions.api.action_graph import AcrylDataHubGraph
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext

from datahub_integrations.actions.forward.forwarding_action import (
    ForwardingAction,
    ForwardingActionConfig,
    create_schema_mcp,
    create_tags_mcp,
    create_terms_mcp,
)


class TestCreateMcpFunctions(unittest.TestCase):

    def setUp(self) -> None:
        # Sample old and new objects for testing
        self.old_schema_obj = _make_generic_aspect(
            EditableSchemaMetadataClass(
                editableSchemaFieldInfo=[
                    EditableSchemaFieldInfoClass(
                        fieldPath="field1",
                        globalTags=GlobalTagsClass(
                            tags=[TagAssociationClass(tag="tag1")]
                        ),
                        glossaryTerms=GlossaryTermsClass(
                            terms=[
                                GlossaryTermAssociationClass(
                                    urn="urn:li:glossaryTerm:term2"
                                )
                            ],
                            auditStamp=AuditStampClass(
                                actor="urn:li:corpuser:actor", time=123
                            ),
                        ),
                    )
                ]
            )
        )
        self.new_schema_obj = _make_generic_aspect(
            EditableSchemaMetadataClass(
                editableSchemaFieldInfo=[
                    EditableSchemaFieldInfoClass(
                        fieldPath="field1",
                        globalTags=GlobalTagsClass(
                            tags=[TagAssociationClass(tag="tag2")]
                        ),
                    ),
                    EditableSchemaFieldInfoClass(
                        fieldPath="field2",
                        globalTags=GlobalTagsClass(
                            tags=[TagAssociationClass(tag="tag3")]
                        ),
                        glossaryTerms=GlossaryTermsClass(
                            terms=[
                                GlossaryTermAssociationClass(
                                    urn="urn:li:glossaryTerm:term3"
                                )
                            ],
                            auditStamp=AuditStampClass(
                                actor="urn:li:corpuser:actor", time=123
                            ),
                        ),
                    ),
                ]
            )
        )

        self.old_terms_obj = _make_generic_aspect(
            GlossaryTermsClass(
                terms=[GlossaryTermAssociationClass(urn="urn:li:glossaryTerm:term1")],
                auditStamp=AuditStampClass(actor="urn:li:corpuser:actor", time=123),
            )
        )
        self.new_terms_obj = _make_generic_aspect(
            GlossaryTermsClass(
                terms=[GlossaryTermAssociationClass(urn="urn:li:glossaryTerm:term2")],
                auditStamp=AuditStampClass(actor="urn:li:corpuser:actor", time=123),
            )
        )

        self.old_tags_obj = _make_generic_aspect(
            GlobalTagsClass(tags=[TagAssociationClass(tag="tag1")])
        )
        self.new_tags_obj = _make_generic_aspect(
            GlobalTagsClass(tags=[TagAssociationClass(tag="tag2")])
        )

        self.orig_event = MetadataChangeLogClass(
            entityUrn="urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
        )

    def test_create_schema_mcp(self) -> None:
        mcps = create_schema_mcp(
            self.old_schema_obj, self.new_schema_obj, self.orig_event
        )
        self.assertIsNotNone(mcps)
        self.assertTrue(
            all(isinstance(mcp, MetadataChangeProposalClass) for mcp in mcps)
        )
        for mcp in mcps:
            aspect = mcp.aspect
            serialized = aspect.value.decode() if aspect.value else ""
            objs = post_json_transform(json.loads(serialized))
            self.assertEqual(objs[0].get("op"), "add", msg=objs[0])
            self.assertEqual(
                objs[0].get("path"),
                "/editableSchemaFieldInfo/field1/globalTags/tags/urn:li:tag:tag2",
                msg=objs[0],
            )
            self.assertEqual(objs[1].get("op"), "add", msg=objs[1])
            self.assertEqual(
                objs[1].get("path"),
                "/editableSchemaFieldInfo/field2/glossaryTerms/terms/urn:li:glossaryTerm:term3",
                msg=objs[1],
            )
            self.assertEqual(objs[2].get("op"), "add", msg=objs[2])
            self.assertEqual(
                objs[2].get("path"),
                "/editableSchemaFieldInfo/field2/globalTags/tags/urn:li:tag:tag3",
                msg=objs[2],
            )
            self.assertEqual(objs[3].get("op"), "remove", msg=objs[3])
            self.assertEqual(
                objs[3].get("path"),
                "/editableSchemaFieldInfo/field1/glossaryTerms/terms/urn:li:glossaryTerm:term2",
                msg=objs[3],
            )
            self.assertEqual(objs[4].get("op"), "remove", msg=objs[4])
            self.assertEqual(
                objs[4].get("path"),
                "/editableSchemaFieldInfo/field1/globalTags/tags/urn:li:tag:tag1",
                msg=objs[4],
            )

    def test_create_terms_mcp(self) -> None:
        mcps = create_terms_mcp(self.old_terms_obj, self.new_terms_obj, self.orig_event)
        self.assertIsNotNone(mcps)
        self.assertTrue(
            all(isinstance(mcp, MetadataChangeProposalClass) for mcp in mcps)
        )
        for mcp in mcps:
            aspect = mcp.aspect
            serialized = aspect.value.decode() if aspect.value else ""
            objs = post_json_transform(json.loads(serialized))
            self.assertEqual(objs[0].get("op"), "add", msg=objs[0])
            self.assertEqual(
                objs[0].get("path"), "/terms/urn:li:glossaryTerm:term2", msg=objs[0]
            )
            self.assertEqual(objs[1].get("op"), "remove", msg=objs[1])
            self.assertEqual(
                objs[1].get("path"), "/terms/urn:li:glossaryTerm:term1", msg=objs[1]
            )

    def test_create_tags_mcp(self) -> None:
        mcps = create_tags_mcp(self.old_tags_obj, self.new_tags_obj, self.orig_event)
        self.assertIsNotNone(mcps)
        self.assertTrue(
            all(isinstance(mcp, MetadataChangeProposalClass) for mcp in mcps)
        )
        for mcp in mcps:
            aspect = mcp.aspect
            serialized = aspect.value.decode() if aspect.value else ""
            objs = post_json_transform(json.loads(serialized))
            self.assertEqual(objs[0].get("op"), "add", msg=objs[0])
            self.assertEqual(objs[0].get("path"), "/tags/urn:li:tag:tag2", msg=objs[0])
            self.assertEqual(objs[1].get("op"), "remove", msg=objs[1])
            self.assertEqual(objs[1].get("path"), "/tags/urn:li:tag:tag1", msg=objs[1])


class TestForwardingAction(unittest.TestCase):

    @patch.object(
        ForwardingAction,
        "__init__",
        lambda self, config, ctx: setattr(self, "config", MagicMock())
        or setattr(self, "kafka_emitter", MagicMock()),
    )
    def setUp(self) -> None:
        self.config_dict = {
            "kafka_server": "localhost:9092",
            "schema_registry_url": "http://localhost:8081",
            "ssl_ca_location": "/path/to/ca",
            "ssl_cert_location": "/path/to/cert",
            "ssl_key_location": "/path/to/key",
            "ssl_key_password": "password",
            "group_id": "group1",
            "schema_registry_ca_location": "/path/to/schema/ca",
            "schema_registry_cert_location": "/path/to/schema/cert",
            "schema_registry_key_location": "/path/to/schema/key",
            "mcp_topic": "MetadataChangeProposal",
        }
        self.ctx = PipelineContext(
            graph=MagicMock(spec=AcrylDataHubGraph), pipeline_name="pipeline"
        )
        self.action = ForwardingAction(
            ForwardingActionConfig.parse_obj(self.config_dict), self.ctx
        )

        self.old_test_results = _make_generic_aspect(
            TestResultsClass(
                passing=[
                    TestResultClass(
                        test="test1",
                        type=TestResultTypeClass.SUCCESS,
                        testDefinitionMd5="12345",
                        lastComputed=AuditStampClass(
                            actor="urn:li:corpuser:actor", time=123
                        ),
                    ),
                    TestResultClass(
                        test="test2",
                        type=TestResultTypeClass.SUCCESS,
                        testDefinitionMd5="12345",
                        lastComputed=AuditStampClass(
                            actor="urn:li:corpuser:actor", time=123
                        ),
                    ),
                ],
                failing=[
                    TestResultClass(
                        test="test3",
                        type=TestResultTypeClass.FAILURE,
                        testDefinitionMd5="12345",
                        lastComputed=AuditStampClass(
                            actor="urn:li:corpuser:actor", time=123
                        ),
                    ),
                    TestResultClass(
                        test="test4",
                        type=TestResultTypeClass.FAILURE,
                        testDefinitionMd5="12345",
                        lastComputed=AuditStampClass(
                            actor="urn:li:corpuser:actor", time=123
                        ),
                    ),
                ],
            )
        )

        self.new_test_results = _make_generic_aspect(
            TestResultsClass(
                passing=[
                    TestResultClass(
                        test="test5",
                        type=TestResultTypeClass.SUCCESS,
                        testDefinitionMd5="12345",
                        lastComputed=AuditStampClass(
                            actor="urn:li:corpuser:actor", time=123
                        ),
                    ),
                    TestResultClass(
                        test="test6",
                        type=TestResultTypeClass.SUCCESS,
                        testDefinitionMd5="12345",
                        lastComputed=AuditStampClass(
                            actor="urn:li:corpuser:actor", time=123
                        ),
                    ),
                ],
                failing=[
                    TestResultClass(
                        test="test7",
                        type=TestResultTypeClass.FAILURE,
                        testDefinitionMd5="12345",
                        lastComputed=AuditStampClass(
                            actor="urn:li:corpuser:actor", time=123
                        ),
                    ),
                    TestResultClass(
                        test="test8",
                        type=TestResultTypeClass.FAILURE,
                        testDefinitionMd5="12345",
                        lastComputed=AuditStampClass(
                            actor="urn:li:corpuser:actor", time=123
                        ),
                    ),
                ],
            )
        )

        self.old_test_info = _make_generic_aspect(
            TestInfoClass(
                name="testOldName",
                category="testOldCategory",
                lastUpdatedTimestamp=1234,
                definition=TestDefinitionClass(
                    type=TestDefinitionTypeClass.JSON,
                ),
            )
        )

        self.new_test_info = _make_generic_aspect(
            TestInfoClass(
                name="testName",
                category="testCategory",
                lastUpdatedTimestamp=12345,
                definition=TestDefinitionClass(
                    type=TestDefinitionTypeClass.JSON,
                ),
            )
        )


    @patch.object(
        ForwardingAction,
        "__init__",
        lambda self, config, ctx: setattr(self, "config", MagicMock())
        or setattr(self, "kafka_emitter", MagicMock()),
    )
    def test_buildMcpTestResults(self) -> None:
        mock_event = MetadataChangeLogClass(
            aspect=self.new_test_results,
            previousAspectValue=self.old_test_results,
            aspectName="testResults",
            entityUrn="urn:li:test:test123",
            entityType="test",
            changeType=ChangeTypeClass.UPSERT,
        )

        mcps = self.action.buildMcp(mock_event)

        self.assertIsNotNone(mcps)
        for mcp in mcps:
            aspect = mcp.aspect
            serialized = aspect.value.decode() if aspect.value else ""
            aspect_converted = post_json_transform(json.loads(serialized))
            for failingTest in aspect_converted.get("failing"):
                self.assertEqual(failingTest.get("lastComputed"), None, failingTest)
                self.assertEqual(
                    failingTest.get("testDefinitionMd5"), None, failingTest
                )
                self.assertEqual(failingTest.get("type"), TestResultTypeClass.FAILURE, failingTest)
            for passingTest in aspect_converted.get("passing"):
                self.assertEqual(passingTest.get("lastComputed"), None, passingTest)
                self.assertEqual(
                    passingTest.get("testDefinitionMd5"), None, passingTest
                )
                self.assertEqual(passingTest.get("type"), TestResultTypeClass.SUCCESS, passingTest)

    @patch.object(
        ForwardingAction,
        "__init__",
        lambda self, config, ctx: setattr(self, "config", MagicMock())
        or setattr(self, "kafka_emitter", MagicMock()),
    )
    def test_buildMcpGlobalTags(self) -> None:
        global_tags = _make_generic_aspect(
            GlobalTagsClass(
                tags=[
                    TagAssociationClass(
                        tag="tag1",
                    )
                ]
            )
        )

        mock_event = MetadataChangeLogClass(
            aspect=global_tags,
            aspectName="globalTags",
            entityUrn="urn:li:dataset:(urn:li:dataPlatform:kafka,kafkadata,PROD)",
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            systemMetadata=SystemMetadataClass(
                lastObserved=0,
                runId="no-run-id-provided",
                lastRunId="no-run-id-provided",
                properties={
                    "appSource": "metadataTests",
                },
                version=0,
            ),
        )

        mcps = self.action.buildMcp(mock_event)

        self.assertIsNotNone(mcps)
        for mcp in mcps:
            aspect = mcp.aspect
            serialized = aspect.value.decode() if aspect.value else ""
            aspect_converted = post_json_transform(json.loads(serialized))
            for aspect_patch in aspect_converted:
                self.assertEqual(aspect_patch.get("op"), "add")
                self.assertEqual(aspect_patch.get("path"), "/tags/urn:li:tag:tag1")

    @patch.object(
        ForwardingAction,
        "__init__",
        lambda self, config, ctx: setattr(self, "config", MagicMock())
        or setattr(self, "kafka_emitter", MagicMock()),
    )
    def test_act(self) -> None:
        mock_event = EventEnvelope(
            event_type="MetadataChangeLogEvent_v1",
            event=MagicMock(spec=MetadataChangeLogClass),
            meta=MagicMock(spec=dict[str, Any]),
        )
        mock_event.event.systemMetadata.properties.get.return_value = "metadataTests"
        self.action.buildMcp = MagicMock(
            return_value=[MagicMock(spec=MetadataChangeProposalClass)]
        )
        self.action.emit = MagicMock()

        self.action.act(mock_event)

        mock_event.event.systemMetadata.properties.get.assert_called_once_with(
            "appSource"
        )
        self.action.buildMcp.assert_called_once_with(mock_event.event)
        self.action.emit.assert_called()

    @patch.object(
        ForwardingAction,
        "__init__",
        lambda self, config, ctx: setattr(self, "config", MagicMock())
        or setattr(self, "kafka_emitter", MagicMock()),
    )
    def test_buildMcp(self) -> None:
        mock_event = MagicMock(spec=MetadataChangeLogClass)
        mock_event.get.side_effect = lambda key: self.config_dict.get(key)

        mcps = self.action.buildMcp(mock_event)

        self.assertIsNotNone(mcps)

    @patch.object(
        ForwardingAction,
        "__init__",
        lambda self, config, ctx: setattr(self, "config", MagicMock())
        or setattr(self, "kafka_emitter", MagicMock()),
    )
    def test_emit(self) -> None:
        mock_mcp = MagicMock(spec=MetadataChangeProposalClass)
        self.action.emit(mock_mcp)

        self.action.kafka_emitter.emit.assert_called_once_with(mock_mcp)

    @patch.object(
        ForwardingAction,
        "__init__",
        lambda self, config, ctx: setattr(self, "config", MagicMock())
        or setattr(self, "kafka_emitter", MagicMock()),
    )
    def test_buildMcpTestInfo(self) -> None:
        mock_event = MetadataChangeLogClass(
            aspect=self.new_test_info,
            previousAspectValue=self.old_test_info,
            aspectName="testInfo",
            entityUrn="urn:li:test:test123",
            entityType="test",
            changeType=ChangeTypeClass.UPSERT,
        )

        mcps = self.action.buildMcp(mock_event)

        self.assertIsNotNone(mcps)
        for mcp in mcps:
            aspect = mcp.aspect
            serialized = aspect.value.decode() if aspect.value else ""
            aspect_converted = post_json_transform(json.loads(serialized))
            self.assertEqual(aspect_converted.get("lastUpdatedTimestamp"), None, aspect_converted)
            self.assertEqual(aspect_converted.get("name"), "testName", aspect_converted)

    @patch.object(
        ForwardingAction,
        "__init__",
        lambda self, config, ctx: setattr(self, "config", MagicMock())
                                  or setattr(self, "kafka_emitter", MagicMock())
                                  or setattr(self, "forms_enabled", False),
    )
    def test_act_forms_disabled(self) -> None:
        """Test that when forms_enabled is False, no MCPs are produced for form aspects."""
        # Create a mock event with form-related aspect
        mock_event = EventEnvelope(
            event_type="MetadataChangeLogEvent_v1",
            event=MagicMock(spec=MetadataChangeLogClass),
            meta=MagicMock(spec=dict[str, Any]),
        )
        mock_event.event.aspectName = "formInfo"

        # Mock the buildMcp method to track if it's called
        self.action.buildMcp = MagicMock()
        self.action.emit = MagicMock()
        self.action.forms_enabled = False

        # Call act
        self.action.act(mock_event)

        # Verify buildMcp was not called because forms_enabled is False
        self.action.buildMcp.assert_not_called()
        self.action.emit.assert_not_called()

    @patch.object(
        ForwardingAction,
        "__init__",
        lambda self, config, ctx: setattr(self, "config", MagicMock())
                                  or setattr(self, "kafka_emitter", MagicMock())
                                  or setattr(self, "forms_enabled", True),
    )
    def test_act_forms_enabled(self) -> None:
        """Test that when forms_enabled is True, MCPs are produced for form aspects."""
        # Create a mock event with form-related aspect
        mock_event = EventEnvelope(
            event_type="MetadataChangeLogEvent_v1",
            event=MagicMock(spec=MetadataChangeLogClass),
            meta=MagicMock(spec=dict[str, Any]),
        )
        mock_event.event.aspectName = "formInfo"

        # Create a mock MCP to return
        mock_mcp = MagicMock(spec=MetadataChangeProposalClass)

        # Mock the buildMcp method to return our mock MCP
        self.action.buildMcp = MagicMock(return_value=[mock_mcp])
        self.action.emit = MagicMock()
        self.action.forms_enabled = True

        # Call act
        self.action.act(mock_event)

        # Verify buildMcp was called because forms_enabled is True
        self.action.buildMcp.assert_called_once_with(mock_event.event)
        self.action.emit.assert_called_once_with(mock_mcp)


if __name__ == "__main__":
    unittest.main()
