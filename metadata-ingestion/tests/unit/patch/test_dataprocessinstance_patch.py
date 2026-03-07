# ABOUTME: Unit tests for DataProcessInstancePatchBuilder.
# ABOUTME: Tests input/output and edge add/remove operations.
from datahub.metadata.schema_classes import EdgeClass
from datahub.specific.dataprocessinstance import DataProcessInstancePatchBuilder


def test_add_input():
    """Test adding an input URN."""
    builder = DataProcessInstancePatchBuilder(
        "urn:li:dataProcessInstance:test-instance"
    )
    builder.add_input("urn:li:dataset:(urn:li:dataPlatform:hive,input_table,PROD)")

    result = builder.build()
    assert len(result) == 1
    assert result[0].aspectName == "dataProcessInstanceInput"
    assert result[0].changeType == "PATCH"
    assert b'"path": "/inputs/' in result[0].aspect.value


def test_remove_input():
    """Test removing an input URN."""
    builder = DataProcessInstancePatchBuilder(
        "urn:li:dataProcessInstance:test-instance"
    )
    builder.remove_input("urn:li:dataset:(urn:li:dataPlatform:hive,input_table,PROD)")

    result = builder.build()
    assert len(result) == 1
    assert result[0].aspectName == "dataProcessInstanceInput"
    assert b'"op": "remove"' in result[0].aspect.value
    assert b'"path": "/inputs/' in result[0].aspect.value


def test_add_input_edge_with_string():
    """Test adding an input edge using string URN."""
    builder = DataProcessInstancePatchBuilder(
        "urn:li:dataProcessInstance:test-instance"
    )
    builder.add_input_edge("urn:li:dataset:(urn:li:dataPlatform:hive,input_table,PROD)")

    result = builder.build()
    assert len(result) == 1
    assert result[0].aspectName == "dataProcessInstanceInput"
    assert b'"path": "/inputEdges/' in result[0].aspect.value
    assert b'"destinationUrn"' in result[0].aspect.value


def test_add_input_edge_with_edge_object():
    """Test adding an input edge using Edge object."""
    edge = EdgeClass(
        destinationUrn="urn:li:dataset:(urn:li:dataPlatform:hive,input_table,PROD)"
    )
    builder = DataProcessInstancePatchBuilder(
        "urn:li:dataProcessInstance:test-instance"
    )
    builder.add_input_edge(edge)

    result = builder.build()
    assert len(result) == 1
    assert result[0].aspectName == "dataProcessInstanceInput"
    assert b'"path": "/inputEdges/' in result[0].aspect.value


def test_remove_input_edge():
    """Test removing an input edge."""
    builder = DataProcessInstancePatchBuilder(
        "urn:li:dataProcessInstance:test-instance"
    )
    builder.remove_input_edge(
        "urn:li:dataset:(urn:li:dataPlatform:hive,input_table,PROD)"
    )

    result = builder.build()
    assert len(result) == 1
    assert result[0].aspectName == "dataProcessInstanceInput"
    assert b'"op": "remove"' in result[0].aspect.value
    assert b'"path": "/inputEdges/' in result[0].aspect.value


def test_add_output():
    """Test adding an output URN."""
    builder = DataProcessInstancePatchBuilder(
        "urn:li:dataProcessInstance:test-instance"
    )
    builder.add_output("urn:li:dataset:(urn:li:dataPlatform:hive,output_table,PROD)")

    result = builder.build()
    assert len(result) == 1
    assert result[0].aspectName == "dataProcessInstanceOutput"
    assert result[0].changeType == "PATCH"
    assert b'"path": "/outputs/' in result[0].aspect.value


def test_remove_output():
    """Test removing an output URN."""
    builder = DataProcessInstancePatchBuilder(
        "urn:li:dataProcessInstance:test-instance"
    )
    builder.remove_output("urn:li:dataset:(urn:li:dataPlatform:hive,output_table,PROD)")

    result = builder.build()
    assert len(result) == 1
    assert result[0].aspectName == "dataProcessInstanceOutput"
    assert b'"op": "remove"' in result[0].aspect.value
    assert b'"path": "/outputs/' in result[0].aspect.value


def test_add_output_edge_with_string():
    """Test adding an output edge using string URN."""
    builder = DataProcessInstancePatchBuilder(
        "urn:li:dataProcessInstance:test-instance"
    )
    builder.add_output_edge(
        "urn:li:dataset:(urn:li:dataPlatform:hive,output_table,PROD)"
    )

    result = builder.build()
    assert len(result) == 1
    assert result[0].aspectName == "dataProcessInstanceOutput"
    assert b'"path": "/outputEdges/' in result[0].aspect.value
    assert b'"destinationUrn"' in result[0].aspect.value


def test_add_output_edge_with_edge_object():
    """Test adding an output edge using Edge object."""
    edge = EdgeClass(
        destinationUrn="urn:li:dataset:(urn:li:dataPlatform:hive,output_table,PROD)"
    )
    builder = DataProcessInstancePatchBuilder(
        "urn:li:dataProcessInstance:test-instance"
    )
    builder.add_output_edge(edge)

    result = builder.build()
    assert len(result) == 1
    assert result[0].aspectName == "dataProcessInstanceOutput"
    assert b'"path": "/outputEdges/' in result[0].aspect.value


def test_remove_output_edge():
    """Test removing an output edge."""
    builder = DataProcessInstancePatchBuilder(
        "urn:li:dataProcessInstance:test-instance"
    )
    builder.remove_output_edge(
        "urn:li:dataset:(urn:li:dataPlatform:hive,output_table,PROD)"
    )

    result = builder.build()
    assert len(result) == 1
    assert result[0].aspectName == "dataProcessInstanceOutput"
    assert b'"op": "remove"' in result[0].aspect.value
    assert b'"path": "/outputEdges/' in result[0].aspect.value


def test_multiple_operations():
    """Test multiple operations create separate MCPs per aspect."""
    builder = DataProcessInstancePatchBuilder(
        "urn:li:dataProcessInstance:test-instance"
    )
    builder.add_input_edge("urn:li:dataset:(urn:li:dataPlatform:hive,input1,PROD)")
    builder.add_output_edge("urn:li:dataset:(urn:li:dataPlatform:hive,output1,PROD)")

    result = builder.build()
    assert len(result) == 2

    aspect_names = {mcp.aspectName for mcp in result}
    assert "dataProcessInstanceInput" in aspect_names
    assert "dataProcessInstanceOutput" in aspect_names


def test_combined_input_operations():
    """Test that multiple input operations are combined in single MCP."""
    builder = DataProcessInstancePatchBuilder(
        "urn:li:dataProcessInstance:test-instance"
    )
    builder.add_input("urn:li:dataset:(urn:li:dataPlatform:hive,input1,PROD)")
    builder.add_input_edge("urn:li:dataset:(urn:li:dataPlatform:hive,input2,PROD)")

    result = builder.build()
    # Both operations should be in the same aspect MCP
    input_mcps = [mcp for mcp in result if mcp.aspectName == "dataProcessInstanceInput"]
    assert len(input_mcps) == 1
    # The single MCP should contain both operations
    assert b"/inputs/" in input_mcps[0].aspect.value
    assert b"/inputEdges/" in input_mcps[0].aspect.value
