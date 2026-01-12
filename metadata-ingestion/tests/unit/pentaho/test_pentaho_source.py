import xml.etree.ElementTree as ET
from unittest.mock import Mock, patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.pentaho.config import PentahoSourceConfig
from datahub.ingestion.source.pentaho.context import ProcessingContext
from datahub.ingestion.source.pentaho.pentaho import PentahoSource
from datahub.ingestion.source.pentaho.step_processors.table_input import (
    TableInputProcessor,
)
from datahub.ingestion.source.pentaho.step_processors.table_output import (
    TableOutputProcessor,
)


class TestPentahoSourceConfig:
    """Test configuration parsing and validation."""

    def test_config_defaults(self):
        """Test that default configuration values are set correctly."""
        config = PentahoSourceConfig(base_folder="/test/path")

        assert config.base_folder == "/test/path"
        assert config.platform_instance is None
        assert config.env == "PROD"
        assert config.default_owner == "pentaho_admin"
        assert config.file_size_limit_mb == 50
        assert "bigquery" in config.platform_mappings
        assert config.platform_mappings["googlebigquery"] == "bigquery"

    def test_config_custom_values(self):
        """Test configuration with custom values."""
        config = PentahoSourceConfig(
            base_folder="/custom/path",
            platform_instance="test_instance",
            env="DEV",
            default_owner="test_owner",
            file_size_limit_mb=100,
        )

        assert config.base_folder == "/custom/path"
        assert config.platform_instance == "test_instance"
        assert config.env == "DEV"
        assert config.default_owner == "test_owner"
        assert config.file_size_limit_mb == 100


class TestProcessingContext:
    """Test the ProcessingContext class."""

    def test_processing_context_init(self):
        """Test ProcessingContext initialization."""
        config = PentahoSourceConfig(base_folder="/test")
        context = ProcessingContext("/test/file.ktr", config)

        assert context.file_path == "/test/file.ktr"
        assert context.config == config
        assert len(context.input_datasets) == 0
        assert len(context.output_datasets) == 0
        assert len(context.step_sequence) == 0

    def test_add_datasets(self):
        """Test adding input and output datasets."""
        config = PentahoSourceConfig(base_folder="/test")
        context = ProcessingContext("/test/file.ktr", config)

        context.add_input_dataset(
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,table1,PROD)"
        )
        context.add_output_dataset(
            "urn:li:dataset:(urn:li:dataPlatform:vertica,table2,PROD)"
        )

        assert len(context.input_datasets) == 1
        assert len(context.output_datasets) == 1
        assert (
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,table1,PROD)"
            in context.input_datasets
        )
        assert (
            "urn:li:dataset:(urn:li:dataPlatform:vertica,table2,PROD)"
            in context.output_datasets
        )

    def test_add_step_info(self):
        """Test tracking step information."""
        config = PentahoSourceConfig(base_folder="/test")
        context = ProcessingContext("/test/file.ktr", config)

        context.add_step_info("test_step", "TableInput")

        assert len(context.step_sequence) == 1
        assert context.step_sequence[0]["name"] == "test_step"
        assert context.step_sequence[0]["type"] == "TableInput"

    def test_get_custom_properties(self):
        """Test custom properties generation."""
        config = PentahoSourceConfig(base_folder="/test")
        context = ProcessingContext("/test/file.ktr", config)

        context.add_input_dataset(
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,table1,PROD)"
        )
        context.add_step_info("test_step", "TableInput")

        properties = context.get_custom_properties()

        assert properties["source"] == "Pentaho"
        assert properties["file_path"] == "/test/file.ktr"
        assert properties["input_count"] == "1"
        assert properties["output_count"] == "0"
        assert properties["steps_processed"] == "1"


class TestStepProcessors:
    """Test step processor classes."""

    def setup_method(self):
        """Set up test fixtures."""
        self.config = PentahoSourceConfig(base_folder="/test")
        self.mock_source = Mock()
        self.mock_source.config = self.config

    def test_table_input_processor_can_process(self):
        """Test TableInputProcessor can process TableInput steps."""
        processor = TableInputProcessor(self.mock_source)

        assert processor.can_process("TableInput") is True
        assert processor.can_process("TableOutput") is False
        assert processor.can_process("GetVariable") is False

    def test_table_output_processor_can_process(self):
        """Test TableOutputProcessor can process TableOutput steps."""
        processor = TableOutputProcessor(self.mock_source)

        assert processor.can_process("TableOutput") is True
        assert processor.can_process("TableInput") is False
        assert processor.can_process("GetVariable") is False

    def test_table_input_processor_with_table_name(self):
        """Test TableInputProcessor with explicit table name."""
        processor = TableInputProcessor(self.mock_source)

        # Mock the source methods
        self.mock_source._get_connection_type.return_value = "GOOGLEBIGQUERY"
        self.mock_source._get_platform_from_connection.return_value = "bigquery"
        self.mock_source._create_dataset_urn.return_value = (
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,test_table,PROD)"
        )

        # Create test XML element
        step_xml = ET.fromstring(
            """
            <step>
                <type>TableInput</type>
                <connection>bq_conn</connection>
                <table>test_table</table>
                <sql></sql>
            </step>
        """
        )

        context = ProcessingContext("/test/file.ktr", self.config)
        root = ET.fromstring("<transformation></transformation>")

        processor.process(step_xml, context, root)

        assert len(context.input_datasets) == 1
        assert (
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,test_table,PROD)"
            in context.input_datasets
        )

    def test_table_output_processor_with_schema_and_table(self):
        """Test TableOutputProcessor with schema and table name."""
        processor = TableOutputProcessor(self.mock_source)

        # Mock the source methods
        self.mock_source._get_connection_type.return_value = "VERTICA5"
        self.mock_source._get_platform_from_connection.return_value = "vertica"
        self.mock_source._create_dataset_urn.return_value = (
            "urn:li:dataset:(urn:li:dataPlatform:vertica,test_schema.test_table,PROD)"
        )

        # Create test XML element
        step_xml = ET.fromstring(
            """
            <step>
                <type>TableOutput</type>
                <connection>vertica_conn</connection>
                <schema>test_schema</schema>
                <table>test_table</table>
            </step>
        """
        )

        context = ProcessingContext("/test/file.ktr", self.config)
        root = ET.fromstring("<transformation></transformation>")

        processor.process(step_xml, context, root)

        assert len(context.output_datasets) == 1
        assert (
            "urn:li:dataset:(urn:li:dataPlatform:vertica,test_schema.test_table,PROD)"
            in context.output_datasets
        )


class TestPentahoSource:
    """Test the main PentahoSource class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.config = PentahoSourceConfig(base_folder="/test")
        self.ctx = PipelineContext(run_id="test_run")
        self.source = PentahoSource(self.config, self.ctx)

    def test_source_initialization(self):
        """Test source initialization."""
        assert self.source.config == self.config
        assert len(self.source.step_processors) == 2
        assert isinstance(self.source.step_processors[0], TableInputProcessor)
        assert isinstance(self.source.step_processors[1], TableOutputProcessor)

    def test_normalize_platform_name(self):
        """Test platform name normalization."""
        # Test exact matches
        assert self.source._normalize_platform_name("googlebigquery") == "bigquery"
        assert self.source._normalize_platform_name("VERTICA5") == "vertica"
        assert self.source._normalize_platform_name("postgresql") == "postgres"

        # Test case insensitive matching
        assert self.source._normalize_platform_name("MYSQL") == "mysql"
        assert self.source._normalize_platform_name("Oracle") == "oracle"

        # Test substring matching
        assert (
            self.source._normalize_platform_name("some_bigquery_connection")
            == "bigquery"
        )

        # Test file indicators
        assert self.source._normalize_platform_name("csv_file") == "file"
        assert self.source._normalize_platform_name("excel_input") == "file"

        # Test unknown platforms
        assert self.source._normalize_platform_name("unknown_platform") == "unknown"
        assert self.source._normalize_platform_name("") == "file"
        assert self.source._normalize_platform_name(None or "") == "file"

    def test_get_platform_from_connection(self):
        """Test platform determination from connection."""
        # Test with connection type
        result = self.source._get_platform_from_connection(
            "test_conn", "TableInput", "GOOGLEBIGQUERY"
        )
        assert result == "bigquery"

        # Test with connection name
        result = self.source._get_platform_from_connection(
            "mysql_conn", "TableInput", None
        )
        assert result == "mysql"

        # Test with step type
        result = self.source._get_platform_from_connection(
            "unknown_conn", "CSVInput", None
        )
        assert result == "file"

        # Test fallback
        result = self.source._get_platform_from_connection("unknown", "Unknown", None)
        assert result == "file"

    def test_create_dataset_urn(self):
        """Test dataset URN creation."""
        # Test normal case
        urn = self.source._create_dataset_urn("bigquery", "dataset.table")
        assert urn == "urn:li:dataset:(urn:li:dataPlatform:bigquery,dataset.table,PROD)"

        # Test with platform instance
        source_with_instance = PentahoSource(
            PentahoSourceConfig(base_folder="/test", platform_instance="test_instance"),
            self.ctx,
        )
        urn = source_with_instance._create_dataset_urn("bigquery", "dataset.table")
        assert urn == "urn:li:dataset:(urn:li:dataPlatform:bigquery,test_instance.dataset.table,PROD)"

        # Test file platform with path
        urn = self.source._create_dataset_urn("file", "/path/to/file.csv")
        assert urn == "urn:li:dataset:(urn:li:dataPlatform:file,file.csv,PROD)"

        # Test file platform without extension
        urn = self.source._create_dataset_urn("file", "filename")
        assert urn == "urn:li:dataset:(urn:li:dataPlatform:file,filename.csv,PROD)"

        # Test empty inputs
        assert self.source._create_dataset_urn("", "table") is None
        assert self.source._create_dataset_urn("platform", "") is None
        assert self.source._create_dataset_urn("platform", None or "") is None

    @patch("os.path.exists")
    @patch("os.path.getsize")
    def test_should_process_file(self, mock_getsize, mock_exists):
        """Test file processing decision."""
        # Mock file existence and size for valid files
        mock_exists.return_value = True
        mock_getsize.return_value = 1024

        # Test valid file extensions
        assert self.source._should_process_file("test.ktr") is True
        assert self.source._should_process_file("test.kjb") is True
        assert self.source._should_process_file("test.txt") is False

        # Test with non-existent file
        mock_exists.return_value = False
        assert self.source._should_process_file("nonexistent.ktr") is True
        assert self.source._should_process_file("nonexistent.kjb") is True

    def test_get_connection_type(self):
        """Test connection type extraction from XML."""
        root_xml = ET.fromstring(
            """
            <transformation>
                <connection>
                    <name>test_conn</name>
                    <type>GOOGLEBIGQUERY</type>
                </connection>
                <connection>
                    <name>other_conn</name>
                    <type>VERTICA5</type>
                </connection>
            </transformation>
        """
        )

        assert (
            self.source._get_connection_type(root_xml, "test_conn") == "GOOGLEBIGQUERY"
        )
        assert self.source._get_connection_type(root_xml, "other_conn") == "VERTICA5"
        assert self.source._get_connection_type(root_xml, "nonexistent") is None
        assert self.source._get_connection_type(None, "test_conn") is None

    def test_get_step_processor(self):
        """Test step processor retrieval."""
        processor = self.source._get_step_processor("TableInput")
        assert isinstance(processor, TableInputProcessor)

        processor = self.source._get_step_processor("TableOutput")
        assert isinstance(processor, TableOutputProcessor)

        processor = self.source._get_step_processor("UnknownStep")
        assert processor is None

    @patch("os.walk")
    def test_get_workunits_no_files(self, mock_walk):
        """Test get_workunits with no files."""
        mock_walk.return_value = [("/test", [], [])]

        workunits = list(self.source.get_workunits())
        assert len(workunits) == 0

    @patch("os.walk")
    @patch("os.path.getsize")
    def test_get_workunits_with_files(self, mock_getsize, mock_walk):
        """Test get_workunits with files."""
        mock_walk.return_value = [("/test", [], ["test.ktr", "test.kjb", "test.txt"])]
        mock_getsize.return_value = 1024  # 1KB file

        # Mock the file processing methods
        with patch.object(
            self.source, "_process_ktr"
        ) as mock_process_ktr, patch.object(
            self.source, "_process_kjb"
        ) as mock_process_kjb:

            mock_process_ktr.return_value = []
            mock_process_kjb.return_value = []

            list(self.source.get_workunits())

            # Should call processing methods for ktr and kjb files only
            mock_process_ktr.assert_called_once_with("/test/test.ktr")
            mock_process_kjb.assert_called_once_with("/test/test.kjb")

    def test_create_method(self):
        """Test the create class method."""
        config_dict = {
            "base_folder": "/test/path",
            "env": "DEV",
            "default_owner": "test_user",
        }

        source = PentahoSource.create(config_dict, self.ctx)

        assert isinstance(source, PentahoSource)
        assert source.config.base_folder == "/test/path"
        assert source.config.env == "DEV"
        assert source.config.default_owner == "test_user"

    def test_get_report(self):
        """Test report retrieval."""
        report = self.source.get_report()
        assert report is not None
        assert hasattr(report, "report_failure")
        assert hasattr(report, "report_warning")


class TestPentahoSourceWithVariables:
    """Test handling of variables in Pentaho files."""

    def setup_method(self):
        """Set up test fixtures."""
        self.config = PentahoSourceConfig(base_folder="/test")
        self.ctx = PipelineContext(run_id="test_run")
        self.source = PentahoSource(self.config, self.ctx)

    def test_table_input_with_variable_in_sql(self):
        """Test TableInput with variable in SQL."""
        processor = TableInputProcessor(self.source)

        # Mock the source methods
        with patch.object(
            self.source, "_get_connection_type", return_value="GOOGLEBIGQUERY"
        ), patch.object(
            self.source, "_get_platform_from_connection", return_value="bigquery"
        ), patch.object(
            self.source,
            "_create_dataset_urn",
            return_value="urn:li:dataset:(urn:li:dataPlatform:bigquery,test_table,PROD)",
        ):

            # Create test XML element with variable in SQL
            step_xml = ET.fromstring(
                """
                <step>
                    <type>TableInput</type>
                    <connection>bq_conn</connection>
                    <table></table>
                    <sql>SELECT * FROM ${tableString}</sql>
                </step>
            """
            )

            context = ProcessingContext("/test/file.ktr", self.config)
            root = ET.fromstring("<transformation></transformation>")

            # Mock the SQL parser to simulate failure (as it would with variables)
            with patch(
                "datahub.sql_parsing.sqlglot_lineage.create_lineage_sql_parsed_result"
            ) as mock_parser:
                mock_parser.side_effect = Exception("Cannot parse SQL with variables")

                processor.process(step_xml, context, root)

                # Should fallback but have no table name, so no datasets added
                assert len(context.input_datasets) == 0

    def test_table_output_with_variable_in_table_name(self):
        """Test TableOutput with variable in table name."""
        processor = TableOutputProcessor(self.source)

        # Mock the source methods
        with patch.object(
            self.source, "_get_connection_type", return_value="VERTICA5"
        ), patch.object(
            self.source, "_get_platform_from_connection", return_value="vertica"
        ), patch.object(
            self.source,
            "_create_dataset_urn",
            return_value="urn:li:dataset:(urn:li:dataPlatform:vertica,schema.${tableString},PROD)",
        ):

            # Create test XML element with variable in table name
            step_xml = ET.fromstring(
                """
                <step>
                    <type>TableOutput</type>
                    <connection>vertica_conn</connection>
                    <schema>schema</schema>
                    <table>${tableString}</table>
                </step>
            """
            )

            context = ProcessingContext("/test/file.ktr", self.config)
            root = ET.fromstring("<transformation></transformation>")

            processor.process(step_xml, context, root)

            # Should create dataset URN with variable (as is)
            assert len(context.output_datasets) == 1
            assert (
                "urn:li:dataset:(urn:li:dataPlatform:vertica,schema.${tableString},PROD)"
                in context.output_datasets
            )