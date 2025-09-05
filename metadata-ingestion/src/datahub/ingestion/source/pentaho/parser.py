import logging
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Union

logger = logging.getLogger(__name__)


@dataclass
class PentahoStep:
    """Represents a step in a Pentaho transformation or job"""
    name: str
    type: str
    sql: Optional[str] = None
    table_name: Optional[str] = None
    connection_name: Optional[str] = None
    schema_name: Optional[str] = None
    custom_properties: Dict[str, str] = None
    
    def __post_init__(self):
        if self.custom_properties is None:
            self.custom_properties = {}


@dataclass 
class PentahoJob:
    """Represents a Pentaho transformation (.ktr) or job (.kjb)"""
    name: str
    type: str  # 'transformation' or 'job'
    description: Optional[str] = None
    file_path: str = ""
    steps: List[PentahoStep] = None
    custom_properties: Dict[str, str] = None
    
    def __post_init__(self):
        if self.steps is None:
            self.steps = []
        if self.custom_properties is None:
            self.custom_properties = {}


class PentahoKettleParser:
    """Parser for Pentaho Kettle files (.ktr and .kjb)"""
    
    def __init__(self, resolve_variables: bool = False, variable_values: Dict[str, str] = None):
        self.resolve_variables = resolve_variables
        self.variable_values = variable_values or {}
        
    def parse_file(self, file_path: Union[str, Path]) -> Optional[PentahoJob]:
        """Parse a Pentaho Kettle file and return a PentahoJob object"""
        file_path = Path(file_path)
        
        if not file_path.exists():
            logger.error(f"File does not exist: {file_path}")
            return None
            
        if file_path.suffix.lower() not in ['.ktr', '.kjb']:
            logger.warning(f"Unsupported file type: {file_path.suffix}")
            return None
            
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            if file_path.suffix.lower() == '.ktr':
                return self._parse_transformation(root, str(file_path))
            else:  # .kjb
                return self._parse_job(root, str(file_path))
                
        except ET.ParseError as e:
            logger.error(f"Failed to parse XML file {file_path}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error parsing file {file_path}: {e}")
            return None
    
    def _parse_transformation(self, root: ET.Element, file_path: str) -> PentahoJob:
        """Parse a transformation (.ktr) file"""
        info_element = root.find('info')
        name = self._get_text(info_element, 'name', default=Path(file_path).stem)
        description = self._get_text(info_element, 'description')
        
        job = PentahoJob(
            name=name,
            type='transformation',
            description=description,
            file_path=file_path
        )
        
        # Extract custom properties from info section
        if info_element is not None:
            job.custom_properties.update({
                'created_user': self._get_text(info_element, 'created_user', ''),
                'created_date': self._get_text(info_element, 'created_date', ''),
                'modified_user': self._get_text(info_element, 'modified_user', ''),
                'modified_date': self._get_text(info_element, 'modified_date', ''),
                'size_rowset': self._get_text(info_element, 'size_rowset', ''),
            })
        
        # Parse steps
        steps_element = root.find('step')
        if steps_element is not None:
            for step_element in root.findall('step'):
                step = self._parse_step(step_element)
                if step:
                    job.steps.append(step)
        
        return job
    
    def _parse_job(self, root: ET.Element, file_path: str) -> PentahoJob:
        """Parse a job (.kjb) file"""
        name = self._get_text(root, 'name', default=Path(file_path).stem)
        description = self._get_text(root, 'description')
        
        job = PentahoJob(
            name=name,
            type='job',
            description=description,
            file_path=file_path
        )
        
        # Extract custom properties
        job.custom_properties.update({
            'created_user': self._get_text(root, 'created_user', ''),
            'created_date': self._get_text(root, 'created_date', ''),
            'modified_user': self._get_text(root, 'modified_user', ''),
            'modified_date': self._get_text(root, 'modified_date', ''),
        })
        
        # Parse job entries (similar to steps in transformations)
        for entry_element in root.findall('entries/entry'):
            step = self._parse_job_entry(entry_element)
            if step:
                job.steps.append(step)
        
        return job
    
    def _parse_step(self, step_element: ET.Element) -> Optional[PentahoStep]:
        """Parse a transformation step"""
        name = self._get_text(step_element, 'name')
        step_type = self._get_text(step_element, 'type')
        
        if not name or not step_type:
            return None
        
        step = PentahoStep(name=name, type=step_type)
        
        # Parse step-specific properties
        if step_type.lower() == 'tableinput':
            step = self._parse_table_input_step(step_element, step)
        elif step_type.lower() == 'tableoutput':
            step = self._parse_table_output_step(step_element, step)
        
        return step
    
    def _parse_job_entry(self, entry_element: ET.Element) -> Optional[PentahoStep]:
        """Parse a job entry"""
        name = self._get_text(entry_element, 'name')
        entry_type = self._get_text(entry_element, 'type')
        
        if not name or not entry_type:
            return None
        
        # For jobs, we mainly care about SQL entries and table operations
        step = PentahoStep(name=name, type=entry_type)
        
        if entry_type.lower() == 'sql':
            sql = self._get_text(entry_element, 'sql')
            if sql:
                step.sql = self._resolve_variables(sql)
        
        return step
    
    def _parse_table_input_step(self, step_element: ET.Element, step: PentahoStep) -> PentahoStep:
        """Parse a TableInput step to extract SQL and connection info"""
        connection = self._get_text(step_element, 'connection')
        sql = self._get_text(step_element, 'sql')
        
        if connection:
            step.connection_name = connection
            
        if sql:
            step.sql = self._resolve_variables(sql)
            # Try to extract table name from SQL
            step.table_name = self._extract_table_name_from_sql(step.sql)
        
        # Extract other properties
        step.custom_properties.update({
            'execute_each_row': self._get_text(step_element, 'execute_each_row', ''),
            'variables_active': self._get_text(step_element, 'variables_active', ''),
            'lazy_conversion_active': self._get_text(step_element, 'lazy_conversion_active', ''),
        })
        
        return step
    
    def _parse_table_output_step(self, step_element: ET.Element, step: PentahoStep) -> PentahoStep:
        """Parse a TableOutput step to extract table and connection info"""
        connection = self._get_text(step_element, 'connection')
        schema = self._get_text(step_element, 'schema')
        table = self._get_text(step_element, 'table')
        
        if connection:
            step.connection_name = connection
            
        if schema:
            step.schema_name = self._resolve_variables(schema)
            
        if table:
            step.table_name = self._resolve_variables(table)
            
        # Combine schema and table if both exist
        if step.schema_name and step.table_name:
            step.table_name = f"{step.schema_name}.{step.table_name}"
        
        # Extract other properties
        step.custom_properties.update({
            'commit_size': self._get_text(step_element, 'commit_size', ''),
            'truncate': self._get_text(step_element, 'truncate', ''),
            'ignore_errors': self._get_text(step_element, 'ignore_errors', ''),
            'use_batch': self._get_text(step_element, 'use_batch', ''),
        })
        
        return step
    
    def _get_text(self, element: Optional[ET.Element], tag: str, default: str = '') -> str:
        """Safely get text from an XML element"""
        if element is None:
            return default
        child = element.find(tag)
        if child is not None and child.text:
            return child.text.strip()
        return default
    
    def _resolve_variables(self, text: str) -> str:
        """Resolve Pentaho variables in text if enabled"""
        if not self.resolve_variables or not text:
            return text
            
        # Pattern to match ${variable_name}
        variable_pattern = re.compile(r'\$\{([^}]+)\}')
        
        def replace_variable(match):
            var_name = match.group(1)
            if var_name in self.variable_values:
                return self.variable_values[var_name]
            else:
                logger.warning(f"Unresolved variable: {var_name}")
                return match.group(0)  # Return original if not found
        
        return variable_pattern.sub(replace_variable, text)
    
    def _extract_table_name_from_sql(self, sql: str) -> Optional[str]:
        """Extract table name from SQL query (basic implementation)"""
        if not sql:
            return None
            
        # Simple regex to extract table name from SELECT statements
        # This is a basic implementation and may not cover all cases
        sql_lower = sql.lower().strip()
        
        # Pattern for SELECT ... FROM table_name
        from_pattern = re.compile(r'\bfrom\s+([`"]?)([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)\1', re.IGNORECASE)
        match = from_pattern.search(sql)
        
        if match:
            return match.group(2)
            
        return None