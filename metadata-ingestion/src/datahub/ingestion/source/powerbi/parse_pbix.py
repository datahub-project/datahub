#!/usr/bin/env python3
"""
Power BI (.pbix) File Parser

This script extracts and parses metadata from Power BI .pbix files.
.pbix files are ZIP archives containing various JSON and binary files.
"""

import json
import zipfile
import sys
import re
from pathlib import Path
from typing import Dict, Any, Optional
import argparse


class PBIXParser:
    """Parser for Power BI .pbix files."""
    
    def __init__(self, pbix_path: str):
        """
        Initialize the parser with a .pbix file path.
        
        Args:
            pbix_path: Path to the .pbix file
        """
        self.pbix_path = Path(pbix_path)
        if not self.pbix_path.exists():
            raise FileNotFoundError(f"File not found: {pbix_path}")
        if not self.pbix_path.suffix.lower() == '.pbix':
            raise ValueError(f"File must have .pbix extension: {pbix_path}")
        
        self.metadata = {}
        self.data_model_schema = None
        self.layout = None
        self.version = None
        
    def extract_zip(self) -> Dict[str, Any]:
        """
        Extract and parse the .pbix ZIP archive.
        
        Returns:
            Dictionary containing parsed metadata
        """
        result = {
            'file_info': {
                'filename': self.pbix_path.name,
                'size_bytes': self.pbix_path.stat().st_size
            },
            'version': None,
            'metadata': None,
            'data_model': None,
            'layout': None,
            'file_list': []
        }
        
        try:
            with zipfile.ZipFile(self.pbix_path, 'r') as zip_ref:
                # List all files in the archive
                file_list = zip_ref.namelist()
                result['file_list'] = sorted(file_list)
                
                # Extract and parse key files
                for filename in file_list:
                    try:
                        if filename == 'Version':
                            content = zip_ref.read(filename).decode('utf-8')
                            result['version'] = content.strip()
                            
                        elif filename == 'Metadata':
                            content = zip_ref.read(filename).decode('utf-8')
                            # Metadata might not be JSON, try to parse it
                            try:
                                result['metadata'] = json.loads(content)
                            except json.JSONDecodeError:
                                # Metadata might be in a different format, store as raw text
                                result['metadata'] = content
                            
                        elif filename in ('DataModelSchema', 'DataModel'):
                            raw_content = zip_ref.read(filename)
                            # Try UTF-16LE first (common in .pbix files), then UTF-8
                            try:
                                content = raw_content.decode('utf-16-le')
                            except UnicodeDecodeError:
                                try:
                                    content = raw_content.decode('utf-8')
                                except UnicodeDecodeError:
                                    # If it's compressed or binary, skip for now
                                    print(f"Warning: Could not decode {filename} (might be compressed)", file=sys.stderr)
                                    continue
                            
                            # Check if it's compressed (XPress9)
                            if content.startswith('This backup was created using XPress9 compression.'):
                                print(f"Warning: {filename} is compressed with XPress9 (decompression not yet supported)", file=sys.stderr)
                                continue
                            
                            result['data_model'] = json.loads(content)
                            
                        elif filename in ('Layout', 'Report/Layout'):
                            raw_content = zip_ref.read(filename)
                            # Try UTF-16LE first (common in .pbix files), then UTF-8
                            try:
                                content = raw_content.decode('utf-16-le')
                            except UnicodeDecodeError:
                                content = raw_content.decode('utf-8')
                            result['layout'] = json.loads(content)
                            
                    except (UnicodeDecodeError, json.JSONDecodeError) as e:
                        # Some files might be binary or have encoding issues
                        print(f"Warning: Could not parse {filename}: {e}", file=sys.stderr)
                        continue
                        
        except zipfile.BadZipFile:
            raise ValueError(f"Invalid ZIP file: {self.pbix_path}")
        
        return result
    
    def parse_data_model(self, data_model: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse the DataModelSchema to extract tables, columns, relationships, etc.
        
        Args:
            data_model: The parsed DataModelSchema JSON
            
        Returns:
            Structured information about the data model
        """
        if not data_model:
            return {}
        
        parsed = {
            'tables': [],
            'relationships': [],
            'cultures': data_model.get('culture', 'en-US'),
            'version': data_model.get('version', 'Unknown')
        }
        
        # Extract tables and their columns
        model = data_model.get('model', {})
        tables = model.get('tables', [])
        
        for table in tables:
            table_info = {
                'name': table.get('name', 'Unknown'),
                'columns': [],
                'measures': [],
                'partitions': []
            }
            
            # Extract columns
            for column in table.get('columns', []):
                col_info = {
                    'name': column.get('name', 'Unknown'),
                    'dataType': column.get('dataType', 'Unknown'),
                    'formatString': column.get('formatString'),
                    'isHidden': column.get('isHidden', False),
                    'isNullable': column.get('isNullable', True),
                    'sourceColumn': column.get('sourceColumn'),
                    'summarizeBy': column.get('summarizeBy')
                }
                table_info['columns'].append(col_info)
            
            # Extract measures
            for measure in table.get('measures', []):
                measure_info = {
                    'name': measure.get('name', 'Unknown'),
                    'expression': measure.get('expression', ''),
                    'formatString': measure.get('formatString'),
                    'isHidden': measure.get('isHidden', False)
                }
                table_info['measures'].append(measure_info)
            
            # Extract partitions (data sources)
            for partition in table.get('partitions', []):
                partition_info = {
                    'name': partition.get('name', 'Unknown'),
                    'mode': partition.get('mode', 'Unknown'),
                    'source': partition.get('source', {})
                }
                table_info['partitions'].append(partition_info)
            
            parsed['tables'].append(table_info)
        
        # Extract relationships
        relationships = model.get('relationships', [])
        for rel in relationships:
            rel_info = {
                'name': rel.get('name', 'Unknown'),
                'fromTable': rel.get('fromTable', 'Unknown'),
                'fromColumn': rel.get('fromColumn', 'Unknown'),
                'toTable': rel.get('toTable', 'Unknown'),
                'toColumn': rel.get('toColumn', 'Unknown'),
                'crossFilter': rel.get('crossFilter', 'Unknown')
            }
            parsed['relationships'].append(rel_info)
        
        return parsed
    
    def parse_visualization(self, visual_container: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse a single visualization container to extract visualization details and columns.
        
        Args:
            visual_container: A visual container from the layout
            
        Returns:
            Structured information about the visualization
        """
        visual_info = {
            'id': visual_container.get('id'),
            'position': {
                'x': visual_container.get('x', 0),
                'y': visual_container.get('y', 0),
                'z': visual_container.get('z', 0),
                'width': visual_container.get('width', 0),
                'height': visual_container.get('height', 0)
            },
            'visualType': None,
            'columns': [],
            'measures': [],
            'dataRoles': {}
        }
        
        # Parse config JSON string
        config_str = visual_container.get('config', '{}')
        try:
            config = json.loads(config_str) if isinstance(config_str, str) else config_str
            single_visual = config.get('singleVisual', {})
            visual_info['visualType'] = single_visual.get('visualType', 'Unknown')
            
            # Extract projections (data roles)
            projections = single_visual.get('projections', {})
            for role, refs in projections.items():
                visual_info['dataRoles'][role] = []
                for ref in refs:
                    query_ref = ref.get('queryRef', '')
                    visual_info['dataRoles'][role].append({
                        'queryRef': query_ref,
                        'active': ref.get('active', False)
                    })
            
            # Also parse prototypeQuery from config for additional column/measure info
            prototype_query = single_visual.get('prototypeQuery', {})
            if prototype_query:
                select_items = prototype_query.get('Select', [])
                for select_item in select_items:
                    name = select_item.get('Name', '')
                    native_ref = select_item.get('NativeReferenceName', '')
                    
                    if 'Measure' in select_item:
                        # Check if we already have this measure
                        if not any(m.get('name') == name for m in visual_info['measures']):
                            measure_info = {
                                'name': name,
                                'nativeReferenceName': native_ref,
                                'entity': name.split('.')[0] if '.' in name else '',
                                'property': name.split('.')[1] if '.' in name else name
                            }
                            visual_info['measures'].append(measure_info)
                    elif 'Column' in select_item:
                        # Check if we already have this column
                        if not any(c.get('name') == name for c in visual_info['columns']):
                            column_info = {
                                'name': name,
                                'nativeReferenceName': native_ref,
                                'table': name.split('.')[0] if '.' in name else '',
                                'column': name.split('.')[1] if '.' in name else name
                            }
                            visual_info['columns'].append(column_info)
                    elif 'Aggregation' in select_item:
                        agg = select_item.get('Aggregation', {})
                        agg_func = agg.get('Function', 0)  # 0 = Sum
                        agg_expr = agg.get('Expression', {})
                        if 'Column' in agg_expr:
                            # Check if we already have this column
                            if not any(c.get('name') == name for c in visual_info['columns']):
                                func_names = {0: 'Sum', 1: 'Min', 2: 'Max', 3: 'Count', 4: 'Average', 5: 'DistinctCount'}
                                # Extract table and column from the column expression
                                col_expr = agg_expr.get('Column', {})
                                col_prop = col_expr.get('Property', '')
                                col_source = col_expr.get('Expression', {}).get('SourceRef', {})
                                col_source_entity = col_source.get('Source', '')
                                
                                # Parse table and column
                                if col_source_entity and col_prop:
                                    table = col_source_entity
                                    column = col_prop
                                elif '.' in native_ref:
                                    parts = native_ref.split('.', 1)
                                    table = parts[0] if len(parts) > 1 else ''
                                    column = parts[1] if len(parts) > 1 else native_ref
                                elif '.' in name:
                                    # Handle "Sum(DAX_ORDER_DETAILS.Avg_Order_Value)" format
                                    match = re.search(r'\((\w+)\.(\w+)\)', name)
                                    if match:
                                        table = match.group(1)
                                        column = match.group(2)
                                    else:
                                        parts = name.split('.', 1)
                                        table = parts[0] if len(parts) > 1 else ''
                                        column = parts[1] if len(parts) > 1 else name
                                else:
                                    table = col_source_entity or ''
                                    column = col_prop or name
                                
                                column_info = {
                                    'name': name,
                                    'nativeReferenceName': native_ref,
                                    'table': table,
                                    'column': column,
                                    'aggregation': func_names.get(agg_func, 'Sum')
                                }
                                visual_info['columns'].append(column_info)
                    elif 'HierarchyLevel' in select_item:
                        # Check if we already have this hierarchy
                        if not any(c.get('name') == name for c in visual_info['columns']):
                            hierarchy_info = {
                                'name': name,
                                'nativeReferenceName': native_ref,
                                'type': 'hierarchy'
                            }
                            visual_info['columns'].append(hierarchy_info)
        except (json.JSONDecodeError, AttributeError):
            pass
        
        # Parse query JSON string to extract columns and measures
        query_str = visual_container.get('query', '{}')
        try:
            query = json.loads(query_str) if isinstance(query_str, str) else query_str
            commands = query.get('Commands', [])
            for command in commands:
                semantic_query = command.get('SemanticQueryDataShapeCommand', {})
                query_obj = semantic_query.get('Query', {})
                select_items = query_obj.get('Select', [])
                
                for select_item in select_items:
                    name = select_item.get('Name', '')
                    native_ref = select_item.get('NativeReferenceName', '')
                    
                    # Check if it's a measure or column
                    if 'Measure' in select_item:
                        # Check if we already have this measure
                        if not any(m.get('name') == name for m in visual_info['measures']):
                            measure_info = {
                                'name': name,
                                'nativeReferenceName': native_ref,
                                'entity': name.split('.')[0] if '.' in name else '',
                                'property': name.split('.')[1] if '.' in name else name
                            }
                            visual_info['measures'].append(measure_info)
                    elif 'Column' in select_item:
                        # Check if we already have this column
                        if not any(c.get('name') == name for c in visual_info['columns']):
                            column_info = {
                                'name': name,
                                'nativeReferenceName': native_ref,
                                'table': name.split('.')[0] if '.' in name else '',
                                'column': name.split('.')[1] if '.' in name else name
                            }
                            visual_info['columns'].append(column_info)
                    elif 'Aggregation' in select_item:
                        # Aggregated column
                        agg = select_item.get('Aggregation', {})
                        agg_func = agg.get('Function', 0)  # 0 = Sum
                        agg_expr = agg.get('Expression', {})
                        if 'Column' in agg_expr:
                            # Check if we already have this column
                            if not any(c.get('name') == name for c in visual_info['columns']):
                                func_names = {0: 'Sum', 1: 'Min', 2: 'Max', 3: 'Count', 4: 'Average', 5: 'DistinctCount'}
                                # Extract table and column from the column expression
                                col_expr = agg_expr.get('Column', {})
                                col_prop = col_expr.get('Property', '')
                                col_source = col_expr.get('Expression', {}).get('SourceRef', {})
                                col_source_entity = col_source.get('Source', '')
                                
                                # Parse name like "Sum(DAX_ORDER_DETAILS.Avg_Order_Value)" 
                                # Extract table and column from native reference or name
                                if '.' in native_ref:
                                    parts = native_ref.split('.', 1)
                                    table = parts[0] if len(parts) > 1 else ''
                                    column = parts[1] if len(parts) > 1 else native_ref
                                elif col_source_entity and col_prop:
                                    table = col_source_entity
                                    column = col_prop
                                elif '.' in name:
                                    # Fallback: try to parse from name
                                    # Handle "Sum(DAX_ORDER_DETAILS.Avg_Order_Value)" format
                                    match = re.search(r'\((\w+)\.(\w+)\)', name)
                                    if match:
                                        table = match.group(1)
                                        column = match.group(2)
                                    else:
                                        parts = name.split('.', 1)
                                        table = parts[0] if len(parts) > 1 else ''
                                        column = parts[1] if len(parts) > 1 else name
                                else:
                                    table = col_source_entity or ''
                                    column = col_prop or name
                                
                                column_info = {
                                    'name': name,
                                    'nativeReferenceName': native_ref,
                                    'table': table,
                                    'column': column,
                                    'aggregation': func_names.get(agg_func, 'Sum')
                                }
                                visual_info['columns'].append(column_info)
                    elif 'HierarchyLevel' in select_item:
                        # Hierarchy level (date hierarchy, etc.)
                        # Check if we already have this hierarchy
                        if not any(c.get('name') == name for c in visual_info['columns']):
                            hierarchy_info = {
                                'name': name,
                                'nativeReferenceName': native_ref,
                                'type': 'hierarchy'
                            }
                            visual_info['columns'].append(hierarchy_info)
        except (json.JSONDecodeError, AttributeError):
            pass
        
        # Parse dataTransforms JSON string for additional column details
        data_transforms_str = visual_container.get('dataTransforms', '{}')
        try:
            data_transforms = json.loads(data_transforms_str) if isinstance(data_transforms_str, str) else data_transforms_str
            selects = data_transforms.get('selects', [])
            
            # Enhance column/measure info with display names and types
            for select in selects:
                query_name = select.get('queryName', '')
                display_name = select.get('displayName', '')
                roles = select.get('roles', {})
                data_type = select.get('type', {})
                
                # Update existing column/measure info
                for col in visual_info['columns']:
                    if col['name'] == query_name:
                        col['displayName'] = display_name
                        col['roles'] = roles
                        col['dataType'] = data_type.get('underlyingType')
                        break
                
                for measure in visual_info['measures']:
                    if measure['name'] == query_name:
                        measure['displayName'] = display_name
                        measure['roles'] = roles
                        measure['dataType'] = data_type.get('underlyingType')
                        measure['format'] = select.get('format')
                        break
        except (json.JSONDecodeError, AttributeError):
            pass
        
        return visual_info
    
    def parse_layout(self, layout: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse the Layout to extract report information including visualizations and columns.
        
        Args:
            layout: The parsed Layout JSON
            
        Returns:
            Structured information about the report layout
        """
        if not layout:
            return {}
        
        parsed = {
            'sections': [],
            'themes': layout.get('themes', []),
            'defaultLayout': layout.get('defaultLayout', {}),
            'visualizations': []
        }
        
        # Extract sections (pages) with detailed visualization information
        sections = layout.get('sections', [])
        for section in sections:
            section_info = {
                'name': section.get('name', 'Unknown'),
                'displayName': section.get('displayName', 'Unknown'),
                'id': section.get('id'),
                'width': section.get('width', 0),
                'height': section.get('height', 0),
                'visualContainers': []
            }
            
            # Parse each visualization in the section
            visual_containers = section.get('visualContainers', [])
            for visual_container in visual_containers:
                visual_info = self.parse_visualization(visual_container)
                section_info['visualContainers'].append(visual_info)
                # Also add to top-level visualizations list
                visual_info_with_section = visual_info.copy()
                visual_info_with_section['sectionName'] = section_info['displayName']
                visual_info_with_section['sectionId'] = section_info['id']
                parsed['visualizations'].append(visual_info_with_section)
            
            parsed['sections'].append(section_info)
        
        return parsed
    
    def build_column_lineage(self, layout_parsed: Dict[str, Any], data_model_parsed: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Build column-level lineage from visualizations back to data model.
        
        Args:
            layout_parsed: Parsed layout information with visualizations
            data_model_parsed: Optional parsed data model (if available)
            
        Returns:
            Lineage information mapping visualizations to source columns
        """
        lineage = {
            'visualization_lineage': [],
            'column_mapping': {},
            'measure_mapping': {},
            'table_usage': {},
            'page_lineage': {},
            'page_mapping': {}
        }
        
        if not layout_parsed or 'visualizations' not in layout_parsed:
            return lineage
        
        # Build a mapping of all columns and measures used
        for viz in layout_parsed.get('visualizations', []):
            viz_lineage = {
                'visualizationId': viz.get('id'),
                'visualizationType': viz.get('visualType'),
                'sectionName': viz.get('sectionName'),
                'sectionId': viz.get('sectionId'),
                'columns': [],
                'measures': []
            }
            
            # Process columns
            for col in viz.get('columns', []):
                col_name = col.get('name', '')
                table = col.get('table', '')
                column = col.get('column', '')
                display_name = col.get('displayName', column)
                
                # Build lineage entry with page information
                page_name = viz.get('sectionName', 'Unknown')
                page_id = viz.get('sectionId')
                viz_type = viz.get('visualType', 'Unknown')
                viz_id = viz.get('id')
                
                col_lineage = {
                    'visualizationColumn': col_name,
                    'displayName': display_name,
                    'sourceTable': table,
                    'sourceColumn': column,
                    'dataRole': list(col.get('roles', {}).keys()) if col.get('roles') else [],
                    'aggregation': col.get('aggregation'),
                    'columnType': col.get('type', 'column'),
                    'page': page_name,
                    'pageId': page_id,
                    'visualization': viz_type,
                    'visualizationId': viz_id,
                    'lineagePath': [
                        f"Page: {page_name}",
                        f"Visualization: {viz_type}",
                        f"Column: {col_name}",
                        f"Source: {table}.{column}"
                    ] if table and column else [
                        f"Page: {page_name}",
                        f"Visualization: {viz_type}",
                        f"Column: {col_name}"
                    ]
                }
                
                # If we have data model info, we could add more details here
                if data_model_parsed:
                    # Try to find the column in the data model
                    for table_info in data_model_parsed.get('tables', []):
                        if table_info.get('name') == table:
                            for col_info in table_info.get('columns', []):
                                if col_info.get('name') == column:
                                    col_lineage['sourceColumnDetails'] = {
                                        'dataType': col_info.get('dataType'),
                                        'isHidden': col_info.get('isHidden'),
                                        'isNullable': col_info.get('isNullable'),
                                        'formatString': col_info.get('formatString')
                                    }
                                    break
                
                viz_lineage['columns'].append(col_lineage)
                
                # Update column mapping
                if table and column:
                    key = f"{table}.{column}"
                    if key not in lineage['column_mapping']:
                        lineage['column_mapping'][key] = {
                            'table': table,
                            'column': column,
                            'usedInVisualizations': []
                        }
                    lineage['column_mapping'][key]['usedInVisualizations'].append({
                        'visualizationId': viz.get('id'),
                        'visualizationType': viz.get('visualType'),
                        'page': viz.get('sectionName'),
                        'pageId': viz.get('sectionId'),
                        'dataRole': list(col.get('roles', {}).keys()) if col.get('roles') else []
                    })
                    
                    # Update table usage
                    if table not in lineage['table_usage']:
                        lineage['table_usage'][table] = {
                            'columns': set(),
                            'visualizations': []
                        }
                    lineage['table_usage'][table]['columns'].add(column)
                    lineage['table_usage'][table]['visualizations'].append({
                        'id': viz.get('id'),
                        'type': viz.get('visualType'),
                        'page': viz.get('sectionName'),
                        'pageId': viz.get('sectionId')
                    })
            
            # Process measures
            for measure in viz.get('measures', []):
                measure_name = measure.get('name', '')
                entity = measure.get('entity', '')
                property_name = measure.get('property', '')
                display_name = measure.get('displayName', property_name)
                
                # Build lineage entry for measure with page information
                page_name = viz.get('sectionName', 'Unknown')
                page_id = viz.get('sectionId')
                viz_type = viz.get('visualType', 'Unknown')
                viz_id = viz.get('id')
                
                measure_lineage = {
                    'visualizationMeasure': measure_name,
                    'displayName': display_name,
                    'sourceEntity': entity,
                    'measureName': property_name,
                    'dataRole': list(measure.get('roles', {}).keys()) if measure.get('roles') else [],
                    'format': measure.get('format'),
                    'page': page_name,
                    'pageId': page_id,
                    'visualization': viz_type,
                    'visualizationId': viz_id,
                    'lineagePath': [
                        f"Page: {page_name}",
                        f"Visualization: {viz_type}",
                        f"Measure: {measure_name}",
                        f"Source: {entity}.{property_name}"
                    ] if entity and property_name else [
                        f"Page: {page_name}",
                        f"Visualization: {viz_type}",
                        f"Measure: {measure_name}"
                    ]
                }
                
                # If we have data model info, try to find measure definition
                if data_model_parsed:
                    for table_info in data_model_parsed.get('tables', []):
                        if table_info.get('name') == entity:
                            for measure_info in table_info.get('measures', []):
                                if measure_info.get('name') == property_name:
                                    measure_lineage['measureDetails'] = {
                                        'expression': measure_info.get('expression'),
                                        'formatString': measure_info.get('formatString'),
                                        'isHidden': measure_info.get('isHidden')
                                    }
                                    # Try to extract referenced tables/columns from DAX expression
                                    if measure_info.get('expression'):
                                        expr = measure_info.get('expression', '')
                                        # Simple extraction of table.column patterns from DAX
                                        referenced = re.findall(r'(\w+)\.(\w+)', expr)
                                        measure_lineage['referencedColumns'] = [
                                            {'table': t, 'column': c} for t, c in referenced
                                        ]
                                    break
                
                viz_lineage['measures'].append(measure_lineage)
                
                # Update measure mapping
                if entity and property_name:
                    key = f"{entity}.{property_name}"
                    if key not in lineage['measure_mapping']:
                        lineage['measure_mapping'][key] = {
                            'entity': entity,
                            'measure': property_name,
                            'usedInVisualizations': []
                        }
                    lineage['measure_mapping'][key]['usedInVisualizations'].append({
                        'visualizationId': viz.get('id'),
                        'visualizationType': viz.get('visualType'),
                        'page': viz.get('sectionName'),
                        'pageId': viz.get('sectionId'),
                        'dataRole': list(measure.get('roles', {}).keys()) if measure.get('roles') else []
                    })
            
            lineage['visualization_lineage'].append(viz_lineage)
        
        # Build page-level lineage
        for section in layout_parsed.get('sections', []):
            page_name = section.get('displayName', 'Unknown')
            page_id = section.get('id')
            
            if page_name not in lineage['page_lineage']:
                lineage['page_lineage'][page_name] = {
                    'pageId': page_id,
                    'pageName': page_name,
                    'columns': {},
                    'measures': {},
                    'tables': set(),
                    'visualizations': []
                }
            
            # Collect all columns and measures used on this page
            for viz in section.get('visualContainers', []):
                viz_type = viz.get('visualType', 'Unknown')
                viz_id = viz.get('id')
                
                lineage['page_lineage'][page_name]['visualizations'].append({
                    'id': viz_id,
                    'type': viz_type
                })
                
                # Add columns from this visualization
                for col in viz.get('columns', []):
                    table = col.get('table', '')
                    column = col.get('column', '')
                    if table and column:
                        col_key = f"{table}.{column}"
                        if col_key not in lineage['page_lineage'][page_name]['columns']:
                            lineage['page_lineage'][page_name]['columns'][col_key] = {
                                'table': table,
                                'column': column,
                                'visualizations': []
                            }
                        lineage['page_lineage'][page_name]['columns'][col_key]['visualizations'].append({
                            'id': viz_id,
                            'type': viz_type,
                            'dataRole': list(col.get('roles', {}).keys()) if col.get('roles') else []
                        })
                        lineage['page_lineage'][page_name]['tables'].add(table)
                
                # Add measures from this visualization
                for measure in viz.get('measures', []):
                    entity = measure.get('entity', '')
                    property_name = measure.get('property', '')
                    if entity and property_name:
                        measure_key = f"{entity}.{property_name}"
                        if measure_key not in lineage['page_lineage'][page_name]['measures']:
                            lineage['page_lineage'][page_name]['measures'][measure_key] = {
                                'entity': entity,
                                'measure': property_name,
                                'visualizations': []
                            }
                        lineage['page_lineage'][page_name]['measures'][measure_key]['visualizations'].append({
                            'id': viz_id,
                            'type': viz_type,
                            'dataRole': list(measure.get('roles', {}).keys()) if measure.get('roles') else []
                        })
            
            # Build page mapping (reverse: which pages use each column/measure)
            for col_key, col_info in lineage['page_lineage'][page_name]['columns'].items():
                if col_key not in lineage['page_mapping']:
                    lineage['page_mapping'][col_key] = {
                        'table': col_info['table'],
                        'column': col_info['column'],
                        'usedInPages': []
                    }
                lineage['page_mapping'][col_key]['usedInPages'].append({
                    'page': page_name,
                    'pageId': page_id,
                    'visualizationCount': len(col_info['visualizations'])
                })
            
            for measure_key, measure_info in lineage['page_lineage'][page_name]['measures'].items():
                if measure_key not in lineage['page_mapping']:
                    lineage['page_mapping'][measure_key] = {
                        'entity': measure_info['entity'],
                        'measure': measure_info['measure'],
                        'usedInPages': []
                    }
                lineage['page_mapping'][measure_key]['usedInPages'].append({
                    'page': page_name,
                    'pageId': page_id,
                    'visualizationCount': len(measure_info['visualizations'])
                })
        
        # Convert sets to lists for JSON serialization
        for table in lineage['table_usage']:
            lineage['table_usage'][table]['columns'] = sorted(list(lineage['table_usage'][table]['columns']))
            # Remove duplicates from visualizations list
            seen = set()
            unique_viz = []
            for viz in lineage['table_usage'][table]['visualizations']:
                viz_key = (viz['id'], viz['type'], viz.get('page'))
                if viz_key not in seen:
                    seen.add(viz_key)
                    unique_viz.append(viz)
            lineage['table_usage'][table]['visualizations'] = unique_viz
        
        # Convert sets in page_lineage to lists
        for page_name in lineage['page_lineage']:
            lineage['page_lineage'][page_name]['tables'] = sorted(list(lineage['page_lineage'][page_name]['tables']))
        
        return lineage
    
    def extract_metadata(self) -> Dict[str, Any]:
        """
        Main method to extract all metadata from the .pbix file.
        
        Returns:
            Complete metadata dictionary
        """
        extracted = self.extract_zip()
        
        result = {
            'file_info': extracted['file_info'],
            'version': extracted['version'],
            'metadata': extracted['metadata'],
            'file_list': extracted['file_list']
        }
        
        # Parse data model if available
        if extracted['data_model']:
            result['data_model_parsed'] = self.parse_data_model(extracted['data_model'])
            result['data_model_raw'] = extracted['data_model']  # Keep raw for reference
        
        # Parse layout if available
        if extracted['layout']:
            result['layout_parsed'] = self.parse_layout(extracted['layout'])
            result['layout_raw'] = extracted['layout']  # Keep raw for reference
        
        # Build column-level lineage
        if extracted['layout']:
            data_model = result.get('data_model_parsed') if extracted.get('data_model') else None
            result['lineage'] = self.build_column_lineage(
                result.get('layout_parsed', {}),
                data_model
            )
        
        return result
    
    def print_summary(self, metadata: Dict[str, Any]):
        """
        Print a human-readable summary of the extracted metadata.
        
        Args:
            metadata: The extracted metadata dictionary
        """
        print("=" * 80)
        print("Power BI File Metadata Summary")
        print("=" * 80)
        print(f"\nFile: {metadata['file_info']['filename']}")
        print(f"Size: {metadata['file_info']['size_bytes']:,} bytes")
        if metadata.get('version'):
            print(f"Version: {metadata['version']}")
        
        # Data Model Summary
        if 'data_model_parsed' in metadata:
            dm = metadata['data_model_parsed']
            print(f"\n{'=' * 80}")
            print("Data Model")
            print(f"{'=' * 80}")
            print(f"Culture: {dm.get('cultures', 'N/A')}")
            print(f"Version: {dm.get('version', 'N/A')}")
            print(f"\nTables: {len(dm.get('tables', []))}")
            
            for table in dm.get('tables', []):
                print(f"\n  Table: {table['name']}")
                print(f"    Columns: {len(table['columns'])}")
                print(f"    Measures: {len(table['measures'])}")
                print(f"    Partitions: {len(table['partitions'])}")
                
                if table['columns']:
                    print("    Column Details:")
                    for col in table['columns'][:5]:  # Show first 5 columns
                        hidden = " (hidden)" if col.get('isHidden') else ""
                        print(f"      - {col['name']}: {col['dataType']}{hidden}")
                    if len(table['columns']) > 5:
                        print(f"      ... and {len(table['columns']) - 5} more columns")
                
                if table['measures']:
                    print("    Measures:")
                    for measure in table['measures'][:5]:  # Show first 5 measures
                        hidden = " (hidden)" if measure.get('isHidden') else ""
                        print(f"      - {measure['name']}{hidden}")
                    if len(table['measures']) > 5:
                        print(f"      ... and {len(table['measures']) - 5} more measures")
            
            print(f"\nRelationships: {len(dm.get('relationships', []))}")
            for rel in dm.get('relationships', [])[:5]:  # Show first 5 relationships
                print(f"  - {rel['fromTable']}.{rel['fromColumn']} -> {rel['toTable']}.{rel['toColumn']}")
            if len(dm.get('relationships', [])) > 5:
                print(f"  ... and {len(dm.get('relationships', [])) - 5} more relationships")
        
        # Layout Summary
        if 'layout_parsed' in metadata:
            layout = metadata['layout_parsed']
            print(f"\n{'=' * 80}")
            print("Report Layout")
            print(f"{'=' * 80}")
            print(f"Sections (Pages): {len(layout.get('sections', []))}")
            print(f"Total Visualizations: {len(layout.get('visualizations', []))}")
            
            for section in layout.get('sections', []):
                print(f"\n  Section: {section['displayName']} ({section['name']})")
                print(f"    Visualizations: {len(section.get('visualContainers', []))}")
                
                for idx, visual in enumerate(section.get('visualContainers', []), 1):
                    visual_type = visual.get('visualType', 'Unknown')
                    print(f"\n    Visualization {idx}: {visual_type}")
                    print(f"      Position: ({visual['position']['x']:.1f}, {visual['position']['y']:.1f})")
                    print(f"      Size: {visual['position']['width']:.1f} x {visual['position']['height']:.1f}")
                    
                    # Show columns used
                    columns = visual.get('columns', [])
                    if columns:
                        print(f"      Columns ({len(columns)}):")
                        for col in columns[:10]:  # Show first 10 columns
                            col_name = col.get('displayName') or col.get('column') or col.get('name', 'Unknown')
                            table = col.get('table', '')
                            roles = col.get('roles', {})
                            role_list = [k for k, v in roles.items() if v]
                            role_str = f" [{', '.join(role_list)}]" if role_list else ""
                            print(f"        - {table}.{col_name}{role_str}")
                        if len(columns) > 10:
                            print(f"        ... and {len(columns) - 10} more columns")
                    
                    # Show measures used
                    measures = visual.get('measures', [])
                    if measures:
                        print(f"      Measures ({len(measures)}):")
                        for measure in measures[:10]:  # Show first 10 measures
                            measure_name = measure.get('displayName') or measure.get('property') or measure.get('name', 'Unknown')
                            entity = measure.get('entity', '')
                            roles = measure.get('roles', {})
                            role_list = [k for k, v in roles.items() if v]
                            role_str = f" [{', '.join(role_list)}]" if role_list else ""
                            print(f"        - {entity}.{measure_name}{role_str}")
                        if len(measures) > 10:
                            print(f"        ... and {len(measures) - 10} more measures")
        
        # Lineage Summary
        if 'lineage' in metadata:
            lineage = metadata['lineage']
            print(f"\n{'=' * 80}")
            print("Column-Level Lineage")
            print(f"{'=' * 80}")
            
            # Show page-level lineage
            page_lineage = lineage.get('page_lineage', {})
            if page_lineage:
                print(f"\nPage-Level Lineage:")
                print(f"  Pages with visualizations: {len(page_lineage)}")
                for page_name, page_info in sorted(page_lineage.items()):
                    cols = page_info.get('columns', {})
                    measures = page_info.get('measures', {})
                    tables = page_info.get('tables', [])
                    viz_count = len(page_info.get('visualizations', []))
                    print(f"\n  Page: {page_name}")
                    print(f"    Visualizations: {viz_count}")
                    print(f"    Tables used: {len(tables)}")
                    if tables:
                        print(f"    Table list: {', '.join(tables[:10])}")
                        if len(tables) > 10:
                            print(f"      ... and {len(tables) - 10} more tables")
                    print(f"    Columns used: {len(cols)}")
                    if cols:
                        col_list = list(cols.keys())[:5]
                        for col_key in col_list:
                            col_info = cols[col_key]
                            print(f"      - {col_info['table']}.{col_info['column']} ({len(col_info.get('visualizations', []))} viz)")
                        if len(cols) > 5:
                            print(f"      ... and {len(cols) - 5} more columns")
                    print(f"    Measures used: {len(measures)}")
                    if measures:
                        measure_list = list(measures.keys())[:5]
                        for measure_key in measure_list:
                            measure_info = measures[measure_key]
                            print(f"      - {measure_info['entity']}.{measure_info['measure']} ({len(measure_info.get('visualizations', []))} viz)")
                        if len(measures) > 5:
                            print(f"      ... and {len(measures) - 5} more measures")
            
            # Show page mapping (which pages use each column/measure)
            page_mapping = lineage.get('page_mapping', {})
            if page_mapping:
                print(f"\n\nPage Mapping (Columns/Measures -> Pages):")
                print(f"  Total unique columns/measures: {len(page_mapping)}")
                for key, info in sorted(list(page_mapping.items())[:10]):
                    if 'table' in info:  # It's a column
                        print(f"\n  Column: {info['table']}.{info['column']}")
                    else:  # It's a measure
                        print(f"\n  Measure: {info['entity']}.{info['measure']}")
                    pages = info.get('usedInPages', [])
                    print(f"    Used in {len(pages)} page(s):")
                    for page_info in pages[:5]:
                        print(f"      - {page_info['page']} ({page_info['visualizationCount']} visualization(s))")
                    if len(pages) > 5:
                        print(f"      ... and {len(pages) - 5} more pages")
                if len(page_mapping) > 10:
                    print(f"\n  ... and {len(page_mapping) - 10} more columns/measures")
            
            # Show table usage summary
            table_usage = lineage.get('table_usage', {})
            if table_usage:
                print(f"\nTable Usage Summary:")
                print(f"  Tables used in visualizations: {len(table_usage)}")
                for table, usage in sorted(table_usage.items()):
                    cols = usage.get('columns', [])
                    viz_count = len(usage.get('visualizations', []))
                    print(f"\n  Table: {table}")
                    print(f"    Columns used: {len(cols)}")
                    if cols:
                        print(f"    Column list: {', '.join(cols[:10])}")
                        if len(cols) > 10:
                            print(f"      ... and {len(cols) - 10} more columns")
                    print(f"    Used in {viz_count} visualization(s)")
            
            # Show column mapping (reverse lookup: which columns are used where)
            column_mapping = lineage.get('column_mapping', {})
            if column_mapping:
                print(f"\n\nColumn Mapping (Columns -> Visualizations):")
                print(f"  Total unique columns used: {len(column_mapping)}")
                for col_key, col_info in sorted(list(column_mapping.items())[:10]):
                    table = col_info.get('table', '')
                    column = col_info.get('column', '')
                    viz_list = col_info.get('usedInVisualizations', [])
                    print(f"\n  {table}.{column}")
                    print(f"    Used in {len(viz_list)} visualization(s):")
                    for viz in viz_list[:5]:
                        page = viz.get('page', viz.get('sectionName', 'Unknown'))
                        print(f"      - Page: {page} / {viz.get('visualizationType', 'Unknown')} [{', '.join(viz.get('dataRole', []))}]")
                    if len(viz_list) > 5:
                        print(f"      ... and {len(viz_list) - 5} more")
                if len(column_mapping) > 10:
                    print(f"\n  ... and {len(column_mapping) - 10} more columns")
            
            # Show measure mapping
            measure_mapping = lineage.get('measure_mapping', {})
            if measure_mapping:
                print(f"\n\nMeasure Mapping (Measures -> Visualizations):")
                print(f"  Total unique measures used: {len(measure_mapping)}")
                for measure_key, measure_info in sorted(list(measure_mapping.items())[:10]):
                    entity = measure_info.get('entity', '')
                    measure = measure_info.get('measure', '')
                    viz_list = measure_info.get('usedInVisualizations', [])
                    print(f"\n  {entity}.{measure}")
                    print(f"    Used in {len(viz_list)} visualization(s):")
                    for viz in viz_list[:5]:
                        page = viz.get('page', viz.get('sectionName', 'Unknown'))
                        print(f"      - Page: {page} / {viz.get('visualizationType', 'Unknown')} [{', '.join(viz.get('dataRole', []))}]")
                    if len(viz_list) > 5:
                        print(f"      ... and {len(viz_list) - 5} more")
                if len(measure_mapping) > 10:
                    print(f"\n  ... and {len(measure_mapping) - 10} more measures")
        
        print(f"\n{'=' * 80}")
        print(f"Total files in archive: {len(metadata.get('file_list', []))}")
        print("=" * 80)


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Parse Power BI .pbix files and extract metadata',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        'pbix_file',
        type=str,
        help='Path to the .pbix file to parse'
    )
    parser.add_argument(
        '-o', '--output',
        type=str,
        help='Output JSON file path (optional)'
    )
    parser.add_argument(
        '-s', '--summary',
        action='store_true',
        help='Print human-readable summary to stdout'
    )
    parser.add_argument(
        '--raw',
        action='store_true',
        help='Include raw JSON data in output (default: parsed only)'
    )
    
    args = parser.parse_args()
    
    try:
        # Parse the .pbix file
        pbix_parser = PBIXParser(args.pbix_file)
        metadata = pbix_parser.extract_metadata()
        
        # Prepare output
        output_data = metadata.copy()
        if not args.raw:
            # Remove raw data if not requested
            output_data.pop('data_model_raw', None)
            output_data.pop('layout_raw', None)
        
        # Print summary if requested
        if args.summary:
            pbix_parser.print_summary(metadata)
        
        # Save to file if requested
        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            print(f"\nMetadata saved to: {args.output}")
        elif not args.summary:
            # If no output file and no summary, print JSON to stdout
            print(json.dumps(output_data, indent=2, ensure_ascii=False))
            
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()

