import { Button, Checkbox, Input, PageTitle, Pill, SearchBar, Select, SimpleSelect, Table, ActionsBar } from '@components';
import React, { useCallback, useEffect, useRef, useState } from 'react';
import { useDebounce } from 'react-use';
import styled from 'styled-components';
import { useHistory } from 'react-router';

import { Column } from '@components/components/Table/types';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';
import { Message } from '@app/shared/Message';
import { scrollToTop } from '@app/shared/searchUtils';
import { PageRoutes } from '@conf/Global';
import { Entity, EntityData } from '../glossary.types';
import { EntityDetailsModal } from './EntityDetailsModal/EntityDetailsModal';
import { DiffModal } from './DiffModal/DiffModal';
import { ImportProgressModal } from './ImportProgressModal/ImportProgressModal';
import { useMockImportProcessing } from '../shared/hooks/useMockImportProcessing';
import { useMockCsvProcessing } from '../shared/hooks/useMockCsvProcessing';
import { useMockEntityManagement } from '../shared/hooks/useMockEntityManagement';
import { useMockGraphQLOperations } from '../shared/hooks/useMockGraphQLOperations';
import { useMockEntityComparison } from '../shared/hooks/useMockEntityComparison';
import { useApolloClient } from '@apollo/client';
import DropzoneTable from './DropzoneTable/DropzoneTable';
import { useImportState } from './WizardPage.hooks';
import { BreadcrumbHeader } from '../shared/components/BreadcrumbHeader';

const PageContainer = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    padding-top: 20px;
    background-color: white;
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        overflow: hidden;
        margin: 5px;
        box-shadow: ${props.theme.styles['box-shadow-navbar-redesign']};
        height: 100%;
    `}
`;

const PageContentContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 24px;
    flex: 1;
    margin: 0 20px 20px 20px;
    height: calc(100% - 60px); /* Increased space for ActionsBar */
    /* min-height: 0; Allow flex shrinking */
`;

const PageHeaderContainer = styled.div`
    && {
        padding-left: 20px;
        padding-right: 20px;
        display: flex;
        flex-direction: row;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 16px;
    }
`;

const TitleContainer = styled.div`
    flex: 1;
`;

const HeaderActionsContainer = styled.div`
    display: flex;
    justify-content: flex-end;
`;

const SourceContainer = styled.div`
    display: flex;
    flex-direction: column;
    height: 100%;
    overflow: auto;
`;

const HeaderContainer = styled.div`
    flex-shrink: 0;
`;

const StyledTabToolbar = styled.div`
    display: flex;
    justify-content: space-between;
    padding: 1px 0 16px 0; // 1px at the top to prevent Select's border outline from cutting-off
    height: auto;
    z-index: unset;
    box-shadow: none;
    flex-shrink: 0;
`;

const SearchContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const FilterButtonsContainer = styled.div`
    display: flex;
    gap: 8px;
`;

const StyledSearchBar = React.forwardRef<any, any>((props, ref) => (
    <SearchBar {...props} ref={ref} style={{ ...props.style, width: '400px' }} />
));

const StyledSimpleSelect = styled(SimpleSelect)`
    display: flex;
    align-self: start;
`;

const TableContainer = styled.div`
    flex: 1;
    overflow: auto;
    padding-bottom: 80px; /* Add space for ActionsBar */
`;

const PaginationContainer = styled.div`
    display: flex;
    justify-content: center;
    flex-shrink: 0;
`;

// Editable input styles for DataHub components
const EditableInputWrapper = styled.div`
    .InputWrapper {
        margin: 0;
    }
    
    .InputContainer {
        border: none;
        background: transparent;
        box-shadow: none;
        padding: 0;
        
        &:focus-within {
            border: 1px solid #1890ff;
            background: white;
            box-shadow: 0 0 0 2px rgba(24, 144, 255, 0.2);
        }
    }
    
    .InputField {
        border: none;
        background: transparent;
        padding: 4px 8px;
        
        &:focus {
            border: none;
            background: transparent;
            box-shadow: none;
        }
    }
    
    .Label {
        display: none;
    }
`;

const EditableSelectWrapper = styled.div`
    .SelectWrapper {
        margin: 0;
    }
    
    .SelectContainer {
        border: none;
        background: transparent;
        box-shadow: none;
        padding: 0;
        
        &:focus-within {
            border: 1px solid #1890ff;
            background: white;
            box-shadow: 0 0 0 2px rgba(24, 144, 255, 0.2);
        }
    }
    
    .Label {
        display: none;
    }
`;

const LeftAlignedSimpleSelect = styled(SimpleSelect)`
    .Container {
        text-align: left;
    }
    
    .SelectBase {
        justify-content: flex-start;
        text-align: left;
    }
    
    .SelectLabelContainer {
        justify-content: flex-start;
        text-align: left;
    }
`;

const EditableCell = styled.div`
    cursor: pointer;
    padding: 4px 8px;
    border-radius: 4px;
    
    &:hover {
        background-color: #f5f5f5;
    }
`;


const StepActions = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-top: 16px;
    padding-top: 16px;
    border-top: 1px solid #e8e8e8;
`;

const StepButtons = styled.div`
    display: flex;
    gap: 12px;
`;


// Wizard step definitions
const wizardSteps = [
    {
        title: 'Upload CSV',
        description: 'Select and upload your glossary CSV file',
        key: 'upload',
    },
    {
        title: 'Preview Data',
        description: 'Review and edit imported data',
        key: 'preview',
    },
    {
        title: 'Compare & Validate',
        description: 'Compare with existing entities',
        key: 'compare',
    },
    {
        title: 'Manage Hierarchy',
        description: 'Set up parent-child relationships',
        key: 'hierarchy',
    },
    {
        title: 'Import',
        description: 'Execute the import process',
        key: 'import',
    },
];

// Mock data for now - will be replaced with real data
const mockEntities: Entity[] = [
    {
        id: '1',
        name: 'Data Quality',
        type: 'glossaryNode',
        urn: 'urn:glossaryNode:data-quality',
        parentNames: ['Root'],
        parentUrns: ['urn:glossaryNode:root'],
        level: 1,
        data: {
            entity_type: 'glossaryNode',
            urn: 'urn:glossaryNode:data-quality',
            name: 'Data Quality',
            description: 'Terms related to data quality metrics and processes',
            term_source: 'Internal',
            source_ref: 'DQ-001',
            source_url: 'https://example.com/dq',
            ownership: 'Data Team',
            parent_nodes: 'Root',
            related_contains: 'Data Validation, Data Profiling',
            related_inherits: 'Quality Standards',
            domain_urn: 'urn:domain:data-quality',
            domain_name: 'Data Quality',
            custom_properties: '{"priority": "high"}',
        },
        status: 'new' as const,
    },
    {
        id: '2',
        name: 'Data Validation',
        type: 'glossaryTerm',
        urn: 'urn:glossaryTerm:data-validation',
        parentNames: ['Data Quality'],
        parentUrns: ['urn:glossaryNode:data-quality'],
        level: 2,
        data: {
            entity_type: 'glossaryTerm',
            urn: 'urn:glossaryTerm:data-validation',
            name: 'Data Validation',
            description: 'Process of checking data for accuracy and completeness',
            term_source: 'Internal',
            source_ref: 'DV-001',
            source_url: 'https://example.com/validation',
            ownership: 'Data Team',
            parent_nodes: 'Data Quality',
            related_contains: '',
            related_inherits: 'Data Quality',
            domain_urn: 'urn:domain:data-quality',
            domain_name: 'Data Quality',
            custom_properties: '{"type": "process"}',
        },
        status: 'updated' as const,
    },
    {
        id: '3',
        name: 'Data Profiling',
        type: 'glossaryTerm',
        urn: 'urn:glossaryTerm:data-profiling',
        parentNames: ['Data Quality'],
        parentUrns: ['urn:glossaryNode:data-quality'],
        level: 2,
        data: {
            entity_type: 'glossaryTerm',
            urn: 'urn:glossaryTerm:data-profiling',
            name: 'Data Profiling',
            description: 'Analysis of data to understand its structure and content',
            term_source: 'Internal',
            source_ref: 'DP-001',
            source_url: 'https://example.com/profiling',
            ownership: 'Data Team',
            parent_nodes: 'Data Quality',
            related_contains: '',
            related_inherits: 'Data Quality',
            domain_urn: 'urn:domain:data-quality',
            domain_name: 'Data Quality',
            custom_properties: '{"type": "analysis"}',
        },
        status: 'conflict' as const,
    },
    {
        id: '4',
        name: 'Data Governance',
        type: 'glossaryNode',
        urn: 'urn:glossaryNode:data-governance',
        parentNames: ['Root'],
        parentUrns: ['urn:glossaryNode:root'],
        level: 1,
        data: {
            entity_type: 'glossaryNode',
            urn: 'urn:glossaryNode:data-governance',
            name: 'Data Governance',
            description: 'Framework for managing data assets and ensuring compliance',
            term_source: 'External',
            source_ref: 'DG-001',
            source_url: 'https://example.com/governance',
            ownership: 'Governance Team',
            parent_nodes: 'Root',
            related_contains: 'Data Policies, Data Standards',
            related_inherits: 'Compliance Framework',
            domain_urn: 'urn:domain:governance',
            domain_name: 'Data Governance',
            custom_properties: '{"framework": "DAMA"}',
        },
        status: 'new' as const,
    },
    {
        id: '5',
        name: 'Data Policies',
        type: 'glossaryTerm',
        urn: 'urn:glossaryTerm:data-policies',
        parentNames: ['Data Governance'],
        parentUrns: ['urn:glossaryNode:data-governance'],
        level: 2,
        data: {
            entity_type: 'glossaryTerm',
            urn: 'urn:glossaryTerm:data-policies',
            name: 'Data Policies',
            description: 'Rules and guidelines for data management and usage',
            term_source: 'External',
            source_ref: 'DP-002',
            source_url: 'https://example.com/policies',
            ownership: 'Governance Team',
            parent_nodes: 'Data Governance',
            related_contains: '',
            related_inherits: 'Data Governance',
            domain_urn: 'urn:domain:governance',
            domain_name: 'Data Governance',
            custom_properties: '{"category": "compliance"}',
        },
        status: 'updated' as const,
    },
    {
        id: '6',
        name: 'Data Standards',
        type: 'glossaryTerm',
        urn: 'urn:glossaryTerm:data-standards',
        parentNames: ['Data Governance'],
        parentUrns: ['urn:glossaryNode:data-governance'],
        level: 2,
        data: {
            entity_type: 'glossaryTerm',
            urn: 'urn:glossaryTerm:data-standards',
            name: 'Data Standards',
            description: 'Technical specifications for data formats and structures',
            term_source: 'Internal',
            source_ref: 'DS-001',
            source_url: 'https://example.com/standards',
            ownership: 'Data Architecture Team',
            parent_nodes: 'Data Governance',
            related_contains: '',
            related_inherits: 'Data Governance',
            domain_urn: 'urn:domain:governance',
            domain_name: 'Data Governance',
            custom_properties: '{"version": "2.1"}',
        },
        status: 'conflict' as const,
    },
    {
        id: '7',
        name: 'Data Security',
        type: 'glossaryNode',
        urn: 'urn:glossaryNode:data-security',
        parentNames: ['Root'],
        parentUrns: ['urn:glossaryNode:root'],
        level: 1,
        data: {
            entity_type: 'glossaryNode',
            urn: 'urn:glossaryNode:data-security',
            name: 'Data Security',
            description: 'Measures to protect data from unauthorized access and breaches',
            term_source: 'Internal',
            source_ref: 'SEC-001',
            source_url: 'https://example.com/security',
            ownership: 'Security Team',
            parent_nodes: 'Root',
            related_contains: 'Encryption, Access Control',
            related_inherits: 'Security Framework',
            domain_urn: 'urn:domain:security',
            domain_name: 'Data Security',
            custom_properties: '{"level": "critical"}',
        },
        status: 'new' as const,
    },
    {
        id: '8',
        name: 'Encryption',
        type: 'glossaryTerm',
        urn: 'urn:glossaryTerm:encryption',
        parentNames: ['Data Security'],
        parentUrns: ['urn:glossaryNode:data-security'],
        level: 2,
        data: {
            entity_type: 'glossaryTerm',
            urn: 'urn:glossaryTerm:encryption',
            name: 'Encryption',
            description: 'Process of encoding data to prevent unauthorized access',
            term_source: 'External',
            source_ref: 'ENC-001',
            source_url: 'https://example.com/encryption',
            ownership: 'Security Team',
            parent_nodes: 'Data Security',
            related_contains: '',
            related_inherits: 'Data Security',
            domain_urn: 'urn:domain:security',
            domain_name: 'Data Security',
            custom_properties: '{"algorithm": "AES-256"}',
        },
        status: 'updated' as const,
    },
    {
        id: '9',
        name: 'Access Control',
        type: 'glossaryTerm',
        urn: 'urn:glossaryTerm:access-control',
        parentNames: ['Data Security'],
        parentUrns: ['urn:glossaryNode:data-security'],
        level: 2,
        data: {
            entity_type: 'glossaryTerm',
            urn: 'urn:glossaryTerm:access-control',
            name: 'Access Control',
            description: 'Mechanisms to control who can access data resources',
            term_source: 'Internal',
            source_ref: 'AC-001',
            source_url: 'https://example.com/access-control',
            ownership: 'Security Team',
            parent_nodes: 'Data Security',
            related_contains: '',
            related_inherits: 'Data Security',
            domain_urn: 'urn:domain:security',
            domain_name: 'Data Security',
            custom_properties: '{"model": "RBAC"}',
        },
        status: 'conflict' as const,
    },
    {
        id: '10',
        name: 'Data Analytics',
        type: 'glossaryNode',
        urn: 'urn:glossaryNode:data-analytics',
        parentNames: ['Root'],
        parentUrns: ['urn:glossaryNode:root'],
        level: 1,
        data: {
            entity_type: 'glossaryNode',
            urn: 'urn:glossaryNode:data-analytics',
            name: 'Data Analytics',
            description: 'Process of analyzing data to extract insights and patterns',
            term_source: 'Internal',
            source_ref: 'DA-001',
            source_url: 'https://example.com/analytics',
            ownership: 'Analytics Team',
            parent_nodes: 'Root',
            related_contains: 'Machine Learning, Statistical Analysis',
            related_inherits: 'Analytics Framework',
            domain_urn: 'urn:domain:analytics',
            domain_name: 'Data Analytics',
            custom_properties: '{"methodology": "CRISP-DM"}',
        },
        status: 'new' as const,
    },
    {
        id: '11',
        name: 'Machine Learning',
        type: 'glossaryTerm',
        urn: 'urn:glossaryTerm:machine-learning',
        parentNames: ['Data Analytics'],
        parentUrns: ['urn:glossaryNode:data-analytics'],
        level: 2,
        data: {
            entity_type: 'glossaryTerm',
            urn: 'urn:glossaryTerm:machine-learning',
            name: 'Machine Learning',
            description: 'Algorithm-based approach to data analysis and pattern recognition',
            term_source: 'External',
            source_ref: 'ML-001',
            source_url: 'https://example.com/ml',
            ownership: 'Analytics Team',
            parent_nodes: 'Data Analytics',
            related_contains: '',
            related_inherits: 'Data Analytics',
            domain_urn: 'urn:domain:analytics',
            domain_name: 'Data Analytics',
            custom_properties: '{"type": "supervised"}',
        },
        status: 'updated' as const,
    },
    {
        id: '12',
        name: 'Statistical Analysis',
        type: 'glossaryTerm',
        urn: 'urn:glossaryTerm:statistical-analysis',
        parentNames: ['Data Analytics'],
        parentUrns: ['urn:glossaryNode:data-analytics'],
        level: 2,
        data: {
            entity_type: 'glossaryTerm',
            urn: 'urn:glossaryTerm:statistical-analysis',
            name: 'Statistical Analysis',
            description: 'Mathematical methods for analyzing data patterns and relationships',
            term_source: 'External',
            source_ref: 'SA-001',
            source_url: 'https://example.com/stats',
            ownership: 'Analytics Team',
            parent_nodes: 'Data Analytics',
            related_contains: '',
            related_inherits: 'Data Analytics',
            domain_urn: 'urn:domain:analytics',
            domain_name: 'Data Analytics',
            custom_properties: '{"method": "regression"}',
        },
        status: 'conflict' as const,
    },
    {
        id: '13',
        name: 'Data Integration',
        type: 'glossaryNode',
        urn: 'urn:glossaryNode:data-integration',
        parentNames: ['Root'],
        parentUrns: ['urn:glossaryNode:root'],
        level: 1,
        data: {
            entity_type: 'glossaryNode',
            urn: 'urn:glossaryNode:data-integration',
            name: 'Data Integration',
            description: 'Process of combining data from multiple sources into a unified view',
            term_source: 'Internal',
            source_ref: 'DI-001',
            source_url: 'https://example.com/integration',
            ownership: 'Data Engineering Team',
            parent_nodes: 'Root',
            related_contains: 'ETL, Data Pipelines',
            related_inherits: 'Integration Framework',
            domain_urn: 'urn:domain:integration',
            domain_name: 'Data Integration',
            custom_properties: '{"pattern": "batch"}',
        },
        status: 'new' as const,
    },
    {
        id: '14',
        name: 'ETL',
        type: 'glossaryTerm',
        urn: 'urn:glossaryTerm:etl',
        parentNames: ['Data Integration'],
        parentUrns: ['urn:glossaryNode:data-integration'],
        level: 2,
        data: {
            entity_type: 'glossaryTerm',
            urn: 'urn:glossaryTerm:etl',
            name: 'ETL',
            description: 'Extract, Transform, Load - process for moving and transforming data',
            term_source: 'External',
            source_ref: 'ETL-001',
            source_url: 'https://example.com/etl',
            ownership: 'Data Engineering Team',
            parent_nodes: 'Data Integration',
            related_contains: '',
            related_inherits: 'Data Integration',
            domain_urn: 'urn:domain:integration',
            domain_name: 'Data Integration',
            custom_properties: '{"type": "batch"}',
        },
        status: 'updated' as const,
    },
    {
        id: '15',
        name: 'Data Pipelines',
        type: 'glossaryTerm',
        urn: 'urn:glossaryTerm:data-pipelines',
        parentNames: ['Data Integration'],
        parentUrns: ['urn:glossaryNode:data-integration'],
        level: 2,
        data: {
            entity_type: 'glossaryTerm',
            urn: 'urn:glossaryTerm:data-pipelines',
            name: 'Data Pipelines',
            description: 'Automated workflows for processing and moving data',
            term_source: 'Internal',
            source_ref: 'DP-003',
            source_url: 'https://example.com/pipelines',
            ownership: 'Data Engineering Team',
            parent_nodes: 'Data Integration',
            related_contains: '',
            related_inherits: 'Data Integration',
            domain_urn: 'urn:domain:integration',
            domain_name: 'Data Integration',
            custom_properties: '{"framework": "Apache Airflow"}',
        },
        status: 'conflict' as const,
    },
    {
        id: '16',
        name: 'Data Architecture',
        type: 'glossaryNode',
        urn: 'urn:glossaryNode:data-architecture',
        parentNames: ['Root'],
        parentUrns: ['urn:glossaryNode:root'],
        level: 1,
        data: {
            entity_type: 'glossaryNode',
            urn: 'urn:glossaryNode:data-architecture',
            name: 'Data Architecture',
            description: 'Design and structure of data systems and infrastructure',
            term_source: 'Internal',
            source_ref: 'ARCH-001',
            source_url: 'https://example.com/architecture',
            ownership: 'Data Architecture Team',
            parent_nodes: 'Root',
            related_contains: 'Data Models, Data Warehouses',
            related_inherits: 'Architecture Framework',
            domain_urn: 'urn:domain:architecture',
            domain_name: 'Data Architecture',
            custom_properties: '{"style": "layered"}',
        },
        status: 'new' as const,
    },
    {
        id: '17',
        name: 'Data Models',
        type: 'glossaryTerm',
        urn: 'urn:glossaryTerm:data-models',
        parentNames: ['Data Architecture'],
        parentUrns: ['urn:glossaryNode:data-architecture'],
        level: 2,
        data: {
            entity_type: 'glossaryTerm',
            urn: 'urn:glossaryTerm:data-models',
            name: 'Data Models',
            description: 'Conceptual representations of data structures and relationships',
            term_source: 'External',
            source_ref: 'DM-001',
            source_url: 'https://example.com/models',
            ownership: 'Data Architecture Team',
            parent_nodes: 'Data Architecture',
            related_contains: '',
            related_inherits: 'Data Architecture',
            domain_urn: 'urn:domain:architecture',
            domain_name: 'Data Architecture',
            custom_properties: '{"type": "conceptual"}',
        },
        status: 'updated' as const,
    },
    {
        id: '18',
        name: 'Data Warehouses',
        type: 'glossaryTerm',
        urn: 'urn:glossaryTerm:data-warehouses',
        parentNames: ['Data Architecture'],
        parentUrns: ['urn:glossaryNode:data-architecture'],
        level: 2,
        data: {
            entity_type: 'glossaryTerm',
            urn: 'urn:glossaryTerm:data-warehouses',
            name: 'Data Warehouses',
            description: 'Centralized repositories for storing and analyzing large datasets',
            term_source: 'External',
            source_ref: 'DW-001',
            source_url: 'https://example.com/warehouses',
            ownership: 'Data Architecture Team',
            parent_nodes: 'Data Architecture',
            related_contains: '',
            related_inherits: 'Data Architecture',
            domain_urn: 'urn:domain:architecture',
            domain_name: 'Data Architecture',
            custom_properties: '{"type": "OLAP"}',
        },
        status: 'conflict' as const,
    },
    {
        id: '19',
        name: 'Data Privacy',
        type: 'glossaryNode',
        urn: 'urn:glossaryNode:data-privacy',
        parentNames: ['Root'],
        parentUrns: ['urn:glossaryNode:root'],
        level: 1,
        data: {
            entity_type: 'glossaryNode',
            urn: 'urn:glossaryNode:data-privacy',
            name: 'Data Privacy',
            description: 'Protection of personal and sensitive data from unauthorized use',
            term_source: 'External',
            source_ref: 'PRIV-001',
            source_url: 'https://example.com/privacy',
            ownership: 'Privacy Team',
            parent_nodes: 'Root',
            related_contains: 'GDPR, Data Anonymization',
            related_inherits: 'Privacy Framework',
            domain_urn: 'urn:domain:privacy',
            domain_name: 'Data Privacy',
            custom_properties: '{"regulation": "GDPR"}',
        },
        status: 'new' as const,
    },
    {
        id: '20',
        name: 'GDPR',
        type: 'glossaryTerm',
        urn: 'urn:glossaryTerm:gdpr',
        parentNames: ['Data Privacy'],
        parentUrns: ['urn:glossaryNode:data-privacy'],
        level: 2,
        data: {
            entity_type: 'glossaryTerm',
            urn: 'urn:glossaryTerm:gdpr',
            name: 'GDPR',
            description: 'General Data Protection Regulation - EU privacy law',
            term_source: 'External',
            source_ref: 'GDPR-001',
            source_url: 'https://example.com/gdpr',
            ownership: 'Privacy Team',
            parent_nodes: 'Data Privacy',
            related_contains: '',
            related_inherits: 'Data Privacy',
            domain_urn: 'urn:domain:privacy',
            domain_name: 'Data Privacy',
            custom_properties: '{"jurisdiction": "EU"}',
        },
        status: 'updated' as const,
    },
    {
        id: '21',
        name: 'Data Anonymization',
        type: 'glossaryTerm',
        urn: 'urn:glossaryTerm:data-anonymization',
        parentNames: ['Data Privacy'],
        parentUrns: ['urn:glossaryNode:data-privacy'],
        level: 2,
        data: {
            entity_type: 'glossaryTerm',
            urn: 'urn:glossaryTerm:data-anonymization',
            name: 'Data Anonymization',
            description: 'Process of removing or masking personally identifiable information',
            term_source: 'Internal',
            source_ref: 'ANON-001',
            source_url: 'https://example.com/anonymization',
            ownership: 'Privacy Team',
            parent_nodes: 'Data Privacy',
            related_contains: '',
            related_inherits: 'Data Privacy',
            domain_urn: 'urn:domain:privacy',
            domain_name: 'Data Privacy',
            custom_properties: '{"method": "k-anonymity"}',
        },
        status: 'conflict' as const,
    },
    {
        id: '22',
        name: 'Data Lineage',
        type: 'glossaryNode',
        urn: 'urn:glossaryNode:data-lineage',
        parentNames: ['Root'],
        parentUrns: ['urn:glossaryNode:root'],
        level: 1,
        data: {
            entity_type: 'glossaryNode',
            urn: 'urn:glossaryNode:data-lineage',
            name: 'Data Lineage',
            description: 'Tracking the origin and transformation of data through systems',
            term_source: 'Internal',
            source_ref: 'LINE-001',
            source_url: 'https://example.com/lineage',
            ownership: 'Data Governance Team',
            parent_nodes: 'Root',
            related_contains: 'Data Provenance, Impact Analysis',
            related_inherits: 'Lineage Framework',
            domain_urn: 'urn:domain:lineage',
            domain_name: 'Data Lineage',
            custom_properties: '{"type": "technical"}',
        },
        status: 'new' as const,
    },
    {
        id: '23',
        name: 'Data Provenance',
        type: 'glossaryTerm',
        urn: 'urn:glossaryTerm:data-provenance',
        parentNames: ['Data Lineage'],
        parentUrns: ['urn:glossaryNode:data-lineage'],
        level: 2,
        data: {
            entity_type: 'glossaryTerm',
            urn: 'urn:glossaryTerm:data-provenance',
            name: 'Data Provenance',
            description: 'Documentation of the origin and history of data',
            term_source: 'External',
            source_ref: 'PROV-001',
            source_url: 'https://example.com/provenance',
            ownership: 'Data Governance Team',
            parent_nodes: 'Data Lineage',
            related_contains: '',
            related_inherits: 'Data Lineage',
            domain_urn: 'urn:domain:lineage',
            domain_name: 'Data Lineage',
            custom_properties: '{"standard": "W3C-PROV"}',
        },
        status: 'updated' as const,
    },
    {
        id: '24',
        name: 'Impact Analysis',
        type: 'glossaryTerm',
        urn: 'urn:glossaryTerm:impact-analysis',
        parentNames: ['Data Lineage'],
        parentUrns: ['urn:glossaryNode:data-lineage'],
        level: 2,
        data: {
            entity_type: 'glossaryTerm',
            urn: 'urn:glossaryTerm:impact-analysis',
            name: 'Impact Analysis',
            description: 'Assessment of how changes affect downstream data consumers',
            term_source: 'Internal',
            source_ref: 'IMP-001',
            source_url: 'https://example.com/impact',
            ownership: 'Data Governance Team',
            parent_nodes: 'Data Lineage',
            related_contains: '',
            related_inherits: 'Data Lineage',
            domain_urn: 'urn:domain:lineage',
            domain_name: 'Data Lineage',
            custom_properties: '{"scope": "downstream"}',
        },
        status: 'conflict' as const,
    },
    {
        id: '25',
        name: 'Data Catalog',
        type: 'glossaryNode',
        urn: 'urn:glossaryNode:data-catalog',
        parentNames: ['Root'],
        parentUrns: ['urn:glossaryNode:root'],
        level: 1,
        data: {
            entity_type: 'glossaryNode',
            urn: 'urn:glossaryNode:data-catalog',
            name: 'Data Catalog',
            description: 'Centralized inventory of data assets and their metadata',
            term_source: 'Internal',
            source_ref: 'CAT-001',
            source_url: 'https://example.com/catalog',
            ownership: 'Data Management Team',
            parent_nodes: 'Root',
            related_contains: 'Metadata Management, Data Discovery',
            related_inherits: 'Catalog Framework',
            domain_urn: 'urn:domain:catalog',
            domain_name: 'Data Catalog',
            custom_properties: '{"type": "automated"}',
        },
        status: 'new' as const,
    },
    {
        id: '26',
        name: 'Metadata Management',
        type: 'glossaryTerm',
        urn: 'urn:glossaryTerm:metadata-management',
        parentNames: ['Data Catalog'],
        parentUrns: ['urn:glossaryNode:data-catalog'],
        level: 2,
        data: {
            entity_type: 'glossaryTerm',
            urn: 'urn:glossaryTerm:metadata-management',
            name: 'Metadata Management',
            description: 'Process of collecting, storing, and maintaining data metadata',
            term_source: 'External',
            source_ref: 'META-001',
            source_url: 'https://example.com/metadata',
            ownership: 'Data Management Team',
            parent_nodes: 'Data Catalog',
            related_contains: '',
            related_inherits: 'Data Catalog',
            domain_urn: 'urn:domain:catalog',
            domain_name: 'Data Catalog',
            custom_properties: '{"standard": "Dublin Core"}',
        },
        status: 'updated' as const,
    },
    {
        id: '27',
        name: 'Data Discovery',
        type: 'glossaryTerm',
        urn: 'urn:glossaryTerm:data-discovery',
        parentNames: ['Data Catalog'],
        parentUrns: ['urn:glossaryNode:data-catalog'],
        level: 2,
        data: {
            entity_type: 'glossaryTerm',
            urn: 'urn:glossaryTerm:data-discovery',
            name: 'Data Discovery',
            description: 'Process of finding and identifying relevant data assets',
            term_source: 'Internal',
            source_ref: 'DISC-001',
            source_url: 'https://example.com/discovery',
            ownership: 'Data Management Team',
            parent_nodes: 'Data Catalog',
            related_contains: '',
            related_inherits: 'Data Catalog',
            domain_urn: 'urn:domain:catalog',
            domain_name: 'Data Catalog',
            custom_properties: '{"method": "search"}',
        },
        status: 'conflict' as const,
    },
    {
        id: '28',
        name: 'Data Stewardship',
        type: 'glossaryNode',
        urn: 'urn:glossaryNode:data-stewardship',
        parentNames: ['Root'],
        parentUrns: ['urn:glossaryNode:root'],
        level: 1,
        data: {
            entity_type: 'glossaryNode',
            urn: 'urn:glossaryNode:data-stewardship',
            name: 'Data Stewardship',
            description: 'Responsibility for data quality, governance, and lifecycle management',
            term_source: 'External',
            source_ref: 'STEW-001',
            source_url: 'https://example.com/stewardship',
            ownership: 'Data Stewards',
            parent_nodes: 'Root',
            related_contains: 'Data Ownership, Data Quality Management',
            related_inherits: 'Stewardship Framework',
            domain_urn: 'urn:domain:stewardship',
            domain_name: 'Data Stewardship',
            custom_properties: '{"role": "business"}',
        },
        status: 'new' as const,
    },
    {
        id: '29',
        name: 'Data Ownership',
        type: 'glossaryTerm',
        urn: 'urn:glossaryTerm:data-ownership',
        parentNames: ['Data Stewardship'],
        parentUrns: ['urn:glossaryNode:data-stewardship'],
        level: 2,
        data: {
            entity_type: 'glossaryTerm',
            urn: 'urn:glossaryTerm:data-ownership',
            name: 'Data Ownership',
            description: 'Assignment of responsibility and accountability for data assets',
            term_source: 'Internal',
            source_ref: 'OWN-001',
            source_url: 'https://example.com/ownership',
            ownership: 'Data Stewards',
            parent_nodes: 'Data Stewardship',
            related_contains: '',
            related_inherits: 'Data Stewardship',
            domain_urn: 'urn:domain:stewardship',
            domain_name: 'Data Stewardship',
            custom_properties: '{"type": "business"}',
        },
        status: 'updated' as const,
    },
    {
        id: '30',
        name: 'Data Quality Management',
        type: 'glossaryTerm',
        urn: 'urn:glossaryTerm:data-quality-management',
        parentNames: ['Data Stewardship'],
        parentUrns: ['urn:glossaryNode:data-stewardship'],
        level: 2,
        data: {
            entity_type: 'glossaryTerm',
            urn: 'urn:glossaryTerm:data-quality-management',
            name: 'Data Quality Management',
            description: 'Processes and practices for ensuring data accuracy and reliability',
            term_source: 'External',
            source_ref: 'DQM-001',
            source_url: 'https://example.com/quality-management',
            ownership: 'Data Stewards',
            parent_nodes: 'Data Stewardship',
            related_contains: '',
            related_inherits: 'Data Stewardship',
            domain_urn: 'urn:domain:stewardship',
            domain_name: 'Data Stewardship',
            custom_properties: '{"framework": "DMBOK"}',
        },
        status: 'conflict' as const,
    },
];

const RefreshButton = ({ onClick }: { onClick?: () => void }) => {
    return (
        <Button variant="text" onClick={onClick} icon={{ icon: 'ArrowClockwise', source: 'phosphor' }}>
            Restart
        </Button>
    );
};

// This is the main content component that replaces IngestionSourceList
const GlossaryImportList = ({ 
    entities, 
    setEntities, 
    onRestart, 
    csvProcessing, 
    entityManagement,
    onStartImport,
    isImportModalVisible,
    setIsImportModalVisible,
    progress,
    isProcessing,
    resetProgress,
    retryFailed
}: { 
    entities: Entity[], 
    setEntities: (entities: Entity[]) => void, 
    onRestart: () => void,
    csvProcessing: any,
    entityManagement: any,
    onStartImport: () => void,
    isImportModalVisible: boolean,
    setIsImportModalVisible: (visible: boolean) => void,
    progress: any,
    isProcessing: boolean,
    resetProgress: () => void,
    retryFailed: () => void
}) => {
    const [query, setQuery] = useState<undefined | string>(undefined);
    const [searchInput, setSearchInput] = useState('');
    const searchInputRef = useRef<any>(null);
    const [statusFilter, setStatusFilter] = useState<number>(0); // 0 = All, 1 = New, 2 = Updated, 3 = Conflict
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    // const [selectedRowKeys, setSelectedRowKeys] = useState<string[]>([]); // Hidden for now
    const [editingCell, setEditingCell] = useState<{ rowId: string; field: string } | null>(null);
    
    // Entity Details Modal state
    const [isModalVisible, setIsModalVisible] = useState(false);
    const [isDiffModalVisible, setIsDiffModalVisible] = useState(false);
    const [selectedEntity, setSelectedEntity] = useState<EntityData | null>(null);


    // Initialize search input from URL parameter (if needed)
    useEffect(() => {
        if (query?.length) {
            setSearchInput(query);
            setTimeout(() => {
                searchInputRef.current?.focus?.();
            }, 0);
        }
    }, [query]);

    const handleSearchInputChange = (value: string) => {
        setSearchInput(value);
    };

    // Debounce the search query
    useDebounce(
        () => {
            setQuery(searchInput);
        },
        300,
        [searchInput],
    );

    const onChangeSort = useCallback((field: string, order: 'ascend' | 'descend' | undefined) => {
        // Handle sorting logic here
        console.log('Sort by:', field, order);
    }, []);

    const handleSortColumnChange = ({ sortColumn, sortOrder }) => {
        onChangeSort(sortColumn, sortOrder);
    };

    const handleRefresh = () => {
        onRestart();
    };

    // Checkbox handlers - Hidden for now
    // const handleSelectAll = () => {
    //     if (selectedRowKeys.length === totalItems) {
    //         // Deselect all
    //         setSelectedRowKeys([]);
    //     } else {
    //         // Select all
    //         setSelectedRowKeys(entities.map(entity => entity.id));
    //     }
    // };

    // const handleSelectRow = (entityId: string, checked: boolean) => {
    //     if (checked) {
    //         setSelectedRowKeys(prev => [...prev, entityId]);
    //     } else {
    //         setSelectedRowKeys(prev => prev.filter(id => id !== entityId));
    //         // If we deselect any item, reset to manual selection mode
    //         setSelectionMode('none');
    //     }
    // };

    // Cell editing handlers
    const handleCellEdit = (rowId: string, field: string) => {
        setEditingCell({ rowId, field });
    };

    const handleCellSave = (rowId: string, field: string, value: string) => {
        const updatedEntities = entities.map(entity => 
            entity.id === rowId 
                ? { ...entity, data: { ...entity.data, [field]: value } }
                : entity
        );
        setEntities(updatedEntities);
        setEditingCell(null);
    };

    const handleCellCancel = () => {
        setEditingCell(null);
    };

    const isEditing = (rowId: string, field: string) => {
        return editingCell?.rowId === rowId && editingCell?.field === field;
    };

    // Modal handlers

    const handleCloseModal = useCallback(() => {
        setIsModalVisible(false);
        setSelectedEntity(null);
    }, []);

    const handleShowDiff = useCallback((entity: Entity) => {
        setSelectedEntity(entity.data);
        setIsDiffModalVisible(true);
    }, []);

    const handleCloseDiffModal = useCallback(() => {
        setIsDiffModalVisible(false);
        setSelectedEntity(null);
    }, []);

    const handleSaveEntity = useCallback((updatedData: EntityData) => {
        if (selectedEntity) {
            const updatedEntities = entities.map(entity => 
                entity.data === selectedEntity 
                    ? { ...entity, data: updatedData }
                    : entity
            );
            setEntities(updatedEntities);
        }
        handleCloseModal();
    }, [selectedEntity, handleCloseModal, entities, setEntities]);


    const handleCloseImportModal = useCallback(() => {
        setIsImportModalVisible(false);
        if (!isProcessing) {
            resetProgress();
        }
    }, [isProcessing, resetProgress]);

    const handleRetryFailed = useCallback(async () => {
        try {
            await retryFailed();
        } catch (error) {
            console.error('Retry failed:', error);
        }
    }, [retryFailed]);

    // Calculate total items for display
    const totalItems = entities.length;

    // Smart selection state logic - Hidden for now
    // const isChecked = selectedRowKeys.length === totalItems && totalItems > 0;
    // const isIndeterminate = selectedRowKeys.length > 0 && selectedRowKeys.length < totalItems;

    const tableColumns: Column<Entity>[] = [
        {
            title: 'Diff',
            key: 'diff',
            render: (record) => (
                <Button
                    variant="text"
                    size="xs"
                    onClick={(e) => {
                        e.stopPropagation();
                        handleShowDiff(record);
                    }}
                    style={{ padding: '2px 6px', minWidth: 'auto' }}
                >
                    Diff
                </Button>
            ),
            width: '80px',
            minWidth: '80px',
            alignment: 'center',
        },
        {
            title: 'Status',
            key: 'status',
            render: (record) => {
                const getStatusColor = (status: string) => {
                    switch (status) {
                        case 'new':
                            return 'green';
                        case 'updated':
                            return 'blue';
                        case 'conflict':
                            return 'red';
                        default:
                            return 'gray';
                    }
                };

                const getStatusLabel = (status: string) => {
                    return status.charAt(0).toUpperCase() + status.slice(1);
                };

                return (
                    <Pill
                        label={getStatusLabel(record.status)}
                        color={getStatusColor(record.status)}
                        size="sm"
                        variant="filled"
                    />
                );
            },
            width: '100px',
            minWidth: '100px',
            alignment: 'left',
            sorter: (a, b) => a.status.localeCompare(b.status),
        },
        {
            title: 'Type',
            key: 'type',
            render: (record) => record.type === 'glossaryNode' ? 'Term Group' : 'Term',
            width: '100px',
            minWidth: '100px',
            alignment: 'left',
            sorter: (a, b) => a.type.localeCompare(b.type),
        },
        {
            title: 'Name',
            key: 'name',
            render: (record) => {
                if (isEditing(record.id, 'name')) {
                    return (
                        <EditableInputWrapper>
                            <Input
                                value={record.name}
                                setValue={(value) => {
                                    const stringValue = typeof value === 'function' ? value('') : value;
                                    handleCellSave(record.id, 'name', stringValue);
                                }}
                                placeholder="Enter name"
                                label=""
                            />
                        </EditableInputWrapper>
                    );
                }
                return (
                    <EditableCell onClick={() => handleCellEdit(record.id, 'name')}>
                        {record.name}
                    </EditableCell>
                );
            },
            width: '200px',
            minWidth: '200px',
            alignment: 'left',
            sorter: (a, b) => a.name.localeCompare(b.name),
        },
        {
            title: 'Parent Node',
            key: 'parentNode',
            render: (record) => {
                if (isEditing(record.id, 'parentNames')) {
                    return (
                        <EditableInputWrapper>
                            <Input
                                value={record.parentNames.join(' > ')}
                                setValue={(value) => {
                                    const stringValue = typeof value === 'function' ? value('') : value;
                                    const newParentNames = stringValue.split(' > ').filter(name => name.trim());
                                    const updatedEntities = entities.map(entity => 
                                        entity.id === record.id 
                                            ? { ...entity, parentNames: newParentNames }
                                            : entity
                                    );
                                    setEntities(updatedEntities);
                                }}
                                placeholder="Enter parent path (e.g., Group > Subgroup)"
                                label=""
                            />
                        </EditableInputWrapper>
                    );
                }
                return (
                    <EditableCell onClick={() => handleCellEdit(record.id, 'parentNames')}>
                        {record.parentNames.join(' > ')}
                    </EditableCell>
                );
            },
            width: '150px',
            minWidth: '150px',
            alignment: 'left',
            sorter: (a, b) => a.parentNames.join(' > ').localeCompare(b.parentNames.join(' > ')),
        },
        {
            title: 'Description',
            key: 'description',
            render: (record) => {
                if (isEditing(record.id, 'description')) {
                    return (
                        <EditableInputWrapper>
                            <Input
                                value={record.data.description}
                                setValue={(value) => {
                                    const stringValue = typeof value === 'function' ? value('') : value;
                                    handleCellSave(record.id, 'description', stringValue);
                                }}
                                placeholder="Enter description"
                                label=""
                            />
                        </EditableInputWrapper>
                    );
                }
                return (
                    <EditableCell onClick={() => handleCellEdit(record.id, 'description')}>
                        {record.data.description}
                    </EditableCell>
                );
            },
            width: '250px',
            minWidth: '250px',
            alignment: 'left',
            sorter: (a, b) => a.data.description.localeCompare(b.data.description),
        },
        {
            title: 'Term Source',
            key: 'termSource',
            render: (record) => {
                if (isEditing(record.id, 'term_source')) {
                    return (
                        <EditableInputWrapper>
                            <Input
                                value={record.data.term_source}
                                setValue={(value) => {
                                    const stringValue = typeof value === 'function' ? value('') : value;
                                    handleCellSave(record.id, 'term_source', stringValue);
                                }}
                                placeholder="Enter term source"
                                label=""
                            />
                        </EditableInputWrapper>
                    );
                }
                return (
                    <EditableCell onClick={() => handleCellEdit(record.id, 'term_source')}>
                        {record.data.term_source}
                    </EditableCell>
                );
            },
            width: '140px',
            minWidth: '140px',
            alignment: 'left',
            sorter: (a, b) => a.data.term_source.localeCompare(b.data.term_source),
        },
        {
            title: 'Source Ref',
            key: 'sourceRef',
            render: (record) => {
                if (isEditing(record.id, 'source_ref')) {
                    return (
                        <EditableInputWrapper>
                            <Input
                                value={record.data.source_ref}
                                setValue={(value) => {
                                    const stringValue = typeof value === 'function' ? value('') : value;
                                    handleCellSave(record.id, 'source_ref', stringValue);
                                }}
                                placeholder="Enter source ref"
                                label=""
                            />
                        </EditableInputWrapper>
                    );
                }
                return (
                    <EditableCell onClick={() => handleCellEdit(record.id, 'source_ref')}>
                        {record.data.source_ref}
                    </EditableCell>
                );
            },
            width: '120px',
            minWidth: '120px',
            alignment: 'left',
            sorter: (a, b) => a.data.source_ref.localeCompare(b.data.source_ref),
        },
        {
            title: 'Source URL',
            key: 'sourceUrl',
            render: (record) => {
                if (isEditing(record.id, 'source_url')) {
                    return (
                        <EditableInputWrapper>
                            <Input
                                value={record.data.source_url}
                                setValue={(value) => {
                                    const stringValue = typeof value === 'function' ? value('') : value;
                                    handleCellSave(record.id, 'source_url', stringValue);
                                }}
                                placeholder="Enter source URL"
                                label=""
                            />
                        </EditableInputWrapper>
                    );
                }
                return (
                    <EditableCell onClick={() => handleCellEdit(record.id, 'source_url')}>
                        {record.data.source_url}
                    </EditableCell>
                );
            },
            width: '150px',
            minWidth: '150px',
            alignment: 'left',
            sorter: (a, b) => a.data.source_url.localeCompare(b.data.source_url),
        },
        {
            title: 'Ownership (Users)',
            key: 'ownership_users',
            render: (record) => {
                if (isEditing(record.id, 'ownership_users')) {
                    return (
                        <EditableInputWrapper>
                            <Input
                                value={record.data.ownership_users}
                                setValue={(value) => {
                                    const stringValue = typeof value === 'function' ? value('') : value;
                                    handleCellSave(record.id, 'ownership_users', stringValue);
                                }}
                                placeholder="Enter user ownership (e.g., admin:DEVELOPER)"
                                label=""
                            />
                        </EditableInputWrapper>
                    );
                }
                return (
                    <EditableCell onClick={() => handleCellEdit(record.id, 'ownership_users')}>
                        {record.data.ownership_users}
                    </EditableCell>
                );
            },
            width: '180px',
            minWidth: '180px',
            alignment: 'left',
            sorter: (a, b) => a.data.ownership_users.localeCompare(b.data.ownership_users),
        },
        {
            title: 'Ownership (Groups)',
            key: 'ownership_groups',
            render: (record) => {
                if (isEditing(record.id, 'ownership_groups')) {
                    return (
                        <EditableInputWrapper>
                            <Input
                                value={record.data.ownership_groups}
                                setValue={(value) => {
                                    const stringValue = typeof value === 'function' ? value('') : value;
                                    handleCellSave(record.id, 'ownership_groups', stringValue);
                                }}
                                placeholder="Enter group ownership (e.g., bfoo:Technical Owner)"
                                label=""
                            />
                        </EditableInputWrapper>
                    );
                }
                return (
                    <EditableCell onClick={() => handleCellEdit(record.id, 'ownership_groups')}>
                        {record.data.ownership_groups}
                    </EditableCell>
                );
            },
            width: '180px',
            minWidth: '180px',
            alignment: 'left',
            sorter: (a, b) => a.data.ownership_groups.localeCompare(b.data.ownership_groups),
        },
        {
            title: 'Related Contains',
            key: 'relatedContains',
            render: (record) => {
                if (isEditing(record.id, 'related_contains')) {
                    return (
                        <EditableInputWrapper>
                            <Input
                                value={record.data.related_contains}
                                setValue={(value) => {
                                    const stringValue = typeof value === 'function' ? value('') : value;
                                    handleCellSave(record.id, 'related_contains', stringValue);
                                }}
                                placeholder="Enter related contains"
                                label=""
                            />
                        </EditableInputWrapper>
                    );
                }
                return (
                    <EditableCell onClick={() => handleCellEdit(record.id, 'related_contains')}>
                        {record.data.related_contains}
                    </EditableCell>
                );
            },
            width: '130px',
            minWidth: '130px',
            alignment: 'left',
            sorter: (a, b) => a.data.related_contains.localeCompare(b.data.related_contains),
        },
        {
            title: 'Related Inherits',
            key: 'relatedInherits',
            render: (record) => {
                if (isEditing(record.id, 'related_inherits')) {
                    return (
                        <EditableInputWrapper>
                            <Input
                                value={record.data.related_inherits}
                                setValue={(value) => {
                                    const stringValue = typeof value === 'function' ? value('') : value;
                                    handleCellSave(record.id, 'related_inherits', stringValue);
                                }}
                                placeholder="Enter related inherits"
                                label=""
                            />
                        </EditableInputWrapper>
                    );
                }
                return (
                    <EditableCell onClick={() => handleCellEdit(record.id, 'related_inherits')}>
                        {record.data.related_inherits}
                    </EditableCell>
                );
            },
            width: '130px',
            minWidth: '130px',
            alignment: 'left',
            sorter: (a, b) => a.data.related_inherits.localeCompare(b.data.related_inherits),
        },
        {
            title: 'Domain Name',
            key: 'domainName',
            render: (record) => {
                if (isEditing(record.id, 'domain_name')) {
                    return (
                        <EditableInputWrapper>
                            <Input
                                value={record.data.domain_name}
                                setValue={(value) => {
                                    const stringValue = typeof value === 'function' ? value('') : value;
                                    handleCellSave(record.id, 'domain_name', stringValue);
                                }}
                                placeholder="Enter domain name"
                                label=""
                            />
                        </EditableInputWrapper>
                    );
                }
                return (
                    <EditableCell onClick={() => handleCellEdit(record.id, 'domain_name')}>
                        {record.data.domain_name}
                    </EditableCell>
                );
            },
            width: '120px',
            minWidth: '120px',
            alignment: 'left',
            sorter: (a, b) => a.data.domain_name.localeCompare(b.data.domain_name),
        },
        {
            title: 'Custom Properties',
            key: 'customProperties',
            render: (record) => {
                if (isEditing(record.id, 'custom_properties')) {
                    return (
                        <EditableInputWrapper>
                            <Input
                                value={record.data.custom_properties}
                                setValue={(value) => {
                                    const stringValue = typeof value === 'function' ? value('') : value;
                                    handleCellSave(record.id, 'custom_properties', stringValue);
                                }}
                                placeholder="Enter custom properties"
                                label=""
                            />
                        </EditableInputWrapper>
                    );
                }
                return (
                    <EditableCell onClick={() => handleCellEdit(record.id, 'custom_properties')}>
                        {record.data.custom_properties}
                    </EditableCell>
                );
            },
            width: '150px',
            minWidth: '150px',
            alignment: 'left',
            sorter: (a, b) => a.data.custom_properties.localeCompare(b.data.custom_properties),
        },
    ];


    return (
        <>
            {error && (
                <Message type="error" content="Failed to load glossary import data! An unexpected error occurred." />
            )}
            <SourceContainer>
                <HeaderContainer>
                    <StyledTabToolbar>
                        <SearchContainer>
                            <StyledSearchBar
                                placeholder="Search..."
                                value={searchInput || ''}
                                onChange={(value) => handleSearchInputChange(value)}
                                ref={searchInputRef}
                            />
                            <StyledSimpleSelect
                                options={[
                                    { label: 'All', value: '0' },
                                    { label: 'New', value: '1' },
                                    { label: 'Updated', value: '2' },
                                    { label: 'Conflict', value: '3' },
                                ]}
                                values={[statusFilter.toString()]}
                                onUpdate={(values) => setStatusFilter(Number(values[0]))}
                                showClear={false}
                                isMultiSelect={false}
                                width="fit-content"
                                size="lg"
                            />
                        </SearchContainer>
                        <FilterButtonsContainer>
                            {/* Restart button moved to ActionsBar */}
                        </FilterButtonsContainer>
                    </StyledTabToolbar>
                </HeaderContainer>
                {/* Selection display - Hidden for now */}
                {/* {selectedRowKeys.length > 0 && (
                    <div style={{ 
                        padding: '8px 16px', 
                        backgroundColor: '#f0f8ff', 
                        border: '1px solid #d1e7ff',
                        borderRadius: '4px',
                        margin: '0 0 16px 0',
                        fontSize: '14px',
                        color: '#0066cc'
                    }}>
                        {selectedRowKeys.length} of {totalItems} items selected
                    </div>
                )} */}
                {!loading && entities.length === 0 ? (
                    <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '200px' }}>
                        <div style={{ fontSize: '16px', fontWeight: 'bold', color: '#666' }}>
                            No glossary terms to import yet!
                        </div>
                    </div>
                ) : (
                    <>
                        <TableContainer>
                            <Table
                                columns={tableColumns}
                                data={entities}
                                rowKey="id"
                                isScrollable
                                handleSortColumnChange={handleSortColumnChange}
                                isLoading={loading && entities.length === 0}
                                pagination={false}
                                scroll={{ y: 450 }}
                            />
                        </TableContainer>
                        <PaginationContainer>
                            {/* Actions moved to always visible ActionsBar */}
                        </PaginationContainer>
                    </>
                )}
            </SourceContainer>
            
            {/* Entity Details Modal */}
            <EntityDetailsModal
                visible={isModalVisible}
                onClose={handleCloseModal}
                entityData={selectedEntity}
                onSave={handleSaveEntity}
            />
            
            {/* Diff Modal */}
            <DiffModal
                visible={isDiffModalVisible}
                onClose={handleCloseDiffModal}
                entity={selectedEntity ? entities.find(e => e.data === selectedEntity) || null : null}
                existingEntity={selectedEntity ? entities.find(e => e.data === selectedEntity)?.existingEntity || null : null}
            />
            
            {/* Import Progress Modal */}
            <ImportProgressModal
                visible={isImportModalVisible}
                onClose={handleCloseImportModal}
                progress={progress}
                isProcessing={isProcessing}
            />
        </>
    );
};

export const WizardPage = () => {
    console.log('🎭 Mock UI: WizardPage component rendering');
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const history = useHistory();
    const [currentStep, setCurrentStep] = useState(1); // Start at step 1 for mock UI to show data
    
    // Import Processing state
    const [isImportModalVisible, setIsImportModalVisible] = useState(false);
    const apolloClient = useApolloClient();
    
    const {
        progress,
        isProcessing,
        startImport,
        pauseImport,
        resumeImport,
        cancelImport,
        retryFailed,
        resetProgress,
    } = useMockImportProcessing({
        apolloClient,
        onProgress: (progress) => {
            // Progress updates are handled automatically
        },
    });
    
    // Import state management
    const {
        csvData,
        parseResult,
        isDataLoaded,
        entities,
        existingEntities,
        comparisonResult,
        isComparisonComplete,
        setCsvDataAndResult,
        setEntities,
        setExistingEntities,
        setComparisonResult,
        clearData
    } = useImportState();
    
    // Initialize CSV processing hooks at component level
    const csvProcessing = useMockCsvProcessing();
    const entityManagement = useMockEntityManagement();
    const { executeUnifiedGlossaryQuery } = useMockGraphQLOperations();
    const { categorizeEntities } = useMockEntityComparison();

    // Import handlers
    const handleStartImport = useCallback(async () => {
        try {
            setIsImportModalVisible(true);
            await startImport(entities, existingEntities);
        } catch (error) {
            console.error('Import failed:', error);
        }
    }, [entities, existingEntities, startImport]);

    const handleNext = () => {
        if (currentStep < wizardSteps.length - 1) {
            setCurrentStep(currentStep + 1);
        }
    };

    const handlePrevious = () => {
        if (currentStep > 0) {
            setCurrentStep(currentStep - 1);
        }
    };

    const [uploadFile, setUploadFile] = useState<File | null>(null);
    const [uploadProgress, setUploadProgress] = useState(0);
    const [uploadError, setUploadError] = useState<string | null>(null);

    const handleFileSelect = async (file: File) => {
        setUploadFile(file);
        setUploadError(null);
        setUploadProgress(0);
        
        try {
            // Read file content
            const csvText = await new Promise<string>((resolve, reject) => {
                const reader = new FileReader();
                reader.onload = (e) => resolve(e.target?.result as string);
                reader.onerror = () => reject(new Error('Failed to read file'));
                reader.readAsText(file);
            });
            
            setUploadProgress(50);
            
            // Use proper CSV parsing with dynamic header mapping
            const parseResult = csvProcessing.parseCsvText(csvText);
            
            setUploadProgress(75);
            
            // Validate parsed data
            const validationResult = csvProcessing.validateCsvData(parseResult.data);
            
            if (!validationResult.isValid) {
                setUploadError(`CSV validation failed: ${validationResult.errors.map(e => e.message).join(', ')}`);
                return;
            }
            
            // Normalize CSV data to entities
            const normalizedEntities = entityManagement.normalizeCsvData(parseResult.data);
            
            setCsvDataAndResult(parseResult.data, parseResult);
            setEntities(normalizedEntities);
            
            // Fetch existing entities from DataHub for comparison
            setUploadProgress(90);
            try {
                const existingEntities = await executeUnifiedGlossaryQuery({
                    input: {
                        types: ['GLOSSARY_TERM', 'GLOSSARY_NODE'],
                        query: '*',
                        count: 1000
                    }
                });
                
                // Create URN to name mapping for relationship lookups
                const urnToNameMap = new Map<string, string>();
                existingEntities.forEach((entity: any) => {
                    const name = entity.properties?.name || entity.name || '';
                    if (name) {
                        urnToNameMap.set(entity.urn, name);
                    }
                });

                // Helper function to convert relationship URNs to names
                const convertRelationshipUrnsToNames = (relationships: any[]): string => {
                    if (!relationships || relationships.length === 0) return '';
                    
                    return relationships
                        .map((rel: any) => {
                            const entity = rel?.entity;
                            if (!entity) return '';
                            
                            const name = entity.properties?.name || entity.name || '';
                            const parentNodes = entity.parentNodes?.nodes || [];
                            
                            if (name) {
                                if (parentNodes.length > 0) {
                                    // Create hierarchical name: parent.child
                                    const parentName = parentNodes[0].properties?.name || '';
                                    return parentName ? `${parentName}.${name}` : name;
                                } else {
                                    // No parent, just return the name
                                    return name;
                                }
                            }
                            
                            return '';
                        })
                        .filter(name => name)
                        .join(',');
                };

                // Convert GraphQL entities to our Entity format
                const convertedExistingEntities: Entity[] = existingEntities.map((entity: any) => {
                    const isTerm = entity.__typename === 'GlossaryTerm';
                    const properties = entity.properties || {};
                    const parentNodes = entity.parentNodes?.nodes || [];
                    
                    return {
                        id: entity.urn,
                        name: properties.name || entity.name || '',
                        type: (isTerm ? 'glossaryTerm' : 'glossaryNode') as 'glossaryTerm' | 'glossaryNode',
                        urn: entity.urn,
                        parentNames: parentNodes.map((node: any) => node.properties?.name || ''),
                        parentUrns: parentNodes.map((node: any) => node.urn),
                        level: parentNodes.length,
                        data: {
                            entity_type: (isTerm ? 'glossaryTerm' : 'glossaryNode') as 'glossaryTerm' | 'glossaryNode',
                            urn: entity.urn,
                            name: properties.name || entity.name || '',
                            description: properties.description || '',
                            term_source: properties.termSource || '',
                            source_ref: properties.sourceRef || '',
                            source_url: properties.sourceUrl || '',
                            ownership_users: entity.ownership?.owners?.filter((owner: any) => 
                              owner.owner.__typename === 'CorpUser'
                            ).map((owner: any) => 
                              `${owner.owner.username || owner.owner.name || owner.owner.urn}:${owner.ownershipType?.info?.name || 'NONE'}`
                            ).join('|') || '',
                            ownership_groups: entity.ownership?.owners?.filter((owner: any) => 
                              owner.owner.__typename === 'CorpGroup'
                            ).map((owner: any) => 
                              `${owner.owner.username || owner.owner.name || owner.owner.urn}:${owner.ownershipType?.info?.name || 'NONE'}`
                            ).join('|') || '',
                            parent_nodes: parentNodes.map((node: any) => node.properties?.name || '').join(','),
                            related_contains: convertRelationshipUrnsToNames(entity.contains?.relationships || []),
                            related_inherits: convertRelationshipUrnsToNames(entity.inherits?.relationships || []),
                            domain_urn: '', // TODO: Extract from domain aspect
                            domain_name: '', // TODO: Extract from domain aspect
                            custom_properties: properties.customProperties?.map((cp: any) => `${cp.key}:${cp.value}`).join(',') || '',
                            status: 'existing'
                        },
                        status: 'existing' as const,
                        originalRow: undefined
                    };
                });
                
                setExistingEntities(convertedExistingEntities);
                
                // Perform entity comparison
                const comparison = categorizeEntities(normalizedEntities, convertedExistingEntities);
                
                // Update entities with comparison results
                const updatedEntities = [
                    ...comparison.newEntities.map(entity => ({ ...entity, status: 'new' as const })),
                    ...comparison.updatedEntities,
                    ...comparison.unchangedEntities,
                    ...comparison.conflictedEntities
                ];
                
                setEntities(updatedEntities);
                setComparisonResult({
                    newEntities: comparison.newEntities.map(entity => ({ ...entity, status: 'new' as const })),
                    existingEntities: convertedExistingEntities,
                    updatedEntities: comparison.updatedEntities,
                    conflicts: comparison.conflictedEntities
                });
                
            } catch (error) {
                console.error('Failed to fetch existing entities:', error);
                setUploadError(`Failed to fetch existing entities: ${error instanceof Error ? error.message : 'Unknown error'}`);
                return;
            }
            
            setUploadProgress(100);
            handleNext(); // Auto-advance to next step after successful upload
            
        } catch (error) {
            setUploadError(`Failed to parse CSV file: ${error instanceof Error ? error.message : 'Unknown error'}`);
        }
    };

    const handleFileRemove = () => {
        setUploadFile(null);
        setUploadError(null);
        setUploadProgress(0);
    };

    const handleRestart = () => {
        // Reset to step 1
        setCurrentStep(0);
        
        // Clear file upload state
        setUploadFile(null);
        setUploadError(null);
        setUploadProgress(0);
        
        // Clear all import data
        clearData();
    };

    const breadcrumbItems = [
        {
            label: 'Glossary',
            href: PageRoutes.GLOSSARY
        },
        {
            label: 'Import',
            isActive: true
        }
    ];

    const renderStepContent = () => {
        switch (currentStep) {
            case 0: // Upload CSV
                return (
                    <DropzoneTable
                        onFileSelect={handleFileSelect}
                        onFileRemove={handleFileRemove}
                        file={uploadFile}
                        isProcessing={isProcessing}
                        progress={uploadProgress}
                        error={uploadError}
                        acceptedFileTypes={['.csv']}
                        maxFileSize={10}
                    />
                );
            case 1: // Preview Data
                return <GlossaryImportList 
                    entities={entities} 
                    setEntities={setEntities} 
                    onRestart={handleRestart} 
                    csvProcessing={csvProcessing} 
                    entityManagement={entityManagement} 
                    onStartImport={handleStartImport}
                    isImportModalVisible={isImportModalVisible}
                    setIsImportModalVisible={setIsImportModalVisible}
                    progress={progress}
                    isProcessing={isProcessing}
                    resetProgress={resetProgress}
                    retryFailed={retryFailed}
                />;
            case 2: // Compare & Validate
                return <div>Compare & Validate Step - Coming Soon</div>;
            case 3: // Manage Hierarchy
                return <div>Manage Hierarchy Step - Coming Soon</div>;
            case 4: // Import
                return <div>Import Step - Coming Soon</div>;
            default:
                return <div>Unknown step</div>;
        }
    };

    const canProceed = () => {
        switch (currentStep) {
            case 0: // Upload CSV
                return isDataLoaded && entities.length > 0;
            case 1: // Preview Data
                return true; // Always can proceed from preview
            case 2: // Compare & Validate
                return isComparisonComplete;
            case 3: // Manage Hierarchy
                return true; // Always can proceed from hierarchy
            case 4: // Import
                return true; // Always can proceed to import
            default:
                return false;
        }
    };

    return (
        <PageContainer $isShowNavBarRedesign={isShowNavBarRedesign}>
            <BreadcrumbHeader
                items={breadcrumbItems}
                title="Import Glossary"
                subtitle="Import glossary terms from CSV files and manage their import status"
                pillLabel="Mock"
                pillColor="blue"
            />
            <PageContentContainer>
                {/* Step Content - Full Width, No Container */}
                {renderStepContent()}

                {/* Actions Bar - Always visible */}
                <div style={{ display: 'flex', justifyContent: 'center', padding: '20px 0' }}>
                    <ActionsBar>
                        {entities.length > 0 && (
                            <>
                                <Button
                                    variant="outline"
                                    onClick={handleRestart}
                                    icon={{ icon: 'ArrowClockwise', source: 'phosphor' }}
                                >
                                    Reset
                                </Button>
                                <Button
                                    variant="filled"
                                    color="primary"
                                    onClick={handleStartImport}
                                    disabled={isProcessing}
                                >
                                    Import All ({entities.length})
                                </Button>
                            </>
                        )}
                    </ActionsBar>
                </div>

                {/* Step Actions - Only show if not on data preview step */}
                {currentStep !== 1 && (
                    <StepActions>
                        <div>
                            {currentStep > 0 && (
                                <Button variant="outline" onClick={handlePrevious}>
                                    Previous
                                </Button>
                            )}
                        </div>
                        <StepButtons>
                            <Button variant="outline" onClick={() => history.back()}>
                                Cancel
                            </Button>
                            {currentStep < wizardSteps.length - 1 ? (
                                <Button 
                                    variant="filled" 
                                    onClick={handleNext}
                                    disabled={!canProceed()}
                                >
                                    Next
                                </Button>
                            ) : (
                                <Button 
                                    variant="filled" 
                                    color="green"
                                    disabled={!canProceed()}
                                >
                                    Import
                                </Button>
                            )}
                        </StepButtons>
                    </StepActions>
                )}
            </PageContentContainer>
        </PageContainer>
    );
};

export default WizardPage;