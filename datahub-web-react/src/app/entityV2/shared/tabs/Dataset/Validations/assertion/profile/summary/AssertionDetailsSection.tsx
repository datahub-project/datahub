import { CodeOutlined, InfoCircleOutlined } from '@ant-design/icons';
import { Collapse } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';

import { Assertion, AssertionType } from '@types';

const { Panel } = Collapse;

const StyledCollapse = styled(Collapse)`
    background: #fafafa;
    border-radius: 8px;
    margin-bottom: 16px;

    .ant-collapse-header {
        font-weight: 600;
        color: ${ANTD_GRAY[9]};
    }

    .ant-collapse-content-box {
        padding: 16px;
    }
`;

const SectionTitle = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const LogicContainer = styled.div`
    background: ${ANTD_GRAY[2]};
    border-radius: 6px;
    padding: 16px;
    overflow-x: auto;
    max-height: 400px;
    overflow-y: auto;

    pre {
        margin: 0;
        white-space: pre-wrap;
        word-wrap: break-word;
    }

    code {
        color: ${ANTD_GRAY[9]};
        font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
        font-size: 12px;
        line-height: 1.5;
    }
`;

const PropertiesContainer = styled.div`
    background: ${ANTD_GRAY[2]};
    border-radius: 6px;
    padding: 16px;
    overflow-x: auto;
    max-height: 300px;
    overflow-y: auto;

    pre {
        margin: 0;
        white-space: pre-wrap;
        word-wrap: break-word;
        font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
        font-size: 12px;
        line-height: 1.5;
        color: ${ANTD_GRAY[9]};
    }
`;

type Props = {
    assertion: Assertion;
};

/**
 * Renders a collapsible section showing assertion logic (SQL) and custom properties.
 */
export const AssertionDetailsSection = ({ assertion }: Props) => {
    const info = assertion?.info;

    // Get logic from the appropriate assertion type
    const logic = getLogicFromAssertion(assertion);

    // Get custom properties
    const customProperties = info?.customProperties;
    const hasCustomProperties = customProperties && customProperties.length > 0;

    // Convert custom properties array to object for JSON display
    const customPropertiesObject = hasCustomProperties
        ? customProperties.reduce(
              (acc, prop) => {
                  acc[prop.key] = prop.value ?? '';
                  return acc;
              },
              {} as Record<string, string>,
          )
        : null;

    // Don't render if there's nothing to show
    if (!logic && !hasCustomProperties) {
        return null;
    }

    const defaultActiveKeys: string[] = [];
    if (logic) defaultActiveKeys.push('logic');

    return (
        <StyledCollapse defaultActiveKey={defaultActiveKeys} expandIconPosition="end">
            {logic && (
                <Panel
                    header={
                        <SectionTitle>
                            <CodeOutlined />
                            <span>Logic / SQL</span>
                        </SectionTitle>
                    }
                    key="logic"
                >
                    <LogicContainer>
                        <pre>
                            <code>{logic}</code>
                        </pre>
                    </LogicContainer>
                </Panel>
            )}
            {hasCustomProperties && (
                <Panel
                    header={
                        <SectionTitle>
                            <InfoCircleOutlined />
                            <span>Custom Properties</span>
                        </SectionTitle>
                    }
                    key="properties"
                >
                    <PropertiesContainer>
                        <pre>{JSON.stringify(customPropertiesObject, null, 2)}</pre>
                    </PropertiesContainer>
                </Panel>
            )}
        </StyledCollapse>
    );
};

/**
 * Extract logic/SQL from the assertion based on its type.
 */
function getLogicFromAssertion(assertion: Assertion): string | null {
    const info = assertion?.info;
    if (!info) return null;

    switch (info.type) {
        case AssertionType.Custom:
            return info.customAssertion?.logic || null;
        case AssertionType.Dataset:
            return info.datasetAssertion?.logic || null;
        case AssertionType.Sql:
            return info.sqlAssertion?.statement || null;
        default:
            return null;
    }
}
