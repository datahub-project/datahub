import { CodeOutlined, InfoCircleOutlined } from '@ant-design/icons';
import { Collapse } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { Assertion, AssertionType } from '@types';

const { Panel } = Collapse;

const StyledCollapse = styled(Collapse)`
    background: ${(props) => props.theme.colors.bgSurface};
    border-radius: 8px;
    margin-bottom: 16px;

    .ant-collapse-header {
        font-weight: 600;
        color: ${(props) => props.theme.colors.text};
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
    background: ${(props) => props.theme.colors.bgSurface};
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
        color: ${(props) => props.theme.colors.text};
        font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
        font-size: 12px;
        line-height: 1.5;
    }
`;

const PropertiesContainer = styled.div`
    background: ${(props) => props.theme.colors.bgSurface};
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
        color: ${(props) => props.theme.colors.text};
    }
`;

type Props = {
    assertion: Assertion;
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

export function hasAssertionDetails(assertion: Assertion): boolean {
    return !!getLogicFromAssertion(assertion) || !!assertion?.info?.customProperties?.length;
}

/**
 * Renders a collapsible section showing assertion logic (SQL) and custom properties.
 */
export const AssertionDetailsSection = ({ assertion }: Props) => {
    const info = assertion?.info;
    const logic = getLogicFromAssertion(assertion);

    const customProperties = info?.customProperties;
    const hasCustomProperties = customProperties && customProperties.length > 0;

    const customPropertiesObject = hasCustomProperties
        ? customProperties.reduce(
              (acc, prop) => {
                  acc[prop.key] = prop.value ?? '';
                  return acc;
              },
              {} as Record<string, string>,
          )
        : null;

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
