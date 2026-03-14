import React from 'react';
import { Collapse, Descriptions, Tag, Typography } from 'antd';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';

import { CompliancePropertyQualifiedName } from '@app/entity/shared/tabs/Dataset/Privacy/constant';
import { Annotation } from '@app/entity/shared/tabs/Dataset/Privacy/pii/Annotation';
import { RecordsClass } from '@app/entity/shared/tabs/Dataset/Privacy/recordsClass/RecordsClass';
import { getStructuredList, getStructuredValue } from '@app/entity/shared/tabs/Dataset/Privacy/utils';

const { Title, Text } = Typography;
const { Panel } = Collapse;

const Container = styled.div`
    padding: 24px;
`;

const DEFAULT_ACTIVE_KEYS = ['annotations', 'exemption', 'last-check', 'records-class', 'retention', 'scrubbing'];

/**
 * Component used for managing the Dataset's Privacy Compliance.
 */
export function PrivacyTab() {
    const { entityData } = useEntityData();
    const structuredProps = entityData?.structuredProperties?.properties || [];

    const isExempted = getStructuredValue(structuredProps, CompliancePropertyQualifiedName.IsExempted);
    const lastCheckDate = getStructuredValue(structuredProps, CompliancePropertyQualifiedName.LastCheckDate);
    const lastStatus = getStructuredValue(structuredProps, CompliancePropertyQualifiedName.LastStatus);
    const nonComplyingRules = getStructuredList(structuredProps, CompliancePropertyQualifiedName.NonComplyingRules);
    const retentionColumn = getStructuredValue(structuredProps, CompliancePropertyQualifiedName.RetentionColumn);
    const retentionDays = getStructuredValue(structuredProps, CompliancePropertyQualifiedName.RetentionDays);
    const retentionJira = getStructuredValue(structuredProps, CompliancePropertyQualifiedName.RetentionJira);
    const scrubbingOp = getStructuredValue(structuredProps, CompliancePropertyQualifiedName.ScrubbingOp);
    const scrubbingState = getStructuredValue(structuredProps, CompliancePropertyQualifiedName.ScrubbingState);
    const scrubbingStatus = getStructuredValue(structuredProps, CompliancePropertyQualifiedName.ScrubbingStatus);

    const getStatusColor = (status: string) => (status.toLowerCase() === 'compliant' ? 'green' : 'red');

    return (
        <Container>
            <Title level={3}>Privacy Compliance Metadata</Title>

            <Collapse defaultActiveKey={DEFAULT_ACTIVE_KEYS} accordion>
                <Panel header="PII Annotations" key="annotations">
                    <Annotation />
                </Panel>

                <Panel header="Exemption" key="exemption">
                    <Descriptions bordered column={1}>
                        <Descriptions.Item label="Is Exempted">
                            {isExempted ? (
                                <Tag color="green">{isExempted}</Tag>
                            ) : (
                                <Text type="secondary">Not set</Text>
                            )}
                        </Descriptions.Item>
                    </Descriptions>
                </Panel>

                <Panel header="Last Compliance Check" key="last-check">
                    <Descriptions bordered column={1}>
                        <Descriptions.Item label="Last Compliance State Check Date">
                            {lastCheckDate || <Text type="secondary">—</Text>}
                        </Descriptions.Item>
                        <Descriptions.Item label="Last Compliance Status">
                            {lastStatus ? (
                                <Tag color={getStatusColor(String(lastStatus))}>{lastStatus}</Tag>
                            ) : (
                                <Text type="secondary">—</Text>
                            )}
                        </Descriptions.Item>
                        <Descriptions.Item label="Non Complying Rule">
                            {nonComplyingRules.length > 0 ? (
                                nonComplyingRules.map((rule) => (
                                    <Tag key={rule} color="orange">
                                        {rule}
                                    </Tag>
                                ))
                            ) : (
                                <Text type="secondary">None</Text>
                            )}
                        </Descriptions.Item>
                    </Descriptions>
                </Panel>

                <Panel header="Records Class" key="records-class">
                    <RecordsClass />
                </Panel>

                <Panel header="Retention" key="retention">
                    <Descriptions bordered column={1}>
                        <Descriptions.Item label="Retention Column">{retentionColumn || '—'}</Descriptions.Item>
                        <Descriptions.Item label="Retention Days">
                            {retentionDays ? <Tag color="purple">{retentionDays} days</Tag> : '—'}
                        </Descriptions.Item>
                        <Descriptions.Item label="Retention Exception Jira">{retentionJira || '—'}</Descriptions.Item>
                    </Descriptions>
                </Panel>

                <Panel header="Scrubbing" key="scrubbing">
                    <Descriptions bordered column={1}>
                        <Descriptions.Item label="Scrubbing Operation">{scrubbingOp || '—'}</Descriptions.Item>
                        <Descriptions.Item label="Scrubbing State">{scrubbingState || '—'}</Descriptions.Item>
                        <Descriptions.Item label="Scrubbing Status">
                            {scrubbingStatus ? <Tag color="cyan">{scrubbingStatus}</Tag> : '—'}
                        </Descriptions.Item>
                    </Descriptions>
                </Panel>
            </Collapse>
        </Container>
    );
}
