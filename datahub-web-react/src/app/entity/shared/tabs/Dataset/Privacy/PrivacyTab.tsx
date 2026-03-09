import React,{ useState } from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { Typography, Tag, Spin, Collapse, Descriptions } from 'antd';
const { Title, Text } = Typography;
const { Panel } = Collapse;
import { CompliancePropertyQualifiedName } from './constant';

/**
 * Component used for manage the Dataset's Privacy Compliance.
 */
export const PrivacyTab = () => {
	const { entityData } = useEntityData();
	const structuredProps = entityData?.structuredProperties?.properties || [];

	const getStructuredValue = (qualifiedName: string): string | number | undefined => {
		const prop = structuredProps.find((p: any) =>
			p.structuredProperty?.definition?.qualifiedName === qualifiedName ||
			p.structuredProperty?.urn === ("urn:li:structuredProperty:" + qualifiedName) // fallback if only short name used
			);

		if (!prop) return undefined;

		const firstValue = prop.values?.[0];
		if (!firstValue) return undefined;

		// Handle different value types (adjust based on your property types)
		if (firstValue.stringValue !== undefined) return firstValue.stringValue;
		if (firstValue.numberValue !== undefined) return firstValue.numberValue;
		if (firstValue.booleanValue !== undefined) return firstValue.booleanValue ? 'true' : 'false';
		return firstValue.value; // generic fallback
	};

	const getStructuredList = (qualifiedName: string): string[] => {
		const prop = structuredProps.find((p: any) =>
			p.structuredProperty?.definition?.qualifiedName === qualifiedName ||
			p.structuredProperty?.urn === ("urn:li:structuredProperty:" + qualifiedName)
			);

		if (!prop || !prop.values) return [];

		return prop.values
		.map((v: any) => v.stringValue || v.value || '')
		.filter(Boolean);
	};

	const annotations = getStructuredValue(CompliancePropertyQualifiedName.Annotations);
	const isExempted = getStructuredValue(CompliancePropertyQualifiedName.IsExempted);
	const lastCheckDate = getStructuredValue(CompliancePropertyQualifiedName.LastCheckDate);
	const lastStatus = getStructuredValue(CompliancePropertyQualifiedName.LastStatus);
	const nonComplyingRules = getStructuredList(CompliancePropertyQualifiedName.NonComplyingRules);
	const recordsClasses = getStructuredList(CompliancePropertyQualifiedName.RecordsClasses);
	const retentionColumn = getStructuredValue(CompliancePropertyQualifiedName.RetentionColumn);
	const retentionDays = getStructuredValue(CompliancePropertyQualifiedName.RetentionDays);
	const retentionJira = getStructuredValue(CompliancePropertyQualifiedName.RetentionJira);
	const scrubbingOp = getStructuredValue(CompliancePropertyQualifiedName.ScrubbingOp);
	const scrubbingState = getStructuredValue(CompliancePropertyQualifiedName.ScrubbingState);
	const scrubbingStatus = getStructuredValue(CompliancePropertyQualifiedName.ScrubbingStatus);
	return (
		<div style={{ padding: '24px' }}>
			<Title level={3}>Privacy Compliance Metadata</Title>

			<Collapse defaultActiveKey={['annotations', 'exemption', 'last-check', 'records-class', 'retention', 'scrubbing']} accordion>

				{/* ==================== ANNOTATIONS ==================== */}
				<Panel header="PII Annotations" key="annotations">
					<Descriptions bordered column={1}>
						<Descriptions.Item label="PII Annotations">
							{annotations ? <Tag color="green">{annotations}</Tag> : <Text type="secondary">Not set</Text>}
						</Descriptions.Item>
					</Descriptions>
				</Panel>

				{/* ==================== EXEMPTION ==================== */}
				<Panel header="Exemption" key="exemption">
					<Descriptions bordered column={1}>
						<Descriptions.Item label="Is Exempted">
							{isExempted ? <Tag color="green">{isExempted}</Tag> : <Text type="secondary">Not set</Text>}
						</Descriptions.Item>
					</Descriptions>
				</Panel>

				{/* ==================== LAST COMPLIANCE CHECK ==================== */}
				<Panel header="Last Compliance Check" key="last-check">
					<Descriptions bordered column={1}>
						<Descriptions.Item label="Last Compliance State Check Date">
							{lastCheckDate || <Text type="secondary">—</Text>}
						</Descriptions.Item>
						<Descriptions.Item label="Last Compliance Status">
							{lastStatus ? <Tag color={lastStatus.toLowerCase() === 'compliant' ? 'green' : 'red'}>{lastStatus}</Tag> : <Text type="secondary">—</Text>}
							</Descriptions.Item>
							<Descriptions.Item label="Non Complying Rule">
								{nonComplyingRules.length > 0 ? (
									nonComplyingRules.map((rule, i) => <Tag key={i} color="orange">{rule}</Tag>)
									) : <Text type="secondary">None</Text>}
							</Descriptions.Item>
						</Descriptions>
					</Panel>

				{/* ==================== RECORDS CLASS ==================== */}
					<Panel header="Records Class" key="records-class">
						<Descriptions bordered column={1}>
							<Descriptions.Item label="Records Class">
								{recordsClasses.length > 0 ? (
									recordsClasses.map((cls, i) => <Tag key={i} color="blue">{cls}</Tag>)
									) : <Text type="secondary">None</Text>}
							</Descriptions.Item>
						</Descriptions>
					</Panel>

				{/* ==================== RETENTION ==================== */}
					<Panel header="Retention" key="retention">
						<Descriptions bordered column={1}>
							<Descriptions.Item label="Retention Column">{retentionColumn || '—'}</Descriptions.Item>
							<Descriptions.Item label="Retention Days">
								{retentionDays ? <Tag color="purple">{retentionDays} days</Tag> : '—'}
							</Descriptions.Item>
							<Descriptions.Item label="Retention Exception Jira">
								{retentionJira || '—'}
							</Descriptions.Item>
						</Descriptions>
					</Panel>

				{/* ==================== SCRUBBING ==================== */}
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
			</div>
    );
}
