import React, { useMemo } from 'react';
import { Button, Descriptions, Space, Table, Tag, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import styled from 'styled-components';

import { CompliancePropertyQualifiedName } from '@app/entity/shared/tabs/Dataset/Privacy/constant';
import { buildStructuredPropertyUrn, getStructuredValue } from '@app/entity/shared/tabs/Dataset/Privacy/utils';

const { Text } = Typography;

const Container = styled.div`
    padding: 24px;
`;

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type SchemaField = any;

type ViewAnnotationProps = {
    fields: SchemaField[];
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    structuredProps: any[];
    onEdit: () => void;
};

export function ViewAnnotation({ fields, structuredProps, onEdit }: ViewAnnotationProps) {
    const targetPropUrn = buildStructuredPropertyUrn(CompliancePropertyQualifiedName.PiiAnnotation);

    const annotationsMap = useMemo(() => {
        const map = new Map<string, string[]>();
        fields.forEach((field) => {
            field.schemaFieldEntity?.structuredProperties?.properties.forEach((prop) => {
                if (prop.structuredProperty?.urn === targetPropUrn && prop.structuredProperty?.exists && prop.values?.length) {
                    const vals = prop.values.map((v) => v.stringValue || v.value || '').filter(Boolean);
                    map.set(field.fieldPath, vals);
                }
            });
        });
        return map;
    }, [fields, targetPropUrn]);

    const hasPersonalData = getStructuredValue(structuredProps, CompliancePropertyQualifiedName.HasPersonalData);
    const hasPersonalDataUpdatedAt = getStructuredValue(
        structuredProps,
        CompliancePropertyQualifiedName.HasPersonalDataUpdatedAt,
    );
    const hasPersonalDataUpdatedBy = getStructuredValue(
        structuredProps,
        CompliancePropertyQualifiedName.HasPersonalDataUpdatedBy,
    );

    const fieldAnnotations = useMemo(
        () => fields.filter((field) => annotationsMap.has(field.fieldPath)),
        [fields, annotationsMap],
    );

    const columns: ColumnsType<any> = [
        {
            title: 'Field Name',
            dataIndex: 'fieldPath',
            key: 'fieldPath',
            width: 300,
            render: (text: string) => <strong>{text}</strong>,
        },
        {
            title: 'Personal Data Types',
            key: 'current',
            width: 300,
            render: (_, record) => {
                const vals = annotationsMap.get(record.fieldPath) || [];
                if (vals.length === 0) {
                    return <Text type="secondary">Not set</Text>;
                }
                return (
                    <Space size={[0, 4]} wrap>
                        {vals.map((v) => (
                            <Tag key={v} color="blue">
                                {v}
                            </Tag>
                        ))}
                    </Space>
                );
            },
        },
    ];

    const showFieldAnnotations = hasPersonalData === 'Yes' && fieldAnnotations.length > 0;

    return (
        <Container>
            <Button type="primary" onClick={onEdit}>
                Edit
            </Button>

            <Descriptions bordered column={1}>
                <Descriptions.Item label="Has Personal Data?">
                    {hasPersonalData ? (
                        <Text>
                            {hasPersonalData}, set by {hasPersonalDataUpdatedBy} on {hasPersonalDataUpdatedAt}
                        </Text>
                    ) : (
                        <Text type="secondary">Not set</Text>
                    )}
                </Descriptions.Item>

                {showFieldAnnotations && (
                    <Descriptions.Item label="Field Annotations">
                        <Table
                            rowKey="fieldPath"
                            dataSource={fieldAnnotations}
                            columns={columns}
                            pagination={{ pageSize: 20 }}
                            bordered
                            size="middle"
                        />
                    </Descriptions.Item>
                )}
            </Descriptions>
        </Container>
    );
}

export default ViewAnnotation;
