import { OwnershipTypeEntity } from '@src/types.generated';
import { Select, Typography } from 'antd';
import React from 'react';

interface Props {
    selectedOwnerTypeUrn?: string;
    ownershipTypes: OwnershipTypeEntity[];
    onSelectOwnerType: (typeUrn: string) => void;
}

export default function OwnershipTypesSelect({ selectedOwnerTypeUrn, ownershipTypes, onSelectOwnerType }: Props) {
    return (
        <Select value={selectedOwnerTypeUrn} onChange={onSelectOwnerType}>
            {ownershipTypes.map((ownershipType: OwnershipTypeEntity | undefined) => {
                const ownershipTypeUrn = ownershipType?.urn || '';
                const ownershipTypeName = ownershipType?.info?.name || ownershipType?.urn || '';
                const ownershipTypeDescription = ownershipType?.info?.description || '';
                return (
                    <Select.Option key={ownershipTypeUrn} value={ownershipTypeUrn}>
                        <Typography.Text>{ownershipTypeName}</Typography.Text>
                        <Typography.Paragraph
                            style={{ wordWrap: 'break-word', whiteSpace: 'break-spaces' }}
                            type="secondary"
                        >
                            {ownershipTypeDescription}
                        </Typography.Paragraph>
                    </Select.Option>
                );
            })}
        </Select>
    );
}
