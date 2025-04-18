import { OwnershipTypeEntity } from '@src/types.generated';
import { Select, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

const StyledSelect = styled(Select)`
    .ant-select-selection-placeholder {
        font-size: 12px;
    }
`;

interface Props {
    selectedOwnerTypeUrn?: string;
    ownershipTypes: OwnershipTypeEntity[];
    onSelectOwnerType: (typeUrn: string) => void;
}

export default function OwnershipTypesSelect({ selectedOwnerTypeUrn, ownershipTypes, onSelectOwnerType }: Props) {
    return (
        <StyledSelect
            value={selectedOwnerTypeUrn}
            onChange={(v) => onSelectOwnerType(v as string)}
            placeholder="Select ownership type..."
        >
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
        </StyledSelect>
    );
}
