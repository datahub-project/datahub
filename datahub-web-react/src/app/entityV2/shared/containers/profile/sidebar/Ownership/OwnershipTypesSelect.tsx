import { Text } from '@components';
import { Select } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { OwnershipTypeEntity } from '@src/types.generated';

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
    const { t } = useTranslation('entity.shared.containers');
    return (
        <StyledSelect
            value={selectedOwnerTypeUrn}
            onChange={(v) => onSelectOwnerType(v as string)}
            placeholder={t('sidebar.ownership.selectTypePlaceholder')}
        >
            {ownershipTypes.map((ownershipType: OwnershipTypeEntity | undefined) => {
                const ownershipTypeUrn = ownershipType?.urn || '';
                const ownershipTypeName = ownershipType?.info?.name || ownershipType?.urn || '';
                const ownershipTypeDescription = ownershipType?.info?.description || '';
                return (
                    <Select.Option key={ownershipTypeUrn} value={ownershipTypeUrn}>
                        <Text type="span">{ownershipTypeName}</Text>
                        <Text style={{ wordWrap: 'break-word', whiteSpace: 'break-spaces' }} color="textSecondary">
                            {ownershipTypeDescription}
                        </Text>
                    </Select.Option>
                );
            })}
        </StyledSelect>
    );
}
