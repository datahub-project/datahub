import { Icon } from '@components';
import { MagnifyingGlass } from '@phosphor-icons/react/dist/csr/MagnifyingGlass';
import { Input } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import AddPropertyButton from '@app/entity/shared/tabs/Properties/AddPropertyButton';
import { Maybe, StructuredProperties } from '@src/types.generated';

const StyledInput = styled(Input)`
    border-radius: 70px;
    max-width: 300px;
`;

const TableHeader = styled.div`
    padding: 8px 16px;
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
    display: flex;
    justify-content: space-between;
`;

interface Props {
    setFilterText: (text: string) => void;
    fieldUrn?: string;
    fieldProperties?: Maybe<StructuredProperties>;
    refetch?: () => void;
}

export default function TabHeader({ setFilterText, fieldUrn, fieldProperties, refetch }: Props) {
    const { t } = useTranslation('entity.profile.tabs');
    return (
        <TableHeader>
            <StyledInput
                placeholder={t('properties.searchInProperties.placeholder')}
                onChange={(e) => setFilterText(e.target.value)}
                allowClear
                prefix={<Icon icon={MagnifyingGlass} />}
            />
            <AddPropertyButton fieldUrn={fieldUrn} fieldProperties={fieldProperties} refetch={refetch} />
        </TableHeader>
    );
}
