import { SearchOutlined } from '@ant-design/icons';
import { Input } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { useTranslation } from 'react-i18next';
import { ANTD_GRAY } from '../../constants';

const StyledInput = styled(Input)`
    border-radius: 70px;
    max-width: 300px;
`;

const TableHeader = styled.div`
    padding: 8px 16px;
    border-bottom: 1px solid ${ANTD_GRAY[4.5]};
`;

interface Props {
    setFilterText: (text: string) => void;
}

export default function TabHeader({ setFilterText }: Props) {
    const { t } = useTranslation();
    return (
        <TableHeader>
            <StyledInput
                placeholder={t('placeholder.searchInProperties')}
                onChange={(e) => setFilterText(e.target.value)}
                allowClear
                prefix={<SearchOutlined />}
            />
        </TableHeader>
    );
}
