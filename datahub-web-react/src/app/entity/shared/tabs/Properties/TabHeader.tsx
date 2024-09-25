import { SearchOutlined } from '@ant-design/icons';
import { Input } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../constants';
import AddPropertyButton from './AddPropertyButton';

const StyledInput = styled(Input)`
    border-radius: 70px;
    max-width: 300px;
`;

const TableHeader = styled.div`
    padding: 8px 16px;
    border-bottom: 1px solid ${ANTD_GRAY[4.5]};
    display: flex;
    justify-content: space-between;
`;

interface Props {
    setFilterText: (text: string) => void;
    fieldUrn?: string;
    refetch?: () => void;
}

export default function TabHeader({ setFilterText, fieldUrn, refetch }: Props) {
    return (
        <TableHeader>
            <StyledInput
                placeholder="Search in properties..."
                onChange={(e) => setFilterText(e.target.value)}
                allowClear
                prefix={<SearchOutlined />}
            />
            <AddPropertyButton fieldUrn={fieldUrn} refetch={refetch} />
        </TableHeader>
    );
}
