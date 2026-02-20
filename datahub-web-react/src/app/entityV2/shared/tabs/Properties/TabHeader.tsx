import { Icon } from '@components';
import { Input } from 'antd';
import React from 'react';
import styled from 'styled-components';

const StyledInput = styled(Input)`
    border-radius: 70px;
    max-width: 300px;
`;

const TableHeader = styled.div`
    padding: 8px 16px;
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
`;

interface Props {
    setFilterText: (text: string) => void;
}

export default function TabHeader({ setFilterText }: Props) {
    return (
        <TableHeader>
            <StyledInput
                placeholder="Search in properties..."
                onChange={(e) => setFilterText(e.target.value)}
                allowClear
                prefix={<Icon icon="MagnifyingGlass" source="phosphor" />}
            />
        </TableHeader>
    );
}
