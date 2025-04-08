import { Icon } from '@components';
import { Input } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';

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
