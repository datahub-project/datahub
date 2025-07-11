import { Icon, colors } from '@components';
import { Dropdown } from 'antd';
import React from 'react';
import styled from 'styled-components';

const StyledIcon = styled(Icon)`
    :hover {
        cursor: pointer;
    }
` as typeof Icon;

export default function ModuleMenu() {
    return (
        <Dropdown
            trigger={['click']}
            menu={{
                items: [
                    {
                        title: 'Edit',
                        key: 'edit',
                        label: 'Edit',
                    },
                    {
                        title: 'Duplicate',
                        label: 'Duplicate',
                        key: 'duplicate',
                    },
                    {
                        title: 'Delete',
                        label: 'Delete',
                        key: 'delete',
                        style: {
                            color: colors.red[500],
                        },
                    },
                ],
            }}
        >
            <StyledIcon icon="DotsThreeVertical" source="phosphor" size="lg" />
        </Dropdown>
    );
}
