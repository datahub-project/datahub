import React from 'react';
import styled from 'styled-components';
import { DeleteOutlined, MoreOutlined } from '@ant-design/icons';
import { Dropdown, Menu } from 'antd';

const StyledMoreOutlined = styled(MoreOutlined)`
    font-size: 14px;
`;

type Props = {
    onClickDelete: () => void;
    index?: number;
    urn: string;
};

export default function TestCardActionMenu({ onClickDelete, index, urn }: Props) {
    return (
        <Dropdown
            overlay={
                <Menu>
                    <Menu.Item key="0" onClick={onClickDelete} data-testid={`test-delete-button-${index}`}>
                        <DeleteOutlined /> &nbsp; Delete
                    </Menu.Item>
                    <Menu.Item
                        key="1"
                        onClick={() => {
                            navigator.clipboard.writeText(urn);
                        }}
                        data-testid={`test-copy-button-${index}`}
                    >
                        &nbsp; Copy URN
                    </Menu.Item>
                </Menu>
            }
            trigger={['click']}
        >
            <StyledMoreOutlined data-testid={`test-more-button-${index}`} />
        </Dropdown>
    );
}
