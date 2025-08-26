import { DeleteOutlined, EditOutlined, MoreOutlined } from '@ant-design/icons';
import { colors } from '@components';
import { Dropdown, Menu } from 'antd';
import React from 'react';
import styled from 'styled-components';

const StyledMoreOutlined = styled(MoreOutlined)`
    font-size: 16px;
    color: ${colors.gray[1700]};
`;

type Props = {
    onClickEdit: () => void;
    onClickDelete: () => void;
    index?: number;
    urn: string;
};

export default function TestCardActionMenu({ onClickEdit, onClickDelete, index, urn }: Props) {
    return (
        <Dropdown
            overlay={
                <Menu>
                    <Menu.Item key="0" onClick={onClickEdit}>
                        <EditOutlined /> &nbsp; Edit
                    </Menu.Item>
                    <Menu.Item key="1" onClick={onClickDelete} data-testid={`test-delete-button-${index}`}>
                        <DeleteOutlined /> &nbsp; Delete
                    </Menu.Item>
                    <Menu.Item
                        key="2"
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
