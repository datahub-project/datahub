import { MoreOutlined } from '@ant-design/icons';
import { LineageDisplayContext, onClickPreventSelect } from '@app/lineageV2/common';
import Colors from '@components/theme/foundations/colors';
import { Button, Dropdown } from 'antd';
import React, { useContext } from 'react';
import styled from 'styled-components';

const DROPDOWN_Z_INDEX = 100;

const Wrapper = styled.div`
    border-radius: 4px;
    position: absolute;
    right: 3px;
    top: 8px;

    :hover {
        color: ${Colors.violet[500]};
    }
`;

const StyledIcon = styled(MoreOutlined)`
    background: transparent;
`;

const StyledButton = styled(Button)`
    height: min-content;
    padding: 0;
    border: none;
    box-shadow: none;
    transition: none;

    display: flex;
    align-items: center;
    justify-content: center;

    .ant-dropdown {
        top: 20px !important;
        left: auto !important;
        right: 0 !important;
    }
`;

interface Props {
    urn: string;
}

export default function GhostEntityMenu({ urn }: Props) {
    const { displayedMenuNode, setDisplayedMenuNode } = useContext(LineageDisplayContext);
    const isMenuVisible = displayedMenuNode === urn;

    function handleMenuClick(e: React.MouseEvent<HTMLElement, MouseEvent>) {
        onClickPreventSelect(e);
        if (isMenuVisible) {
            setDisplayedMenuNode(null);
        } else {
            setDisplayedMenuNode(urn);
        }
    }

    return (
        <Wrapper>
            <StyledButton onClick={handleMenuClick} type="text">
                <Dropdown
                    open={isMenuVisible}
                    overlayStyle={{ zIndex: DROPDOWN_Z_INDEX }}
                    getPopupContainer={(t) => t.parentElement || t}
                    menu={{
                        items: [
                            {
                                key: 'copyUrn',
                                label: 'Copy URN',
                                onClick: () => navigator.clipboard.writeText(urn),
                            },
                        ],
                    }}
                >
                    <StyledIcon style={{ fontSize: 'inherit' }} />
                </Dropdown>
            </StyledButton>
        </Wrapper>
    );
}
