import { MoreOutlined } from '@ant-design/icons';
import { Button, Dropdown } from 'antd';
import React, { useContext, useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { LineageDisplayContext, onClickPreventSelect } from '@app/lineageV2/common';

const DROPDOWN_Z_INDEX = 100;

const Wrapper = styled.div`
    border-radius: 4px;
    position: absolute;
    right: 3px;
    top: 8px;

    :hover {
        color: ${(p) => p.theme.colors.textHover};
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
    const { t } = useTranslation('lineage');
    const { displayedMenuNode, setDisplayedMenuNode } = useContext(LineageDisplayContext);
    const isMenuVisible = displayedMenuNode === urn;

    const menuItems = useMemo(
        () => [
            {
                key: 'copyUrn',
                label: t('manualLineage.copyUrn'),
                onClick: () => navigator.clipboard.writeText(urn),
            },
        ],
        [t, urn],
    );

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
                    getPopupContainer={(triggerNode) => triggerNode.parentElement || triggerNode}
                    menu={{
                        items: menuItems,
                    }}
                >
                    <StyledIcon style={{ fontSize: 'inherit' }} />
                </Dropdown>
            </StyledButton>
        </Wrapper>
    );
}
