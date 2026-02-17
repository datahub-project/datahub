import { Tooltip } from '@components';
import { ArrowLeft, ArrowRight } from '@phosphor-icons/react';
import React, { useContext } from 'react';
import styled from 'styled-components';

import EntitySidebarContext from '@app/sharedV2/EntitySidebarContext';
import { getColor } from '@src/alchemy-components/theme/utils';

const Container = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    height: 56px;
    padding: 8px;
`;

const IconWrapper = styled.div<{ direction: 'left' | 'right' }>`
    display: flex;
    align-items: center;
    justify-content: center;
`;

const CloseButton = styled.div<{ $isClosed: boolean }>`
    cursor: pointer;
    margin: 0px;
    padding: 2px 6px;
    display: flex;
    align-items: center;
    height: 40px;
    width: 40px;
    border-radius: 6px;
    justify-content: center;
    color: ${(p) => p.theme.styles['primary-color']};
    ${(props) =>
        props.$isClosed &&
        `
        background-color: ${getColor('primary', 600, props.theme)};
        color: ${props.theme.colors.bg};  
    `}
    :hover {
        background-color: ${(p) => getColor('primary', 600, p.theme)};
        color: ${(props) => props.theme.colors.bg};
    }
`;

export default function SidebarCollapseIcon() {
    const { isClosed, setSidebarClosed } = useContext(EntitySidebarContext);

    return (
        <Container>
            <Tooltip placement="left" showArrow={false} title={!isClosed ? 'Close sidebar' : 'Open sidebar'}>
                <CloseButton
                    $isClosed={isClosed}
                    onClick={() => setSidebarClosed(!isClosed)}
                    data-testid="toggleSidebar"
                >
                    <IconWrapper direction={isClosed ? 'left' : 'right'}>
                        {isClosed ? <ArrowRight size={20} /> : <ArrowLeft size={20} />}
                    </IconWrapper>
                </CloseButton>
            </Tooltip>
        </Container>
    );
}
