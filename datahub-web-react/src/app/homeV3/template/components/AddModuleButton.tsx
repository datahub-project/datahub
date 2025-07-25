import { Button, Dropdown, Icon, colors } from '@components';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';

import useAddModuleMenu from '@app/homeV3/template/components/addModuleMenu/useAddModuleMenu';
import { ModulePositionInput, RowSide } from '@app/homeV3/template/types';

type AddModuleButtonOrientation = 'vertical' | 'horizontal';

const Wrapper = styled.div``;

const StyledDropdownContainer = styled.div`
    max-width: 330px;

    .ant-dropdown-menu {
        border-radius: 12px;
    }

    &&& {
        .ant-dropdown-menu-sub {
            border-radius: 12px;
        }
    }
`;

const StyledButton = styled(Button)<{ $orientation: AddModuleButtonOrientation; $opened?: boolean }>`
    ${(props) =>
        props.$orientation === 'vertical'
            ? `
                height: 100%;
                width: 32px;
            `
            : `
                width: 32px;
                width: 100%;
            `}

    justify-content: center;
    background: ${colors.gray[1600]};

    :hover {
        background: ${colors.gray[1600]};
    }
`;

const StyledVisibleOnHoverButton = styled(StyledButton)`
    visibility: hidden;

    ${Wrapper}:hover & {
        visibility: visible;
    }
`;

interface Props {
    orientation: AddModuleButtonOrientation;
    className?: string;
    rowIndex?: number;
    rowSide?: RowSide;
}

export default function AddModuleButton({ orientation, className, rowIndex, rowSide }: Props) {
    const [isOpened, setIsOpened] = useState<boolean>(false);

    const ButtonComponent = useMemo(() => (isOpened ? StyledButton : StyledVisibleOnHoverButton), [isOpened]);

    // Create position object for the menu
    const position: ModulePositionInput = {
        rowIndex,
        rowSide,
    };

    const closeMenu = () => setIsOpened(false);

    const menu = useAddModuleMenu(position, closeMenu);

    const onClick = (e: React.MouseEvent<HTMLButtonElement, MouseEvent>) => {
        // FYI: Antd can open dropdown in the cursor's position only for contextMenu trigger
        // we handle left click and emit contextmenu event instead to open the dropdown in the cursor's position
        const event = new MouseEvent('contextmenu', {
            bubbles: true,
            cancelable: true,
            clientX: e.clientX,
            clientY: e.clientY,
        });
        event.target?.dispatchEvent(event);

        setIsOpened(true);
    };

    return (
        <Wrapper className={className}>
            <Dropdown
                open={isOpened}
                trigger={['click', 'contextMenu']}
                onOpenChange={(open) => setIsOpened(open)}
                dropdownRender={(originNode) => <StyledDropdownContainer>{originNode}</StyledDropdownContainer>}
                menu={menu}
                resetDefaultMenuStyles
            >
                <ButtonComponent $orientation={orientation} color="gray" variant="text" size="xs" onClick={onClick}>
                    <Icon icon="Plus" source="phosphor" color="primary" />
                </ButtonComponent>
            </Dropdown>
        </Wrapper>
    );
}
