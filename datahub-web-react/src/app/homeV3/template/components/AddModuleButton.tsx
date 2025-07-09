import { Button, Dropdown, Icon, colors } from '@components';
import React, { useCallback, useMemo, useState } from 'react';
import styled from 'styled-components';

import { ModulesAvailableToAdd } from '@app/homeV3/modules/types';
import { AddModuleMenuHandlerInput } from '@app/homeV3/template/components/addModuleMenu/types';
import useMenu from '@app/homeV3/template/components/addModuleMenu/useMenu';
import { AddModuleInput, RowSide } from '@app/homeV3/template/types';

type AddModuleButtonOrientation = 'vertical' | 'horizontal';

const Wrapper = styled.div``;

const StyledDropdownContainer = styled.div`
    max-width: 330px;
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
    modulesAvailableToAdd: ModulesAvailableToAdd;
    onAddModule?: (input: AddModuleInput) => void;
    className?: string;
    rowIndex?: number;
    rowSide?: RowSide;
}

export default function AddModuleButton({
    orientation,
    modulesAvailableToAdd,
    onAddModule,
    className,
    rowIndex,
    rowSide,
}: Props) {
    const [isOpened, setIsOpened] = useState<boolean>(false);

    const ButtonComponent = useMemo(() => (isOpened ? StyledButton : StyledVisibleOnHoverButton), [isOpened]);

    const onAddModuleHandler = useCallback(
        (input: AddModuleMenuHandlerInput) => {
            setIsOpened(false);
            onAddModule?.({
                module: input.module,
                moduleType: input.moduleType,
                rowIndex,
                rowSide,
            });
        },
        [onAddModule, rowIndex, rowSide],
    );

    const menu = useMenu(modulesAvailableToAdd, onAddModuleHandler);

    const onClick = (e: React.MouseEvent<HTMLButtonElement, MouseEvent>) => {
        // FYI: Antd can open dropdown in the cursor's position only with contextMenu trigger
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
