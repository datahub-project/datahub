import { Button, Dropdown, Icon, colors } from '@components';
import React, { useCallback, useMemo, useState } from 'react';
import styled from 'styled-components';

import { ModuleInfo, ModulesAvailableToAdd } from '@app/homeV3/modules/types';
import useAddModuleMenu from '@app/homeV3/template/components/addModuleMenu/useAddModuleMenu';
import { AddModuleHandlerInput, RowSide } from '@app/homeV3/template/types';

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
    onAddModule?: (input: AddModuleHandlerInput) => void;
    className?: string;
    originRowIndex?: number;
    rowIndex?: number;
    rowSide?: RowSide;
}

export default function AddModuleButton({
    orientation,
    modulesAvailableToAdd,
    onAddModule,
    className,
    originRowIndex,
    rowIndex,
    rowSide,
}: Props) {
    const [isOpened, setIsOpened] = useState<boolean>(false);

    const ButtonComponent = useMemo(() => (isOpened ? StyledButton : StyledVisibleOnHoverButton), [isOpened]);

    const onAddModuleHandler = useCallback(
        (module: ModuleInfo) => {
            setIsOpened(false);
            onAddModule?.({
                module,
                originRowIndex,
                rowIndex,
                rowSide,
            });
        },
        [onAddModule, originRowIndex, rowIndex, rowSide],
    );

    const menu = useAddModuleMenu(modulesAvailableToAdd, onAddModuleHandler);

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
