import { Modal } from '@components';
import React from 'react';
import styled, { useTheme } from 'styled-components';

import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import { ChatIconPicker } from '@app/entityV2/shared/containers/profile/header/IconPicker/IconPicker';

import { useUpdateDisplayPropertiesMutation } from '@graphql/mutations.generated';
import { IconLibrary } from '@types';

const ColorInput = styled.input`
    padding: 2px;
    width: 37px;
    margin-bottom: 30px;
    margin-top: 15px;
    height: 40px;
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 6px;
    cursor: pointer;
`;

type IconColorPickerProps = {
    name: string;
    open: boolean;
    onClose: () => void;
    color?: string | null;
    icon?: string | null;
    onChangeColor?: (color: string) => void;
    onChangeIcon?: (icon: string) => void;
};

const IconColorPicker: React.FC<IconColorPickerProps> = ({
    name,
    open,
    onClose,
    color,
    icon,
    onChangeColor,
    onChangeIcon,
}) => {
    const theme = useTheme();
    const refetch = useRefetch();
    const { urn } = useEntityData();
    const [updateDisplayProperties] = useUpdateDisplayPropertiesMutation();

    const [stagedColor, setStagedColor] = React.useState<string>(color || theme.colors.text);
    const [stagedIcon, setStagedIcon] = React.useState<string>(icon || '');

    return (
        <Modal
            open={open}
            title={`Choose an icon for ${name || 'Domain'}`}
            onCancel={() => onClose()}
            buttons={[
                {
                    text: 'Apply',
                    onClick: () => {
                        updateDisplayProperties({
                            variables: {
                                urn,
                                input: {
                                    colorHex: stagedColor,
                                    icon: {
                                        iconLibrary: IconLibrary.Material,
                                        name: stagedIcon,
                                        style: 'Outlined',
                                    },
                                },
                            },
                        }).then(() => refetch());
                        onChangeColor?.(stagedColor);
                        onChangeIcon?.(stagedIcon);
                        onClose();
                    },
                    variant: 'filled',
                },
            ]}
        >
            <ColorInput type="color" value={stagedColor} onChange={(e) => setStagedColor(e.target.value)} />
            <ChatIconPicker color={stagedColor} onIconPick={(i) => setStagedIcon(i)} />
        </Modal>
    );
};

export default IconColorPicker;
