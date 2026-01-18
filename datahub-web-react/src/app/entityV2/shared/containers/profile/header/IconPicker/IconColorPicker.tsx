import { Modal } from '@components';
import { Input } from 'antd';
import React from 'react';

import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import { ChatIconPicker } from '@app/entityV2/shared/containers/profile/header/IconPicker/IconPicker';

import { useUpdateDisplayPropertiesMutation } from '@graphql/mutations.generated';
import { IconLibrary } from '@types';

type IconColorPickerProps = {
    name: string;
    open: boolean;
    onClose: () => void;
    color?: string | null;
    icon?: string | null;
    onChangeColor?: (color: string) => void;
    onChangeIcon?: (icon: string) => void;
};

function capitalize(string) {
    if (string.length === 0) return '';

    return string[0].toUpperCase() + string.slice(1);
}
function snakeToCamel(string) {
    const [start, ...rest] = string.split('_');

    return start + rest.map(capitalize).join('');
}

const IconColorPicker: React.FC<IconColorPickerProps> = ({
    name,
    open,
    onClose,
    color,
    icon,
    onChangeColor,
    onChangeIcon,
}) => {
    const refetch = useRefetch();
    const { urn } = useEntityData();
    const [updateDisplayProperties] = useUpdateDisplayPropertiesMutation();

    const [stagedColor, setStagedColor] = React.useState<string>(color || '#000000');
    const [stagedIcon, setStagedIcon] = React.useState<string>(icon || 'account_circle');

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
                                        name: capitalize(snakeToCamel(stagedIcon)),
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
            <Input
                type="color"
                size="large"
                value={stagedColor}
                style={{
                    padding: 2,
                    width: 37,
                    marginBottom: 30,
                    marginTop: 15,
                }}
                onChange={(e) => setStagedColor(e.target.value)}
            />
            <ChatIconPicker color={stagedColor} onIconPick={(i) => setStagedIcon(i)} />
        </Modal>
    );
};

export default IconColorPicker;
