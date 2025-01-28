import { Input, Modal } from 'antd';
import { debounce } from 'lodash';
import React from 'react';
import styled from 'styled-components';

import { useUpdateDisplayPropertiesMutation } from '../../../../../../../graphql/mutations.generated';
import { IconLibrary } from '../../../../../../../types.generated';
import { useEntityData, useRefetch } from '../../../../../../entity/shared/EntityContext';
import { ChatIconPicker } from './IconPicker';

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

const Title = styled.span`
    font-size: 16px;
    font-weight: 600;
    position: relative;
    bottom: 6px;
    left: 6px;
`;

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

    // a debounced version of updateDisplayProperties that takes in the same arguments
    // eslint-disable-next-line react-hooks/exhaustive-deps
    const debouncedUpdateDisplayProperties = React.useCallback(
        debounce((...args) => updateDisplayProperties(...args).then(() => setTimeout(() => refetch(), 1000)), 500),
        [],
    );

    return (
        <Modal
            open={open}
            onCancel={() => onClose()}
            onOk={() => {
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
                });
                onChangeColor?.(stagedColor);
                onChangeIcon?.(stagedIcon);
                onClose();
            }}
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
                onChange={(e) => {
                    setStagedColor(e.target.value);
                    debouncedUpdateDisplayProperties?.({
                        variables: {
                            urn,
                            input: {
                                colorHex: e.target.value,
                                icon: {
                                    iconLibrary: IconLibrary.Material,
                                    name: stagedIcon,
                                    style: 'Outlined',
                                },
                            },
                        },
                    });
                }}
            />
            <Title>Choose an icon for {name || 'Domain'}</Title>
            <ChatIconPicker
                color={stagedColor}
                onIconPick={(i) => {
                    console.log('picking icon', i);
                    debouncedUpdateDisplayProperties?.({
                        variables: {
                            urn,
                            input: {
                                colorHex: stagedColor,
                                icon: {
                                    iconLibrary: IconLibrary.Material,
                                    name: capitalize(snakeToCamel(i)),
                                    style: 'Outlined',
                                },
                            },
                        },
                    });
                    setStagedIcon(i);
                }}
            />
        </Modal>
    );
};

export default IconColorPicker;
