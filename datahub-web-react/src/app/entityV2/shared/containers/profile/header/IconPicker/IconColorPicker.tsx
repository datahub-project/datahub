import { ColorPicker, Modal, toast } from '@components';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import { ChatIconPicker } from '@app/entityV2/shared/containers/profile/header/IconPicker/IconPicker';

import { useUpdateDisplayPropertiesMutation } from '@graphql/mutations.generated';
import { EntityType, IconLibrary } from '@types';

type IconColorPickerProps = {
    name: string;
    open: boolean;
    onClose: () => void;
    color?: string | null;
    icon?: string | null;
    onChangeColor?: (color: string) => void;
    onChangeIcon?: (icon: string) => void;
    /**
     * When false, only the color picker is shown (no icon grid).
     * Defaults to true to preserve the original Domain edit experience.
     */
    showIcon?: boolean;
};

const SectionLabel = styled.div`
    font-size: 14px;
    font-weight: 600;
    color: ${(props) => props.theme.colors.text};
    margin-bottom: 8px;
`;

const Section = styled.div`
    margin-bottom: 24px;

    &:last-child {
        margin-bottom: 0;
    }
`;

function capitalize(string: string) {
    if (string.length === 0) return '';

    return string[0].toUpperCase() + string.slice(1);
}

function snakeToCamel(string: string) {
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
    showIcon = true,
}) => {
    const { t } = useTranslation('entity.shared.containers');
    const { t: tc } = useTranslation('common.actions');
    const { t: tcl } = useTranslation('common.labels');
    const refetch = useRefetch();
    const { urn, entityType } = useEntityData();
    const [updateDisplayProperties] = useUpdateDisplayPropertiesMutation();
    const theme = useTheme();

    const initialColor = color || theme.colors.colorPickerDefault;
    const [stagedColor, setStagedColor] = useState<string>(initialColor);
    const [stagedIcon, setStagedIcon] = useState<string>(icon || 'account_circle');

    const resolvedName = name || t('iconPicker.defaultDomainName');
    const title = showIcon
        ? t('iconPicker.chooseIconForTitle', { name: resolvedName })
        : `Choose a color for ${resolvedName}`;

    const onApply = () => {
        const input: { colorHex: string; icon?: { iconLibrary: IconLibrary; name: string; style: string } } = {
            colorHex: stagedColor,
        };
        if (showIcon) {
            input.icon = {
                iconLibrary: IconLibrary.Material,
                name: capitalize(snakeToCamel(stagedIcon)),
                style: 'Outlined',
            };
        }
        // Pick just the relevant refetch query so Apollo doesn't warn about queries that aren't
        // mounted on the current page.
        const refetchQueriesForEntity: string[] = (() => {
            switch (entityType) {
                case EntityType.GlossaryNode:
                    return ['getGlossaryNode'];
                case EntityType.GlossaryTerm:
                    return ['getGlossaryTerm'];
                case EntityType.Domain:
                    return ['getDomain'];
                default:
                    return [];
            }
        })();
        updateDisplayProperties({
            variables: {
                urn,
                input,
            },
            refetchQueries: refetchQueriesForEntity,
            awaitRefetchQueries: true,
        })
            .then((result) => {
                if (result.errors?.length) {
                    console.error('updateDisplayProperties returned GraphQL errors', result.errors);
                    toast.error(`Failed to update color: ${result.errors[0].message}`, { duration: 4 });
                    return;
                }
                refetch();
                toast.success('Color updated', { duration: 2 });
            })
            .catch((e) => {
                console.error('updateDisplayProperties threw', e);
                toast.error(`Failed to update color: ${e?.message || 'unknown error'}`, { duration: 4 });
            });
        onChangeColor?.(stagedColor);
        if (showIcon) onChangeIcon?.(stagedIcon);
        onClose();
    };

    return (
        <Modal
            open={open}
            title={title}
            onCancel={() => onClose()}
            buttons={[
                {
                    text: tc('cancel'),
                    variant: 'text',
                    onClick: onClose,
                },
                {
                    text: tc('apply'),
                    onClick: onApply,
                    variant: 'filled',
                },
            ]}
        >
            <Section>
                {showIcon && <SectionLabel>{tcl('color')}</SectionLabel>}
                <ColorPicker initialColor={initialColor} onChange={setStagedColor} />
            </Section>
            {showIcon && (
                <Section>
                    <SectionLabel>{tcl('icon')}</SectionLabel>
                    <ChatIconPicker color={stagedColor} onIconPick={(i) => setStagedIcon(i)} />
                </Section>
            )}
        </Modal>
    );
};

export default IconColorPicker;
