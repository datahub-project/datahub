import { ColorPicker, Modal, toast } from '@components';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import { Label } from '@components/components/TextArea/components';

import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import { ChatIconPicker } from '@app/entityV2/shared/containers/profile/header/IconPicker/IconPicker';
import { resolveDisplayIconName } from '@app/sharedV2/icons/resolveDisplayIcon';

import { useUpdateDisplayPropertiesMutation } from '@graphql/mutations.generated';
import { EntityType, IconLibrary } from '@types';

type IconColorPickerProps = {
    name: string;
    open: boolean;
    onClose: () => void;
    color?: string | null;
    icon?: string | null;
    iconLibrary?: IconLibrary | null;
    onChangeColor?: (color: string) => void;
    onChangeIcon?: (icon: string) => void;
    /**
     * When false, only the color picker is shown (no icon grid).
     * Defaults to true to preserve the original Domain edit experience.
     */
    showIcon?: boolean;
};

const Section = styled.div`
    margin-bottom: 24px;

    &:last-child {
        margin-bottom: 0;
    }
`;

// Match Alchemy Modal `centered` + tall content: cap body so header/footer stay visible.
const MODAL_BODY_STYLE: React.CSSProperties = {
    maxHeight: 'calc(90vh - 140px)',
    overflowY: 'auto',
};

const DEFAULT_PHOSPHOR_PICK = 'UserCircle';

function IconColorPicker({
    name,
    open,
    onClose,
    color,
    icon,
    iconLibrary,
    onChangeColor,
    onChangeIcon,
    showIcon = true,
}: IconColorPickerProps) {
    const { t } = useTranslation('entity.shared.containers');
    const { t: tc } = useTranslation('common.actions');
    const { t: tcl } = useTranslation('common.labels');
    const refetch = useRefetch();
    const { urn, entityType } = useEntityData();
    const [updateDisplayProperties] = useUpdateDisplayPropertiesMutation();
    const theme = useTheme();

    const initialColor = color || theme.colors.colorPickerDefault;
    // Map legacy Material names to Phosphor for the staged pick; Phosphor names pass through.
    const initialIcon = resolveDisplayIconName(icon, iconLibrary) || DEFAULT_PHOSPHOR_PICK;
    const [stagedColor, setStagedColor] = useState<string>(initialColor);
    const [stagedIcon, setStagedIcon] = useState<string>(initialIcon);

    const resolvedName = name || t('iconPicker.defaultDomainName');
    const title = t(showIcon ? 'iconPicker.chooseIconForTitle' : 'iconPicker.chooseColorForTitle', {
        name: resolvedName,
    });

    const onApply = () => {
        const input: { colorHex: string; icon?: { iconLibrary: IconLibrary; name: string; style: string } } = {
            colorHex: stagedColor,
        };
        if (showIcon) {
            input.icon = {
                iconLibrary: IconLibrary.Phosphor,
                name: stagedIcon,
                style: 'regular',
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
                    toast.error(t('iconPicker.updateFailed', { message: result.errors[0].message }), {
                        duration: 3,
                    });
                    return;
                }
                refetch();
                toast.success(t('iconPicker.updateSuccess'), { duration: 2 });
            })
            .catch((e: unknown) => {
                // Surface a toast for both real `Error`s and any other rejection value (string,
                // GraphQL error array, undefined). The previous shape silently swallowed the
                // failure when the rejection wasn't an `Error` instance, leaving the user with
                // no feedback. Fall back to an empty message in that case so the toast still
                // renders with the localized title.
                const message = e instanceof Error ? e.message || '' : '';
                toast.error(t('iconPicker.updateFailed', { message }), { duration: 3 });
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
            bodyStyle={MODAL_BODY_STYLE}
            width={640}
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
                <Label>{tcl('color')}</Label>
                <ColorPicker initialColor={initialColor} onChange={setStagedColor} />
            </Section>
            {showIcon && (
                <Section>
                    <Label>{tcl('icon')}</Label>
                    <ChatIconPicker
                        color={stagedColor}
                        selectedIcon={stagedIcon}
                        onIconPick={(i) => setStagedIcon(i)}
                    />
                </Section>
            )}
        </Modal>
    );
}

export default IconColorPicker;
