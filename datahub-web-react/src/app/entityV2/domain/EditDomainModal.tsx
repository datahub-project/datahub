import React, { Suspense, lazy, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { Label } from '@components/components/TextArea/components';

import { useDomainsContext } from '@app/domainV2/DomainsContext';
import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import {
    buildDomainDisplayInput,
    buildOptimisticDomainDisplayProperties,
    getDomainEditFieldChanges,
    resolveDomainIconDisplay,
} from '@app/entityV2/domain/utils/displayProperties';
import { Field } from '@app/entityV2/shared/EntityDropdown/glossaryEntityModal.shared';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';
import { getReloadableKeyType } from '@app/sharedV2/reloadableContext/utils';
import { ColorPicker, Input, Modal, toast } from '@src/alchemy-components';

import { useUpdateDisplayPropertiesMutation, useUpdateNameMutation } from '@graphql/mutations.generated';
import { DataHubPageModuleType, EntityType } from '@types';

// The picker chunk statically imports the 141 curated Phosphor icons it renders (see
// domainIconLibrary.ts). Lazy-loading it here means those icon components never touch
// the main bundle — they arrive together as one chunk when the modal opens, avoiding
// the 141-concurrent-lazy-chunks stall we hit when each cell had its own Suspense.
const ChatIconPicker = lazy(() =>
    import('@app/entityV2/shared/containers/profile/header/IconPicker/IconPicker').then((mod) => ({
        default: mod.ChatIconPicker,
    })),
);

const NAME_MAX_LENGTH = 150;

// Cap body height so the whole modal (body + ~120px of header/footer chrome) stays around
// 80vh even when the icon picker is visible. Content that overflows scrolls in-place —
// matches the pattern used in PolicyBuilderModal / QueryModal.
//
// Padding + negative margin on the horizontal axis is a workaround for the CSS rule that
// coerces the sibling axis to non-visible when one axis is set (so `overflow-y: auto`
// implicitly clips X too). Without the 4px inline gutter, the Alchemy Input's
// `outline: 1px solid` focus indicator gets sheared off at the container edge.
const ScrollableBody = styled.div`
    max-height: 65vh;
    overflow-y: auto;
    padding: 4px;
    margin: -4px;
`;

type Props = {
    onClose: () => void;
};

export default function EditDomainModal({ onClose }: Props) {
    const { t } = useTranslation('governance.domain');
    const { t: tc } = useTranslation('common.actions');
    const { t: tl } = useTranslation('common.labels');
    const { t: tcf } = useTranslation('common.feedback');

    const { urn, entityData } = useEntityData();
    const refetch = useRefetch();
    const { setUpdatedDomain } = useDomainsContext();
    const { reloadByKeyType } = useReloadableContext();

    const [updateName] = useUpdateNameMutation();
    const [updateDisplayProperties] = useUpdateDisplayPropertiesMutation();

    const initialName = entityData?.properties?.name || '';
    const initialColor = entityData?.displayProperties?.colorHex || '';
    // `resolveDomainIconDisplay` returns the stored icon name plus a loadability flag. We
    // only need the name here — passing it into `pinnedIcons` guarantees the user's current
    // pick shows up as a picker cell even if it isn't in our curated set, so they can keep
    // their selection without an accidental clear. Legacy MUI-named aspects (pre-Phosphor)
    // pin an unrenderable name — harmless; the user just picks something new.
    const { iconName: displayedIconName } = resolveDomainIconDisplay(entityData?.displayProperties?.icon?.name);

    const [stagedName, setStagedName] = useState<string>(initialName);
    const [stagedColor, setStagedColor] = useState<string>(initialColor);
    const [stagedIconName, setStagedIconName] = useState<string>(displayedIconName);

    const trimmedName = stagedName.trim();
    const nameError = trimmedName.length === 0 ? t('create.nameRequired') : '';
    const saveEnabled = !nameError;

    const onSave = async () => {
        if (!saveEnabled) return;

        const { nameChanged, colorChanged, iconChanged } = getDomainEditFieldChanges(
            { name: initialName, colorHex: initialColor, displayedIconName },
            { trimmedName, colorHex: stagedColor, iconName: stagedIconName },
        );

        toast.loading(tcf('saving'), { key: 'edit-domain' });
        try {
            // Sequence name → display properties (rather than Promise.all) so a partial
            // failure produces a coherent user-visible state: if `updateName` fails we
            // haven't touched display properties, and if `updateDisplayProperties` fails
            // the error toast accurately reflects what broke. Both mutations are
            // idempotent by URN, so retrying is safe.
            if (nameChanged) {
                await updateName({ variables: { input: { name: trimmedName, urn } } });
            }
            // Send only the fields the user actually changed — the resolver leaves omitted
            // fields untouched. This preserves an existing icon when only the color was
            // edited (and vice versa) and avoids writing an icon to a letter-only domain.
            const displayInput = buildDomainDisplayInput({
                colorHex: colorChanged ? stagedColor : undefined,
                iconName: iconChanged ? stagedIconName : undefined,
            });
            if (displayInput) {
                await updateDisplayProperties({ variables: { urn, input: displayInput } });
            }
            toast.destroy('edit-domain');
            toast.success(t('edit.success'));

            // Propagate the full post-edit state to the sidebar / any other consumer of
            // `useManageDomains`. The update mutation returns only a boolean, so Apollo can't
            // normalize color/icon changes into the `listDomains` cache automatically —
            // without this the sidebar would keep the pre-edit visuals until a full page
            // refresh. We ship the full staged displayProperties (not just the changed
            // fields) because the context consumer spreads the update object one level deep,
            // so a partial displayProperties would clobber the unchanged sibling field.
            if ((nameChanged || colorChanged || iconChanged) && setUpdatedDomain !== undefined) {
                setUpdatedDomain({
                    urn,
                    type: EntityType.Domain,
                    id: urn,
                    properties: { name: trimmedName },
                    displayProperties: buildOptimisticDomainDisplayProperties({
                        colorHex: stagedColor || undefined,
                        iconName: stagedIconName || undefined,
                    }),
                });
            }

            refetch();
            // Reload domain modules so name/icon updates propagate to home page and hierarchy views.
            reloadByKeyType(
                [
                    getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.Domains),
                    getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.ChildHierarchy),
                ],
                3000,
            );
            onClose();
        } catch (e: unknown) {
            toast.destroy('edit-domain');
            const errorMessage = e instanceof Error ? e.message : '';
            toast.error(t('edit.error', { errorMessage }));
        }
    };

    return (
        <Modal
            title={t('edit.title')}
            open
            onCancel={onClose}
            buttons={[
                { text: tc('cancel'), variant: 'text', onClick: onClose },
                {
                    text: tc('save'),
                    onClick: onSave,
                    disabled: !saveEnabled,
                    buttonDataTestId: 'edit-domain-save-button',
                },
            ]}
        >
            <ScrollableBody>
                <Field>
                    <Input
                        label={tl('name')}
                        value={stagedName}
                        setValue={setStagedName}
                        placeholder={t('create.namePlaceholder')}
                        data-testid="edit-domain-name"
                        isRequired
                        error={stagedName.length > 0 ? nameError : ''}
                        maxLength={NAME_MAX_LENGTH}
                    />
                </Field>
                <Field>
                    <Label>{tl('color')}</Label>
                    <ColorPicker initialColor={initialColor} onChange={setStagedColor} />
                </Field>
                <Field>
                    <Label>{`${tl('icon')} ${tl('optional')}`}</Label>
                    <Suspense fallback={null}>
                        <ChatIconPicker
                            color={stagedColor}
                            onIconPick={setStagedIconName}
                            selectedIcon={stagedIconName}
                            pinnedIcons={displayedIconName ? [displayedIconName] : undefined}
                        />
                    </Suspense>
                </Field>
            </ScrollableBody>
        </Modal>
    );
}
