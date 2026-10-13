import { useApolloClient } from '@apollo/client';
import { toast } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { useUserContext } from '@app/context/useUserContext';
import { removeFromListMyViewsCache, removeFromViewSelectCaches } from '@app/entityV2/view/cacheUtils';
import { DEFAULT_LIST_VIEWS_PAGE_SIZE } from '@app/entityV2/view/utils';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';

import { useDeleteViewMutation } from '@graphql/view.generated';
import { DataHubView } from '@types';

type Props = {
    view?: DataHubView;
    onClose: () => void;
};

export const DeleteViewConfirmationModal = ({ view, onClose }: Props) => {
    const { t } = useTranslation('entity.views');
    const { t: tc } = useTranslation('common.actions');
    const userContext = useUserContext();
    const client = useApolloClient();
    const [deleteViewMutation] = useDeleteViewMutation();

    const deleteView = (viewUrn: string) => {
        deleteViewMutation({ variables: { urn: viewUrn } })
            .then(({ errors }) => {
                if (!errors) {
                    removeFromViewSelectCaches(viewUrn, client);
                    removeFromListMyViewsCache(viewUrn, client, 1, DEFAULT_LIST_VIEWS_PAGE_SIZE, undefined, undefined);
                    if (viewUrn === userContext.localState?.selectedViewUrn) {
                        userContext.updateLocalState({
                            ...userContext.localState,
                            selectedViewUrn: undefined,
                        });
                    }
                    toast.success(t('deleteSuccess'), { duration: 2 });
                }
            })
            .catch(() => {
                toast.error(t('deleteError'), { duration: 3 });
            });
    };

    const onConfirm = () => {
        onClose();
        if (view) deleteView(view.urn);
    };

    return (
        <ConfirmationModal
            isOpen={!!view}
            handleClose={onClose}
            handleConfirm={onConfirm}
            modalTitle={t('deleteConfirm.title', { name: view?.name })}
            modalText={t('deleteConfirm.content')}
            confirmButtonText={tc('yes')}
            isDeleteModal
        />
    );
};
