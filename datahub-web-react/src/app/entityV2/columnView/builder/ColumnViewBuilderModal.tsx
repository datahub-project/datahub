import { useApolloClient } from '@apollo/client';
import { Button, Form, Input, Modal, Radio, Tooltip, message } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import { useUserContext } from '@app/context/useUserContext';
import ColumnViewDefinitionBuilder from '@app/entityV2/columnView/builder/ColumnViewDefinitionBuilder';
import { updateColumnViewSelectCache } from '@app/entityV2/columnView/cacheUtils';
import { isValidColumn, toColumnInput } from '@app/entityV2/columnView/columnKinds';
import { ColumnViewBuilderState, DEFAULT_COLUMN_VIEW_BUILDER_STATE } from '@app/entityV2/columnView/types';
import { useIsNarrowViewport } from '@app/entityV2/columnView/useIsNarrowViewport';

import { useCreateColumnViewMutation, useUpdateColumnViewMutation } from '@graphql/columnView.generated';
import { DataHubColumnView, DataHubColumnViewDefinitionInput, DataHubViewType } from '@types';

type Props = {
    urn?: string;
    initialState?: ColumnViewBuilderState;
    onSubmit?: (view: DataHubColumnView) => void;
    onCancel: () => void;
};

const fromView = (view: DataHubColumnView): ColumnViewBuilderState => ({
    viewType: view.viewType,
    name: view.name,
    description: view.description,
    target: view.target,
    definition: {
        columns: view.definition.columns.map(toColumnInput),
        sort: view.definition.sort
            ? { column: toColumnInput(view.definition.sort.column), order: view.definition.sort.order }
            : null,
        filter: view.definition.filter ?? null,
    },
});
export { fromView as columnViewToBuilderState };

/** Strip __typename and map the builder state to the GraphQL definition input. */
export function toDefinitionInput(state: ColumnViewBuilderState): DataHubColumnViewDefinitionInput {
    const filter = state.definition?.filter;
    return {
        columns: (state.definition?.columns || []).filter(isValidColumn).map(toColumnInput),
        sort: state.definition?.sort
            ? { column: toColumnInput(state.definition.sort.column), order: state.definition.sort.order }
            : undefined,
        filter: filter
            ? {
                  operator: filter.operator,
                  filters: filter.filters.map(({ field, condition, values, negated }) => ({ field, condition, values, negated })),
              }
            : undefined,
    };
}

/** Parameterized counterpart of view/builder/ViewBuilderModal + ViewBuilderForm. */
export default function ColumnViewBuilderModal({ urn, initialState, onSubmit, onCancel }: Props) {
    const { t } = useTranslation('entity.views');
    const client = useApolloClient();
    const userContext = useUserContext();
    const canManageGlobal = Boolean(userContext.platformPrivileges?.manageGlobalViews);
    const isNarrow = useIsNarrowViewport();
    const [state, setState] = useState<ColumnViewBuilderState>(initialState || DEFAULT_COLUMN_VIEW_BUILDER_STATE);
    const [create] = useCreateColumnViewMutation();
    const [update] = useUpdateColumnViewMutation();

    const save = () => {
        const definition = toDefinitionInput(state);
        const done = (view?: DataHubColumnView | null) => {
            if (view) {
                updateColumnViewSelectCache(view.urn, view, client);
                onSubmit?.(view);
            }
        };
        const op = urn
            ? update({
                  variables: { urn, input: { name: state.name, description: state.description, definition } },
              }).then((r) => done(r.data?.updateColumnView as DataHubColumnView))
            : create({
                  variables: {
                      input: {
                          viewType: state.viewType || DataHubViewType.Personal,
                          target: state.target,
                          name: state.name || '',
                          description: state.description,
                          definition,
                      },
                  },
              }).then((r) => done(r.data?.createColumnView as DataHubColumnView));
        op.catch((e) => message.error({ content: `${t('columnViews.saveFailed')} ${e.message}` }));
    };

    return (
        <Modal
            open
            // One breakpoint: full-screen on narrow viewports, fixed width otherwise.
            width={isNarrow ? '100vw' : 880}
            style={isNarrow ? { top: 0, margin: 0, maxWidth: '100vw', paddingBottom: 0 } : undefined}
            bodyStyle={isNarrow ? { height: 'calc(100vh - 110px)', overflowY: 'auto' } : undefined}
            title={urn ? t('columnViews.edit') : t('columnViews.create')}
            onCancel={onCancel}
            footer={
                <>
                    <Button onClick={onCancel}>Cancel</Button>
                    <Button type="primary" disabled={!state.name} onClick={save}>
                        Save
                    </Button>
                </>
            }
        >
            <Form layout="vertical">
                {!urn && (
                    <Form.Item label={t('columnViews.type')}>
                        <Radio.Group value={state.viewType} onChange={(e) => setState({ ...state, viewType: e.target.value })}>
                            <Radio.Button value={DataHubViewType.Personal}>Personal</Radio.Button>
                            <Tooltip title={canManageGlobal ? undefined : t('columnViews.publicRequiresPrivilege')}>
                                <Radio.Button value={DataHubViewType.Global} disabled={!canManageGlobal}>
                                    Public
                                </Radio.Button>
                            </Tooltip>
                        </Radio.Group>
                    </Form.Item>
                )}
                <Form.Item label="Name" required>
                    <Input value={state.name} onChange={(e) => setState({ ...state, name: e.target.value })} />
                </Form.Item>
                <Form.Item label="Description">
                    <Input.TextArea value={state.description || ''} onChange={(e) => setState({ ...state, description: e.target.value })} />
                </Form.Item>
                <ColumnViewDefinitionBuilder state={state} updateState={setState} />
            </Form>
        </Modal>
    );
}
