import { Button, Text, Tooltip } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import useSelectedView from '@app/searchV2/searchBarV2/hooks/useSelectedView';

import { useGetViewQuery } from '@graphql/view.generated';

const HintRow = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
    min-width: 0;
`;

// View names are user-authored and can be long — keep the hint to one line.
const HintText = styled(Text)`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

type Props = {
    /** Show an inline "Clear view" action next to the hint text. */
    showClear?: boolean;
    className?: string;
    dataTestId?: string;
};

/**
 * Subtle indicator that a DataHub View is selected and silently filtering the
 * documents surfaces (tree, sidebar search, stats, home modules). Renders
 * nothing when no View is selected.
 */
export default function ViewAppliedHint({ showClear = false, className, dataTestId }: Props) {
    const { t } = useTranslation('misc');
    const { hasSelectedView, selectedView, clearSelectedView } = useSelectedView();
    const { data } = useGetViewQuery({
        variables: { urn: selectedView || '' },
        skip: !selectedView,
        fetchPolicy: 'cache-first',
    });

    if (!hasSelectedView) return null;

    const viewName = data?.view?.name;

    return (
        <HintRow className={className} data-testid={dataTestId ?? 'view-applied-hint'}>
            <Tooltip title={t('context.viewApplied.tooltip')} showArrow={false}>
                <HintText color="primary" size="sm">
                    {viewName ? t('context.viewApplied.labelNamed', { viewName }) : t('context.viewApplied.label')}
                </HintText>
            </Tooltip>
            {showClear && (
                <Button variant="text" size="sm" onClick={clearSelectedView} data-testid="view-applied-hint-clear">
                    {t('context.clearSelectedView')}
                </Button>
            )}
        </HintRow>
    );
}
