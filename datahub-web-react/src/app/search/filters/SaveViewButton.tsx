import { Tooltip } from 'antd';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';
import { useTranslation } from 'react-i18next';
import { ViewBuilderMode } from '../../entity/view/builder/types';
import { ViewBuilder } from '../../entity/view/builder/ViewBuilder';
import { buildInitialViewState, fromUnionType } from '../../entity/view/builder/utils';
import { FacetFilterInput } from '../../../types.generated';
import { UnionType } from '../utils/constants';
import { TextButton } from './styledComponents';
import { Message } from '../../shared/Message';
import { canCreateViewFromFilters } from './utils';

const ToolTipHeader = styled.div`
    margin-bottom: 12px;
`;

interface Props {
    activeFilters: FacetFilterInput[];
    unionType: UnionType;
}

export default function SaveViewButton({ activeFilters, unionType }: Props) {
    const { t } = useTranslation();
    const [isViewModalVisible, setIsViewModalVisible] = useState(false);
    const isValidViewDefiniton = useMemo(() => canCreateViewFromFilters(activeFilters), [activeFilters]);

    function toggleViewBuilder() {
        setIsViewModalVisible(true);
        if (!isValidViewDefiniton) {
            // TODO: fire off analytics event
            setTimeout(() => setIsViewModalVisible(false), 3000);
        }
    }

    return (
        <>
            <Tooltip
                placement="right"
                title={
                    <>
                        <ToolTipHeader>{t('filter.view.saveTheseFiltersAsANewView')}</ToolTipHeader>
                        <div>{t('filter.view.viewsAllowYouToEasilySaveOrShareSearchFilters')}</div>
                    </>
                }
            >
                <TextButton type="text" onClick={toggleViewBuilder} marginTop={0} data-testid="save-as-view">
                    {t('filter.view.saveAsView')}
                </TextButton>
            </Tooltip>
            {isViewModalVisible && (
                <>
                    {isValidViewDefiniton && (
                        <ViewBuilder
                            mode={ViewBuilderMode.EDITOR}
                            initialState={buildInitialViewState(activeFilters, fromUnionType(unionType))}
                            onSubmit={() => setIsViewModalVisible(false)}
                            onCancel={() => setIsViewModalVisible(false)}
                        />
                    )}
                    {!isValidViewDefiniton && (
                        <Message
                            type="error"
                            content="This combination of filters cannot be saved as a View at this time."
                        />
                    )}
                </>
            )}
        </>
    );
}
