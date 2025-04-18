import { Tooltip } from '@components';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';
import { ViewBuilderMode } from '../../entityV2/view/builder/types';
import { ViewBuilder } from '../../entityV2/view/builder/ViewBuilder';
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
                        <ToolTipHeader>Save these filters as a new View.</ToolTipHeader>
                        <div>Views allow you to easily save or share search filters.</div>
                    </>
                }
            >
                <TextButton type="text" onClick={toggleViewBuilder} marginTop={0} data-testid="save-as-view">
                    Save as a View
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
