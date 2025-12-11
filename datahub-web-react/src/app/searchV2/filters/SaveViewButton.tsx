/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Tooltip } from '@components';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';

import { buildInitialViewState, fromUnionType } from '@app/entity/view/builder/utils';
import { ViewBuilder } from '@app/entityV2/view/builder/ViewBuilder';
import { ViewBuilderMode } from '@app/entityV2/view/builder/types';
import { TextButton } from '@app/searchV2/filters/styledComponents';
import { canCreateViewFromFilters } from '@app/searchV2/filters/utils';
import { UnionType } from '@app/searchV2/utils/constants';
import { Message } from '@app/shared/Message';

import { FacetFilterInput } from '@types';

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
