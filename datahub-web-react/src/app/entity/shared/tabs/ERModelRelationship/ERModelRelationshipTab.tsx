/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import '@app/entity/shared/tabs/ERModelRelationship/ERModelRelationshipTab.less';

import { Divider } from 'antd';
import React from 'react';

import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import { ERModelRelationPreview } from '@app/entity/shared/components/styled/ERModelRelationship/ERModelRelationPreview';

export const ERModelRelationshipTab = () => {
    const { entityData } = useEntityData();
    const refetch = useRefetch();
    const ermodelrelationView = (ermodelrelationData?: any): JSX.Element => {
        return (
            <ERModelRelationPreview
                ermodelrelationData={ermodelrelationData}
                prePageType="ERModelRelationship"
                refetch={refetch}
            />
        );
    };
    return (
        <>
            <div className="ERModelRelationshipTab">
                <div className="ermodelrelation-preview-div">{ermodelrelationView(entityData)}</div>
                <Divider className="thin-divider" />
            </div>
        </>
    );
};
