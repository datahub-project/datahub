import { Tooltip } from '@components';
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { Property } from '@app/sharedV2/queryBuilder/builder/property/types/properties';

const TitleWrapper = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const ValueTypeText = styled(Typography.Text)`
    margin-left: 8px;
`;

type Props = {
    property: Property;
    selectable?: boolean;
    valueType?: string;
};

/**
 * A title displayed for a Property tree Node.
 *
 * This is shown in the select dropdown item list.
 */
export const PropertyTreeNodeTitle = ({ property, selectable = true, valueType }: Props) => {
    return (
        <Tooltip title={property.description} placement="right">
            <TitleWrapper>
                <div>
                    <Tooltip title={property.displayName}>
                        <Typography.Text type={!selectable ? 'secondary' : undefined}>
                            {property.displayName}
                        </Typography.Text>
                    </Tooltip>
                    <ValueTypeText type="secondary">{valueType}</ValueTypeText>
                </div>
            </TitleWrapper>
        </Tooltip>
    );
};
