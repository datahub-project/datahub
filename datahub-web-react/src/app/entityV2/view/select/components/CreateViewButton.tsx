/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import AddOutlinedIcon from '@mui/icons-material/AddOutlined';
import React from 'react';
import styled from 'styled-components';

import {
    CardViewLabel,
    ViewContainer,
    ViewContent,
    ViewDescription,
    ViewIcon,
    ViewIconNavBarRedesign,
} from '@app/entityV2/view/select/styledComponents';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';

const AddOutlinedIconStyle = styled(AddOutlinedIcon)`
    font-size: 18px !important;
`;

interface Props {
    onClick?: () => void;
}

export default function CreateViewButton({ onClick }: Props) {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const IconWrapper = isShowNavBarRedesign ? ViewIconNavBarRedesign : ViewIcon;

    return (
        <ViewContainer onClick={() => onClick?.()} role="none" $isShowNavBarRedesign={isShowNavBarRedesign}>
            <IconWrapper>
                <AddOutlinedIconStyle />
            </IconWrapper>
            <ViewContent $isShowNavBarRedesign={isShowNavBarRedesign}>
                <CardViewLabel className="static" $isShowNavBarRedesign={isShowNavBarRedesign}>
                    Create a View
                </CardViewLabel>
                <ViewDescription $isShowNavBarRedesign={isShowNavBarRedesign}>
                    {isShowNavBarRedesign ? 'Create a set of saved search filters' : 'Create view'}
                </ViewDescription>
            </ViewContent>
        </ViewContainer>
    );
}
