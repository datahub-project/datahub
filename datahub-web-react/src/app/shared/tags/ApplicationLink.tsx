/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Tag } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { useEntityRegistry } from '@app/useEntityRegistry';

import { Application } from '@types';

const StyledTag = styled(Tag)`
    && {
        margin: 2px;
    }
`;

interface Props {
    application: Application;
    closable?: boolean;
    onClose?: (e) => void;
    readOnly?: boolean;
    fontSize?: number;
}

export const ApplicationLink = ({ application, closable, onClose, readOnly, fontSize }: Props) => {
    const entityRegistry = useEntityRegistry();
    const applicationPath = entityRegistry.getPathName(application.type);
    const applicationUrl = `/${applicationPath}/${encodeURIComponent(application.urn)}`;

    return (
        <Link to={applicationUrl}>
            <StyledTag closable={!readOnly && closable} onClose={onClose} style={{ fontSize }}>
                {application.properties?.name || application.urn}
            </StyledTag>
        </Link>
    );
};
