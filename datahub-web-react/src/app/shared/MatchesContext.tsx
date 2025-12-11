/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

export enum PreviewSection {
    MATCHES = 'matches',
    OWNERS = 'owners',
    GLOSSARY_TERMS = 'glossaryTerms',
    TAGS = 'tags',
    COLUMN_PATHS = 'columnPaths',
}

interface MatchesContextProps {
    expandedSection?: PreviewSection;
    setExpandedSection: (section?: PreviewSection) => void;
}

const MatchContext = React.createContext<MatchesContextProps>({
    expandedSection: undefined,
    setExpandedSection: () => {},
});

export default MatchContext;
