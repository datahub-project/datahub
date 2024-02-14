import React from 'react';

export enum PreviewSection {
    MATCHES = 'matches',
    OWNERS = 'owners',
    GLOSSARY_TERMS = 'glossaryTerms',
    TAGS = 'tags',
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
