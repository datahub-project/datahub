
import { RootGlossaryNodeWithFourLayersFragment } from '@graphql/fragments.generated';
import { ChildGlossaryTermFragment } from '@graphql/glossaryNode.generated';
import { GlossaryNode, GlossaryTerm } from '@types';

export type GlossaryNodeType = GlossaryNode | RootGlossaryNodeWithFourLayersFragment;
export type GlossaryTermType = GlossaryTerm | ChildGlossaryTermFragment;
