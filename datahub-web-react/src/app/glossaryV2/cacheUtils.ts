import { ApolloClient } from '@apollo/client';
import { GetGlossaryNodeDocument, GetGlossaryNodeQuery } from '../../graphql/glossaryNode.generated';

export function removeTermFromGlossaryNode(
    client: ApolloClient<object>,
    glossaryNodeUrn: string,
    glossaryTermUrn: string,
) {
    // Read the data from our cache for this query.
    const currData: GetGlossaryNodeQuery | null = client.readQuery({
        query: GetGlossaryNodeDocument,
        variables: { urn: glossaryNodeUrn },
    });

    // Remove the term from the existing children set.
    const newTermChildren = {
        relationships: [
            ...(currData?.glossaryNode?.children?.relationships || []).filter(
                (relationship) => relationship.entity?.urn !== glossaryTermUrn,
            ),
        ],
        total: (currData?.glossaryNode?.children?.total || 1) - 1,
    };

    // Write our data back to the cache.
    client.writeQuery({
        query: GetGlossaryNodeDocument,
        variables: { urn: glossaryNodeUrn },
        data: {
            glossaryNode: {
                ...currData?.glossaryNode,
                children: newTermChildren,
            },
        },
    });
}
