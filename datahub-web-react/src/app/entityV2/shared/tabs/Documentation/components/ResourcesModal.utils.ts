import { RelatedItem } from '@app/entityV2/shared/tabs/Documentation/components/relatedSectionUtils';

export const ALL_RESOURCE_TYPES = 'all';

export type ResourceGroup = {
    key: string;
    label: string;
    items: RelatedItem[];
};

export type ResourceGroupLabels = {
    documents: string;
    links: string;
};

function getResourceTypeKey(item: RelatedItem): string {
    return item.type;
}

export function getResourceTypeOptions(
    items: RelatedItem[],
    labels: ResourceGroupLabels,
): { value: string; label: string }[] {
    const options = new Map<string, string>();
    items.forEach((item) => {
        const key = getResourceTypeKey(item);
        if (options.has(key)) return;
        const label = item.type === 'link' ? labels.links : labels.documents;
        options.set(key, label);
    });
    return Array.from(options, ([value, label]) => ({ value, label }));
}

export function groupResources(items: RelatedItem[], labels: ResourceGroupLabels): ResourceGroup[] {
    const documents: RelatedItem[] = [];
    const links: RelatedItem[] = [];
    items.forEach((item) => (item.type === 'link' ? links : documents).push(item));

    const groups: ResourceGroup[] = [];
    if (documents.length > 0) groups.push({ key: 'document', label: labels.documents, items: documents });
    if (links.length > 0) groups.push({ key: 'link', label: labels.links, items: links });
    return groups;
}

export function filterResourceGroups(groups: ResourceGroup[], selectedType: string, query: string): ResourceGroup[] {
    const normalizedQuery = query.trim().toLocaleLowerCase();

    return groups
        .map((group) => ({
            ...group,
            items: group.items.filter((item) => {
                if (selectedType !== ALL_RESOURCE_TYPES && getResourceTypeKey(item) !== selectedType) return false;
                return matchesQuery(item, normalizedQuery);
            }),
        }))
        .filter((group) => group.items.length > 0);
}

function matchesQuery(item: RelatedItem, normalizedQuery: string): boolean {
    if (!normalizedQuery) return true;
    const url = item.type === 'link' ? item.data.url : '';
    return `${item.sortLabel} ${url}`.toLocaleLowerCase().includes(normalizedQuery);
}
