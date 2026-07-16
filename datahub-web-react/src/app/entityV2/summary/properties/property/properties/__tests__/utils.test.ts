import { describe, expect, it } from 'vitest';

import { ItemType } from '@components/components/Menu/types';

import { filterCurrentItemInReplaceMenu } from '@app/entityV2/summary/properties/property/properties/utils';
import type { AssetProperty } from '@app/entityV2/summary/properties/types';

import { StructuredPropertyFieldsFragment } from '@graphql/fragments.generated';
import { EntityType, StdDataType, SummaryElementType } from '@types';

function isItemType(item: ItemType): item is Extract<ItemType, { children?: unknown }> {
    return 'children' in item;
}

describe('filterCurrentItemInReplaceMenu', () => {
    it('should return items unchanged if no item with key "replace"', () => {
        const menuItems: ItemType[] = [
            { key: 'notReplace', type: 'item', title: 'Item 1' },
            { key: 'anotherItem', type: 'item', title: 'Item 2' },
        ];
        const property: AssetProperty = {
            name: 'test',
            type: SummaryElementType.Owners,
        };

        const result = filterCurrentItemInReplaceMenu(menuItems, property);
        expect(result).toEqual(menuItems);
    });

    it('should not modify replace item if no children present', () => {
        const menuItems: ItemType[] = [{ key: 'replace', type: 'item', title: 'Replace' }];
        const property: AssetProperty = {
            name: 'test',
            type: SummaryElementType.Tags,
        };

        const result = filterCurrentItemInReplaceMenu(menuItems, property);
        expect(result).toEqual(menuItems);
    });

    it('should filter excludeKey from children if property.type is not StructuredProperty', () => {
        const menuItems: ItemType[] = [
            {
                key: 'replace',
                type: 'group',
                title: 'Replace Group',
                children: [
                    { key: SummaryElementType.Tags, type: 'item', title: 'To Remove' },
                    { key: 'keepMe', type: 'item', title: 'Keep Me' },
                ],
            },
        ];
        const property: AssetProperty = {
            name: 'test',
            type: SummaryElementType.Tags,
        };

        const result = filterCurrentItemInReplaceMenu(menuItems, property);
        if (isItemType(result[0])) {
            expect(result[0].children).toEqual([{ key: 'keepMe', type: 'item', title: 'Keep Me' }]);
        }
    });

    it('should filter nested children in structuredProperties if property.type is StructuredProperty', () => {
        const menuItems: ItemType[] = [
            {
                key: 'replace',
                type: 'group',
                title: 'Replace Group',
                children: [
                    {
                        key: 'structuredProperties',
                        type: 'group',
                        title: 'Structured',
                        children: [
                            { key: 'urn-to-remove', type: 'item', title: 'Remove Me' },
                            { key: 'keepMe', type: 'item', title: 'Keep Me' },
                        ],
                    },
                    { key: 'somethingElse', type: 'item', title: 'Other' },
                ],
            },
        ];

        const structuredProperty: StructuredPropertyFieldsFragment = {
            urn: 'urn-to-remove',
            type: EntityType.StructuredProperty,
            exists: true,
            definition: {
                qualifiedName: 'testQualifiedName',
                immutable: false,
                entityTypes: [
                    {
                        type: EntityType.Dataset,
                        urn: 'urn:li:dataset',
                        info: {
                            type: EntityType.Dataset,
                        },
                    },
                ],
                valueType: {
                    urn: 'urn:li:dataType:string',
                    type: EntityType.DataType,
                    info: {
                        type: StdDataType.String,
                    },
                },
            },
        };

        const property: AssetProperty = {
            name: 'test',
            type: SummaryElementType.StructuredProperty,
            structuredProperty,
        };

        const result = filterCurrentItemInReplaceMenu(menuItems, property);
        if (isItemType(result[0]) && result[0].children && isItemType(result[0].children[0])) {
            expect(result[0].children[0].children).toEqual([{ key: 'keepMe', type: 'item', title: 'Keep Me' }]);
            expect(result[0].children[1]).toEqual({ key: 'somethingElse', type: 'item', title: 'Other' });
        }
    });

    it('should handle structuredProperties node without children gracefully', () => {
        const menuItems: ItemType[] = [
            {
                key: 'replace',
                type: 'group',
                title: 'Replace Group',
                children: [{ key: 'structuredProperties', type: 'group', title: 'Structured' }],
            },
        ];

        const structuredProperty: StructuredPropertyFieldsFragment = {
            urn: 'urn-to-remove',
            type: EntityType.StructuredProperty,
            exists: true,
            definition: {
                qualifiedName: 'testQualifiedName',
                immutable: false,
                entityTypes: [
                    {
                        type: EntityType.Dataset,
                        urn: 'urn:li:dataset',
                        info: {
                            type: EntityType.Dataset,
                        },
                    },
                ],
                valueType: {
                    urn: 'urn:li:dataType:string',
                    type: EntityType.DataType,
                    info: {
                        type: StdDataType.String,
                    },
                },
            },
        };

        const property: AssetProperty = {
            name: 'test',
            type: SummaryElementType.StructuredProperty,
            structuredProperty,
        };

        const result = filterCurrentItemInReplaceMenu(menuItems, property);
        if (isItemType(result[0])) {
            expect(result[0].children?.[0]).toEqual({
                key: 'structuredProperties',
                type: 'group',
                title: 'Structured',
            });
        }
    });

    it('should handle empty menuItems array', () => {
        const menuItems: ItemType[] = [];
        const property: AssetProperty = {
            name: 'test',
            type: SummaryElementType.Domain,
        };

        const result = filterCurrentItemInReplaceMenu(menuItems, property);
        expect(result).toEqual([]);
    });
});
