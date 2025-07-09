import { DEFAULT_MODULE_ICON, MODULE_TYPE_TO_DESCRIPTION, MODULE_TYPE_TO_ICON } from '@app/homeV3/modules/constants';
import {
    convertModuleToModuleInfo,
    getModuleDescription,
    getModuleIcon,
    getModuleName,
    getModuleType,
} from '@app/homeV3/modules/utils';

import { DataHubPageModule, DataHubPageModuleType, EntityType, PageModuleScope } from '@types';

const MOCKED_MODULE: DataHubPageModule = {
    urn: 'urn:li:dataHubPageModule:example',
    type: EntityType.DatahubPageModule,
    properties: {
        created: {
            time: 1752056099724,
        },
        lastModified: {
            time: 1752056099724,
        },
        name: 'Link 1 (example)',
        type: DataHubPageModuleType.Link,
        visibility: {
            scope: PageModuleScope.Global,
        },
        params: {},
    },
};

describe('getModuleType', () => {
    it('should return the correct type from module.properties.type', () => {
        const module = MOCKED_MODULE;
        expect(getModuleType(module)).toBe(module.properties.type);
    });
});

describe('getModuleIcon', () => {
    it('should return the corresponding icon from MODULE_TYPE_TO_ICON', () => {
        const module = {
            ...MOCKED_MODULE,
            ...{ properties: { ...MOCKED_MODULE.properties, type: DataHubPageModuleType.Domains } },
        };
        const expectedIcon = MODULE_TYPE_TO_ICON.get(DataHubPageModuleType.Domains);
        expect(getModuleIcon(module)).toBe(expectedIcon);
    });

    it('should return the default icon when the type is not found', () => {
        const module = {
            ...MOCKED_MODULE,
            ...{ properties: { ...MOCKED_MODULE.properties, type: 'UnknownType' as DataHubPageModuleType } },
        };
        expect(getModuleIcon(module)).toBe(DEFAULT_MODULE_ICON);
    });
});

describe('getModuleName', () => {
    it('should return the module name from properties', () => {
        const module = MOCKED_MODULE;
        expect(getModuleName(module)).toBe(module.properties.name);
    });
});

describe('getModuleDescription', () => {
    it('should return description from MODULE_TYPE_TO_DESCRIPTION map', () => {
        const module = MOCKED_MODULE;
        const moduleType = getModuleType(module);
        const expectedDescription = MODULE_TYPE_TO_DESCRIPTION.get(moduleType);
        expect(getModuleDescription(module)).toBe(expectedDescription);
    });
});

describe('convertModuleToModuleInfo', () => {
    it('should correctly convert DataHubPageModule to ModuleInfo', () => {
        const module = MOCKED_MODULE;
        const moduleInfo = convertModuleToModuleInfo(module);

        expect(moduleInfo).toEqual({
            urn: module.urn,
            key: module.urn,
            type: getModuleType(module),
            name: getModuleName(module),
            description: getModuleDescription(module),
            icon: getModuleIcon(module),
        });
    });
});
