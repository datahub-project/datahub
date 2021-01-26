import { EntityType } from '../../types.generated';

export { EntityType };

export const fromPathName = (pathName: string): EntityType => {
    switch (pathName) {
        case 'dataset':
            return EntityType.Dataset;
        case 'user':
            return EntityType.User;
        default:
            throw new Error(`Unrecognized pathName ${pathName} provided.`);
    }
};

export const toPathName = (type: EntityType): string => {
    switch (type) {
        case EntityType.Dataset:
            return 'dataset';
        case EntityType.User:
            return 'user';
        default:
            throw new Error(`Unrecognized type ${type} provided.`);
    }
};

export const fromCollectionName = (name: string): EntityType => {
    switch (name) {
        case 'Datasets':
            return EntityType.Dataset;
        case 'Users':
            return EntityType.User;
        default:
            throw new Error(`Unrecognized name ${name} provided.`);
    }
};

export const toCollectionName = (type: EntityType): string => {
    switch (type) {
        case EntityType.Dataset:
            return 'Datasets';
        case EntityType.User:
            return 'Users';
        default:
            throw new Error(`Unrecognized type ${type} provided.`);
    }
};
