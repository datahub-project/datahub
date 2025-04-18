import { BrowsePath, BrowsePathsInput, EntityType } from '../../types.generated';

const paths = {
    [EntityType.Dataset](urn) {
        const result = urn.replace('urn:li:dataset:(urn:li:dataPlatform:', '').replace(')', '').split(',');
        return [result[result.length - 1].toLowerCase(), result[0], ...result[1].split('.')];
    },
    [EntityType.Dashboard](urn) {
        return urn.replace('urn:li:dashboard:(', '').replace(')', '').split(',');
    },
    [EntityType.Chart](urn) {
        return urn.replace('urn:li:chart:(', '').replace(')', '').split(',');
    },
    [EntityType.DataFlow](urn) {
        const result = urn.replace('urn:li:dataFlow:(', '').replace(')', '').split(',');
        return [result[0], result[result.length - 1].toLowerCase(), result[1]];
    },
    [EntityType.DataJob]() {
        return [];
    },
};

type GetBrowsePaths = {
    data: {
        browsePaths: BrowsePath[];
    };
};

export const getBrowsePathsResolver = {
    getBrowsePaths({ variables: { input } }): GetBrowsePaths {
        const { urn, type }: BrowsePathsInput = input;

        return {
            data: {
                browsePaths: [
                    {
                        path: paths[type](urn),
                        __typename: 'BrowsePath',
                    },
                ],
            },
        };
    },
};
