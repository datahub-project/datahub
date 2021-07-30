import { BrowseInput, BrowseResultGroup, BrowseResults, Entity, EntityType, SearchResult } from '../../types.generated';
import { toLowerCaseEntityType, toTitleCase } from '../helper';
import { EntityBrowseFn, EntityBrowsePath, GetBrowseResults, StringNumber } from '../types';

type ToFlatPathsArg = {
    flatPaths: StringNumber[][];
    paths: EntityBrowsePath[];
    parentPaths: string[];
};

export const toFlatPaths = ({ flatPaths, paths, parentPaths }: ToFlatPathsArg) => {
    paths.forEach(({ name, paths: childPaths, count = 0 }) => {
        if (childPaths.length) {
            parentPaths.push(name);
            toFlatPaths({ flatPaths, parentPaths, paths: childPaths });
        } else {
            flatPaths.push([...parentPaths, name, count]);
        }
    });
    parentPaths.pop();
};

type FilterEntityByPathArg = {
    term: string;
    searchResults: SearchResult[];
};

export const filterEntityByPath = ({ term, searchResults }: FilterEntityByPathArg): Entity[] => {
    return searchResults
        .filter((r) => {
            const regex = new RegExp(term);
            return regex.test(r.entity.urn);
        })
        .map((r) => r.entity);
};

export class BrowsePathResolver {
    private readonly browse: Record<string, EntityBrowseFn>;

    private readonly paths: EntityBrowsePath[];

    private readonly filterEntityHandler: (path: string[]) => Entity[];

    private readonly baseBrowseResult: GetBrowseResults = {
        data: {
            browse: {
                entities: [],
                groups: [],
                start: 0,
                count: 0,
                total: 0,
                metadata: {
                    path: [],
                    totalNumEntities: 0,
                    __typename: 'BrowseResultMetadata',
                },
                __typename: 'BrowseResults',
            },
        },
    };

    constructor({
        entityType,
        paths,
        filterEntityHandler,
    }: {
        entityType: EntityType;
        paths: EntityBrowsePath[];
        filterEntityHandler(path: string[]): Entity[];
    }) {
        this.browse = {};
        this.paths = paths;
        this.filterEntityHandler = filterEntityHandler;
        const browsePathKey = `${toLowerCaseEntityType(entityType)}Browse`;
        const groups = this.paths.map<BrowseResultGroup>(({ name, paths: rootPaths, count = 0 }) => ({
            name,
            count: rootPaths.length ? rootPaths.reduce(this.sumTotalEntityByPaths, 0) : count,
            __typename: 'BrowseResultGroup',
        }));
        this.initBrowsePathResolver({ browsePathKey, groups });
        this.initBrowsePathResolverForPaths({ prefixPathKey: browsePathKey, paths });
    }

    public getBrowse() {
        return this.browse;
    }

    private initBrowsePathResolver({ browsePathKey, groups }: { browsePathKey: string; groups: BrowseResultGroup[] }) {
        if (!this.browse.hasOwnProperty(browsePathKey)) {
            const dataBrowse: BrowseResults = JSON.parse(JSON.stringify(this.baseBrowseResult.data.browse));

            Object.assign(this.browse, {
                [browsePathKey]: ({ start, count, path }: BrowseInput): GetBrowseResults => {
                    const startValue = start as number;
                    const countValue = count as number;
                    const paths = path as string[];
                    const entities = groups.length ? [] : this.filterEntityHandler(paths);
                    const chunkEntities = entities.slice(startValue, startValue + countValue);

                    return {
                        data: {
                            browse: {
                                ...dataBrowse,
                                entities: chunkEntities,
                                groups,
                                start: startValue,
                                count: chunkEntities.length,
                                total: entities.length,
                                metadata: {
                                    ...dataBrowse.metadata,
                                    path: paths,
                                    totalNumEntities: groups.reduce(this.sumTotalEntityByGroups, 0),
                                },
                            },
                        },
                    };
                },
            });
        }
    }

    private initBrowsePathResolverForPaths({
        prefixPathKey,
        paths,
    }: {
        prefixPathKey: string;
        paths: EntityBrowsePath[];
    }) {
        paths.forEach(({ name, paths: childPaths }) => {
            const browsePathKey = `${prefixPathKey}${toTitleCase(name)}`;

            if (childPaths.length) {
                const groups = childPaths.map<BrowseResultGroup>(
                    ({ name: childName, paths: child2Paths, count = 0 }) => ({
                        name: childName,
                        count: child2Paths.length ? child2Paths.reduce(this.sumTotalEntityByPaths, 0) : count,
                        __typename: 'BrowseResultGroup',
                    }),
                );

                this.initBrowsePathResolver({ browsePathKey, groups });
                this.initBrowsePathResolverForPaths({ prefixPathKey: browsePathKey, paths: childPaths });
            } else {
                this.initBrowsePathResolver({ browsePathKey, groups: [] });
            }
        });
    }

    private sumTotalEntityByGroups = (out: number, { count = 0 }: BrowseResultGroup): number => {
        return out + count;
    };

    private sumTotalEntityByPaths = (out: number, { paths, count = 0 }: EntityBrowsePath): number => {
        if (paths.length) {
            return paths.reduce(this.sumTotalEntityByPaths, out);
        }
        return out + count;
    };
}
