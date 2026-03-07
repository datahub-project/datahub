import React, { useState, useEffect, useCallback } from 'react';

// Types for the DataHub API responses
interface Entity {
    urn: string;
    type: string;
    properties?: {
        name?: string;
        description?: string;
    };
    platform?: {
        name: string;
        properties?: {
            displayName?: string;
            logoUrl?: string;
        };
    };
    subTypes?: {
        typeNames?: string[];
    };
}

interface RecommendationContent {
    value: string;
    entity?: Entity;
}

interface RecommendationModule {
    title: string;
    moduleId: string;
    renderType: string;
    content: RecommendationContent[];
}

interface MeResponse {
    me: {
        corpUser: {
            urn: string;
            username: string;
            properties?: {
                displayName?: string;
                email?: string;
            };
        };
    };
}

// Simple hash-based router for MFE internal navigation
type Route = 'home' | 'recommendations' | 'about';

const styles: Record<string, React.CSSProperties> = {
    container: {
        fontFamily: "'Segoe UI', 'Roboto', 'Oxygen', sans-serif",
        padding: '24px',
        maxWidth: '1000px',
        margin: '0 auto',
    },
    nav: {
        display: 'flex',
        gap: '8px',
        marginBottom: '24px',
        padding: '12px',
        background: '#f5f5f5',
        borderRadius: '8px',
    },
    navButton: {
        padding: '8px 16px',
        border: 'none',
        borderRadius: '6px',
        cursor: 'pointer',
        fontWeight: 500,
        transition: 'all 0.2s',
    },
    navButtonActive: {
        background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
        color: 'white',
    },
    navButtonInactive: {
        background: 'white',
        color: '#333',
        border: '1px solid #ddd',
    },
    header: {
        background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
        borderRadius: '12px',
        padding: '32px',
        color: 'white',
        marginBottom: '24px',
    },
    title: {
        fontSize: '2rem',
        fontWeight: 700,
        margin: 0,
        marginBottom: '8px',
    },
    subtitle: {
        fontSize: '1rem',
        opacity: 0.9,
        margin: 0,
    },
    card: {
        background: '#ffffff',
        borderRadius: '12px',
        padding: '20px',
        marginBottom: '16px',
        boxShadow: '0 2px 8px rgba(0, 0, 0, 0.08)',
        border: '1px solid #e8e8e8',
    },
    cardTitle: {
        fontSize: '1.1rem',
        fontWeight: 600,
        color: '#1a1a1a',
        marginTop: 0,
        marginBottom: '12px',
    },
    table: {
        width: '100%',
        borderCollapse: 'collapse' as const,
    },
    th: {
        textAlign: 'left' as const,
        padding: '12px',
        borderBottom: '2px solid #eee',
        color: '#666',
        fontSize: '0.85rem',
        textTransform: 'uppercase' as const,
        letterSpacing: '0.5px',
    },
    td: {
        padding: '12px',
        borderBottom: '1px solid #f0f0f0',
    },
    entityLink: {
        color: '#667eea',
        textDecoration: 'none',
        fontWeight: 500,
        cursor: 'pointer',
    },
    badge: {
        display: 'inline-block',
        padding: '4px 8px',
        borderRadius: '4px',
        fontSize: '0.75rem',
        fontWeight: 500,
        background: '#f0f0f0',
        color: '#666',
    },
    loading: {
        textAlign: 'center' as const,
        padding: '40px',
        color: '#666',
    },
    error: {
        background: '#fff2f0',
        border: '1px solid #ffccc7',
        borderRadius: '8px',
        padding: '16px',
        color: '#a8071a',
    },
    datahubLink: {
        display: 'inline-flex',
        alignItems: 'center',
        gap: '4px',
        color: '#667eea',
        textDecoration: 'none',
        cursor: 'pointer',
    },
    externalIcon: {
        fontSize: '0.8rem',
    },
    userInfo: {
        display: 'flex',
        alignItems: 'center',
        gap: '8px',
        marginBottom: '8px',
    },
    avatar: {
        width: '32px',
        height: '32px',
        borderRadius: '50%',
        background: 'rgba(255,255,255,0.2)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        fontSize: '1rem',
    },
};

// GraphQL query to get current user
const ME_QUERY = `
query GetMe {
    me {
        corpUser {
            urn
            username
            properties {
                displayName
                email
            }
        }
    }
}
`;

// GraphQL query for recommendations
const RECOMMENDATIONS_QUERY = `
query listRecommendations($input: ListRecommendationsInput!) {
    listRecommendations(input: $input) {
        modules {
            title
            moduleId
            renderType
            content {
                value
                entity {
                    urn
                    type
                    ... on Dataset {
                        name
                        properties {
                            name
                            description
                        }
                        platform {
                            name
                            properties {
                                displayName
                                logoUrl
                            }
                        }
                        subTypes {
                            typeNames
                        }
                    }
                    ... on Dashboard {
                        properties {
                            name
                            description
                        }
                        platform {
                            name
                            properties {
                                displayName
                            }
                        }
                    }
                    ... on Chart {
                        properties {
                            name
                            description
                        }
                        platform {
                            name
                            properties {
                                displayName
                            }
                        }
                    }
                }
            }
        }
    }
}
`;

// Helper to call DataHub GraphQL API
async function callGraphQL<T>(query: string, variables?: Record<string, unknown>): Promise<T> {
    const response = await fetch('/api/v2/graphql', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ query, variables }),
    });

    if (!response.ok) {
        throw new Error(`GraphQL request failed: ${response.status}`);
    }

    const result = await response.json();
    if (result.errors) {
        throw new Error(result.errors[0]?.message || 'GraphQL error');
    }

    return result.data;
}

// Navigation helper - navigates to DataHub pages
function navigateToDataHub(path: string) {
    // Navigate within the same window to a DataHub page
    window.location.href = path;
}

// Entity type to readable name
function getEntityTypeName(type: string): string {
    const typeMap: Record<string, string> = {
        DATASET: 'Dataset',
        DASHBOARD: 'Dashboard',
        CHART: 'Chart',
        DATA_FLOW: 'Pipeline',
        DATA_JOB: 'Task',
        GLOSSARY_TERM: 'Glossary Term',
        TAG: 'Tag',
        CORP_USER: 'User',
        CORP_GROUP: 'Group',
    };
    return typeMap[type] || type;
}

// Get entity name from entity object
function getEntityName(entity: Entity): string {
    return entity.properties?.name || entity.urn.split(',').pop()?.replace(')', '') || 'Unknown';
}

// Get platform display name
function getPlatformName(entity: Entity): string {
    return entity.platform?.properties?.displayName || entity.platform?.name || 'Unknown';
}

// Home Page Component
const HomePage: React.FC<{ 
    user: MeResponse['me']['corpUser'] | null;
    onNavigate: (route: Route) => void;
}> = ({ user, onNavigate }) => {
    return (
        <>
            <div style={styles.header}>
                {user && (
                    <div style={styles.userInfo}>
                        <div style={styles.avatar}>👤</div>
                        <span>Welcome, {user.properties?.displayName || user.username}!</span>
                    </div>
                )}
                <h1 style={styles.title}>👋 Hello World MFE</h1>
                <p style={styles.subtitle}>
                    A micro-frontend demonstrating DataHub API integration and routing
                </p>
            </div>

            <div style={styles.card}>
                <h2 style={styles.cardTitle}>🚀 Features Demonstrated</h2>
                <ul style={{ lineHeight: 1.8, color: '#555' }}>
                    <li><strong>DataHub API Integration:</strong> Fetching recommendations via GraphQL</li>
                    <li><strong>MFE Internal Routing:</strong> Navigate between pages within this MFE</li>
                    <li><strong>DataHub Navigation:</strong> Link to DataHub entity pages</li>
                    <li><strong>User Context:</strong> Access current user information</li>
                </ul>
            </div>

            <div style={styles.card}>
                <h2 style={styles.cardTitle}>📊 Quick Actions</h2>
                <div style={{ display: 'flex', gap: '12px', flexWrap: 'wrap' }}>
                    <button
                        style={{
                            ...styles.navButton,
                            ...styles.navButtonActive,
                            padding: '12px 24px',
                        }}
                        onClick={() => onNavigate('recommendations')}
                    >
                        View High Usage Entities →
                    </button>
                    <button
                        style={{
                            ...styles.navButton,
                            ...styles.navButtonInactive,
                            padding: '12px 24px',
                        }}
                        onClick={() => navigateToDataHub('/search')}
                    >
                        Go to DataHub Search ↗
                    </button>
                </div>
            </div>

            <div style={styles.card}>
                <h2 style={styles.cardTitle}>🔗 DataHub Navigation Examples</h2>
                <p style={{ color: '#666', marginBottom: '12px' }}>
                    Click these links to navigate to different parts of DataHub:
                </p>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                    <a style={styles.datahubLink} onClick={() => navigateToDataHub('/')}>
                        🏠 Home
                    </a>
                    <a style={styles.datahubLink} onClick={() => navigateToDataHub('/search')}>
                        🔍 Search
                    </a>
                    <a style={styles.datahubLink} onClick={() => navigateToDataHub('/glossary')}>
                        📖 Glossary
                    </a>
                    <a style={styles.datahubLink} onClick={() => navigateToDataHub('/settings')}>
                        ⚙️ Settings
                    </a>
                </div>
            </div>
        </>
    );
};

// Recommendations Page Component
const RecommendationsPage: React.FC<{ userUrn: string | null }> = ({ userUrn }) => {
    const [modules, setModules] = useState<RecommendationModule[]>([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        const fetchRecommendations = async () => {
            if (!userUrn) {
                setLoading(false);
                return;
            }

            try {
                const data = await callGraphQL<{ listRecommendations: { modules: RecommendationModule[] } }>(
                    RECOMMENDATIONS_QUERY,
                    {
                        input: {
                            userUrn,
                            requestContext: {
                                scenario: 'HOME',
                            },
                            limit: 10,
                        },
                    }
                );
                setModules(data.listRecommendations?.modules || []);
                setError(null);
            } catch (err) {
                setError(err instanceof Error ? err.message : 'Failed to fetch recommendations');
            } finally {
                setLoading(false);
            }
        };

        fetchRecommendations();
    }, [userUrn]);

    // Find the HighUsageEntities module
    const highUsageModule = modules.find(m => m.moduleId === 'HighUsageEntities');

    if (loading) {
        return <div style={styles.loading}>Loading recommendations...</div>;
    }

    if (error) {
        return (
            <div style={styles.error}>
                <strong>Error:</strong> {error}
            </div>
        );
    }

    return (
        <>
            <div style={styles.card}>
                <h2 style={styles.cardTitle}>🔥 {highUsageModule?.title || 'High Usage Entities'}</h2>
                {highUsageModule && highUsageModule.content.length > 0 ? (
                    <table style={styles.table}>
                        <thead>
                            <tr>
                                <th style={styles.th}>Name</th>
                                <th style={styles.th}>Type</th>
                                <th style={styles.th}>Platform</th>
                                <th style={styles.th}>Actions</th>
                            </tr>
                        </thead>
                        <tbody>
                            {highUsageModule.content.map((item, index) => {
                                const entity = item.entity;
                                if (!entity) return null;
                                
                                // Build the entity profile URL
                                const entityPath = `/${entity.type.toLowerCase()}/${encodeURIComponent(entity.urn)}`;
                                
                                return (
                                    <tr key={entity.urn || index}>
                                        <td style={styles.td}>
                                            <a
                                                style={styles.entityLink}
                                                onClick={() => navigateToDataHub(entityPath)}
                                                title={entity.urn}
                                            >
                                                {getEntityName(entity)}
                                            </a>
                                        </td>
                                        <td style={styles.td}>
                                            <span style={styles.badge}>
                                                {getEntityTypeName(entity.type)}
                                            </span>
                                        </td>
                                        <td style={styles.td}>
                                            {getPlatformName(entity)}
                                        </td>
                                        <td style={styles.td}>
                                            <a
                                                style={styles.datahubLink}
                                                onClick={() => navigateToDataHub(entityPath)}
                                            >
                                                View in DataHub ↗
                                            </a>
                                        </td>
                                    </tr>
                                );
                            })}
                        </tbody>
                    </table>
                ) : (
                    <p style={{ color: '#666' }}>
                        No high usage entities found. Try ingesting some metadata and generating usage statistics.
                    </p>
                )}
            </div>

            {/* Show other recommendation modules */}
            {modules.filter(m => m.moduleId !== 'HighUsageEntities').map(module => (
                <div key={module.moduleId} style={styles.card}>
                    <h2 style={styles.cardTitle}>📋 {module.title}</h2>
                    <p style={{ color: '#666', marginBottom: '8px' }}>
                        Module ID: <code>{module.moduleId}</code> | 
                        Render Type: <code>{module.renderType}</code> | 
                        Items: {module.content.length}
                    </p>
                    <ul style={{ margin: 0, paddingLeft: '20px' }}>
                        {module.content.slice(0, 5).map((item, index) => (
                            <li key={index} style={{ marginBottom: '4px' }}>
                                {item.entity ? (
                                    <a
                                        style={styles.entityLink}
                                        onClick={() => navigateToDataHub(
                                            `/${item.entity!.type.toLowerCase()}/${encodeURIComponent(item.entity!.urn)}`
                                        )}
                                    >
                                        {getEntityName(item.entity)} ({getEntityTypeName(item.entity.type)})
                                    </a>
                                ) : (
                                    item.value
                                )}
                            </li>
                        ))}
                    </ul>
                </div>
            ))}
        </>
    );
};

// About Page Component
const AboutPage: React.FC = () => {
    return (
        <>
            <div style={styles.card}>
                <h2 style={styles.cardTitle}>ℹ️ About This MFE</h2>
                <p style={{ color: '#555', lineHeight: 1.8 }}>
                    This is a demonstration micro-frontend (MFE) that shows how to integrate with
                    DataHub using the MFE framework. It demonstrates:
                </p>
                <ul style={{ color: '#555', lineHeight: 1.8 }}>
                    <li>Fetching data from DataHub GraphQL API</li>
                    <li>Internal routing within the MFE (hash-based)</li>
                    <li>Navigation to DataHub pages</li>
                    <li>Accessing user context</li>
                </ul>
            </div>

            <div style={styles.card}>
                <h2 style={styles.cardTitle}>🛠️ Technical Details</h2>
                <table style={styles.table}>
                    <tbody>
                        <tr>
                            <td style={{ ...styles.td, fontWeight: 500 }}>Framework</td>
                            <td style={styles.td}>React 18</td>
                        </tr>
                        <tr>
                            <td style={{ ...styles.td, fontWeight: 500 }}>Bundler</td>
                            <td style={styles.td}>Webpack 5 with Module Federation</td>
                        </tr>
                        <tr>
                            <td style={{ ...styles.td, fontWeight: 500 }}>API</td>
                            <td style={styles.td}>DataHub GraphQL (/api/v2/graphql)</td>
                        </tr>
                        <tr>
                            <td style={{ ...styles.td, fontWeight: 500 }}>Routing</td>
                            <td style={styles.td}>Internal state-based + DataHub navigation</td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </>
    );
};

// Main App Component with Routing
export const App: React.FC = () => {
    const [currentRoute, setCurrentRoute] = useState<Route>('home');
    const [user, setUser] = useState<MeResponse['me']['corpUser'] | null>(null);
    const [userLoading, setUserLoading] = useState(true);

    // Fetch current user on mount
    useEffect(() => {
        const fetchUser = async () => {
            try {
                const data = await callGraphQL<MeResponse>(ME_QUERY);
                setUser(data.me.corpUser);
            } catch (err) {
                console.error('Failed to fetch user:', err);
            } finally {
                setUserLoading(false);
            }
        };
        fetchUser();
    }, []);

    const handleNavigate = useCallback((route: Route) => {
        setCurrentRoute(route);
    }, []);

    const renderNavButton = (route: Route, label: string) => (
        <button
            style={{
                ...styles.navButton,
                ...(currentRoute === route ? styles.navButtonActive : styles.navButtonInactive),
            }}
            onClick={() => handleNavigate(route)}
        >
            {label}
        </button>
    );

    return (
        <div style={styles.container}>
            {/* Navigation Bar - MFE Internal Routing */}
            <nav style={styles.nav}>
                <span style={{ fontWeight: 600, marginRight: '8px', color: '#666' }}>
                    MFE Routes:
                </span>
                {renderNavButton('home', '🏠 Home')}
                {renderNavButton('recommendations', '📊 Recommendations')}
                {renderNavButton('about', 'ℹ️ About')}
            </nav>

            {/* Route Content */}
            {currentRoute === 'home' && (
                <HomePage user={user} onNavigate={handleNavigate} />
            )}
            {currentRoute === 'recommendations' && (
                <RecommendationsPage userUrn={userLoading ? null : user?.urn || null} />
            )}
            {currentRoute === 'about' && <AboutPage />}
        </div>
    );
};

export default App;
