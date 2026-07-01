// Historical MUI (@mui/icons-material) icon names → closest Phosphor (@phosphor-icons/react)
// equivalent. Domains created before the icon-library migration persist MUI names; this table
// lets the current Phosphor-based renderer display them without a visible regression.
//
// The default MUI icon used pre-migration was `AccountCircle`, which is far and away the most
// common stored value in existing data. The remaining entries cover the icons users were most
// likely to have picked for business-domain avatars.
//
// Any name not in this table passes through unchanged — either it is already a Phosphor name
// (post-migration pick) or an unknown value the lazy loader will render as its AppWindow
// fallback.

const MUI_TO_PHOSPHOR: Record<string, string> = {
    // The pre-migration default — must map so untouched domains keep a recognizable avatar.
    AccountCircle: 'UserCircle',
    // People / users
    Person: 'User',
    People: 'Users',
    Group: 'Users',
    Groups: 'Users',
    SupervisorAccount: 'UserGear',
    // Places / structure
    Home: 'House',
    Business: 'Buildings',
    Domain: 'Buildings',
    Store: 'Storefront',
    Storefront: 'Storefront',
    Warehouse: 'Warehouse',
    Factory: 'Factory',
    LocationOn: 'MapPin',
    LocationCity: 'Buildings',
    Place: 'MapPin',
    Map: 'MapPin',
    Public: 'Globe',
    Language: 'Globe',
    Explore: 'Compass',
    // Data / storage
    Storage: 'Database',
    Dns: 'HardDrives',
    Cloud: 'Cloud',
    Folder: 'Folder',
    Description: 'FileText',
    Article: 'Article',
    Assignment: 'ClipboardText',
    LibraryBooks: 'Books',
    Book: 'Book',
    MenuBook: 'BookOpen',
    // Analytics / charts
    BarChart: 'ChartBar',
    ShowChart: 'ChartLine',
    PieChart: 'ChartPie',
    DonutLarge: 'ChartDonut',
    BubbleChart: 'ChartDonut',
    Analytics: 'ChartLineUp',
    Assessment: 'ChartLineUp',
    Leaderboard: 'ChartBar',
    TrendingUp: 'TrendUp',
    TrendingDown: 'TrendDown',
    Insights: 'Lightbulb',
    Speed: 'Gauge',
    Timeline: 'ChartLine',
    // Development / tech
    Code: 'Code',
    Api: 'Plug',
    Terminal: 'Terminal',
    Build: 'Wrench',
    Engineering: 'Wrench',
    Devices: 'Monitor',
    Laptop: 'Laptop',
    PhoneAndroid: 'DeviceMobile',
    Router: 'WifiHigh',
    BugReport: 'Bug',
    Memory: 'Cpu',
    DeveloperMode: 'Code',
    // UI containers
    Dashboard: 'SquaresFour',
    Category: 'SquaresFour',
    Widgets: 'SquaresFour',
    ViewModule: 'SquaresFour',
    Layers: 'Stack',
    Extension: 'PuzzlePiece',
    // Commerce / money
    ShoppingCart: 'ShoppingCart',
    ShoppingCartCheckout: 'ShoppingCart',
    ShoppingBag: 'ShoppingBag',
    ShoppingBasket: 'Basket',
    LocalMall: 'ShoppingBag',
    LocalGroceryStore: 'ShoppingCart',
    LocalShipping: 'Truck',
    LocalOffer: 'Tag',
    Sell: 'Tag',
    AttachMoney: 'CurrencyDollar',
    Payments: 'Money',
    Receipt: 'Receipt',
    CreditCard: 'CreditCard',
    MonetizationOn: 'Coin',
    AccountBalance: 'Bank',
    AccountBalanceWallet: 'Wallet',
    Savings: 'PiggyBank',
    Redeem: 'Gift',
    CardGiftcard: 'Gift',
    Campaign: 'Megaphone',
    Loyalty: 'Heart',
    Handshake: 'Handshake',
    // Communication
    Email: 'Envelope',
    Mail: 'Envelope',
    Chat: 'ChatCircle',
    Message: 'ChatCircleText',
    Notifications: 'Bell',
    Phone: 'Phone',
    // Security
    Security: 'Shield',
    Lock: 'Lock',
    VpnKey: 'Key',
    Fingerprint: 'Fingerprint',
    // Status / feedback
    Info: 'Info',
    Warning: 'Warning',
    Error: 'WarningCircle',
    CheckCircle: 'CheckCircle',
    Star: 'Star',
    Favorite: 'Heart',
    Flag: 'Flag',
    Bookmark: 'Bookmark',
    // Time
    Schedule: 'Clock',
    Event: 'Calendar',
    // Misc common
    Work: 'Briefcase',
    WorkOutline: 'Briefcase',
    School: 'GraduationCap',
    Settings: 'Gear',
    Search: 'MagnifyingGlass',
    Palette: 'Palette',
    Rocket: 'Rocket',
    Edit: 'Pencil',
    Create: 'Pencil',
    Note: 'Note',
    Notes: 'Notebook',
    Camera: 'Camera',
    Videocam: 'VideoCamera',
    PlayArrow: 'Play',
    Send: 'PaperPlaneTilt',
    Whatshot: 'Fire',
    EmojiEvents: 'Trophy',
    LocalHospital: 'FirstAid',
    Restaurant: 'ForkKnife',
    LocalCafe: 'Coffee',
    FlightTakeoff: 'Airplane',
    DirectionsBoat: 'Boat',
    DirectionsCar: 'Car',
    Traffic: 'TrafficSign',
    Brush: 'PaintBrush',
    ContentCut: 'Scissors',
    Handyman: 'Toolbox',
    Construction: 'Hammer',
    StraightenOutlined: 'Ruler',
    Podcasts: 'Broadcast',
    LiveTv: 'MonitorPlay',
};

// Returns the Phosphor equivalent for a stored (potentially MUI) icon name, or the name itself
// when no mapping applies. Callers should treat an empty return as "no icon" and render their
// non-icon fallback (e.g. a letter avatar).
export function resolveIconName(name: string | null | undefined): string {
    if (!name) return '';
    return MUI_TO_PHOSPHOR[name] ?? name;
}
