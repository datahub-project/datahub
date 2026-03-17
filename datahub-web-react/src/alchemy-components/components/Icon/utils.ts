import * as phosphorIcons from '@phosphor-icons/react';

/**
 * Maps legacy MUI icon names to their Phosphor equivalents.
 * Used by getIconComponent to resolve icons saved before the Phosphor migration.
 */
const MUI_TO_PHOSPHOR: Record<string, string> = {
    // UI chrome
    MoreVert: 'DotsThreeVertical',
    ChevronRight: 'CaretRight',
    ChevronLeft: 'CaretLeft',
    Close: 'X',
    Add: 'Plus',
    Delete: 'Trash',
    ContentCopy: 'Copy',
    Edit: 'PencilSimple',
    Search: 'MagnifyingGlass',
    ArrowBack: 'ArrowLeft',
    ArrowDownward: 'ArrowDown',
    ArrowForward: 'ArrowRight',
    VisibilityOff: 'EyeSlash',
    WarningAmber: 'Warning',
    ErrorOutline: 'WarningCircle',
    KeyboardArrowUp: 'CaretUp',
    KeyboardArrowDown: 'CaretDown',
    AutoMode: 'ArrowsClockwise',

    // People & Organizations
    AccountCircle: 'UserCircle',
    Person: 'User',
    PersonOutline: 'User',
    PersonAdd: 'UserPlus',
    PeopleAlt: 'Users',
    People: 'Users',
    Group: 'UsersThree',
    Groups: 'UsersThree',
    SupervisedUserCircle: 'UsersThree',
    Face: 'SmileySticker',
    EmojiPeople: 'User',

    // Business & Finance
    Business: 'Buildings',
    BusinessCenter: 'Briefcase',
    Work: 'Briefcase',
    AccountBalance: 'Bank',
    AccountBalanceWallet: 'Wallet',
    AttachMoney: 'CurrencyDollar',
    MonetizationOn: 'CurrencyCircleDollar',
    Payments: 'Money',
    CreditCard: 'CreditCard',
    Receipt: 'Receipt',
    ReceiptLong: 'Receipt',
    Store: 'Storefront',
    Storefront: 'Storefront',
    ShoppingCart: 'ShoppingCart',
    ShoppingBag: 'Bag',
    LocalShipping: 'Truck',
    Inventory: 'Package',
    Inventory2: 'Package',
    Warehouse: 'Warehouse',
    Factory: 'Factory',

    // Data & Analytics
    Analytics: 'ChartLine',
    Assessment: 'ChartBar',
    BarChart: 'ChartBar',
    PieChart: 'ChartPie',
    ShowChart: 'ChartLineUp',
    TrendingUp: 'TrendUp',
    TrendingDown: 'TrendDown',
    AutoGraph: 'TrendUp',
    BubbleChart: 'ChartScatter',
    DataUsage: 'ChartDonut',
    Insights: 'Lightbulb',
    Leaderboard: 'Trophy',
    Dashboard: 'SquaresFour',
    DashboardCustomize: 'SquaresFour',
    Speed: 'Gauge',
    Timeline: 'Path',
    TableChart: 'Table',

    // Technology & Engineering
    Code: 'Code',
    Terminal: 'Terminal',
    Computer: 'Desktop',
    LaptopMac: 'Laptop',
    Devices: 'DeviceMobile',
    Memory: 'Cpu',
    Storage: 'Database',
    CloudQueue: 'Cloud',
    Cloud: 'Cloud',
    CloudDone: 'CloudCheck',
    Dns: 'HardDrives',
    Hub: 'GitBranch',
    Api: 'Plugs',
    Settings: 'Gear',
    Engineering: 'GearSix',
    Build: 'Wrench',
    Construction: 'Wrench',
    BugReport: 'Bug',
    SmartToy: 'Robot',
    Biotech: 'Dna',

    // Science & Education
    Science: 'Flask',
    School: 'GraduationCap',
    MenuBook: 'BookOpen',
    AutoStories: 'BookOpen',
    LibraryBooks: 'Books',
    Book: 'Book',
    Psychology: 'Brain',
    Microscope: 'Microscope',
    Atom: 'Atom',

    // Healthcare
    LocalHospital: 'Hospital',
    HealthAndSafety: 'ShieldCheck',
    MedicalServices: 'FirstAid',
    Healing: 'Heartbeat',
    MonitorHeart: 'Heartbeat',
    Medication: 'Pill',
    Vaccines: 'Syringe',

    // Communication
    Email: 'Envelope',
    Mail: 'Envelope',
    Chat: 'ChatCircle',
    Forum: 'ChatCircle',
    Campaign: 'Megaphone',
    Notifications: 'Bell',
    NotificationsNone: 'Bell',
    Phone: 'Phone',
    Call: 'Phone',
    Mic: 'Microphone',
    Headset: 'Headset',
    Support: 'Headset',

    // Content & Media
    Description: 'FileText',
    Article: 'Article',
    Newspaper: 'Newspaper',
    PhotoCamera: 'Camera',
    Image: 'Image',
    MusicNote: 'MusicNote',
    Palette: 'Palette',
    Brush: 'PaintBrush',
    ColorLens: 'Palette',
    Movie: 'FilmStrip',

    // Navigation & Location
    Home: 'House',
    Explore: 'Compass',
    Map: 'MapPin',
    Language: 'GlobeSimple',
    Public: 'Globe',
    Domain: 'Globe',
    TravelExplore: 'GlobeSimple',
    Place: 'MapPin',
    LocationOn: 'MapPin',
    NearMe: 'NavigationArrow',

    // Nature & Environment
    Eco: 'Leaf',
    Park: 'Tree',
    Nature: 'Tree',
    NaturePeople: 'Tree',
    Pets: 'PawPrint',
    WaterDrop: 'Drop',
    Terrain: 'Mountains',
    Agriculture: 'Plant',

    // Security & Legal
    Security: 'ShieldCheck',
    Shield: 'Shield',
    Lock: 'Lock',
    LockOpen: 'LockOpen',
    VerifiedUser: 'ShieldCheck',
    Verified: 'SealCheck',
    Gavel: 'Gavel',
    Policy: 'ShieldCheck',
    AdminPanelSettings: 'ShieldCheck',
    PrivacyTip: 'ShieldWarning',

    // Files & Folders
    Folder: 'Folder',
    FolderOpen: 'FolderOpen',
    FolderSpecial: 'FolderStar',
    CreateNewFolder: 'FolderPlus',
    Topic: 'Folder',
    Category: 'Folders',
    Archive: 'Archive',
    Layers: 'Stack',

    // Actions & Status
    Favorite: 'Heart',
    FavoriteBorder: 'Heart',
    Star: 'Star',
    StarBorder: 'Star',
    Flag: 'Flag',
    Bookmark: 'BookmarkSimple',
    BookmarkBorder: 'BookmarkSimple',
    Label: 'Tag',
    LocalOffer: 'Tag',
    Extension: 'PuzzlePiece',
    Visibility: 'Eye',

    // Misc objects
    Rocket: 'Rocket',
    RocketLaunch: 'Rocket',
    Anchor: 'Anchor',
    FlashOn: 'Lightning',
    Bolt: 'Lightning',
    EmojiObjects: 'Lightbulb',
    Lightbulb: 'Lightbulb',
    Target: 'Target',
    TrackChanges: 'Crosshair',
    FilterList: 'Funnel',
    Handshake: 'Handshake',
    Scale: 'Scales',
    Balance: 'Scales',
    CalendarToday: 'CalendarBlank',
    Event: 'CalendarBlank',
    Schedule: 'Clock',
    AccessTime: 'Clock',
    Share: 'ShareNetwork',
    Wifi: 'WifiHigh',
    Power: 'Plug',
    LocalGroceryStore: 'ShoppingCart',
    DirectionsCar: 'Car',
    Flight: 'Airplane',
    Restaurant: 'ForkKnife',
    FitnessCenter: 'Barbell',
    SportsEsports: 'GameController',
    Casino: 'DiceFive',
    Celebration: 'Confetti',
    MilitaryTech: 'Medal',
    EmojiEvents: 'Trophy',
    Savings: 'PiggyBank',
    Toll: 'Coins',
    VolunteerActivism: 'HandHeart',
    Diversity1: 'UsersThree',
    Diversity3: 'UsersThree',
};

export const getIconComponent = (icon: string) => {
    const direct = phosphorIcons[icon];
    if (direct) return direct;

    const mapped = MUI_TO_PHOSPHOR[icon];
    if (mapped) {
        if (process.env.NODE_ENV === 'development') {
            console.warn(`Icon "${icon}" is a legacy MUI name. Use "${mapped}" instead.`);
        }
        return phosphorIcons[mapped];
    }

    return undefined;
};
