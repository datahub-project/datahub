import { Component } from '@angular/core';

@Component({
    selector: 'app-root',
    template: `
        <div class="container">
            <!-- Header -->
            <div class="header">
                <h1 class="title">🅰️ Hello World Angular MFE</h1>
                <p class="subtitle">An Angular micro-frontend running inside DataHub</p>
            </div>

            <!-- Navigation -->
            <nav class="nav">
                <span class="nav-label">Pages:</span>
                <button
                    *ngFor="let page of pages"
                    [class]="currentPage === page.id ? 'nav-btn active' : 'nav-btn'"
                    (click)="navigateTo(page.id)"
                >
                    {{ page.icon }} {{ page.label }}
                </button>
            </nav>

            <!-- Home Page -->
            <div *ngIf="currentPage === 'home'">
                <div class="card">
                    <h2 class="card-title">🎉 Welcome!</h2>
                    <p class="card-text">
                        This is an Angular-based micro-frontend demonstrating integration with DataHub.
                        It uses Webpack Module Federation to load dynamically into the DataHub host.
                    </p>
                    <span class="badge success">✓ Angular {{ angularVersion }}</span>
                </div>

                <div class="card">
                    <h2 class="card-title">🔢 Interactive Counter</h2>
                    <p class="card-text">
                        Angular two-way binding in action:
                    </p>
                    <div class="counter">
                        <button class="btn primary" (click)="decrement()">−</button>
                        <span class="count">{{ count }}</span>
                        <button class="btn primary" (click)="increment()">+</button>
                    </div>
                </div>

                <div class="card">
                    <h2 class="card-title">🔗 DataHub Navigation</h2>
                    <div class="links">
                        <a class="link" (click)="goToDataHub('/')">🏠 Home</a>
                        <a class="link" (click)="goToDataHub('/search')">🔍 Search</a>
                        <a class="link" (click)="goToDataHub('/glossary')">📖 Glossary</a>
                        <a class="link" (click)="goToDataHub('/settings')">⚙️ Settings</a>
                    </div>
                </div>
            </div>

            <!-- About Page -->
            <div *ngIf="currentPage === 'about'">
                <div class="card">
                    <h2 class="card-title">ℹ️ About This MFE</h2>
                    <table class="info-table">
                        <tr>
                            <td class="label">Framework</td>
                            <td>Angular {{ angularVersion }}</td>
                        </tr>
                        <tr>
                            <td class="label">Bundler</td>
                            <td>Webpack 5 with Module Federation</td>
                        </tr>
                        <tr>
                            <td class="label">Integration</td>
                            <td>Standalone webpack (no Angular CLI)</td>
                        </tr>
                        <tr>
                            <td class="label">Port</td>
                            <td>3004</td>
                        </tr>
                    </table>
                </div>
            </div>
        </div>
    `,
    styles: [
        `
            .container {
                font-family: 'Segoe UI', 'Roboto', 'Oxygen', sans-serif;
                padding: 24px;
                max-width: 900px;
                margin: 0 auto;
            }

            .header {
                background: linear-gradient(135deg, #dd0031 0%, #c3002f 100%);
                border-radius: 12px;
                padding: 32px;
                color: white;
                margin-bottom: 24px;
            }

            .title {
                font-size: 2rem;
                font-weight: 700;
                margin: 0 0 8px 0;
            }

            .subtitle {
                font-size: 1rem;
                opacity: 0.9;
                margin: 0;
            }

            .nav {
                display: flex;
                gap: 8px;
                align-items: center;
                margin-bottom: 24px;
                padding: 12px;
                background: #f5f5f5;
                border-radius: 8px;
            }

            .nav-label {
                font-weight: 600;
                color: #666;
                margin-right: 8px;
            }

            .nav-btn {
                padding: 8px 16px;
                border: 1px solid #ddd;
                border-radius: 6px;
                background: white;
                cursor: pointer;
                font-weight: 500;
                transition: all 0.2s;
            }

            .nav-btn:hover {
                border-color: #dd0031;
            }

            .nav-btn.active {
                background: linear-gradient(135deg, #dd0031 0%, #c3002f 100%);
                color: white;
                border-color: transparent;
            }

            .card {
                background: white;
                border-radius: 12px;
                padding: 20px;
                margin-bottom: 16px;
                box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
                border: 1px solid #e8e8e8;
            }

            .card-title {
                font-size: 1.1rem;
                font-weight: 600;
                margin: 0 0 12px 0;
                color: #1a1a1a;
            }

            .card-text {
                color: #555;
                line-height: 1.6;
                margin: 0 0 12px 0;
            }

            .badge {
                display: inline-block;
                padding: 4px 8px;
                border-radius: 4px;
                font-size: 0.75rem;
                font-weight: 500;
                background: #f0f0f0;
                color: #666;
            }

            .badge.success {
                background: #e8f4e8;
                color: #2d6a2d;
            }

            .counter {
                display: flex;
                align-items: center;
                gap: 16px;
                margin-top: 12px;
            }

            .count {
                font-size: 1.5rem;
                font-weight: 700;
                color: #dd0031;
                min-width: 60px;
                text-align: center;
            }

            .btn {
                padding: 10px 20px;
                border: none;
                border-radius: 6px;
                cursor: pointer;
                font-size: 1.2rem;
                font-weight: 600;
                transition: all 0.2s;
            }

            .btn.primary {
                background: linear-gradient(135deg, #dd0031 0%, #c3002f 100%);
                color: white;
            }

            .btn.primary:hover {
                transform: translateY(-2px);
                box-shadow: 0 4px 12px rgba(221, 0, 49, 0.3);
            }

            .links {
                display: flex;
                flex-direction: column;
                gap: 8px;
            }

            .link {
                color: #dd0031;
                text-decoration: none;
                cursor: pointer;
                display: flex;
                align-items: center;
                gap: 4px;
            }

            .link:hover {
                text-decoration: underline;
            }

            .info-table {
                width: 100%;
            }

            .info-table td {
                padding: 8px 0;
                border-bottom: 1px solid #f0f0f0;
            }

            .info-table .label {
                font-weight: 500;
                color: #666;
                width: 200px;
            }
        `,
    ],
})
export class AppComponent {
    // Angular version
    angularVersion = '17';

    // Navigation
    currentPage: 'home' | 'about' = 'home';
    pages = [
        { id: 'home' as const, label: 'Home', icon: '🏠' },
        { id: 'about' as const, label: 'About', icon: 'ℹ️' },
    ];

    // Counter
    count = 0;

    navigateTo(page: 'home' | 'about'): void {
        this.currentPage = page;
    }

    increment(): void {
        this.count++;
    }

    decrement(): void {
        this.count--;
    }

    goToDataHub(path: string): void {
        window.location.href = path;
    }
}
