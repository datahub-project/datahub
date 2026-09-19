-- 1. SESSIONS TABLE
CREATE TABLE IF NOT EXISTS sessions (
                                        id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    system_prompt TEXT,
    status ENUM('active', 'completed', 'failed', 'paused') DEFAULT 'active',
    summary TEXT DEFAULT NULL,          -- rolling summary of messages older than the keep-window (6 msgs)
    metadata JSON NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_user_id (user_id)
    );

-- 2. MESSAGES TABLE
CREATE TABLE IF NOT EXISTS messages (
                                        id VARCHAR(36) PRIMARY KEY,
    session_id VARCHAR(36) NOT NULL,
    role ENUM('user', 'assistant', 'system') NOT NULL,
    content LONGTEXT NOT NULL,
    tokens_used INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE,
    INDEX idx_session_id (session_id)
    );