import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import NextStepButton from '@site/src/components/NextStepButton';
import TutorialProgress from '@site/src/components/TutorialProgress';
import OSDetectionTabs from '@site/src/components/OSDetectionTabs';

# Step 1: Setup DataHub (5 minutes)

<TutorialProgress
tutorialId="quickstart"
currentStep={0}
steps={[
{ title: "Setup DataHub", time: "5 min", description: "Deploy DataHub locally with Docker" },
{ title: "Ingest Your First Dataset", time: "15 min", description: "Connect sources and ingest metadata" },
{ title: "Discovery Basics", time: "10 min", description: "Find, evaluate, and understand datasets" },
{ title: "Explore Data Lineage", time: "15 min", description: "Trace dependencies and assess impact" }
]}
/>

In this step, you'll deploy DataHub locally using Docker. This gives you a complete DataHub environment running on your machine for learning and experimentation.

## What You'll Accomplish

By the end of this step, you'll have:

- DataHub running locally at `http://localhost:9002`
- Understanding of DataHub's core components
- Access to the DataHub web interface

## Prerequisites Check

Before we begin, verify you have the required software:

<Tabs>
<TabItem value="check" label="Quick Check">

**Run these commands to verify your setup:**

<div className="command-checklist">

```bash
# Check Docker
docker --version
docker-compose --version

# Check Python
python3 --version

# Check Docker is running
docker ps
```

**Expected output:**

- Docker version 20.10+
- Docker Compose version 2.0+
- Python 3.9+
- Docker ps should run without errors

</div>

:::tip Success Indicator
If all commands run without errors, you're ready to proceed!
:::

</TabItem>
<TabItem value="install" label="Need to Install?">

If you're missing any prerequisites, follow the OS-specific instructions below:

<OSDetectionTabs>
<TabItem value="windows" label="Windows">

**Install Docker Desktop:**

1. Download [Docker Desktop for Windows](https://www.docker.com/products/docker-desktop/)
2. Run the installer and follow the setup wizard
3. Restart your computer when prompted
4. Launch Docker Desktop from the Start menu

**Install Python 3.9+:**

1. Download from [python.org](https://www.python.org/downloads/windows/)
2. **Important**: Check "Add Python to PATH" during installation
3. Verify installation: Open Command Prompt and run `python --version`

**System Requirements:**

- Windows 10 64-bit: Pro, Enterprise, or Education (Build 16299 or later)
- WSL 2 feature enabled (Docker Desktop will help set this up)
- 2 CPUs minimum, 8GB RAM minimum, 12GB free disk space

</TabItem>
<TabItem value="macos" label="macOS">

**Install Docker Desktop:**

1. Download [Docker Desktop for Mac](https://www.docker.com/products/docker-desktop/)
2. Drag Docker.app to your Applications folder
3. Launch Docker Desktop from Applications
4. Follow the setup assistant

**Install Python 3.9+:**

```bash
# Using Homebrew (recommended)
brew install python@3.9

# Or download from python.org
# Visit: https://www.python.org/downloads/macos/
```

**System Requirements:**

- macOS 10.15 or newer
- Apple chip (M1/M2) or Intel chip
- 2 CPUs minimum, 8GB RAM minimum, 12GB free disk space

</TabItem>
<TabItem value="linux" label="Linux">

**Install Docker:**

```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install docker.io docker-compose-plugin
sudo systemctl start docker
sudo systemctl enable docker

# CentOS/RHEL/Fedora
sudo yum install docker docker-compose
sudo systemctl start docker
sudo systemctl enable docker

# Add your user to docker group (logout/login required)
sudo usermod -aG docker $USER
```

**Install Python 3.9+:**

```bash
# Ubuntu/Debian
sudo apt-get install python3 python3-pip

# CentOS/RHEL/Fedora
sudo yum install python3 python3-pip

# Verify installation
python3 --version
```

**System Requirements:**

- 64-bit Linux distribution
- Kernel version 3.10 or higher
- 2 CPUs minimum, 8GB RAM minimum, 12GB free disk space

</TabItem>
</OSDetectionTabs>

**Common Resource Requirements:**

- 2 CPUs minimum
- 8GB RAM minimum
- 12GB free disk space

</TabItem>
</Tabs>

## Install DataHub CLI

The DataHub CLI is your primary tool for managing DataHub deployments and ingestion.

<OSDetectionTabs>
<TabItem value="windows" label="Windows">

```cmd
# Install the DataHub CLI (Command Prompt or PowerShell)
python -m pip install --upgrade pip wheel setuptools
python -m pip install --upgrade acryl-datahub

# Verify installation
datahub version
```

**Troubleshooting:**

- If `python` command not found, try `py` instead
- If `datahub` command not found, use `python -m datahub version`
- Ensure Python was added to PATH during installation

</TabItem>
<TabItem value="macos" label="macOS">

```bash
# Install the DataHub CLI
python3 -m pip install --upgrade pip wheel setuptools
python3 -m pip install --upgrade acryl-datahub

# Verify installation
datahub version
```

**Troubleshooting:**

- If `datahub` command not found, use `python3 -m datahub version`
- On M1/M2 Macs, you might need to install Rosetta 2 for some dependencies

</TabItem>
<TabItem value="linux" label="Linux">

```bash
# Install the DataHub CLI
python3 -m pip install --upgrade pip wheel setuptools
python3 -m pip install --upgrade acryl-datahub

# Verify installation
datahub version

# Alternative: Install with user flag if permission issues
python3 -m pip install --user --upgrade acryl-datahub
```

**Troubleshooting:**

- If `datahub` command not found, use `python3 -m datahub version`
- Add `~/.local/bin` to PATH if using `--user` flag
- Use `sudo` only if installing system-wide (not recommended)

</TabItem>
</OSDetectionTabs>

**Expected output:**

```
DataHub CLI version: 0.13.x
Python version: 3.x.x
```

## Deploy DataHub

Now let's start DataHub using the quickstart deployment:

<OSDetectionTabs>
<TabItem value="windows" label="Windows">

```cmd
# Deploy DataHub locally (Command Prompt)
datahub docker quickstart

# If datahub command not found, use:
python -m datahub docker quickstart
```

**Windows-specific notes:**

- Ensure Docker Desktop is running before executing
- The process may take longer on Windows due to WSL 2 overhead
- If you encounter permission issues, run Command Prompt as Administrator

</TabItem>
<TabItem value="macos" label="macOS">

```bash
# Deploy DataHub locally
datahub docker quickstart

# If datahub command not found, use:
python3 -m datahub docker quickstart
```

**macOS-specific notes:**

- Ensure Docker Desktop is running and has sufficient resources allocated
- On M1/M2 Macs, some images may need to be built for ARM architecture
- Grant Docker Desktop access to your file system when prompted

</TabItem>
<TabItem value="linux" label="Linux">

```bash
# Deploy DataHub locally
datahub docker quickstart

# If datahub command not found, use:
python3 -m datahub docker quickstart

# If permission issues with Docker:
sudo datahub docker quickstart
```

**Linux-specific notes:**

- Ensure Docker service is running: `sudo systemctl status docker`
- If using sudo, DataHub files will be owned by root
- Consider adding your user to the docker group to avoid sudo

</TabItem>
</OSDetectionTabs>

This command will:

1. **Download** the DataHub Docker Compose configuration
2. **Pull** all required Docker images (this may take a few minutes)
3. **Start** all DataHub services

**What's happening behind the scenes:**

### DataHub Deployment Process

The `datahub docker quickstart` command orchestrates a complete DataHub deployment:

**Phase 1: Environment Preparation**

- Validates Docker installation and system requirements
- Checks available ports (9002 for frontend, 8080 for backend)
- Prepares configuration files and networking

**Phase 2: Infrastructure Setup**

- Downloads the latest docker-compose configuration
- Pulls required Docker images:
  - `acryldata/datahub-gms` (Backend services)
  - `acryldata/datahub-frontend-react` (Web interface)
  - `mysql:8` (Metadata storage)
  - `opensearchproject/opensearch` (Search index)
  - `confluentinc/cp-kafka` (Message queue)

**Phase 3: Service Orchestration**

- Starts core infrastructure (MySQL, OpenSearch, Kafka)
- Initializes DataHub backend services (GMS)
- Launches the web frontend
- Configures DataHub Actions for automation

**Expected Timeline**: Initial deployment takes 3-5 minutes depending on your internet connection and system performance.

## Verify Deployment

When deployment completes successfully, you should see:

```
DataHub is now running
Ingest some demo data using `datahub docker ingest-sample-data`,
or head to http://localhost:9002 (username: datahub, password: datahub) to play around with the frontend.
```

**Let's verify everything is working:**

1. **Check running containers:**

   ```bash
   docker ps
   ```

   You should see 6-8 containers running with names like:

   - `datahub-frontend-quickstart-1`
   - `datahub-datahub-gms-quickstart-1`
   - `datahub-mysql-1`
   - `datahub-opensearch-1`

2. **Access the DataHub UI:**

   - Open your browser to [http://localhost:9002](http://localhost:9002)
   - You should see the DataHub login page

3. **Sign in with default credentials:**
   ```
   Username: datahub
   Password: datahub
   ```

## Understanding DataHub Architecture

Now that DataHub is running, let's understand what you've deployed:

| Component            | Purpose                           | Port |
| -------------------- | --------------------------------- | ---- |
| **DataHub Frontend** | Web UI for users                  | 9002 |
| **DataHub GMS**      | Metadata API and business logic   | 8080 |
| **MySQL**            | Stores metadata and configuration | 3306 |
| **OpenSearch**       | Powers search and discovery       | 9200 |
| **Kafka**            | Handles real-time metadata events | 9092 |
| **DataHub Actions**  | Automation and workflows          | -    |

**Data Flow:**

1. **Metadata ingestion** → GMS API → MySQL (storage) + OpenSearch (search)
2. **User searches** → Frontend → GMS → OpenSearch → Results
3. **Real-time updates** → Kafka → Actions → UI notifications

## Troubleshooting

**Common issues and solutions:**

<Tabs>
<TabItem value="port-conflict" label="Port Conflicts">

**Error:** `Port already in use`

**Solution:**

```bash
# Check what's using the port
lsof -i :9002

# Stop conflicting services or use different ports
datahub docker quickstart --port 9003
```

</TabItem>
<TabItem value="docker-resources" label="Docker Resources">

**Error:** `Container fails to start` or `Out of memory`

**Solution:**

1. Increase Docker Desktop memory to 8GB+
2. Close other applications
3. Restart Docker Desktop

</TabItem>
<TabItem value="slow-startup" label="Slow Startup">

**Issue:** Services taking a long time to start

**This is normal for first-time setup:**

- Image downloads: 5-10 minutes
- Service initialization: 2-3 minutes
- Total first-time setup: 10-15 minutes

</TabItem>
</Tabs>

## Success Checkpoint

**You've successfully completed Step 1 when:**

- DataHub UI loads at http://localhost:9002
- You can sign in with datahub/datahub credentials
- You see the empty DataHub home page
- All Docker containers are running properly

**What you've learned:**

- How to deploy DataHub locally using Docker
- DataHub's core architecture components
- How to verify a successful deployment

<NextStepButton
to="first-ingestion"
tutorialId="quickstart"
currentStep={0}
>
Next: Ingest Your First Dataset
</NextStepButton>
