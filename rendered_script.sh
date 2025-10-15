#!/bin/bash
# AWS EC2 user-data script for cclaude development instances (Ubuntu 24.04 LTS)

set -e

# Log all output to file for debugging
exec > >(tee /var/log/user-data.log) 2>&1


# Update system packages
export DEBIAN_FRONTEND=noninteractive
# Permanently disable command-not-found to avoid apt_pkg errors (causes issues with Python 3.11)
if [ -f /usr/lib/cnf-update-db ]; then
    rm -f /usr/lib/cnf-update-db
    echo "✅ Permanently disabled cnf-update-db to prevent apt_pkg errors"
fi
apt update && apt upgrade -y

# Create ec2-user EARLY for cclaude compatibility
if ! id ec2-user &>/dev/null; then
    useradd -m -s /bin/bash ec2-user
    echo "✅ Created ec2-user account"
else
    echo "✅ ec2-user account already exists"
fi
usermod -aG sudo ec2-user

# Set up passwordless sudo for ec2-user
if [ ! -f /etc/sudoers.d/90-ec2-user ]; then
    echo "ec2-user ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/90-ec2-user
    chmod 440 /etc/sudoers.d/90-ec2-user
    echo "✅ Sudo access configured for ec2-user"
fi

# Set up SSH public key for ec2-user

echo "🔑 Setting up SSH authorized keys for ec2-user..."
# Ensure .ssh directory exists with correct permissions
sudo -u ec2-user mkdir -p /home/ec2-user/.ssh
sudo -u ec2-user chmod 700 /home/ec2-user/.ssh

# Add the SSH public key to authorized_keys
echo "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACAQDcoqBVlsl5xKvC2Q7/27KfcyM5MQxCLKoO6mANksjRPNAMx3IurZmK+Pm6njeRFaopOScSyBIC5PfRlnS2X04sAzhWOY0gAH0xg08DLN7okhOCHl28YhjoZo5P1RrJdSbgp/kvSifj+yDm0aLUXwDnrSKaBpQYMU0ax4oMXjCbLwZbpOg4hyTr3HWMqyqhWCYp15lEKlpeZdJxBo7L+4B21iVZi1yDfaqZhb4Y2Yh2JGh6cvzW9di/7NPR/O/pX9eGvGMNxSNPBvjyXB7lZ3YgfdzZAQ24VttDnsp+44DtoUi3K/y4GKYvlYTiouolEr28d9Ie72BYtL0miPKOX6uJwHXd7VxoN8f/npC28GzrbuLc0JSMnI3/ULglJMf3G9Gd75c7AfT9tYWYoC9TCXOYA0t1T0itEtHhcmCsHgdk/8E+SHTnc/MuIvMtVypNHRXC/AMKMrspm2laipSZakJb+wsnZMlAAFvWy/1+Kn4FMHCa1Z+kMWCMetuw7CJIpb28PYkvVmwyiYFMYnqYsPF/jxXq5t64lOIblqH3dy9ACvT30yspkfugsrw3m+7aSto+9DkMHmcCcJ1jk6wh6qEaqvFXTzY9foKim5kCUUPwciqnMv4Lo7oDpBjmGDBK2vS7+br65V0rgg7tjf6PeRhXPinXUe/dCZJ4ldehBbNs+w== cclaude-jaypatel@Jays-MacBook-Pro.local" | sudo -u ec2-user tee /home/ec2-user/.ssh/authorized_keys > /dev/null
sudo -u ec2-user chmod 600 /home/ec2-user/.ssh/authorized_keys

echo "🔑 Setting up SSH authorized keys for ubuntu..."
# Ensure .ssh directory exists with correct permissions
sudo -u ubuntu mkdir -p /home/ubuntu/.ssh
sudo -u ubuntu chmod 700 /home/ubuntu/.ssh

# Add the SSH public key to authorized_keys
echo "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACAQDcoqBVlsl5xKvC2Q7/27KfcyM5MQxCLKoO6mANksjRPNAMx3IurZmK+Pm6njeRFaopOScSyBIC5PfRlnS2X04sAzhWOY0gAH0xg08DLN7okhOCHl28YhjoZo5P1RrJdSbgp/kvSifj+yDm0aLUXwDnrSKaBpQYMU0ax4oMXjCbLwZbpOg4hyTr3HWMqyqhWCYp15lEKlpeZdJxBo7L+4B21iVZi1yDfaqZhb4Y2Yh2JGh6cvzW9di/7NPR/O/pX9eGvGMNxSNPBvjyXB7lZ3YgfdzZAQ24VttDnsp+44DtoUi3K/y4GKYvlYTiouolEr28d9Ie72BYtL0miPKOX6uJwHXd7VxoN8f/npC28GzrbuLc0JSMnI3/ULglJMf3G9Gd75c7AfT9tYWYoC9TCXOYA0t1T0itEtHhcmCsHgdk/8E+SHTnc/MuIvMtVypNHRXC/AMKMrspm2laipSZakJb+wsnZMlAAFvWy/1+Kn4FMHCa1Z+kMWCMetuw7CJIpb28PYkvVmwyiYFMYnqYsPF/jxXq5t64lOIblqH3dy9ACvT30yspkfugsrw3m+7aSto+9DkMHmcCcJ1jk6wh6qEaqvFXTzY9foKim5kCUUPwciqnMv4Lo7oDpBjmGDBK2vS7+br65V0rgg7tjf6PeRhXPinXUe/dCZJ4ldehBbNs+w== cclaude-jaypatel@Jays-MacBook-Pro.local" | sudo -u ubuntu tee /home/ubuntu/.ssh/authorized_keys > /dev/null
sudo -u ubuntu chmod 600 /home/ubuntu/.ssh/authorized_keys

echo "✅ SSH public key added to authorized_keys"


# Remount instance storage after restart (format if needed)
for dev in /dev/nvme*n1; do
    if [ -b "$dev" ] && [[ "$(basename "$dev")" != "nvme0n1" ]]; then
        SERIAL=$(lsblk -o SERIAL --noheadings "$dev" 2>/dev/null | tr -d ' ')
        if [[ "$SERIAL" == AWS* ]] && ! mountpoint -q /mnt/local-ssd; then
            mkdir -p /mnt/local-ssd
            # Instance storage is wiped on restart, so format it first
            echo "📝 Formatting instance storage $dev..."
            if mkfs.ext4 "$dev" >/dev/null 2>&1; then
                if mount "$dev" /mnt/local-ssd 2>/dev/null; then
                    chown -R ec2-user:ec2-user /mnt/local-ssd
                    echo "✅ Formatted and mounted instance storage: $dev -> /mnt/local-ssd"
                    systemctl restart docker 2>/dev/null || true
                else
                    echo "⚠️ Failed to mount formatted instance storage: $dev"
                fi
            else
                echo "⚠️ Failed to format instance storage: $dev"
            fi
            break
        fi
    fi
done

# Update the marker file to indicate completion
echo "✅ Basic instance initialization completed at $(date)" > /tmp/cclaude-init-ready
chmod 644 /tmp/cclaude-init-ready