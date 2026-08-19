# FlowFirst - Multi-Process RabbitMQ Communication Pipeline

This project implements inter-process communication using RabbitMQ and Python (`pika`) across two data flows with data mutation/reflection at intermediate steps.

---

## Architecture & Data Flows

### Flow 1
1. **Process 1 (`process1.py`)**: Prepares initial data and publishes it to queue `flow1_p1_to_p2`.
2. **Process 2 (`process2.py`)**: Consumes the message from `flow1_p1_to_p2`, modifies the payload (increments counter and appends to history log), and **reflects** it back to queue `flow1_p2_to_p3`.
3. **Process 3 (`process3.py`)**: Consumes from `flow1_p2_to_p3` and logs the full audit trail.

```
+---------------+      flow1_p1_to_p2      +---------------+      flow1_p2_to_p3      +---------------+
|   Process 1   | -----------------------> |   Process 2   | -----------------------> |   Process 3   |
|  (Generator)  |                          |  (Reflector)  |                          |  (End Sink)   |
+---------------+                          +---------------+                          +---------------+
```

---

### Flow 2
1. **Process 1 (`process1.py`)**: Prepares metric payload and publishes it to queue `flow2_p1_to_p2`.
2. **Process 2 (`process2.py`)**: Consumes message, examines value threshold, applies transformation, and publishes it to queue `flow2_p2_to_p3`.
3. **Process 3 (`process3.py`)**: Picks up message from `flow2_p2_to_p3`, enriches it with verification status, and **reflects** it to queue `flow2_p3_reflected`.

```
+---------------+      flow2_p1_to_p2      +---------------+      flow2_p2_to_p3      +---------------+
|   Process 1   | -----------------------> |   Process 2   | -----------------------> |   Process 3   |
|  (Generator)  |                          |   (Examiner)  |                          |  (Reflector)  |
+---------------+                          +---------------+                          +---------------+
                                                                                              |
                                                                                              v
                                                                                    flow2_p3_reflected
```

---

## Installation on RHEL 9.6 (Red Hat Enterprise Linux 9.6)

You can choose either **Docker / Docker Compose** (Option A) or **Direct Native Installation via DNF/RPM** (Option B).

---

### Option A: Install Docker & Docker Compose on RHEL 9.6 (Recommended)

1. **Remove any conflicting legacy packages:**
   ```bash
   sudo dnf remove -y podman buildah
   ```

2. **Set up the official Docker CE repository:**
   ```bash
   sudo dnf install -y dnf-plugins-core
   sudo dnf config-manager --add-repo https://download.docker.com/linux/rhel/docker-ce.repo
   ```

3. **Install Docker Engine, CLI, and Docker Compose plugin:**
   ```bash
   sudo dnf install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
   ```

4. **Start and enable Docker service:**
   ```bash
   sudo systemctl enable --now docker
   ```

5. **(Optional) Allow current user to run Docker without `sudo`:**
   ```bash
   sudo usermod -aG docker $USER
   newgrp docker
   ```

6. **Configure firewall (firewalld) if accessing from another machine:**
   ```bash
   sudo firewall-cmd --permanent --add-port=5672/tcp
   sudo firewall-cmd --permanent --add-port=15672/tcp
   sudo firewall-cmd --reload
   ```

7. **Start RabbitMQ using Docker Compose:**
   ```bash
   docker compose up -d
   ```
   *Dashboard will be available at `http://<RHEL_IP>:15672` (Username: `guest`, Password: `guest`).*

---

### Option B: Native RabbitMQ Installation on RHEL 9.6 (Direct RPM / DNF)

RabbitMQ on RHEL 9.6 requires Erlang/OTP 26+ (provided via the official Team RabbitMQ Cloudsmith repositories).

1. **Import Erlang and RabbitMQ GPG signing keys:**
   ```bash
   sudo rpm --import 'https://dl.cloudsmith.io/public/rabbitmq/rabbitmq-erlang/gpg.E495BB49CC4BBE5B.key'
   sudo rpm --import 'https://dl.cloudsmith.io/public/rabbitmq/rabbitmq-server/gpg.9F4587F22620D4E7.key'
   ```

2. **Add Erlang and RabbitMQ repository definitions for RHEL 9:**
   ```bash
   sudo tee /etc/yum.repos.d/rabbitmq.repo << 'EOF'
   [rabbitmq-erlang]
   name=rabbitmq-erlang
   baseurl=https://dl.cloudsmith.io/public/rabbitmq/rabbitmq-erlang/rpm/el/9/$basearch
   repo_gpgcheck=1
   enabled=1
   gpgkey=https://dl.cloudsmith.io/public/rabbitmq/rabbitmq-erlang/gpg.E495BB49CC4BBE5B.key
   gpgcheck=0
   sslverify=1
   sslcacert=/etc/pki/tls/certs/ca-bundle.crt
   metadata_expire=300

   [rabbitmq-server]
   name=rabbitmq-server
   baseurl=https://dl.cloudsmith.io/public/rabbitmq/rabbitmq-server/rpm/el/9/noarch
   repo_gpgcheck=1
   enabled=1
   gpgkey=https://dl.cloudsmith.io/public/rabbitmq/rabbitmq-server/gpg.9F4587F22620D4E7.key
   gpgcheck=0
   sslverify=1
   sslcacert=/etc/pki/tls/certs/ca-bundle.crt
   metadata_expire=300
   EOF
   ```

3. **Install Erlang and RabbitMQ Server:**
   ```bash
   sudo dnf update -y
   sudo dnf install -y erlang rabbitmq-server
   ```

4. **Enable the Management Plugin & Start RabbitMQ:**
   ```bash
   # Enable web UI plugin
   sudo rabbitmq-plugins enable rabbitmq_management

   # Enable and start systemd service
   sudo systemctl enable --now rabbitmq-server
   ```

5. **Configure admin user (by default, `guest` can only connect from localhost):**
   ```bash
   sudo rabbitmqctl add_user admin YourSecurePassword123
   sudo rabbitmqctl set_user_tags admin administrator
   sudo rabbitmqctl set_permissions -p / admin ".*" ".*" ".*"
   ```

6. **Open ports in `firewalld` (RHEL 9 default firewall):**
   ```bash
   sudo firewall-cmd --permanent --add-port=5672/tcp   # AMQP
   sudo firewall-cmd --permanent --add-port=15672/tcp  # Management UI
   sudo firewall-cmd --reload
   ```

---

## Other Operating Systems (macOS / Ubuntu / Windows)

<details>
<summary>Click to expand</summary>

### macOS (Homebrew)
```bash
brew install rabbitmq
brew services start rabbitmq
```

### Ubuntu / Debian
```bash
sudo apt update
sudo apt install -y rabbitmq-server
sudo systemctl enable --now rabbitmq-server
```

### Windows (Chocolatey)
```powershell
choco install rabbitmq
```

</details>

---

## Python Virtual Environment Setup

1. **Create a virtual environment:**
   ```bash
   python3 -m venv .venv
   ```

2. **Activate the virtual environment:**
   - **macOS / Linux:**
     ```bash
     source .venv/bin/activate
     ```
   - **Windows (PowerShell):**
     ```powershell
     .venv\Scripts\Activate.ps1
     ```
   - **Windows (cmd.exe):**
     ```cmd
     .venv\Scripts\activate.bat
     ```

3. **Install dependencies:**
   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

---

## Configuration

Default environment parameters connect to `localhost:5672` with default credentials (`guest`/`guest`).

Optionally copy `.env.example` to `.env` to customize settings:
```bash
cp .env.example .env
```

---

## Running the Processes

Open **3 separate terminal windows**, activate the virtual environment in each, and run the processes in the following order:

### Terminal 1: Start Process 3 (Sink & Flow-2 Reflector)
```bash
source .venv/bin/activate
python process3.py
```

### Terminal 2: Start Process 2 (Processor & Flow-1 Reflector)
```bash
source .venv/bin/activate
python process2.py
```

### Terminal 3: Start Process 1 (Producer)
```bash
source .venv/bin/activate
python process1.py
```

---

## Verification
- Observe logs in Terminal 2 showing Process 2 modifying and reflecting Flow 1 messages, and examining Flow 2 messages.
- Observe logs in Terminal 3 showing Process 3 consuming Flow 1 messages and reflecting Flow 2 messages.
- Check RabbitMQ Management console at `http://localhost:15672` to inspect queue activity and message delivery rates.
