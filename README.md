# Agentic Modern Data

## Instalación

### macOS

#### Homebrew (si no lo tienes)

```bash
/bin/bash -c "$(curl -fsSL https://brew.sh/install.sh)"
```

#### Herramientas

```bash
brew install uv
brew install libpq
echo 'export PATH="...libpq/bin:$PATH"' >> ~/.zshrc
brew install databricks
```

#### opencode

```bash
curl -fsSL opencode.ai/install | sh
```

#### Verificar

```bash
uv --version && psql --version
```

---

### Windows

#### winget (viene con Windows 11)

```powershell
winget install astral-sh.uv
winget install Databricks.DatabricksCLI
```

#### psql

Descargar desde [postgresql.org](https://www.postgresql.org/download/windows/). En el installer seleccionar solo **"Command Line Tools"** y añadir `bin` al PATH del sistema.

#### opencode

```powershell
iwr opencode.ai/install.ps1 | iex
```

#### Verificar (nueva terminal)

```powershell
uv --version
psql --version
databricks --version
```