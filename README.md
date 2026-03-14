# MAC INSTALL

## Homebrew (si no lo tienes)
/bin/bash -c "$(curl -fsSL \
  https://brew.sh/install.sh)"

## Herramientas
brew install uv
brew install libpq
echo 'export PATH="...libpq/bin:$PATH"' >> ~/.zshrc
brew install databricks

## opencode
curl -fsSL opencode.ai/install | sh

## Verificar
uv --version && psql --version


# WINDOWS INSTALL

## winget (viene con Windows 11)
winget install astral-sh.uv
winget install Databricks.DatabricksCLI

## psql: descargar desde postgresql.org
## Installer → solo "Command Line Tools"
## Añadir bin al PATH del sistema

## opencode
iwr opencode.ai/install.ps1 | iex

## Verificar (nueva terminal)
uv --version
psql --version
databricks --version