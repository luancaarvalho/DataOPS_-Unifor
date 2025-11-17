# Makefiles para Gerenciamento de Pacotes Python

Este diretório contém Makefiles para facilitar o uso de diferentes gerenciadores de pacotes Python.

## 📋 Arquivos Disponíveis

- `Makefile.pixi` - Gerenciamento com Pixi
- `Makefile.conda` - Gerenciamento com Conda/Miniconda
- `Makefile.uv` - Gerenciamento com UV

## 🚀 Como Usar

### Ver comandos disponíveis

Para qualquer Makefile, use o target `help`:

```bash
make -f Makefile.pixi help
make -f Makefile.conda help
make -f Makefile.uv help
```

### Executar comandos individuais

Escolha apenas o que você precisa:

```bash
# Pixi - criar projeto
make -f Makefile.pixi init

# Conda - criar ambiente
make -f Makefile.conda create-env

# UV - adicionar dependências
make -f Makefile.uv add-deps
```

### Workflow completo

Se quiser executar tudo de uma vez:

```bash
# Pixi - workflow completo
make -f Makefile.pixi all

# Conda - workflow completo
make -f Makefile.conda all

# UV - workflow completo
make -f Makefile.uv all
```

## 📦 Pixi (Makefile.pixi)

### Targets principais:
- `help` - Mostra ajuda
- `install-pixi` - Instala Pixi (⚠️ use só se não tiver instalado)
- `init` - Inicializa projeto
- `add-deps` - Adiciona dependências (pandas, requests)
- `install` - Instala dependências
- `run-example` - Executa exemplo
- `info` - Mostra informações do projeto
- `clean` - Remove projeto
- `all` - Workflow completo

### Exemplo de uso:
```bash
# Ver comandos disponíveis
make -f Makefile.pixi help

# Criar e configurar projeto
make -f Makefile.pixi init
make -f Makefile.pixi add-deps
make -f Makefile.pixi install

# Testar
make -f Makefile.pixi run-example

# Limpar
make -f Makefile.pixi clean
```

## 🐍 Conda (Makefile.conda)

### Targets principais:
- `help` - Mostra ajuda
- `install-conda` - Instala Miniconda (⚠️ use só se não tiver instalado)
- `create-env` - Cria ambiente
- `install-deps` - Instala dependências
- `activate-info` - Mostra como ativar ambiente
- `run-example` - Executa exemplo
- `list-envs` - Lista ambientes
- `info` - Mostra pacotes instalados
- `remove-env` - Remove ambiente
- `all` - Workflow completo

### Exemplo de uso:
```bash
# Ver comandos disponíveis
make -f Makefile.conda help

# Criar ambiente
make -f Makefile.conda create-env

# Instalar dependências
make -f Makefile.conda install-deps

# Ver como ativar manualmente
make -f Makefile.conda activate-info

# Testar
make -f Makefile.conda run-example

# Limpar
make -f Makefile.conda remove-env
```

## ⚡ UV (Makefile.uv)

### Targets principais:
- `help` - Mostra ajuda
- `install-uv` - Instala UV (⚠️ use só se não tiver instalado)
- `init` - Inicializa projeto
- `add-deps` - Adiciona dependências
- `sync` - Sincroniza/instala dependências
- `run-example` - Executa exemplo
- `info` - Lista pacotes instalados
- `tree` - Mostra árvore de dependências
- `clean` - Remove projeto
- `all` - Workflow completo

### Exemplo de uso:
```bash
# Ver comandos disponíveis
make -f Makefile.uv help

# Criar e configurar projeto
make -f Makefile.uv init
make -f Makefile.uv add-deps
make -f Makefile.uv sync

# Testar
make -f Makefile.uv run-example

# Limpar
make -f Makefile.uv clean
```

## 🎯 Dicas

1. **Sempre use `help` primeiro** para ver os comandos disponíveis
2. **Não execute `install-*`** se já tiver o gerenciador instalado
3. **Execute targets individualmente** para ter mais controle
4. **Use `all`** apenas se quiser automatizar tudo
5. **Os comandos podem ser copiados** dos Makefiles para usar diretamente na CLI

## 🔧 Personalização

Você pode modificar as variáveis no início de cada Makefile:

```makefile
# Pixi
PROJECT_DIR = my_pixi_project

# Conda
ENV_NAME = my_conda_env
PYTHON_VERSION = 3.9

# UV
PROJECT_DIR = my_uv_project
PYTHON_VERSION = 3.9
```

## 📝 Copiando comandos para CLI

Todos os comandos dentro dos Makefiles podem ser executados diretamente na sua CLI. 

Por exemplo, em vez de:
```bash
make -f Makefile.conda create-env
```

Você pode copiar e executar:
```bash
conda create -n my_conda_env python=3.9 -y
```

Os Makefiles servem como **referência rápida** e **automação opcional**.
