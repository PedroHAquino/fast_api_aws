# 🚀 API de Produtos com Arquitetura Orientada a Eventos na AWS

Um projeto de exemplo de uma API RESTful simples para gerenciamento de itens (CRUD), construída com **FastAPI** e **Python**. A aplicação utiliza uma arquitetura orientada a eventos para desacoplar a lógica de negócio, integrando-se a serviços de mensageria da **Amazon Web Services (AWS)**.

## ✨ Funcionalidades

- **CRUD Completo:** Crie, leia, atualize e delete itens.
- **Persistência de Dados:** Usa um banco de dados local **SQLite** para armazenar os dados de forma persistente.
- **Arquitetura de Eventos:** Após cada operação de criação, atualização ou deleção, a API dispara um evento.
- **Integração AWS:**
    - **Amazon EventBridge:** Atua como um barramento de eventos, recebendo e roteando os eventos da API.
    - **Amazon SNS:** Distribui os eventos para múltiplos assinantes, como e-mail.
    - **Amazon SQS:** Envia os eventos para uma fila de mensagens para processamento assíncrono.

## ⚙️ Tecnologias Utilizadas

- **Backend:**
    - [Python 3.10+](https://www.python.org/)
    - [FastAPI](https://fastapi.tiangolo.com/)
    - [SQLModel](https://sqlmodel.tiangolo.com/)
    - [Uvicorn](https://www.uvicorn.org/)
    - [Boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) (SDK da AWS para Python)
- **Banco de Dados:**
    - SQLite
- **Serviços AWS:**
    - EventBridge
    - SQS
    - SNS

## 🌐 Arquitetura

A aplicação segue um padrão de arquitetura orientada a eventos:

`FastAPI (API REST) -> EventBridge (Roteador de Eventos) -> [SNS Topic & SQS Queue]`

Quando um item é modificado, o evento é enviado para o EventBridge. Uma regra no EventBridge encaminha esse evento para um Tópico SNS (que o envia para e-mail) e para uma Fila SQS.

## 🚀 Como Executar o Projeto Localmente

### Pré-requisitos

- Python 3.10+
- `pip` (gerenciador de pacotes do Python)
- `git`
- Credenciais AWS configuradas (através do `aws configure`)

### Instalação

1.  Clone o repositório:
    ```bash
    git clone [https://github.com/seu-usuario/nome-do-repositorio.git](https://github.com/seu-usuario/nome-do-repositorio.git)
    cd nome-do-repositorio
    ```

2.  Crie e ative um ambiente virtual:
    ```bash
    python -m venv venv
    source venv/bin/activate  # macOS/Linux
    .\venv\Scripts\activate   # Windows
    ```

3.  Instale as dependências:
    ```bash
    pip install -r requirements.txt
    ```

4.  Crie um arquivo `.env` para suas variáveis de ambiente na raiz do projeto:
    ```env
    AWS_EVENTBRIDGE_BUS_NAME="arn:aws:events:sua-regiao:seu-id-de-conta:event-bus/seu-bus"
    AWS_REGION="sua-regiao"
    ```
    ⚠️ **ATENÇÃO**: Este arquivo `.env` está no `.gitignore` e não deve ser enviado para o repositório por questões de segurança.

5.  Execute a aplicação:
    ```bash
    uvicorn main:app --host 0.0.0.0 --port 8000 --reload
    ```

A API estará disponível em `http://localhost:8000`.

## 📖 Documentação da API

A documentação interativa (Swagger UI) está disponível em `http://localhost:8000/docs`.

## 🔑 Segurança

As credenciais da AWS são gerenciadas de forma segura através do comando `aws configure` e das variáveis de ambiente (`.env`), garantindo que nenhum segredo seja exposto no código ou no repositório Git.

## 📝 Licença

Este projeto está licenciado sob a [Licença MIT](https://github.com/seu-usuario/nome-do-repositorio/blob/main/LICENSE).