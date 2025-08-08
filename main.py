from typing import List, Optional
from fastapi import FastAPI, HTTPException, Depends, Query 
import json
import boto3
import os
from starlette.status import HTTP_204_NO_CONTENT
from sqlmodel import Field, SQLModel, create_engine, Session, select
from dotenv import load_dotenv

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")
AWS_EVENTBRIDGE_BUS_NAME = os.getenv("AWS_EVENTBRIDGE_BUS_NAME")

app = FastAPI(title="API de CRUD com FastAPI e SQLite", version="0.1.0")
eventbridge_client = boto3.client("events", region_name=AWS_REGION)

class ItemBase(SQLModel):
    """
    Classe base para os atributos comuns de um Item.
    Não é uma tabela por si só, mas serve de base para outras classes.
    """
    nome: str = Field(index=True)
    descricao: Optional[str] = None
    preco: float
    imposto: Optional[float] = None

class Item(ItemBase, table=True):
    """
    Representa a tabela 'item' no banco de dados.
    Herda de ItemBase e adiciona o 'id' que será a chave primária.
    """
    id: Optional[int] = Field(default=None, primary_key=True)

class ItemCreate(ItemBase):
    """
    Modelo de dados para a criação de um novo item (POST request).
    Não precisamos do 'id' aqui, pois ele será gerado pelo banco de dados.
    """
    pass

class ItemUpdate(SQLModel):
    """
    Modelo de dados para a atualização de um item (PUT request).
    Todos os campos são opcionais, permitindo atualizações parciais.
    """
    nome: Optional[str] = None
    descricao: Optional[str] = None
    preco: Optional[float] = None
    imposto: Optional[float] = None


sqlite_file_name = "database.db"
sqlite_url = f"sqlite:///{sqlite_file_name}"

# Cria o "motor" do banco de dados (engine).
# `echo=True` mostra os comandos SQL gerados no terminal (útil para depuração).
engine = create_engine(sqlite_url, echo=True)

def create_db_and_tables():
    """
    Cria as tabelas no banco de dados com base nos modelos SQLModel.
    Esta função deve ser chamada na inicialização da aplicação.
    """
    SQLModel.metadata.create_all(engine)

# --- 4. Dependência para a Sessão do Banco de Dados ---
# Isso é um "dependency injection" do FastAPI.
# Para cada requisição, ele vai criar uma nova sessão de banco de dados,
# usá-la e garantir que ela seja fechada no final.

def get_session():
    """
    Retorna uma nova sessão de banco de dados.
    Usado com `Depends` para injetar a sessão nas rotas.
    """
    with Session(engine) as session:
        yield session

# --- 5. Evento de Inicialização (Startup Event) ---
# FastAPI tem eventos de ciclo de vida. O `on_event("startup")`
# garante que nossa função `create_db_and_tables` seja chamada
# assim que a aplicação iniciar, criando o arquivo do DB e as tabelas.

@app.on_event("startup")
def on_startup():
    """
    Função executada quando a aplicação FastAPI é iniciada.
    Cria o banco de dados e as tabelas se eles ainda não existirem.
    """
    create_db_and_tables()

# --- 6. Rotas (Endpoints da API) ---

@app.get("/")
async def read_root():
    """
    Endpoint inicial para testar se a API está funcionando.
    """
    return {"mensagem": "Bem-vindo à API de CRUD com FastAPI e SQLite!"}

@app.post("/items/", response_model=Item, status_code=201)
def create_item(*, item: ItemCreate, session: Session = Depends(get_session)):
    """
    Cria um novo item no banco de dados.
    - Recebe um objeto ItemCreate (sem ID) no corpo da requisição.
    - Adiciona o item à sessão e o commit para o banco de dados.
    - Retorna o item criado (agora com ID).
    """
    db_item = Item.model_validate(item) # Converte ItemCreate para Item
    session.add(db_item)
    session.commit() # Salva no banco de dados
    session.refresh(db_item) # Atualiza o objeto com o ID gerado pelo DB
    send_event_to_eventbridge("ItemCreated", db_item.model_dump())

    return db_item


@app.get("/items/", response_model=List[Item])
def read_items(
    offset: int = 0,
    limit: int = Query(default=100, le=100), # <-- Mude Field para Query aqui!
    session: Session = Depends(get_session)
):
    """
    Retorna todos os itens do banco de dados, com paginação opcional.
    - `offset`: Número de itens para pular.
    - `limit`: Número máximo de itens para retornar (máximo 100).
    """
    items = session.exec(select(Item).offset(offset).limit(limit)).all()
    return items

@app.get("/items/{item_id}", response_model=Item)
def read_item(*, item_id: int, session: Session = Depends(get_session)):
    """
    Retorna um item específico pelo seu ID.
    - Se o item não for encontrado, retorna um erro 404.
    """
    item = session.get(Item, item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Item não encontrado")
    return item

@app.put("/items/{item_id}", response_model=Item)
def update_item(*, item_id: int, item: ItemUpdate, session: Session = Depends(get_session)):
    """
    Atualiza um item existente no banco de dados.
    - Encontra o item pelo ID.
    - Atualiza apenas os campos fornecidos no corpo da requisição.
    - Se o item não for encontrado, retorna um erro 404.
    """
    db_item = session.get(Item, item_id)
    if not db_item:
        raise HTTPException(status_code=404, detail="Item não encontrado")

    # Atualiza apenas os campos que foram fornecidos na requisição
    item_data = item.model_dump(exclude_unset=True) # exclude_unset=True ignora campos não fornecidos
    for key, value in item_data.items():
        setattr(db_item, key, value)

    session.add(db_item) # Adiciona o item modificado de volta à sessão
    session.commit() # Salva as alterações no banco de dados
    session.refresh(db_item) # Atualiza o objeto Python com os dados do DB

    send_event_to_eventbridge("ItemUpdated", db_item.model_dump())   

    return db_item

@app.delete("/items/{item_id}", status_code=HTTP_204_NO_CONTENT)
def delete_item(*, item_id: int, session: Session = Depends(get_session)):
    """
    Deleta um item pelo seu ID.
    - Se o item for deletado com sucesso, retorna um status 204 (Sem Conteúdo).
    - Se o item não for encontrado, retorna um erro 404.
    """
    item = session.get(Item, item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Item não encontrado")
    
    send_event_to_eventbridge("ItemDeleted", item.model_dump())   

    session.delete(item) # Marca o item para exclusão
    session.commit() # Executa a exclusão no banco de dados
    # Não há retorno de item para 204 No Content


def send_event_to_eventbridge(detail_type: str, item_data:dict):
    """Envia o evento para aws"""
    try:
        response = eventbridge_client.put_events(
            Entries=[
                {
                    'Source': 'com.fastapi.items',
                    'DetailType': detail_type,
                    'Detail': json.dumps(item_data),
                    'EventBusName': AWS_EVENTBRIDGE_BUS_NAME
                }
            ]
        )
        print(f"Evento: '{detail_type}' enviado ao Eventbridge, rezsponse: {response}")
    except Exception as e:
        print(f"Erro ao enviar evento para o eventBridge: {e}")
