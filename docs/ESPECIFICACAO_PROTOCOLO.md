```markdown
# Especificação do Protocolo de Aplicação - ChatTCP

**Arquitetura:** Cliente-Servidor  
**Camada de Transporte:** TCP (IPv4)  
**Porta Padrão:** 5050  
**Formato de Dados:** JSON (JavaScript Object Notation)  

## 1. Formato das Mensagens e Enquadramento (Framing)

Como o TCP é um protocolo orientado a fluxo de bytes (não possui limites definidos de mensagens), nosso protocolo de aplicação utiliza um **delimitador de quadro**. 
Toda mensagem trocada entre cliente e servidor consiste em um objeto JSON válido, codificado em UTF-8, e **obrigatoriamente finalizado pelo caractere de quebra de linha (`\n` / LF)**. 

- **Limite de tamanho:** Um quadro (mensagem + delimitador) não pode exceder 16 KB (16384 bytes).
- **Limite de texto:** O conteúdo legível (atributo `content`) é limitado a 2000 caracteres.

---

## 2. Requisições do Cliente (Ações)

O cliente se comunica com o servidor enviando objetos JSON que contêm, obrigatoriamente, a chave `"action"`.

| Ação (`action`) | Campos Obrigatórios e Significado | Exemplo de Payload |
| :--- | :--- | :--- |
| **`LOGIN`** | `username` (str): Apelido do usuário (3 a 20 caracteres, alfanuméricos ou `_`). Identifica o cliente na sessão. | `{"action": "LOGIN", "username": "alessandro"}` |
| **`MSG_PRIV`** | `target` (str): Nome do destinatário.<br>`content` (str): Texto da mensagem. | `{"action": "MSG_PRIV", "target": "miria", "content": "Olá!"}` |
| **`CREATE_GRP`**| `group_name` (str): Nome do novo grupo (3 a 25 caracteres, sem espaços). O criador entra automaticamente. | `{"action": "CREATE_GRP", "group_name": "redes_furg"}` |
| **`JOIN_GRP`** | `group_name` (str): Nome do grupo existente que deseja ingressar. | `{"action": "JOIN_GRP", "group_name": "redes_furg"}` |
| **`LEAVE_GRP`** | `group_name` (str): Nome do grupo que deseja sair. | `{"action": "LEAVE_GRP", "group_name": "redes_furg"}` |
| **`MSG_GRP`** | `group_name` (str): Grupo de destino.<br>`content` (str): Texto da mensagem. | `{"action": "MSG_GRP", "group_name": "redes_furg", "content": "Ata!"}` |
| **`LIST`** | *(Nenhum campo extra)*. Solicita a lista de usuários online e grupos ativos. | `{"action": "LIST"}` |
| **`PING`** | *(Nenhum campo extra)*. Testa a latência e a disponibilidade da camada de aplicação do servidor. | `{"action": "PING"}` |
| **`DISCONNECT`**| *(Nenhum campo extra)*. Informa ao servidor o encerramento voluntário da conexão. | `{"action": "DISCONNECT"}` |

---

## 3. Respostas e Eventos do Servidor

O servidor envia mensagens em três categorias: **Status de Execução** (respostas diretas a uma requisição), **Eventos Assíncronos** (mensagens de terceiros) e **Sincronização**.

### 3.1. Status de Execução (Sucesso ou Erro)
Toda requisição gera uma resposta com a chave `"status"`.

**Exemplo de Sucesso (`"status": "OK"`):**
```json
{
  "status": "OK",
  "action_reply": "MSG_PRIV",
  "message": "Mensagem privada entregue.",
  "timestamp": "2026-10-17T15:30:00+00:00"
}

Exemplo de Erro ("status": "ERROR"):
JSON

{
  "status": "ERROR",
  "code": 404,
  "error": "USER_NOT_FOUND",
  "message": "Usuário 'miria' não existe ou está offline.",
  "timestamp": "2026-10-17T15:30:05+00:00"
}

3.2. Eventos Assíncronos ("type")

Enviados pelo servidor de forma espontânea (ex: quando alguém manda uma mensagem para você).

    Mensagem Privada Recebida:{"type": "MSG_PRIV", "from": "alessandro", "content": "Olá!", "timestamp": "..."}

    Mensagem de Grupo Recebida:{"type": "MSG_GRP", "group_name": "redes_furg", "from": "alessandro", "content": "Olá grupo!", "timestamp": "..."}

    Notificação do Sistema (Entrada/Saída de membros):{"type": "SYS_INFO", "message": "miria entrou no grupo.", "timestamp": "..."}

3.3. Sincronização de Histórico

Quando um usuário realiza a ação JOIN_GRP, o servidor envia imediatamente o histórico de mensagens daquele grupo lido do disco.
JSON

{
  "type": "HISTORY_SYNC",
  "group_name": "redes_furg",
  "messages": [
    {"type": "MSG_GRP", "from": "mark", "content": "Trabalho pronto", "timestamp": "..."}
  ],
  "message": "Carregadas 1 mensagem(ns)."
}

4. Sequência Esperada das Interações

Abaixo, os fluxos lógicos estabelecidos pelo protocolo:
4.1. Conexão e Autenticação

    Cliente abre o socket TCP e conecta na porta 5050.

    Cliente envia {"action": "LOGIN", "username": "cintia"}\n.

    Servidor valida o nome. Se não houver duplicidade, registra a sessão e devolve {"status": "OK", "action_reply": "LOGIN"}\n.

    Servidor realiza um broadcast assíncrono do tipo "SYS_INFO" avisando os demais clientes que cintia conectou-se.

4.2. Troca de Mensagem Privada

    cintia envia {"action": "MSG_PRIV", "target": "mark", "content": "Tudo certo?"}\n.

    Servidor localiza o socket de mark.

    Servidor despacha para o socket de mark: {"type": "MSG_PRIV", "from": "cintia", "content": "Tudo certo?"}\n.

    Servidor confirma a entrega para cintia enviando {"status": "OK", "action_reply": "MSG_PRIV"}\n.

5. Comportamento Diante de Mensagens Inválidas e Falhas

A aplicação possui mecanismos para não interromper a disponibilidade do servidor em caso de erros:

    JSON Malformado ou Violação de Codificação:

        Se o cliente enviar bytes que não formam um UTF-8 ou JSON válido, o servidor descarta o buffer e retorna um ERROR com código 400 (INVALID_JSON ou INVALID_ENCODING), mantendo o socket aberto.

    Quadro Gigante (Proteção contra estouro de memória):

        Se o servidor acumular mais de 16 KB no buffer de leitura sem encontrar um \n, ele responde com ERROR de código 413 (FRAME_TOO_LARGE) e encerra a conexão ativamente.

    Comandos sem Identificação:

        Enviar qualquer ação diferente de LOGIN ou PING antes de estar autenticado gera o erro 401 (AUTH_REQUIRED).

    Desconexão Abrupta (Cabo de Rede Desconectado / Timeout):

        O servidor utiliza verificações via TCP Keep-Alive e blocos try/except em chamadas recv(). Se a conexão quebrar fisicamente, uma exceção é levantada e o servidor executa a rotina de limpeza silenciosa (retira o usuário dos grupos e da lista de clientes online) sem corromper as estruturas em memória para os clientes restantes.
