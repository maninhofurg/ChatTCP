#!/usr/bin/env python3
"""
Servidor TCP de Troca de Mensagens - Redes de Computadores (FURG)
Implementação em baixo nível utilizando exclusivamente a API nativa de sockets.
Integrantes:
- Alessandro Goldas da Cruz (166600)
- Miriã do Amaral Evangelista (166627)
- Markgregor Alves Altê (169518)
- Cintia Gonzaga Gonçalves (162561)
"""

import argparse
import json
import logging
import os
import re
import socket
import threading
from datetime import datetime, timezone
from typing import Any

# Configurações de limites de baixo nível do protocolo
MAX_BUFFER_SIZE = 16384  # Máximo de bytes acumulados por quadro (16 KB)
READ_CHUNK_SIZE = 4096   # Tamanho do bloco lido da chamada de sistema recv()
MAX_MSG_LENGTH = 2000    # Limite de caracteres de texto por mensagem
HISTORICO_FILE = "historico.json"

# Expressões regulares para validação de identificadores
USER_REGEX = re.compile(r"^[a-zA-Z0-9_]{3,20}$")
GROUP_REGEX = re.compile(r"^[a-zA-Z0-9_-]{3,25}$")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(threadName)s) %(message)s",
    datefmt="%H:%M:%S",
)
LOG = logging.getLogger("ServidorRedes")


def timestamp_atual() -> str:
    """Retorna o timestamp atual em formato ISO 8601 (UTC)."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def empacotar_mensagem(dados: dict[str, Any]) -> bytes:
    """
    Serializa o dicionário do protocolo para JSON em formato compacto,
    adiciona o delimitador de quadro LF (\\n) e codifica em bytes UTF-8.
    """
    json_str = json.dumps(dados, ensure_ascii=False, separators=(",", ":"))
    return (json_str + "\n").encode("utf-8")


class ClienteConectado:
    """Representa o estado e o socket de uma conexão TCP ativa."""

    def __init__(self, sock: socket.socket, endereco: tuple[str, int]):
        self.sock = sock
        self.endereco = endereco
        self.username: str | None = None
        # Lock individual para evitar que duas threads misturem bytes no sendall()
        self.write_lock = threading.Lock()

    def enviar_quadro(self, payload: dict[str, Any]) -> bool:
        """Envia todos os bytes do quadro de forma atômica pela conexão TCP."""
        dados_bytes = empacotar_mensagem(payload)
        try:
            with self.write_lock:
                self.sock.sendall(dados_bytes)
            return True
        except (BrokenPipeError, ConnectionResetError, OSError) as erro:
            LOG.warning("Falha de escrita no socket de %s (%s): %s", self.username, self.endereco, erro)
            return False

    def fechar_conexao(self) -> None:
        """Encerra o descritor de arquivo do socket no sistema operacional."""
        try:
            # Envia FIN/RST para ambos os sentidos (leitura e escrita)
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass


class ServidorMensagensTCP:
    """Gerenciador central de conexões, protocolo de aplicação e estado."""

    def __init__(self, host: str = "0.0.0.0", port: int = 5050, arquivo_historico: str = HISTORICO_FILE):
        self.host = host
        self.port = port
        self.arquivo_historico = arquivo_historico

        # Estruturas de gerenciamento de estado em memória
        self.clientes: dict[str, ClienteConectado] = {}  # username -> ClienteConectado
        self.grupos: dict[str, set[str]] = {}            # group_name -> set(usernames)

        # Primitivas de sincronização (Concorrência)
        self.estado_lock = threading.RLock()     # Protege dicionários de clientes e grupos
        self.historico_lock = threading.Lock()   # Protege leitura/escrita em disco (historico.json)
        self.evento_parada = threading.Event()
        self.server_socket: socket.socket | None = None

        self._inicializar_arquivo_historico()

    def _inicializar_arquivo_historico(self) -> None:
        """Garante que o arquivo de histórico exista e carrega grupos salvos."""
        with self.historico_lock:
            if not os.path.exists(self.arquivo_historico):
                with open(self.arquivo_historico, "w", encoding="utf-8") as f:
                    pass  # Cria arquivo vazio (formato JSON Lines)
            else:
                # Recria os grupos que já tinham mensagens no histórico anterior
                try:
                    with open(self.arquivo_historico, "r", encoding="utf-8") as f:
                        for linha in f:
                            linha = linha.strip()
                            if not linha:
                                continue
                            registro = json.loads(linha)
                            nome_grupo = registro.get("group_name")
                            if nome_grupo and nome_grupo not in self.grupos:
                                self.grupos[nome_grupo] = set()
                except Exception as exc:
                    LOG.error("Erro ao ler histórico inicial: %s", exc)

    def _salvar_mensagem_historico(self, mensagem_grupo: dict[str, Any]) -> None:
        """Grava uma mensagem de grupo no arquivo historico.json de forma Thread-Safe."""
        linha_json = json.dumps(mensagem_grupo, ensure_ascii=False) + "\n"
        with self.historico_lock:
            try:
                with open(self.arquivo_historico, "a", encoding="utf-8") as f:
                    f.write(linha_json)
                    f.flush()  # Força o sistema operacional a gravar no disco imediatamente
            except OSError as exc:
                LOG.error("Falha ao gravar no historico.json: %s", exc)

    def _obter_historico_grupo(self, group_name: str, limite: int = 50) -> list[dict[str, Any]]:
        """Recupera do disco as últimas mensagens enviadas em um grupo específico."""
        mensagens: list[dict[str, Any]] = []
        with self.historico_lock:
            if not os.path.exists(self.arquivo_historico):
                return mensagens
            try:
                with open(self.arquivo_historico, "r", encoding="utf-8") as f:
                    for linha in f:
                        linha = linha.strip()
                        if not linha:
                            continue
                        try:
                            registro = json.loads(linha)
                            if registro.get("group_name") == group_name:
                                mensagens.append(registro)
                        except json.JSONDecodeError:
                            continue
            except OSError as exc:
                LOG.error("Falha ao ler historico.json: %s", exc)
        return mensagens[-limite:]

    def _configurar_keepalive_socket(self, sock: socket.socket) -> None:
        """
        Configura o TCP Keep-Alive em nível de Sistema Operacional.
        Fundamental para detectar queda física de cabo de rede ou desligamento abrupto.
        """
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        # desativa algoritmo de Nagle para menor latência em pacotes pequenos de texto
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        # Ajustes finos específicos para Kernel Linux (Ubuntu 22.04)
        # Se o cabo cair, detecta em ~11 segundos (5s ocioso + 3 tentativas de 2s)
        if hasattr(socket, "TCP_KEEPIDLE"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 5)
        if hasattr(socket, "TCP_KEEPINTVL"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 2)
        if hasattr(socket, "TCP_KEEPCNT"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)

    def iniciar(self) -> None:
        """Cria o socket passivo TCP, faz o bind na interface de rede e inicia escuta."""
        # AF_INET = Camada de Rede IPv4 | SOCK_STREAM = Camada de Transporte TCP
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Permite reusar a porta imediatamente após reiniciar o servidor (evita erro TIME_WAIT)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(50)
        self.server_socket.settimeout(0.5)  # Timeout curto para permitir captura de Ctrl+C

        ip_real, porta_real = self.server_socket.getsockname()
        LOG.info("Servidor TCP iniciado em %s:%s (Aguardando conexões...)", ip_real, porta_real)

    def loop_principal(self) -> None:
        """Loop de aceitação de múltiplos clientes (Multiplexação por Threads)."""
        if self.server_socket is None:
            self.iniciar()
        assert self.server_socket is not None

        contador_threads = 0
        while not self.evento_parada.is_set():
            try:
                client_sock, client_addr = self.server_socket.accept()
            except socket.timeout:
                continue
            except OSError:
                if self.evento_parada.is_set():
                    break
                raise

            # Aplica Keep-Alive e TCP_NODELAY no novo socket do cliente
            self._configurar_keepalive_socket(client_sock)

            cliente = ClienteConectado(client_sock, client_addr)
            contador_threads += 1
            nome_thread = f"Cliente-{contador_threads}"

            # Cria uma Thread dedicada para tratar o fluxo de bytes deste cliente
            t = threading.Thread(target=self._tratar_cliente, args=(cliente,), name=nome_thread, daemon=True)
            t.start()

    def parar(self) -> None:
        """Encerra o servidor e fecha todos os sockets de clientes de forma limpa."""
        self.evento_parada.set()
        if self.server_socket:
            try:
                self.server_socket.close()
            except OSError:
                pass
        with self.estado_lock:
            clientes_ativos = list(self.clientes.values())
        for cli in clientes_ativos:
            cli.fechar_conexao()
        LOG.info("Servidor encerrado com segurança.")

    # =========================================================================
    # CAMADA DE RECEPÇÃO DE BYTES (ENQUADRAMENTO MANUAL DE FLUXO TCP)
    # =========================================================================
    def _tratar_cliente(self, cliente: ClienteConectado) -> None:
        LOG.info("Nova conexão TCP estabelecida de %s:%s", *cliente.endereco)
        buffer_recepcao = bytearray()

        try:
            while not self.evento_parada.is_set():
                # Leitura direta de baixo nível da pilha TCP do S.O.
                bloco = cliente.sock.recv(READ_CHUNK_SIZE)
                if not bloco:
                    # recv() retornando 0 bytes significa que o cliente enviou um pacote FIN (EOF)
                    LOG.info("Cliente %s (%s) encerrou a conexão (EOF).", cliente.username, cliente.endereco)
                    break

                buffer_recepcao.extend(bloco)

                # Proteção contra ataque de exaustão de memória (quadro sem \n gigante)
                if len(buffer_recepcao) > MAX_BUFFER_SIZE and b"\n" not in buffer_recepcao:
                    self._enviar_erro(cliente, 413, "FRAME_TOO_LARGE", "Mensagem excedeu o limite de 16KB.")
                    break

                # Processa todos os quadros completos (delimitados por \n) presentes no buffer
                while b"\n" in buffer_recepcao:
                    indice_lf = buffer_recepcao.index(b"\n")
                    quadro_bruto = bytes(buffer_recepcao[:indice_lf])
                    # Remove do buffer os bytes já extraídos (incluindo o \n)
                    del buffer_recepcao[: indice_lf + 1]

                    if len(quadro_bruto) > MAX_BUFFER_SIZE:
                        self._enviar_erro(cliente, 413, "FRAME_TOO_LARGE", "Quadro excedeu 16KB.")
                        return

                    if not quadro_bruto.strip():
                        continue  # Ignora linhas em branco vazias

                    self._decodificar_e_rotear(cliente, quadro_bruto)

        except (ConnectionResetError, TimeoutError, BrokenPipeError, OSError) as erro_rede:
            # Captura queda abrupta (ex: cabo de rede desconectado ou falha de Keep-Alive)
            LOG.warning("Queda abrupta de conexão com %s (%s): %s", cliente.username, cliente.endereco, erro_rede)
        finally:
            self._desconectar_e_limpar(cliente)

    def _decodificar_e_rotear(self, cliente: ClienteConectado, quadro_bruto: bytes) -> None:
        """Converte os bytes recebidos em JSON e valida a estrutura básica do protocolo."""
        try:
            texto_utf8 = quadro_bruto.decode("utf-8")
            requisicao = json.loads(texto_utf8)
        except UnicodeDecodeError:
            self._enviar_erro(cliente, 400, "INVALID_ENCODING", "Codificação deve ser UTF-8.")
            return
        except json.JSONDecodeError:
            self._enviar_erro(cliente, 400, "INVALID_JSON", "Formato JSON corrompido ou inválido.")
            return

        if not isinstance(requisicao, dict):
            self._enviar_erro(cliente, 400, "INVALID_FORMAT", "A mensagem deve ser um objeto JSON.")
            return

        acao = requisicao.get("action")
        if not isinstance(acao, str) or not acao:
            self._enviar_erro(cliente, 400, "MISSING_ACTION", "O atributo 'action' (string) é obrigatório.")
            return

        self._executar_acao_protocolo(cliente, acao.upper(), requisicao)

    # =========================================================================
    # LÓGICA DO PROTOCOLO DE APLICAÇÃO E GERENCIAMENTO DE ESTADO
    # =========================================================================
    def _executar_acao_protocolo(self, cliente: ClienteConectado, acao: str, req: dict[str, Any]) -> None:
        # Se o cliente ainda não fez LOGIN, só pode executar LOGIN ou PING
        if cliente.username is None and acao not in ("LOGIN", "PING"):
            self._enviar_erro(cliente, 401, "AUTH_REQUIRED", "Faça LOGIN antes de enviar comandos.")
            return

        if acao == "LOGIN":
            self._acao_login(cliente, req)
        elif acao == "MSG_PRIV":
            self._acao_msg_privada(cliente, req)
        elif acao == "CREATE_GRP":
            self._acao_criar_grupo(cliente, req)
        elif acao == "JOIN_GRP":
            self._acao_entrar_grupo(cliente, req)
        elif acao == "LEAVE_GRP":
            self._acao_sair_grupo(cliente, req)
        elif acao == "MSG_GRP":
            self._acao_msg_grupo(cliente, req)
        elif acao == "LIST":
            self._acao_listar_estado(cliente)
        elif acao == "PING":
            cliente.enviar_quadro({"status": "OK", "action_reply": "PING", "type": "PONG", "timestamp": timestamp_atual()})
        elif acao == "DISCONNECT":
            cliente.enviar_quadro({"status": "OK", "action_reply": "DISCONNECT", "message": "Desconectando..."})
            cliente.fechar_conexao()
        else:
            self._enviar_erro(cliente, 400, "UNKNOWN_ACTION", f"Ação '{acao}' desconhecida no protocolo.")

    def _acao_login(self, cliente: ClienteConectado, req: dict[str, Any]) -> None:
        if cliente.username is not None:
            self._enviar_erro(cliente, 400, "ALREADY_LOGGED", f"Você já está identificado como '{cliente.username}'.")
            return

        raw_user = req.get("username")
        if not isinstance(raw_user, str) or not USER_REGEX.match(raw_user.strip()):
            self._enviar_erro(
                cliente, 400, "INVALID_USERNAME", "Nome de usuário inválido (use 3 a 20 letras, números ou '_')."
            )
            return

        username = raw_user.strip().lower()

        with self.estado_lock:
            if username in self.clientes:
                self._enviar_erro(cliente, 409, "USER_EXISTS", f"O usuário '{username}' já está conectado.")
                return
            cliente.username = username
            self.clientes[username] = cliente

        LOG.info("Usuário '%s' autenticado com sucesso (%s:%s).", username, *cliente.endereco)
        cliente.enviar_quadro({
            "status": "OK",
            "action_reply": "LOGIN",
            "username": username,
            "message": f"Conectado com sucesso como '{username}'.",
            "timestamp": timestamp_atual(),
        })
        self._notificar_presenca(f"Usuário '{username}' conectou-se ao servidor.", excluir=username)

    def _acao_msg_privada(self, cliente: ClienteConectado, req: dict[str, Any]) -> None:
        assert cliente.username is not None
        alvo = req.get("target")
        conteudo = req.get("content")

        if not isinstance(alvo, str) or not alvo.strip():
            self._enviar_erro(cliente, 400, "INVALID_TARGET", "Informe o destinatário no atributo 'target'.")
            return
        alvo = alvo.strip().lower()

        if alvo == cliente.username:
            self._enviar_erro(cliente, 400, "SELF_MESSAGE", "Não é permitido enviar mensagem privada para si mesmo.")
            return

        if not isinstance(conteudo, str) or not conteudo.strip() or len(conteudo) > MAX_MSG_LENGTH:
            self._enviar_erro(cliente, 400, "INVALID_CONTENT", "Conteúdo inválido ou excede 2000 caracteres.")
            return

        with self.estado_lock:
            destinatario = self.clientes.get(alvo)

        if destinatario is None:
            self._enviar_erro(cliente, 404, "USER_NOT_FOUND", f"Usuário '{alvo}' não existe ou está offline.")
            return

        pacote_msg = {
            "type": "MSG_PRIV",
            "from": cliente.username,
            "target": alvo,
            "content": conteudo,
            "timestamp": timestamp_atual(),
        }

        if destinatario.enviar_quadro(pacote_msg):
            cliente.enviar_quadro({
                "status": "OK",
                "action_reply": "MSG_PRIV",
                "target": alvo,
                "message": "Mensagem privada entregue.",
                "timestamp": pacote_msg["timestamp"],
            })
        else:
            self._enviar_erro(cliente, 500, "DELIVERY_ERROR", f"Falha de rede ao entregar para '{alvo}'.")

    def _acao_criar_grupo(self, cliente: ClienteConectado, req: dict[str, Any]) -> None:
        assert cliente.username is not None
        raw_grp = req.get("group_name")
        if not isinstance(raw_grp, str) or not GROUP_REGEX.match(raw_grp.strip()):
            self._enviar_erro(
                cliente, 400, "INVALID_GROUP", "Nome de grupo inválido (use 3 a 25 letras, números, '_' ou '-')."
            )
            return

        group_name = raw_grp.strip().lower()

        with self.estado_lock:
            if group_name in self.grupos:
                self._enviar_erro(cliente, 409, "GROUP_EXISTS", f"O grupo '{group_name}' já existe.")
                return
            # Cria o grupo e já adiciona o criador como membro
            self.grupos[group_name] = {cliente.username}

        LOG.info("Grupo '%s' criado por '%s'.", group_name, cliente.username)
        cliente.enviar_quadro({
            "status": "OK",
            "action_reply": "CREATE_GRP",
            "group_name": group_name,
            "message": f"Grupo '{group_name}' criado com sucesso e você foi adicionado.",
            "timestamp": timestamp_atual(),
        })

    def _acao_entrar_grupo(self, cliente: ClienteConectado, req: dict[str, Any]) -> None:
        assert cliente.username is not None
        raw_grp = req.get("group_name")
        if not isinstance(raw_grp, str) or not raw_grp.strip():
            self._enviar_erro(cliente, 400, "INVALID_GROUP", "Informe 'group_name'.")
            return

        group_name = raw_grp.strip().lower()

        with self.estado_lock:
            if group_name not in self.grupos:
                self._enviar_erro(cliente, 404, "GROUP_NOT_FOUND", f"Grupo '{group_name}' não encontrado.")
                return
            if cliente.username in self.grupos[group_name]:
                self._enviar_erro(cliente, 400, "ALREADY_IN_GROUP", f"Você já faz parte do grupo '{group_name}'.")
                return
            self.grupos[group_name].add(cliente.username)
            membros_atuais = [self.clientes[u] for u in self.grupos[group_name] if u in self.clientes and u != cliente.username]

        cliente.enviar_quadro({
            "status": "OK",
            "action_reply": "JOIN_GRP",
            "group_name": group_name,
            "message": f"Você entrou no grupo '{group_name}'.",
            "timestamp": timestamp_atual(),
        })

        # Avise os outros membros do grupo que alguém entrou
        aviso_entrada = {
            "type": "SYS_INFO",
            "group_name": group_name,
            "message": f"'{cliente.username}' entrou no grupo '{group_name}'.",
            "timestamp": timestamp_atual(),
        }
        for membro in membros_atuais:
            membro.enviar_quadro(aviso_entrada)

        # ENVIO DO HISTÓRICO PERSISTENTE (historico.json) PARA O NOVO MEMBRO
        historico = self._obter_historico_grupo(group_name)
        if historico:
            cliente.enviar_quadro({
                "type": "HISTORY_SYNC",
                "group_name": group_name,
                "messages": historico,
                "message": f"Carregadas {len(historico)} mensagem(ns) anterior(es) do grupo '{group_name}'.",
            })

    def _acao_sair_grupo(self, cliente: ClienteConectado, req: dict[str, Any]) -> None:
        assert cliente.username is not None
        raw_grp = req.get("group_name")
        if not isinstance(raw_grp, str) or not raw_grp.strip():
            self._enviar_erro(cliente, 400, "INVALID_GROUP", "Informe 'group_name'.")
            return

        group_name = raw_grp.strip().lower()

        with self.estado_lock:
            if group_name not in self.grupos:
                self._enviar_erro(cliente, 404, "GROUP_NOT_FOUND", f"Grupo '{group_name}' não encontrado.")
                return
            if cliente.username not in self.grupos[group_name]:
                self._enviar_erro(cliente, 400, "NOT_IN_GROUP", f"Você não pertence ao grupo '{group_name}'.")
                return
            self.grupos[group_name].remove(cliente.username)
            membros_restantes = [self.clientes[u] for u in self.grupos[group_name] if u in self.clientes]

        cliente.enviar_quadro({
            "status": "OK",
            "action_reply": "LEAVE_GRP",
            "group_name": group_name,
            "message": f"Você saiu do grupo '{group_name}'.",
            "timestamp": timestamp_atual(),
        })

        aviso_saida = {
            "type": "SYS_INFO",
            "group_name": group_name,
            "message": f"'{cliente.username}' saiu do grupo '{group_name}'.",
            "timestamp": timestamp_atual(),
        }
        for membro in membros_restantes:
            membro.enviar_quadro(aviso_saida)

    def _acao_msg_grupo(self, cliente: ClienteConectado, req: dict[str, Any]) -> None:
        assert cliente.username is not None
        raw_grp = req.get("group_name")
        conteudo = req.get("content")

        if not isinstance(raw_grp, str) or not raw_grp.strip():
            self._enviar_erro(cliente, 400, "INVALID_GROUP", "Informe 'group_name'.")
            return
        group_name = raw_grp.strip().lower()

        if not isinstance(conteudo, str) or not conteudo.strip() or len(conteudo) > MAX_MSG_LENGTH:
            self._enviar_erro(cliente, 400, "INVALID_CONTENT", "Conteúdo inválido ou excede 2000 caracteres.")
            return

        with self.estado_lock:
            if group_name not in self.grupos:
                self._enviar_erro(cliente, 404, "GROUP_NOT_FOUND", f"Grupo '{group_name}' inexistente.")
                return
            if cliente.username not in self.grupos[group_name]:
                self._enviar_erro(
                    cliente, 403, "NOT_IN_GROUP", f"Você precisa entrar no grupo '{group_name}' antes de enviar mensagens."
                )
                return
            destinatarios = [
                self.clientes[u] for u in self.grupos[group_name] if u in self.clientes and u != cliente.username
            ]

        evento_grupo = {
            "type": "MSG_GRP",
            "group_name": group_name,
            "from": cliente.username,
            "content": conteudo,
            "timestamp": timestamp_atual(),
        }

        # 1. Salva no arquivo historico.json em disco
        self._salvar_mensagem_historico(evento_grupo)

        # 2. Retransmite para todos os membros online do grupo
        for membro in destinatarios:
            membro.enviar_quadro(evento_grupo)

        # 3. Confirma envio ao remetente
        cliente.enviar_quadro({
            "status": "OK",
            "action_reply": "MSG_GRP",
            "group_name": group_name,
            "message": "Mensagem enviada ao grupo.",
            "timestamp": evento_grupo["timestamp"],
        })

    def _acao_listar_estado(self, cliente: ClienteConectado) -> None:
        """Retorna a lista de usuários online e os grupos disponíveis no servidor."""
        with self.estado_lock:
            usuarios_online = sorted(self.clientes.keys())
            info_grupos = {g: sorted(membros) for g, membros in sorted(self.grupos.items())}

        cliente.enviar_quadro({
            "status": "OK",
            "action_reply": "LIST",
            "online_users": usuarios_online,
            "groups": info_grupos,
            "timestamp": timestamp_atual(),
        })

    def _enviar_erro(self, cliente: ClienteConectado, codigo_http_like: int, error_key: str, detalhe: str) -> None:
        cliente.enviar_quadro({
            "status": "ERROR",
            "code": codigo_http_like,
            "error": error_key,
            "message": detalhe,
            "timestamp": timestamp_atual(),
        })

    def _notificar_presenca(self, texto: str, excluir: str | None = None) -> None:
        """Envia notificação de sistema para os usuários logados."""
        with self.estado_lock:
            alvos = [cli for user, cli in self.clientes.items() if user != excluir]
        pacote = {"type": "SYS_INFO", "message": texto, "timestamp": timestamp_atual()}
        for cli in alvos:
            cli.enviar_quadro(pacote)

    def _desconectar_e_limpar(self, cliente: ClienteConectado) -> None:
        """Remove o usuário dos dicionários de estado e fecha o socket."""
        usuario_removido = None
        grupos_afetados: list[ClienteConectado] = []

        with self.estado_lock:
            if cliente.username and self.clientes.get(cliente.username) is cliente:
                usuario_removido = cliente.username
                del self.clientes[usuario_removido]
                # Remove o usuário desconectado das listas de presença ativa dos grupos
                for nome_grp, membros in self.grupos.items():
                    if usuario_removido in membros:
                        membros.discard(usuario_removido)
                        for m in membros:
                            if m in self.clientes:
                                grupos_afetados.append(self.clientes[m])

        cliente.fechar_conexao()

        if usuario_removido:
            LOG.info("Estado limpo para o usuário desconectado: '%s'.", usuario_removido)
            self._notificar_presenca(f"Usuário '{usuario_removido}' desconectou-se.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Servidor TCP de Mensagens (Sockets Nativos)")
    parser.add_argument("--host", default="0.0.0.0", help="Endereço IP de bind (padrão: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=5050, help="Porta TCP (padrão: 5050)")
    args = parser.parse_args()

    servidor = ServidorMensagensTCP(host=args.host, port=args.port)
    try:
        servidor.iniciar()
        servidor.loop_principal()
    except KeyboardInterrupt:
        print("\n[!] Interrupção via teclado (Ctrl+C) detectada. Desligando servidor...")
    finally:
        servidor.parar()


if __name__ == "__main__":
    main()
