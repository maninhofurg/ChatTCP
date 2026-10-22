#!/usr/bin/env python3
"""
Cliente TCP de Troca de Mensagens (CLI) - Redes de Computadores (FURG)
Implementação em baixo nível utilizando exclusivamente a API nativa de sockets.
Integrantes:
- Alessandro Goldas da Cruz (166600)
- Miriã do Amaral Evangelista (166627)
- Markgregor Alves Altê (169518)
- Cintia Gonzaga Gonçalves (162561)
"""

import argparse
import json
import socket
import sys
import threading
from datetime import datetime
from typing import Any

READ_CHUNK_SIZE = 4096
MAX_BUFFER_SIZE = 65536


def formatar_hora(iso_str: str | None) -> str:
    """Converte timestamp ISO 8601 para HH:MM:SS local."""
    if not iso_str:
        return "--:--:--"
    try:
        dt = datetime.fromisoformat(iso_str).astimezone()
        return dt.strftime("%H:%M:%S")
    except ValueError:
        return "--:--:--"


def empacotar_json(dados: dict[str, Any]) -> bytes:
    """Serializa dicionário para JSON compacto terminado em LF (\\n)."""
    return (json.dumps(dados, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")


class ClienteTerminalTCP:
    def __init__(self, host: str, port: int, username: str | None = None):
        self.host = host
        self.port = port
        self.username = username
        self.sock: socket.socket | None = None
        self.conectado = threading.Event()
        self.write_lock = threading.Lock()
        self.grupo_ativo: str | None = None

    def _configurar_socket(self, sock: socket.socket) -> None:
        """Ativa TCP Keep-Alive e desativa Nagle no socket do cliente."""
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if hasattr(socket, "TCP_KEEPIDLE"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 5)
        if hasattr(socket, "TCP_KEEPINTVL"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 2)
        if hasattr(socket, "TCP_KEEPCNT"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)

    def conectar(self) -> bool:
        """Estabelece a conexão TCP (3-way handshake) com o servidor."""
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._configurar_socket(self.sock)
            self.sock.settimeout(5.0)
            self.sock.connect((self.host, self.port))
            self.sock.settimeout(None)  # Volta para modo bloqueante padrão para a thread de leitura
            self.conectado.set()
            print(f"[*] Conectado via TCP ao servidor {self.host}:{self.port}")
            return True
        except OSError as erro:
            print(f"[!] Falha ao conectar em {self.host}:{self.port} -> {erro}")
            return False

    def enviar_pacote(self, payload: dict[str, Any]) -> bool:
        """Envia um quadro JSON completo usando sendall() protegido por Lock."""
        if not self.conectado.is_set() or self.sock is None:
            print("[!] Você está desconectado.")
            return False
        try:
            dados = empacotar_json(payload)
            with self.write_lock:
                self.sock.sendall(dados)
            return True
        except OSError as erro:
            print(f"\n[!] Erro ao enviar dados pelo socket: {erro}")
            self.fechar()
            return False

    def enviar_bytes_brutos(self, texto_bruto: str) -> None:
        """Envia string arbitrária na rede (útil para demonstrar tratamento de erro no servidor)."""
        if not self.conectado.is_set() or self.sock is None:
            return
        try:
            with self.write_lock:
                self.sock.sendall((texto_bruto + "\n").encode("utf-8"))
        except OSError as erro:
            print(f"\n[!] Erro ao enviar bytes brutos: {erro}")

    def fechar(self) -> None:
        """Fecha o socket do cliente com segurança."""
        self.conectado.clear()
        if self.sock:
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                self.sock.close()
            except OSError:
                pass

    # =========================================================================
    # THREAD 1: ESCUTA CONTÍNUA DA REDE (RECV E FRAMING MANUAL)
    # =========================================================================
    def _thread_escutar_servidor(self) -> None:
        assert self.sock is not None
        buffer = bytearray()

        try:
            while self.conectado.is_set():
                bloco = self.sock.recv(READ_CHUNK_SIZE)
                if not bloco:
                    print("\n[!] O servidor encerrou a conexão (EOF recebido).")
                    break

                buffer.extend(bloco)
                if len(buffer) > MAX_BUFFER_SIZE and b"\n" not in buffer:
                    print("\n[!] Erro: Quadro recebido excede o limite de buffer.")
                    break

                while b"\n" in buffer:
                    idx = buffer.index(b"\n")
                    linha_bytes = bytes(buffer[:idx])
                    del buffer[: idx + 1]

                    if not linha_bytes.strip():
                        continue
                    self._processar_quadro_recebido(linha_bytes)

        except (ConnectionResetError, TimeoutError, BrokenPipeError, OSError) as erro:
            if self.conectado.is_set():
                print(f"\n[!] Conexão perdida abruptamente com o servidor (Cabo desconectado?): {erro}")
        finally:
            self.fechar()
            print("[*] Pressione ENTER para sair.")

    def _processar_quadro_recebido(self, linha_bytes: bytes) -> None:
        try:
            msg = json.loads(linha_bytes.decode("utf-8"))
        except Exception as erro:
            print(f"\n[!] Quadro inválido recebido: {erro}")
            return

        status = msg.get("status")
        tipo = msg.get("type")
        hora = formatar_hora(msg.get("timestamp"))

        if status == "OK":
            acao = msg.get("action_reply", "")
            if acao == "LIST":
                users = ", ".join(msg.get("online_users", []))
                grupos = msg.get("groups", {})
                print(f"\n--- ESTADO DO SERVIDOR ({hora}) ---")
                print(f"  Usuários Online : {users}")
                print(f"  Grupos Ativos   : {grupos if grupos else 'Nenhum'}")
                print("------------------------------------")
            elif tipo == "PONG":
                print(f"\n[PONG] Servidor respondeu ao PING às {hora}")
            else:
                print(f"\n[OK | {hora}] {msg.get('message', '')}")

        elif status == "ERROR":
            print(f"\n[ERRO {msg.get('code')} - {msg.get('error')}] {msg.get('message')}")

        elif tipo == "MSG_PRIV":
            remetente = msg.get("from")
            conteudo = msg.get("content")
            print(f"\n[{hora}] (PRIVADO de {remetente}): {conteudo}")

        elif tipo == "MSG_GRP":
            grupo = msg.get("group_name")
            remetente = msg.get("from")
            conteudo = msg.get("content")
            print(f"\n[{hora}] [Grupo: {grupo}] {remetente}: {conteudo}")

        elif tipo == "HISTORY_SYNC":
            grupo = msg.get("group_name")
            historico = msg.get("messages", [])
            print(f"\n=== HISTÓRICO DO GRUPO '{grupo}' ({len(historico)} mensagens) ===")
            for item in historico:
                h_hora = formatar_hora(item.get("timestamp"))
                print(f"  [{h_hora}] {item.get('from')}: {item.get('content')}")
            print("==========================================================")

        elif tipo == "SYS_INFO":
            print(f"\n[SISTEMA | {hora}] {msg.get('message')}")

        else:
            print(f"\n[PACOTE] {msg}")

    # =========================================================================
    # THREAD 2 (PRINCIPAL): LEITURA DO TECLADO E INTERPRETAÇÃO DE COMANDOS
    # =========================================================================
    def exibir_ajuda(self) -> None:
        print("\n================ COMANDOS DO CLIENTE TCP ================")
        print("  /login <usuario>            -> Identifica-se no servidor")
        print("  /lista                      -> Lista usuários online e grupos")
        print("  /priv <usuario> <mensagem>  -> Envia mensagem privada")
        print("  /criar <grupo>              -> Cria um novo grupo e entra nele")
        print("  /entrar <grupo>             -> Entra em um grupo (e carrega histórico)")
        print("  /sair <grupo>               -> Sai de um grupo")
        print("  /grupo <grupo> <mensagem>   -> Envia mensagem para um grupo")
        print("  /ping                       -> Testa latência/resposta do servidor")
        print("  /raw <texto_livre>          -> Envia bytes puros (testar erro no servidor)")
        print("  /quit                       -> Desconecta e fecha o programa")
        print("=========================================================\n")

    def iniciar_interface_terminal(self) -> None:
        if not self.conectar():
            return

        # Inicia a Thread de leitura em background
        t_leitura = threading.Thread(target=self._thread_escutar_servidor, name="Thread-Leitura", daemon=True)
        t_leitura.start()

        self.exibir_ajuda()

        # Se o usuário já passou --user na linha de comando, faz login automático
        if self.username:
            self.enviar_pacote({"action": "LOGIN", "username": self.username})

        try:
            while self.conectado.is_set():
                linha = sys.stdin.readline()
                if not linha or not self.conectado.is_set():
                    break
                comando = linha.strip()
                if not comando:
                    continue
                self._interpretar_entrada(comando)
        except KeyboardInterrupt:
            print("\n[*] Encerrando cliente...")
        finally:
            if self.conectado.is_set():
                self.enviar_pacote({"action": "DISCONNECT"})
            self.fechar()

    def _interpretar_entrada(self, entrada: str) -> None:
        if not entrada.startswith("/"):
            print("[!] Use comandos iniciados por '/' (digite /ajuda para ver a lista).")
            return

        partes = entrada.split(" ", 2)
        cmd = partes[0].lower()

        if cmd in ("/ajuda", "/help"):
            self.exibir_ajuda()

        elif cmd == "/login":
            if len(partes) < 2:
                print("[!] Uso: /login <nome_usuario>")
                return
            self.enviar_pacote({"action": "LOGIN", "username": partes[1]})

        elif cmd == "/lista":
            self.enviar_pacote({"action": "LIST"})

        elif cmd == "/priv":
            if len(partes) < 3:
                print("[!] Uso: /priv <destinatario> <mensagem>")
                return
            self.enviar_pacote({"action": "MSG_PRIV", "target": partes[1], "content": partes[2]})

        elif cmd == "/criar":
            if len(partes) < 2:
                print("[!] Uso: /criar <nome_grupo>")
                return
            self.enviar_pacote({"action": "CREATE_GRP", "group_name": partes[1]})

        elif cmd == "/entrar":
            if len(partes) < 2:
                print("[!] Uso: /entrar <nome_grupo>")
                return
            self.enviar_pacote({"action": "JOIN_GRP", "group_name": partes[1]})

        elif cmd == "/sair":
            if len(partes) < 2:
                print("[!] Uso: /sair <nome_grupo>")
                return
            self.enviar_pacote({"action": "LEAVE_GRP", "group_name": partes[1]})

        elif cmd == "/grupo":
            if len(partes) < 3:
                print("[!] Uso: /grupo <nome_grupo> <mensagem>")
                return
            self.enviar_pacote({"action": "MSG_GRP", "group_name": partes[1], "content": partes[2]})

        elif cmd == "/ping":
            self.enviar_pacote({"action": "PING"})

        elif cmd == "/raw":
            bruto = entrada[5:] if len(entrada) > 5 else "{json_invalido"
            print(f"[*] Enviando quadro bruto para teste de falha: {bruto}")
            self.enviar_bytes_brutos(bruto)

        elif cmd == "/quit":
            self.enviar_pacote({"action": "DISCONNECT"})
            self.fechar()

        else:
            print(f"[!] Comando '{cmd}' não reconhecido. Digite /ajuda.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Cliente TCP de Terminal (Redes de Computadores)")
    parser.add_argument("--host", default="127.0.0.1", help="IP do servidor (padrão: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=5050, help="Porta TCP do servidor (padrão: 5050)")
    parser.add_argument("--user", default=None, help="Nome de usuário para login automático (opcional)")
    args = parser.parse_args()

    cliente = ClienteTerminalTCP(host=args.host, port=args.port, username=args.user)
    cliente.iniciar_interface_terminal()


if __name__ == "__main__":
    main()
