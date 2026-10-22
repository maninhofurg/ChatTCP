# ChatTCP - Serviço de Troca de Mensagens em Baixo Nível

Este projeto é a implementação da opção **1.1 (Serviço de troca de mensagens)** para a disciplina de Redes de Computadores. Trata-se de uma aplicação multicliente desenvolvida puramente com sockets nativos, rodando via terminal (CLI), que suporta mensagens privadas, gerenciamento de grupos, histórico persistente e resiliência contra desconexões abruptas de rede.

## 1. Integrantes do Grupo

* **Alessandro Goldas da Cruz** - 166600
* **Miriã do Amaral Evangelista** - 166627
* **Markgregor Alves Altê** - 169518
* **Cintia Gonzaga Gonçalves** - 162561

## 2. Linguagem e Bibliotecas Utilizadas

* **Linguagem:** Python 3.10+
* **Bibliotecas Externas (Frameworks):** NENHUMA.
* **Bibliotecas Nativas Utilizadas:** 
  * `socket`: Para criação da infraestrutura de rede em baixo nível e manipulação do fluxo de bytes.
  * `threading`: Para multiplexação de I/O, permitindo a concorrência de múltiplos clientes conectados simultaneamente ao servidor.
  * `json`: Para a serialização e desserialização estruturada do nosso protocolo de aplicação.
  * `os`, `re`, `argparse`, `datetime`: Funcionalidades auxiliares de formatação, regex e manipulação do arquivo de histórico.

## 3. Arquitetura e Protocolo de Transporte

* **Arquitetura Escolhida:** Cliente-Servidor.
  * **Justificativa:** A centralização do controle em um servidor facilita o gerenciamento do estado global da aplicação (como descobrir quem está online e as listas de membros dos grupos), além de simplificar a implementação da persistência em disco do histórico de mensagens.
* **Protocolo de Transporte Escolhido:** TCP (Transmission Control Protocol).
  * **Justificativa:** Optamos pelo TCP devido à necessidade de uma entrega confiável, sem perdas e com garantia de ordenação dos segmentos. Diferente do streaming de mídia onde perdas são toleráveis, um serviço de chat de texto exige integridade total da comunicação. O TCP também lida nativamente com o controle de fluxo e congestionamento da rede.

## 4. Instruções de Execução e Dependências

Não há necessidade de instalar pacotes via `pip`. O projeto utiliza estritamente a biblioteca padrão do Python.

**Passo 1: Iniciando o Servidor**
Em um terminal, inicie o servidor TCP informando a porta desejada (o padrão é 5050):
```bash
python3 server.py --port 5050

Passo 2: Iniciando os Clientes
Em outros terminais (na mesma máquina ou em computadores diferentes na mesma rede), inicie os clientes apontando para o IP do servidor:
Bash

# Se o servidor estiver na mesma máquina:
python3 client.py --host 127.0.0.1 --port 5050 --user seu_nome

# Se o servidor estiver em outro computador na rede local (ex: Wi-Fi Hotspot):
python3 client.py --host 192.168.x.x --port 5050 --user seu_nome

5. Instruções para Testar a Aplicação

A aplicação rodará inteiramente via prompt de comando. Para demonstrar todas as funcionalidades exigidas, sugerimos o seguinte roteiro:

    Login Simultâneo: Conecte três clientes diferentes (alice, bob, cintia) usando o script client.py.

    Mensagem Privada: No terminal de alice, digite /priv bob Ola Bob!. Verifique que a mensagem aparece apenas para bob, isolando cintia.

    Gerenciamento de Grupo:

        alice cria o grupo: /criar redes

        bob e cintia entram: /entrar redes

    Mensagem de Grupo e Histórico: bob envia uma mensagem ao grupo: /grupo redes Bom dia!. A mensagem será entregue aos três. Se um novo usuário entrar posteriormente no grupo, ele receberá a carga do histórico lida do arquivo historico.json.

    Tratamento de Desconexões (O "Teste do Cabo"): Feche o terminal de um cliente bruscamente (ou desconecte-o da rede Wi-Fi). O servidor detectará a quebra física via TCP Keep-Alive e tratamento de exceção de Socket, limpando as estruturas em memória e avisando os demais clientes da saída, sem corromper a execução principal.

6. Declaração sobre o Uso de Inteligência Artificial

Utilizamos ferramentas de IA (Gemini) como apoio ao desenvolvimento deste trabalho para os seguintes fins:

    Discutir alternativas de implementação para a leitura em baixo nível do buffer TCP e evitar o travamento de quadros por Head of Line Blocking.

    Auxílio na estruturação das Threads e na aplicação de travas de exclusão mútua (threading.Lock) para evitar condições de corrida na escrita do arquivo historico.json e nos sockets.

    Sugestões de testes práticos para forçar o tratamento de exceções de rede (ConnectionResetError e BrokenPipeError).

    Revisão gramatical e estrutural deste README e da especificação do protocolo de aplicação.

Todo o código, a lógica de enquadramento do TCP em bytes e a definição do protocolo foram exaustivamente revisados, compreendidos e testados pelos integrantes do grupo em nosso próprio laboratório de rede local. A responsabilidade pelas decisões arquiteturais e pelo código entregue é integralmente da equipe.
