# prova2mod9

## Descrição

Este repositório foi criado para a prova dois do módulo 9.  Para executar o sistema deve ser rodado o comando:

```bash
go run consumer.go
```

E o comando:

```bash
go run producer.go
```

O sistema logara no terminal do consumer 20 mensagens enviadas pelo producer. Segue aqui um vídeo do sistema funcionando: <https://youtu.be/WYEfWpIlbL4>


## Testes

Para executar os testes do sistema, basta rodar o comando:

```bash
go test *.goi -v
```

Temos um teste de integridade que verifica se uma mensagem publica é igual a recebida.