# Compress-BD-SQL

Scripts para análise do ganho com compressão de dados do MS Sql e compressão do banco de dados. São basicamente 3 scripts: 

* [Verificar Ganhos de Compressão](https://github.com/cirilorocha/Compress-BD-SQL/blob/main/Verificar%20Ganhos%20Compress%C3%A3o%20Banco.sql)

* [Análise da Compressão](https://github.com/cirilorocha/Compress-BD-SQL/blob/main/compress%C3%A3o%20Modelo%20An%C3%A1lise.ods)

* [Executar Compressão do Banco](https://github.com/cirilorocha/Compress-BD-SQL/blob/main/Compactar%20Banco%20de%20Dados%20(CX).sql)

* [Executar Descompressão do Banco](https://github.com/cirilorocha/Compress-BD-SQL/blob/main/DesCompactar%20Banco%20de%20Dados.sql)
  
  

## Verificar Ganhos de Compressão

Este [script](https://github.com/cirilorocha/Compress-BD-SQL/blob/main/Verificar%20Ganhos%20Compress%C3%A3o%20Banco.sql) tem a finalidade de simular a compressão do banco de dados usando a procedure padrão do SQL sp_estimate_data_compression_savings. Baseado no percentual de compressão dos dados (tabela e indices são verificados invididualemente) é usado o seguinte critério para indicar qual tipo de compressão será aplicada:

* Compressão Geral (ROW ou PAGE) menor que 25%: **NÃO comprime**
* Compressão ROW maior que 25%, Compressão PAGE maior 25% que ROW e Compressão Page maior que 40%: **Compresão Page**
* Demais casos: **Compressão ROW**

Após diversos testes de performance VS ganho de compressão cheguei a essa estatística que uma compressão menor que 25% não compensa o custo adicional da compressão. Uma compressão PAGE muito próxima da compressão ROW (25 pontos percentuais) também **NÃO** compensa o custo computacional, melhor manter apenas ROW. Como a compressão PAGE é muito mais onerosa computacionalmente ajusto para somente usá-la caso os ganhos seja mais expressivos (maior que 40%).

Usando esses critérios consegui um excelente equilíbrio entre quanto vai custar fazer essa compressão e os ganhos efetivos de armazenamento. Sendo assim para objetos com baixa compressão são armazenados descomprimidos, e os demais objetos utilizam a compressão mais adequada ao seu custo computacional (25% pelo menos para ROW e 40% para PAGE).

Nesse cenário e com o banco de dados Protheus consegui um **ganho de compressão na faixa de  60%**, sendo que apenas comprimindo tudo com **ROW*** ficaria em torno de **22%** e se utilizasse **PAGE** fica em **66%**. Entendo que abrindo mão de um pequeno percentual de compressão 6% existe um ganho razoável de performance nesses objetos que não se beneficiam da compressão.



## Análise dos Dados de Compressão

O script gera a tabela CX_COMPRESS com os dados do processo para análise. Esses dados podem ser analisados usando a [planilha](https://github.com/cirilorocha/Compress-BD-SQL/blob/main/compress%C3%A3o%20Modelo%20An%C3%A1lise.ods) que deixei em anexo, bastando fazer uma consulta simples a tabela, copiar os dados e colar na planilha. Nela serão exibidos todos os dados compilados de forma a mostrar quanto de ganho com cada algoritmo e comparando com os valores sugeridos com a regra descrita acima.



## Compressão do Banco

Este [script](https://github.com/cirilorocha/Compress-BD-SQL/blob/main/Compactar%20Banco%20de%20Dados%20(CX).sql) executa efetivamente a compressão das tabelas e índices conforme foi gravado na tabela de análise CX_COMPRESS, ele somente pode ser executado caso esta tabela exista, porque todo o processamento é baseado nela.

Ele executa diversos passos para efetuar a compressão e garantir a sua integridade:



* Compressão das tabelas e índices

* Redução/Shrink do banco de dados

* Desfragmentação dos dados

* Verificação de integridade



Ainda tenho uma pendência aqui porque essa desfragmentação não está sendo efetiva, infelizmente fica essa pendência aqui. Apesar de usar o mesmo algoritmo que uso de forma segregada e funciona, dentro deste script o mesmo não surte efeito, terminando o processo sem um resultado esperado, as tabelas ainda continuam com um alto índice de fragmentação, sendo necessário executar novamente uma etapa de desfragmentação. **Por este motivo o parâmetro que controla esta operação está 100% para ignorar esse processamento.**

Um recurso interessante que adicionei foi o acompanhamento de todo o processo através das mensagens durante todo o processo, mostrando cada etapa com o percentual da etapa atual.

## Descompressão do Banco



Caso os resultados não seja satisfatórios após a compressão do banco de dados (com o meu script ou qualquer outra forma), esse [script](https://github.com/cirilorocha/Compress-BD-SQL/blob/main/DesCompactar%20Banco%20de%20Dados.sql) permite desfazer a compressão de todas tabelas e seus índices.

De forma semelhante ao algoritmo de compressão são efetuadas diversas operações visando a integridade dos dados:



* Descompressão dos dados

* Desfragmentação das tabelas

* Verificação de Integridade

* Redução/Shrink do banco de dados



De forma identica ao processo de compressão são exibidas mensagens informando todo o progresso do script.
