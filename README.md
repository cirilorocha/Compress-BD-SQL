# Compress-BD-SQL



A compressão de bancos de dados é uma técnica que traz diversos benefícios, como :

* Menor utilização do espaço em disco
* Menor espaço utilizado pelo backup
* Menor tempo de gravação do backup (devido ao consequente menor uso de disco em leituras e gravações)
* Menos acessos ao disco são necessários por conta do volume menor e pelo ganho com mais dados em cache/memória
* Menor consumo de memória RAM, ou na mesma memória permite que mais dados fiquem em cache otimizando o tempo de resposta. Os dados na memória RAM / Cache também ficam compactados e consequentemente reduz e otimiza seu uso
* Melhoria no tempo de resposta com mais dados disponíveis na memória e menos acessos ao disco
* Menor tempo de restauração de backup (menos tráfego de IO é necessário durante o processo)

Em contrapartida existem um maior custo de CPU, principalmente nas operações de atualização ou inclusão de dados, e um pouco também nas leituras para efetuar a descompressão. Com processadores de múltiplos núcleos e cada vez mais velozes esse impacto é bastante minimizado pelo paralelismo e principalmente com os substanciais ganhos no uso de disco e memória.



# Scripts

Scripts para análise do ganho com compressão de dados do MS Sql e compressão do banco de dados. São basicamente 3 scripts: 

* [Verificar Ganhos de Compressão](https://github.com/cirilorocha/Compress-BD-SQL/blob/main/Verificar%20Ganhos%20Compress%C3%A3o%20Banco.sql)

* [Análise da Compressão](https://github.com/cirilorocha/Compress-BD-SQL/blob/main/compress%C3%A3o%20Modelo%20An%C3%A1lise.ods)

* [Executar Compressão do Banco](https://github.com/cirilorocha/Compress-BD-SQL/blob/main/Compactar%20Banco%20de%20Dados%20(CX).sql)

* [Executar Descompressão do Banco](https://github.com/cirilorocha/Compress-BD-SQL/blob/main/DesCompactar%20Banco%20de%20Dados.sql)
  
  

## Verificar Ganhos de Compressão

Este [script](https://github.com/cirilorocha/Compress-BD-SQL/blob/main/Verificar%20Ganhos%20Compress%C3%A3o%20Banco.sql) tem a finalidade de simular a compressão do banco de dados usando a procedure padrão do SQL sp_estimate_data_compression_savings. Baseado no percentual de compressão dos dados (tabela e indices são verificados invididualemente) é usado o seguinte critério para indicar qual tipo de compressão será aplicada:

* Tabela pequena (menor que 100kb): **NÃO comprime**
* Compressão Geral (ROW ou PAGE) menor que 25%: **NÃO comprime**
* Compressão ROW maior que 25%, Compressão PAGE maior 25% que ROW e Compressão Page maior que 40%: **Compresão Page**
* Demais casos: **Compressão ROW**

É feita também uma reavaliação das tabelas pequenas, pode ocorrer do sistema criar novas tabelas vazias já com compressão e não compensa mantê-las dessa forma, apenas quando houver um volume maior de dados (pelo menos 100kb no algoritmo), após esse limite é verificada a taxa de compressão da tabela. Assim mantenho as tabelas muito pequenas ou vazias sem compressão, porque geram muitos dados inconsistentes de compressão com uma amostragem tão pequena.

Após diversos testes de performance VS ganho de compressão cheguei a essa estatística que uma compressão menor que 25% não compensa o custo adicional da compressão. Uma compressão PAGE muito próxima da compressão ROW (25 pontos percentuais) também **NÃO** compensa o custo computacional, melhor manter apenas ROW. Como a compressão PAGE é muito mais onerosa computacionalmente ajusto para somente usá-la caso os ganhos seja mais expressivos (maior que 40%).

Usando esses critérios consegui um excelente equilíbrio entre quanto vai custar fazer essa compressão e os ganhos efetivos de armazenamento. Sendo assim para objetos com baixa compressão são armazenados descomprimidos, e os demais objetos utilizam a compressão mais adequada ao seu custo computacional (25% pelo menos para ROW e 40% para PAGE).

Nesse cenário e com o banco de dados Protheus consegui um **ganho de compressão na faixa de  60%**, sendo que apenas comprimindo tudo com **ROW*** ficaria em torno de **22%** e se utilizasse **PAGE** fica em **66%**. Entendo que abrindo mão de um pequeno percentual de compressão 6% existe um ganho razoável de performance nesses objetos que não se beneficiam da compressão.

Este script pode ser executar também após a compressão do banco para que seja reavaliada a compressão das tabelas após algum tempo de uso, e mesmo para verificar novas tabelas que tenham sido criadas depois da compressão inicial. Ele refaz as estimativas sem compressão, e com cada tipo de compressão. Depois que os dados estão atualizados basta executar o [script](https://github.com/cirilorocha/Compress-BD-SQL/blob/main/Compactar%20Banco%20de%20Dados%20(CX).sql) de compressão novamente, este irá apenas alterar o padrão de compressão das tabelas necesárias, as demais serão ignoradas.

A execução de instancias do script de veficação em paralelo vai gerar alguns erros, devido ao fato de que a procedure do SQL, que faz as estimativas de compressão, de alguma forma compartilha dados e eventualmente ocorrem erros entre os scripts que estão em execução. Por isso também que o script não foi otimizado para execução paralela o que diminuiria bastante o tempo de execução.

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

Este script pode ser executado novamente após um tempo de uso do banco de dados após uma nova etapa de análise. Desta forma ele processa apenas as tabelas que foram elencadas como necessária alteração no tipo de compressão.

## Descompressão do Banco



Caso os resultados não seja satisfatórios após a compressão do banco de dados (com o meu script ou qualquer outra forma), esse [script](https://github.com/cirilorocha/Compress-BD-SQL/blob/main/DesCompactar%20Banco%20de%20Dados.sql) permite desfazer a compressão de todas tabelas e seus índices.

De forma semelhante ao algoritmo de compressão são efetuadas diversas operações visando a integridade dos dados:



* Descompressão dos dados

* Desfragmentação das tabelas

* Verificação de Integridade

* Redução/Shrink do banco de dados
  
  

De forma identica ao processo de compressão são exibidas mensagens informando todo o progresso do script.



## Configuração Adicional no DbAccess

Após esse processo de compressão é altamente recomendado deixar o DbAccess configurado para fazer a compressão de novos dados e tabelas de forma automática. Para tal basta incluir a configuração **compression=2** no arquivo INI de configuração dele. É recomendável fazer uma reavaliação da compressão das novas tabelas criadas usando o primeiro [script](https://github.com/cirilorocha/Compress-BD-SQL/blob/main/Verificar%20Ganhos%20Compress%C3%A3o%20Banco.sql) periodicamente de forma a sempre manter as tabelas criadas o melhor método de compressão.
