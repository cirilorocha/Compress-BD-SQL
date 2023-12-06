/*
 * VERSÃO 1.3
 * NÃO EXECUTAR ESSA QUERY EM PARALELO MESMO EM BANCOS DIFERENTES, O SQL SE PERDE E DÁ ERROS ALEATÓRIOS
 *
 * ESTA QUERY FAZ UMA ANÁLISE ENTRE AS COMPRESSÕES ROW E PAGE E FAZ UMA RECOMENDAÇÃO SOBRE QUAL COMPRESSÃO É INTERESSANTE
 * UTILIZAR, SEGUINDO O SEGUINTE CRITÉRIO, A COMPRESSÃO GERAL PRECISA SER MAIOR QUE 25% ROW OU 40% PAGE E SE PAGE FOR MAIOR
 * 25 PONTOS PERCENTUAIS EM RELAÇÃO A ROW ENTÃO RECOMENDA PAGE, SE A COMPRESSÃO FOR INFERIOR A 25% NÃO RECOMENDA COMPRIMIR
 * 
 * ESTA ROTINA IRÁ GERAR A TABELA CX_COMPRESS COM AS RECOMENDAÇÕES PARA SEREM USADAS NA COMPRESSÃO DO BANCO
 * 
 * EM ANÁLISE FOI VISTO QUE ESSES PERCENTUAIS UTILIZADOS TEM UMA DIFERENÇA PEQUENA (MENOS DE 5% NO TAMANHO FINAL) E QUE
 *  AS COMPRESSÕES SOMENTE SÃO FEITAS EM TABELAS/ÍNDICES RELEVANTES E PRIORIZANDO ROW QUE É MAIS RÁPIDA E SE A 
 *  TABELA/ÍNDICE É COMPRIMÍVEL (MUITOS TEM COMPRESSÃO MUITO BAIXA)
 *  
 * FICA PARA O FUTURO TAMBÉM ANALISAR TABELA MUITO PEQUENAS, TALVEZ NÃO DEVEM SER COMPRIMIDAS
 * 
 */

SET NOCOUNT ON
BEGIN
	DECLARE @schema_name    SYSNAME,
	        @table_name     SYSNAME,
			@index_id		INT,
			@tamanho		FLOAT,
			@compres_atual	VARCHAR(10),
	        @Contador		FLOAT = 0,
	        @Qt_Comandos	FLOAT = 0,
	        @Fl_Banco_Compactado BIT,
	        @StartTime AS DATETIME = GETDATE();
	
	PRINT '##### INÍCIO '+convert(varchar(23), @StartTime , 21 );
	PRINT '';
	
	IF (OBJECT_ID('tempdb..#compress_report_tb_none') IS NOT NULL)
		DROP TABLE #compress_report_tb_none;	--Apaga tabela se existir

	IF (OBJECT_ID('tempdb..#compress_report_tb_row') IS NOT NULL)
		DROP TABLE #compress_report_tb_row;	--Apaga tabela se existir

	IF (OBJECT_ID('tempdb..#compress_report_tb_page') IS NOT NULL)
		DROP TABLE #compress_report_tb_page;	--Apaga tabela se existir

	IF (OBJECT_ID('dbo.CX_COMPRESS') IS NOT NULL)
		DROP TABLE dbo.CX_COMPRESS;	--Apaga tabela se existir

	CREATE TABLE #compress_report_tb_none
	(
		ObjName           SYSNAME,
		schemaName        SYSNAME,
		indx_ID           INT,
		partit_number     INT,
		size_with_current_compression_setting FLOAT,
		size_with_requested_compression_setting FLOAT,
		sample_size_with_current_compression_setting FLOAT,
		sample_size_with_requested_compression_setting FLOAT
	)
	
	--Cria tabela com a mesma estrutura	
	SELECT *
	INTO #compress_report_tb_page
	FROM #compress_report_tb_none
	
	--Cria tabela com a mesma estrutura
	SELECT *
	INTO #compress_report_tb_row
	FROM #compress_report_tb_none
	
	--Banco já compactado?
	SET @Fl_Banco_Compactado = (
			SELECT IIF (COUNT(1)>0,1,0)
			FROM   INFORMATION_SCHEMA.TABLES
			INNER JOIN sys.partitions
				ON  OBJECT_NAME(object_id) = TABLE_NAME
			JOIN sys.allocation_units
				ON container_id = partition_id
			WHERE  TABLE_TYPE LIKE 'BASE%'
				   AND UPPER(TABLE_CATALOG) = UPPER(DB_NAME())
				   AND data_compression_desc <> 'NONE')

	IF ( @Fl_Banco_Compactado = 1 )
		PRINT 'BANCO JÁ COMPACTADO'
	ELSE
		PRINT 'BANCO SEM COMPRESSÃO'

	--####################################################################################################
	DECLARE c_sch_tb_crs INSENSITIVE CURSOR 
	FOR SELECT TABLE_SCHEMA,
			   TABLE_NAME,
			   CAST(8 * SUM(used_pages) AS float) tamanho
		FROM   INFORMATION_SCHEMA.TABLES
		INNER JOIN sys.partitions
			ON  OBJECT_NAME(object_id) = TABLE_NAME
		JOIN sys.allocation_units
			ON container_id = partition_id
		WHERE  TABLE_TYPE LIKE 'BASE%'
			   AND UPPER(TABLE_CATALOG) = UPPER(DB_NAME())
			   --AND OBJECT_NAME(object_id) = 'CONFIGUR_SEGUR_DOCTO' --para debug
			   --Com pelo menos 12 paginas diminui bastante a quantidade total de processamento, as tabelas muito pequenas não vale a pena comprimir
			   AND ( used_pages > 12 or data_compression_desc <> 'NONE' )
		GROUP BY TABLE_SCHEMA,TABLE_NAME

	--Contagem do total de registros, ainda não encontrei uma forma mais simples de fazer isso!
	--PORQUE NÃO É POSSÍVEL EXECUTAR USANDO EXEC() E RETORNAR PARA UMA VARIÁVEL E O CURSOR NÃO
	--PERMITE FAZER A CONTAGEM DIRETAMENTE.
	OPEN c_sch_tb_crs

	FETCH NEXT FROM c_sch_tb_crs INTO @schema_name, @table_name, @tamanho
	WHILE @@Fetch_Status = 0
	BEGIN
		SET @Qt_Comandos = @Qt_Comandos+1;
		FETCH NEXT FROM c_sch_tb_crs INTO @schema_name, @table_name, @tamanho
	END
	CLOSE c_sch_tb_crs
	--####################################################################################################	
	--Fecho e reabro o cursor para forçar voltar ao início para processamento
	OPEN c_sch_tb_crs	
	FETCH NEXT FROM c_sch_tb_crs INTO @schema_name, @table_name, @tamanho	
	WHILE @@Fetch_Status = 0
	BEGIN
		SET @Contador = @Contador+1;
	    PRINT CONVERT(varchar(5), ROUND((@Contador/@Qt_Comandos)*100,1) )+' % Calculando tabela '+@table_name
		
		--FOI MAIS RÁPIDO RODAR TODOS MESMO QUE RODAR INDIVIDUALMENTE POR TABELA E INDICE
		
		BEGIN
			INSERT INTO #compress_report_tb_row
			EXEC sp_estimate_data_compression_savings
					@schema_name = @schema_name,
					@object_name = @table_name,
					@index_id = NULL,
					@partition_number = NULL,
					@data_compression = 'ROW'
		END
		
		BEGIN
			INSERT INTO #compress_report_tb_page
			EXEC sp_estimate_data_compression_savings
					@schema_name = @schema_name,
					@object_name = @table_name,
					@index_id = NULL,
					@partition_number = NULL,
					@data_compression = 'PAGE'
		END
		
		IF (@Fl_Banco_Compactado = 1)
			BEGIN 
				INSERT INTO #compress_report_tb_none
				EXEC sp_estimate_data_compression_savings
						@schema_name = @schema_name,
						@object_name = @table_name,
						@index_id = NULL,
						@partition_number = NULL,
						@data_compression = 'NONE'
			END
		
	    FETCH NEXT FROM c_sch_tb_crs INTO @schema_name, @table_name, @tamanho
	END
	CLOSE c_sch_tb_crs 
	DEALLOCATE c_sch_tb_crs
	
	IF (@Fl_Banco_Compactado = 0)
		BEGIN
			INSERT INTO #compress_report_tb_none (ObjName,schemaName,indx_ID,size_with_current_compression_setting,size_with_requested_compression_setting)
			SELECT ObjName,schemaName,indx_ID,size_with_current_compression_setting,size_with_current_compression_setting
			FROM #compress_report_tb_row
		END
	ELSE
		BEGIN
			--CORRIGE TAMANHO DA TABELA SEM COMPRESSÃO, SE ELA FOR NONE E RODAR A PROCEDURE COM NONE, AS VEZES ELE CALCULA UM VALOR INDEVIDO COMO NA AUDIT_TRAIL
			UPDATE #compress_report_tb_none SET size_with_requested_compression_setting = size_with_current_compression_setting
			FROM #compress_report_tb_none
			INNER JOIN sys.indexes A
				ON  A.index_id = indx_ID
					AND OBJECT_NAME(A.object_id) = ObjName
			INNER JOIN sys.partitions D
				ON  A.object_id = D.object_id
					AND A.index_id = D.index_id
			WHERE data_compression_desc = 'NONE'
		END
	--####################################################################################################	
	--Alimenta tabela base CX_COMPRESS
	SELECT *
	INTO CX_COMPRESS
	FROM (	SELECT 	tnone.ObjName AS table_name,
					tnone.indx_ID,
					SUM(tnone.size_with_requested_compression_setting)/1024 AS size_none_compress_MB,
					SUM(trow.size_with_requested_compression_setting)/1024 size_compression_row_MB,
					SUM(tpage.size_with_requested_compression_setting)/1024 AS size_compression_page_MB,
					CONVERT(FLOAT, 0) AS size_saving_row_MB,
					CONVERT(FLOAT, 0) AS size_saving_page_MB,
					CONVERT(FLOAT, 0) AS compression_row,
					CONVERT(FLOAT, 0) AS compression_page, 
					CONVERT(FLOAT, 0) AS dif_page_x_row,
					D.data_compression_desc compress_current,
					'    ' AS recomend
			FROM #compress_report_tb_none tnone
			INNER JOIN #compress_report_tb_row trow
				ON trow.schemaName = tnone.schemaName
					AND trow.ObjName = tnone.ObjName
					AND trow.indx_ID = tnone.indx_ID
			INNER JOIN #compress_report_tb_page tpage
				ON tpage.schemaName = tnone.schemaName
					AND tpage.ObjName = tnone.ObjName
					AND tpage.indx_ID = tnone.indx_ID
			INNER JOIN sys.indexes A
				ON  A.index_id = tnone.indx_ID
					AND OBJECT_NAME(A.object_id) = tnone.ObjName
			INNER JOIN sys.partitions D
				ON  A.object_id = D.object_id
					AND A.index_id = D.index_id
			--WHERE tnone.size_with_requested_compression_setting > 0 --APENAS SE TEM ESPAÇO OCUPADO
			GROUP BY tnone.ObjName,tnone.indx_ID,D.data_compression_desc
	) Q
	
	--FAZ OS CALCULOS
	UPDATE CX_COMPRESS SET	compression_row 		= (IIF(size_none_compress_MB=0,0,ROUND((1-(size_compression_row_MB/size_none_compress_MB))*100,2))),
							compression_page 		= (IIF(size_none_compress_MB=0,0,ROUND((1-(size_compression_page_MB/size_none_compress_MB))*100,2)))
	UPDATE CX_COMPRESS SET	dif_page_x_row 			= (compression_page-compression_row)
	UPDATE CX_COMPRESS SET	size_none_compress_MB	= Round(size_none_compress_MB,2),	--Arredondo os valores para ficar mais legível
							size_compression_row_MB	= Round(size_compression_row_MB,2),	--Arredondo os valores para ficar mais legível
							size_compression_page_MB= Round(size_compression_page_MB,2),--Arredondo os valores para ficar mais legível
							size_saving_row_MB		= Round((size_none_compress_MB-size_compression_row_MB),2),
							size_saving_page_MB		= Round((size_none_compress_MB-size_compression_page_MB),2),
							recomend		 		= (CASE --REGRAS DA COMPRESSÃO
															WHEN size_none_compress_MB < 0.1 THEN 'NONE'	--Não comprimo tabelas pequenas menores que 100kb
															WHEN size_none_compress_MB > size_compression_page_MB AND dif_page_x_row > 25 AND compression_page > 40 THEN 'PAGE'	--ganho de 25% comparado ao row e pelo menos 40% de compressão
															WHEN compression_row > 25 THEN 'ROW'			--compressão geral maior que 25%
															ELSE 'NONE'
														END)
	DROP TABLE #compress_report_tb_none;
	DROP TABLE #compress_report_tb_row;
	DROP TABLE #compress_report_tb_page;
	
	--Guardo a tabela de forma comprimida para economizar espaço
	ALTER TABLE CX_COMPRESS REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)
	
	--####################################################################################################
	SELECT *
	FROM CX_COMPRESS
	ORDER BY size_none_compress_MB DESC, table_name, indx_ID
	--####################################################################################################
	PRINT '';
	PRINT '##### FINAL '+CONVERT(varchar(23), GETDATE() , 21 );
	PRINT '##### TEMPO TOTAL '+Convert(varchar(30),GETDATE()-@StartTime,108);
END
SET NOCOUNT OFF
GO
