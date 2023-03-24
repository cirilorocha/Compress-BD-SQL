/*
 * N�O EXECUTAR ESSA QUERY EM PARALELO MESMO EM BANCOS DIFERENTES, O SQL SE PERDE E D� ERROS ALEAT�RIOS
 *
 * ESTA QUERY FAZ UMA AN�LISE ENTRE AS COMPRESS�ES ROW E PAGE E FAZ UMA RECOMENDA��O SOBRE QUAL COMPRESS�O � INTERESSANTE
 * UTILIZAR, SEGUINDO O SEGUINTE CRIT�RIO, A COMPRESS�O GERAL PRECISA SER MAIOR QUE 25% ROW OU 40% PAGE E SE PAGE FOR MAIOR
 * 25 PONTOS PERCENTUAIS EM RELA��O A ROW ENT�O RECOMENDA PAGE, SE A COMPRESS�O FOR INFERIOR A 25% N�O RECOMENDA COMPRIMIR
 * 
 * ESTA ROTINA IR� GERAR A TABELA CX_COMPRESS COM AS RECOMENDA��ES PARA SEREM USADAS NA COMPRESS�O DO BANCO
 * 
 * EM AN�LISE FOI VISTO QUE ESSES PERCENTUAIS UTILIZADOS TEM UMA DIFEREN�A PEQUENA (MENOS DE 5% NO TAMANHO FINAL) E QUE
 *  AS COMPRESS�ES SOMENTE S�O FEITAS EM TABELAS/�NDICES RELEVANTES E PRIORIZANDO ROW QUE � MAIS R�PIDA E SE A 
 *  TABELA/�NDICE � COMPRIM�VEL (MUITOS TEM COMPRESS�O MUITO BAIXA)
 *  
 * FICA PARA O FUTURO TAMB�M ANALISAR TABELA MUITO PEQUENAS, TALVEZ N�O DEVEM SER COMPRIMIDAS
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
	
	PRINT '##### IN�CIO '+convert(varchar(23), @StartTime , 21 );
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
	
	--Banco j� compactado?
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
		PRINT 'BANCO J� COMPACTADO'
	ELSE
		PRINT 'BANCO SEM COMPRESS�O'

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
			   AND used_pages > 0
		GROUP BY TABLE_SCHEMA,
			   TABLE_NAME

	--Contagem do total de registros, ainda n�o encontrei uma forma mais simples de fazer isso!
	--PORQUE N�O � POSS�VEL EXECUTAR USANDO EXEC() E RETORNAR PARA UMA VARI�VEL E O CURSOR N�O
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
	--Fecho e reabro o cursor para for�ar voltar ao in�cio para processamento
	OPEN c_sch_tb_crs	
	FETCH NEXT FROM c_sch_tb_crs INTO @schema_name, @table_name, @tamanho	
	WHILE @@Fetch_Status = 0
	BEGIN
		SET @Contador = @Contador+1;
	    PRINT CONVERT(varchar(5), ROUND((@Contador/@Qt_Comandos)*100,1) )+' % Calculando tabela '+@table_name
		
		--FOI MAIS R�PIDO RODAR TODOS MESMO QUE RODAR INDIVIDUALMENTE POR TABELA E INDICE
		
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
			--CORRIGE TAMANHO DA TABELA SEM COMPRESS�O, SE ELA FOR NONE E RODAR A PROCEDURE COM NONE, AS VEZES ELE CALCULA UM VALOR INDEVIDO COMO NA AUDIT_TRAIL
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

	SELECT *,
			CASE
				WHEN size_none_compress > size_compression_page AND dif_page_x_row > 25 AND compression_page > 40 THEN 'PAGE'	--ganho de 25% em row
				WHEN compression_row > 25 THEN 'ROW'	--compress�o geral maior que 25%
				ELSE compress_current
			END AS recomend
	INTO CX_COMPRESS
	FROM (
		SELECT tnone.ObjName          AS [table_name],
				tnone.indx_ID,
				ROUND(SUM(tnone.size_with_requested_compression_setting)/1024,2) AS size_none_compress,
				ROUND(SUM(trow.size_with_requested_compression_setting)/1024,2) size_compression_row,
				ROUND(SUM(tpage.size_with_requested_compression_setting)/1024,2) AS size_compression_page,
				ROUND((SUM(tnone.size_with_requested_compression_setting)-SUM(trow.size_with_requested_compression_setting))/1024,2) AS size_saving_row,
				ROUND((SUM(tnone.size_with_requested_compression_setting)-SUM(tpage.size_with_requested_compression_setting))/1024,2) AS size_saving_page,
				ROUND((1-(SUM(trow.size_with_requested_compression_setting)/SUM(tnone.size_with_requested_compression_setting)))*100,2) AS compression_row,
				ROUND((1-(SUM(tpage.size_with_requested_compression_setting)/SUM(tnone.size_with_requested_compression_setting)))*100,2) AS compression_page,
				ROUND((((SUM(trow.size_with_requested_compression_setting)-SUM(tpage.size_with_requested_compression_setting))/SUM(tnone.size_with_requested_compression_setting)))*100,2) AS dif_page_x_row,
				D.data_compression_desc compress_current
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
		WHERE tnone.size_with_requested_compression_setting > 0 --APENAS SE TEM ESPA�O OCUPADO
		GROUP BY
				tnone.ObjName,
				tnone.indx_ID,
				D.data_compression_desc
	) Q
	
	DROP TABLE #compress_report_tb_none;
	DROP TABLE #compress_report_tb_row;
	DROP TABLE #compress_report_tb_page;
	
	--Guardo a tabela de forma comprimida para economizar espa�o
	ALTER TABLE CX_COMPRESS REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)
	
	--####################################################################################################
	SELECT *
	FROM CX_COMPRESS
	ORDER BY
			size_none_compress DESC,
			table_name,
			indx_ID
	--####################################################################################################
	PRINT '';
	PRINT '##### FINAL '+CONVERT(varchar(23), GETDATE() , 21 );
	PRINT '##### TEMPO TOTAL '+Convert(varchar(30),GETDATE()-@StartTime,108);
END
SET NOCOUNT OFF
GO
