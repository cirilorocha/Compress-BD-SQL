/*
 * PRECISA PRIMEIRAMENTE RODAR A QUERY VERIFICAR GANHOS DE COMPRESSÃO, PARA SALVAR
 *  A TABELA CX_COMPRESS COM A ANÁLISE E AS RECOMENDAÇÕES PARA COMPRESSÃO
 *  
 * A ANÁLISE VERIFICA SE EXISTE COMPRESSÃO QUE COMPENSE
 * 
 * ESTA QUERY IRÁ COMPACTAR TODAS AS TABELAS E ÍNDICES DO BANCO DE DADOS CORRENTE
 *  ALÉM DE FAZER A DESFRAGMENTAÇÃO DOS ÍNDICES E SE CONFIGURADO TAMBÉM A REDUÇÃO
 *  DO ESPAÇO LIVRE
 * AO FINAL FARÁ UMA VERIFICAÇÃO DA INTEGRIDADE DO BANCO DE DADOS TAMBÉM
 * 
 */

-- Declare variables  
SET NOCOUNT ON;  
DECLARE @Fl_Rodar_Shrink BIT = 1;
DECLARE @Fl_Parar_Se_Falhar BIT = 0;
DECLARE @Fl_Exibe_Comparacao_Tamanho BIT = 1;

DECLARE @tablename VARCHAR(255);  
DECLARE @execstr VARCHAR(400);
DECLARE @mensagem VARCHAR(MAX);  
DECLARE @objectid INT;  
DECLARE @indexid INT;
DECLARE @indexname VARCHAR(255);
DECLARE @frag DECIMAL; 
DECLARE @maxfrag DECIMAL;
DECLARE @StartTime AS DATETIME = GETDATE();
DECLARE @StartDefrag AS DATETIME;
DECLARE @StartVerif AS DATETIME;
DECLARE @StartShrink AS DATETIME;

-- Decide on the maximum fragmentation to allow for.  
SET @maxfrag = 100.0;	--NÃO ESTÁ ADIANTANDO DESFRAGMENTAR AQUI!

PRINT '##### INÍCIO '+convert(varchar(23), @StartTime , 21 );
PRINT ''

BEGIN
	SET NOCOUNT ON
	DECLARE @Ds_Query                   VARCHAR(MAX),
	        @Ds_Comando_Compactacao     VARCHAR(MAX)
	
	IF (OBJECT_ID('tempdb..#Comandos_Compactacao') IS NOT NULL)
	    DROP TABLE #Comandos_Compactacao;
	
	CREATE TABLE #Comandos_Compactacao
	(
		Id          BIGINT IDENTITY(1, 1),
		Tabela      SYSNAME,
		Indice      SYSNAME NULL,
		Tamanho		FLOAT,
		Comando     VARCHAR(MAX)
	)
	IF (@Fl_Exibe_Comparacao_Tamanho = 1)
	BEGIN
	    SET @Ds_Query = '
SELECT 
(SUM(a.total_pages) / 128) AS Vl_Tamanho_Tabelas_Antes_Compactacao
FROM 
[' + DB_NAME() + '].sys.tables					t     WITH(NOLOCK)
INNER JOIN [' + DB_NAME() + '].sys.indexes			i     WITH(NOLOCK) ON t.OBJECT_ID = i.object_id
INNER JOIN [' + DB_NAME() + '].sys.partitions			p     WITH(NOLOCK) ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
INNER JOIN [' + DB_NAME() + '].sys.allocation_units		a     WITH(NOLOCK) ON p.partition_id = a.container_id
WHERE 
i.OBJECT_ID > 255 
'
	    EXEC (@Ds_Query)
	END
	
	--#####################################################################################################
	BEGIN
		SET @mensagem = 'Compactando tabelas'
		PRINT @mensagem
		SELECT @mensagem AS MENSAGEM

		INSERT INTO #Comandos_Compactacao( Tabela, Indice, Tamanho, Comando )
		SELECT table_name,	A.name,	size_none_compress_MB,
			   IIF(
				   indx_ID <= 1,
					'ALTER TABLE '+table_name+' REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = ' +recomend + ')'
				   ,
				   'ALTER INDEX [' + A.name COLLATE Latin1_general_BIN + '] ON [' + table_name + 
				   '] REBUILD PARTITION = ALL WITH ( STATISTICS_NORECOMPUTE = OFF, ONLINE = OFF, SORT_IN_TEMPDB = ON, DATA_COMPRESSION = ' + recomend + ')'
			   ) AS Comando
		FROM   CX_COMPRESS
			   INNER JOIN sys.indexes A
					ON  A.index_id = indx_ID
					AND OBJECT_NAME(A.object_id) = table_name
			   INNER JOIN sys.partitions D
					ON  A.object_id = D.object_id
					AND A.index_id = D.index_id
		WHERE  1 = 1 --SO PARA FACILITAR
			AND D.data_compression_desc <> recomend COLLATE Latin1_general_BIN
		ORDER BY size_none_compress_MB Desc
	END

	DECLARE @Qt_Comandos        FLOAT = (
	            SELECT COUNT(*)
	            FROM   #Comandos_Compactacao
	        ),
	        @Contador           FLOAT = 1,
	        @Ds_Mensagem        VARCHAR(MAX),
	        @Nr_Codigo_Erro     INT = (CASE WHEN @Fl_Parar_Se_Falhar = 1 THEN 16 ELSE 10 END)
	
	WHILE (@Contador <= @Qt_Comandos)
	BEGIN
	    SELECT @Ds_Comando_Compactacao = Comando
	    FROM   #Comandos_Compactacao
	    WHERE  Id = @Contador
	    
	    SET @Ds_Mensagem = CONVERT(varchar(10),CONVERT(decimal(10,2), ROUND((@Contador/@Qt_Comandos)*100,1) ))+' % - Executando comando "' + @Ds_Comando_Compactacao + '"... Aguarde...'
	    PRINT @Ds_Mensagem
	    BEGIN TRY
	    	--RAISERROR(@Ds_Mensagem, 10, 1) WITH NOWAIT 
	    	EXEC (@Ds_Comando_Compactacao)
	    END TRY
	    BEGIN CATCH
	    	SELECT ERROR_NUMBER()     AS ErrorNumber,
	    	       ERROR_SEVERITY()   AS ErrorSeverity,
	    	       ERROR_STATE()      AS ErrorState,
	    	       ERROR_PROCEDURE()  AS ErrorProcedure,
	    	       ERROR_LINE()       AS ErrorLine,
	    	       ERROR_MESSAGE()    AS ErrorMessage;
	    	SET @Ds_Mensagem = 'Falha ao executar o comando "' + @Ds_Comando_Compactacao + '"'
	    	RAISERROR(@Ds_Mensagem, @Nr_Codigo_Erro, 1) WITH NOWAIT
	    	RETURN
	    END CATCH	
	    SET @Contador = @Contador + 1
	END
	
	PRINT '##### TÉRMINO DA COMPRESSÃO DAS TABELAS '+CONVERT(varchar(23), GETDATE() , 21 );
	PRINT '##### TEMPO COMPRESSÃO '+Convert(varchar(30),GETDATE()-@StartTime,108);
	PRINT '##### TEMPO PARCIAL '+Convert(varchar(30),GETDATE()-@StartTime,108);
	PRINT ''
	
	SET @StartShrink	= GETDATE();
	IF (@Fl_Rodar_Shrink = 1)
	BEGIN
		
	    SET @Ds_Query = '
DBCC SHRINKDATABASE (0,5) WITH NO_INFOMSGS
'		
		SET @mensagem = 'Reduzindo banco de dados '+@Ds_Query; 
		PRINT @mensagem
		SELECT @mensagem AS MENSAGEM 
	    EXEC (@Ds_Query)
	    
		PRINT '##### TÉRMINO REDUÇÃO '+CONVERT(varchar(23), GETDATE() , 21 );
		PRINT '##### TEMPO REDUÇÃO '+Convert(varchar(30),GETDATE()-@StartShrink,108);
		PRINT '##### TEMPO PARCIAL '+Convert(varchar(30),GETDATE()-@StartTime,108);
		PRINT '';
	END
	---------------------------------------------------------------------------------------------------
	SET @StartDefrag = GETDATE()
	SET @mensagem = 'Desfragmentando banco de dados...'
	PRINT @mensagem
	SELECT @mensagem AS MENSAGEM
	
	-- Declare a cursor.
	DECLARE tables CURSOR 
	FOR
	    SELECT TABLE_SCHEMA + '.' + TABLE_NAME
	    FROM   INFORMATION_SCHEMA.TABLES
	    WHERE  TABLE_TYPE = 'BASE TABLE'
	;  
	
	IF OBJECT_ID(N'#fraglist', N'U') IS NOT NULL
		DROP TABLE #fraglist;
	
	-- Create the table.  
	CREATE TABLE #fraglist
	(
		ObjectName         CHAR(255),
		ObjectId           INT,
		IndexName          CHAR(255),
		IndexId            INT,
		Lvl                INT,
		CountPages         INT,
		CountRows          INT,
		MinRecSize         INT,
		MaxRecSize         INT,
		AvgRecSize         INT,
		ForRecCount        INT,
		Extents            INT,
		ExtentSwitches     INT,
		AvgFreeBytes       INT,
		AvgPageDensity     INT,
		ScanDensity        DECIMAL,
		BestCount          INT,
		ActualCount        INT,
		LogicalFrag        DECIMAL,
		ExtentFrag         DECIMAL
	); 
	
	-- Open the cursor.  
	OPEN tables; 
	
	-- Loop through all the tables in the database.  
	FETCH NEXT 
	FROM tables 
	INTO @tablename;  
	
	WHILE @@FETCH_STATUS = 0
	BEGIN
	    -- Do the showcontig of all indexes of the table  
	    INSERT INTO #fraglist
	    EXEC (
	             'DBCC SHOWCONTIG (''' + @tablename + ''')   
      WITH FAST, TABLERESULTS, ALL_INDEXES, NO_INFOMSGS'
	         ); 
	    FETCH NEXT 
	    FROM tables 
	    INTO @tablename;
	END; 
	
	-- Close and deallocate the cursor.  
	CLOSE tables; 
	
	SET @Qt_Comandos = (
	            SELECT COUNT(*)
	            FROM   #fraglist
				WHERE  LogicalFrag >= @maxfrag
						AND INDEXPROPERTY(ObjectId, IndexName, 'IndexDepth') > 0
					);
	SET @Contador = 1;

	DEALLOCATE tables; 
	
	-- Declare the cursor for the list of indexes to be defragged.  
	DECLARE indexes CURSOR 
	FOR
	    SELECT ObjectName,
	           ObjectId,
	           IndexId,
	           LogicalFrag,
	           IndexName
	    FROM   #fraglist
	    WHERE  LogicalFrag >= @maxfrag
	           AND INDEXPROPERTY(ObjectId, IndexName, 'IndexDepth') > 0
	--ORDER BY CountPages DESC;	--Maiores primeiro! o % fica mais linear sem a ordenação	
	--ORDER BY ObjectName,IndexId
	    ORDER BY NEWID()	--Ordem aleatória
	
	-- Open the cursor.  
	OPEN indexes; 
	
	-- Loop through the indexes.  
	FETCH NEXT 
	FROM indexes 
	INTO @tablename, @objectid, @indexid, @frag, @indexname;
	
	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @Contador = @Contador+1;
		SELECT @execstr = 'ALTER INDEX '+RTRIM(@indexname)+' ON '+RTRIM(@tablename)+' REORGANIZE';
		PRINT CONVERT(varchar(10),CONVERT(decimal(10,2), ROUND((@Contador/@Qt_Comandos)*100,1) ))+' % Executing ' + @execstr +
		', ' + RTRIM(@indexid) + ') - fragmentation currently ' 
		+ RTRIM(CONVERT(VARCHAR(15), @frag)) + '%';
--		EXEC (@execstr); 
	    
	    FETCH NEXT 
	    FROM indexes 
	    INTO @tablename, @objectid, @indexid, @frag, @indexname;
	END; 
	
	-- Close and deallocate the cursor
	CLOSE indexes; 
	DEALLOCATE indexes; 
	
	-- Delete the temporary table
	DROP TABLE #fraglist;
	
	PRINT '##### TÉRMINO DESFRAGMENTAÇÃO '+CONVERT(varchar(23), GETDATE() , 21 );
	PRINT '##### TEMPO DESFRAGMENTAÇÃO '+Convert(varchar(30),GETDATE()-@StartDefrag,108);
	PRINT '##### TEMPO PARCIAL '+Convert(varchar(30),GETDATE()-@StartTime,108);
	PRINT '';
	
	--------------------------------------------------------------------------------------------------
	SET @StartVerif	= GETDATE()
    BEGIN
    	SET @mensagem = 'Verificando banco de dados';
    	PRINT @mensagem
    	SELECT @mensagem AS MENSAGEM
	    DBCC CHECKDB WITH NO_INFOMSGS;
	END
	
	PRINT '##### TÉRMINO VERIFICAÇÃO '+CONVERT(varchar(23), GETDATE() , 21 );
	PRINT '##### TEMPO VERIFICAÇÃO '+Convert(varchar(30),GETDATE()-@StartVerif,108);
	PRINT '##### TEMPO PARCIAL '+Convert(varchar(30),GETDATE()-@StartTime,108);
	PRINT '';
	
	---------------------------------------------------------------------------------------------------
	SET @StartShrink	= GETDATE();
	IF (@Fl_Rodar_Shrink = 1)
	BEGIN
		
	    SET @Ds_Query = '
DBCC SHRINKDATABASE (0,5) WITH NO_INFOMSGS
'		
		SET @mensagem = 'Reduzindo banco de dados '+@Ds_Query; 
		PRINT @mensagem
		SELECT @mensagem AS MENSAGEM 
	    EXEC (@Ds_Query)
	    
		PRINT '##### TÉRMINO REDUÇÃO '+CONVERT(varchar(23), GETDATE() , 21 );
		PRINT '##### TEMPO REDUÇÃO '+Convert(varchar(30),GETDATE()-@StartShrink,108);
		PRINT '##### TEMPO PARCIAL '+Convert(varchar(30),GETDATE()-@StartTime,108);
		PRINT '';
	END

	-----------------------------------------------------------------------------------------------
	IF (@Qt_Comandos > 0)
		BEGIN
			SET @mensagem = 'Processamento da Database "' + DB_NAME() + '" concluído. Verifique o log se houve algum erro.' 
			PRINT @mensagem
			SELECT @mensagem AS MENSAGEM
		END
	ELSE
		BEGIN
			SET @mensagem = 'Nenhum objeto para compactar no database "' + DB_NAME() + '"'
			PRINT @mensagem
			SELECT @mensagem AS MENSAGEM
		END
	IF (@Fl_Exibe_Comparacao_Tamanho = 1)
	BEGIN
	    SET @Ds_Query = '
SELECT 
(SUM(a.total_pages) / 128) AS Vl_Tamanho_Tabelas_Depois_Compactacao
FROM 
[' + DB_NAME() + '].sys.tables					t     WITH(NOLOCK)
INNER JOIN [' + DB_NAME() + '].sys.indexes			i     WITH(NOLOCK) ON t.OBJECT_ID = i.object_id
INNER JOIN [' + DB_NAME() + '].sys.partitions			p     WITH(NOLOCK) ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
INNER JOIN [' + DB_NAME() + '].sys.allocation_units		a     WITH(NOLOCK) ON p.partition_id = a.container_id
WHERE 
i.OBJECT_ID > 255
'
	    EXEC (@Ds_Query)
	END

	PRINT '';
	PRINT '##### FINAL '+CONVERT(varchar(23), GETDATE() , 21 );
	PRINT '';
	PRINT '##### TEMPO COMPRESSÃO '+Convert(varchar(30),@StartDefrag-@StartTime,108);
	PRINT '##### TEMPO DESFRAGMENTAÇÃO '+Convert(varchar(30),@StartVerif-@StartDefrag,108);
	PRINT '##### TEMPO VERIFICAÇÃO '+Convert(varchar(30),@StartShrink-@StartVerif,108);
	PRINT '##### TEMPO REDUÇÃO '+Convert(varchar(30),GETDATE()-@StartShrink,108);
	PRINT '';
	PRINT '##### TEMPO TOTAL '+Convert(varchar(30),GETDATE()-@StartTime,108);

END
