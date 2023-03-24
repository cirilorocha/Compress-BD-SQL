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
DECLARE @frag DECIMAL;  
DECLARE @maxfrag DECIMAL;
DECLARE @StartTime AS DATETIME = GETDATE();
DECLARE @StartDefrag AS DATETIME;
DECLARE @StartVerif AS DATETIME;
DECLARE @StartShrink AS DATETIME;

PRINT '##### INÍCIO '+convert(varchar(23), @StartTime , 21 );
PRINT '';

-- Decide on the maximum fragmentation to allow for.  
SELECT @maxfrag = 100.0;

BEGIN
	SET NOCOUNT ON
	DECLARE @Ds_Query                   VARCHAR(MAX),
	        @Ds_Comando_DesCompactacao  VARCHAR(MAX),
	        @Ds_Metodo_DesCompressao    VARCHAR(20) = 'NONE',
	        @Nr_Metodo_DesCompressao    VARCHAR(20) = 0
	
	IF (OBJECT_ID('tempdb..#Comandos_DesCompactacao') IS NOT NULL)
	    DROP TABLE #Comandos_DesCompactacao
	
	CREATE TABLE #Comandos_DesCompactacao
	(
		Id          BIGINT IDENTITY(1, 1),
		Tabela      SYSNAME,
		Indice      SYSNAME NULL,
		Comando     VARCHAR(MAX)
	)
	IF (@Fl_Exibe_Comparacao_Tamanho = 1)
	BEGIN
	    SET @Ds_Query = '
SELECT 
(SUM(a.total_pages) / 128) AS Vl_Tamanho_Tabelas_Antes_DesCompactacao
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
	
	SET @Ds_Query = 
	    'INSERT INTO #Comandos_DesCompactacao( Tabela, Indice, Comando )
SELECT DISTINCT 
A.name AS Tabela,
NULL AS Indice,
''ALTER TABLE ['' + ''' + DB_NAME() + ''' + ''].['' + C.name + ''].['' + A.name + ''] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = ' + 
	    @Ds_Metodo_DesCompressao + ')'' AS Comando
FROM 
[' + DB_NAME() + '].sys.tables                   A
INNER JOIN [' + DB_NAME() + '].sys.partitions    B   ON A.object_id = B.object_id
INNER JOIN [' + DB_NAME() + '].sys.schemas       C   ON A.schema_id = C.schema_id
WHERE 
B.data_compression <> ' + @Nr_Metodo_DesCompressao + 
	    ' -- NONE
AND B.index_id = 0
AND A.type = ''U''
UNION
SELECT DISTINCT 
B.name AS Tabela,
A.name AS Indice,
''ALTER INDEX ['' + A.name + ''] ON ['' + ''' + DB_NAME() + 
	    ''' + ''].['' + C.name + ''].['' + B.name + ''] REBUILD PARTITION = ALL WITH ( STATISTICS_NORECOMPUTE = OFF, ONLINE = OFF, SORT_IN_TEMPDB = ON, DATA_COMPRESSION = ' 
	    + @Ds_Metodo_DesCompressao + ')''
FROM 
[' + DB_NAME() + '].sys.indexes                  A
INNER JOIN [' + DB_NAME() + '].sys.tables        B   ON A.object_id = B.object_id
INNER JOIN [' + DB_NAME() + '].sys.schemas       C   ON B.schema_id = C.schema_id
INNER JOIN [' + DB_NAME() + '].sys.partitions    D   ON A.object_id = D.object_id AND A.index_id = D.index_id
WHERE 
D.data_compression <> ' + @Nr_Metodo_DesCompressao + 
	    ' -- NONE
AND D.index_id <> 0
AND B.type = ''U''
ORDER BY Tabela,Indice
'
	EXEC (@Ds_Query)
	DECLARE @Qt_Comandos        FLOAT = (
	            SELECT COUNT(*)
	            FROM   #Comandos_DesCompactacao
	        ),
	        @Contador           FLOAT = 1,
	        @Ds_Mensagem        VARCHAR(MAX),
	        @Nr_Codigo_Erro     INT = (CASE WHEN @Fl_Parar_Se_Falhar = 1 THEN 16 ELSE 10 END)
	
	WHILE (@Contador <= @Qt_Comandos)
	BEGIN
	    SELECT @Ds_Comando_DesCompactacao = Comando
	    FROM   #Comandos_DesCompactacao
	    WHERE  Id = @Contador
	    
	    SET @Ds_Mensagem = CONVERT(varchar(5), ROUND((@Contador/@Qt_Comandos)*100,1) )+' % - Executando comando "' + @Ds_Comando_DesCompactacao + '"... Aguarde...'
		PRINT @Ds_Mensagem
		
	    BEGIN TRY
	    	--RAISERROR(@Ds_Mensagem, 10, 1) WITH NOWAIT 
	    	EXEC (@Ds_Comando_DesCompactacao)
	    END TRY
	    BEGIN CATCH
	    	SELECT ERROR_NUMBER()     AS ErrorNumber,
	    	       ERROR_SEVERITY()   AS ErrorSeverity,
	    	       ERROR_STATE()      AS ErrorState,
	    	       ERROR_PROCEDURE()  AS ErrorProcedure,
	    	       ERROR_LINE()       AS ErrorLine,
	    	       ERROR_MESSAGE()    AS ErrorMessage;
	    	SET @Ds_Mensagem = 'Falha ao executar o comando "' + @Ds_Comando_DesCompactacao + '"'
	    	RAISERROR(@Ds_Mensagem, @Nr_Codigo_Erro, 1) WITH NOWAIT
	    	RETURN
	    END CATCH	
	    SET @Contador = @Contador + 1
	END

	PRINT '##### TÉRMINO DA DESCOMPRESSÃO DAS TABELAS '+CONVERT(varchar(23), GETDATE() , 21 );
	PRINT '##### TEMPO DESCOMPRESSÃO '+Convert(varchar(30),GETDATE()-@StartTime,108);
	PRINT '##### TEMPO PARCIAL '+Convert(varchar(30),GETDATE()-@StartTime,108);
	PRINT '';
	
	---------------------------------------------------------------------------------------------------
	SET @StartDefrag = GETDATE()
	SET @mensagem = 'Desfragmentando banco de dados'
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
	           LogicalFrag
	    FROM   #fraglist
	    WHERE  LogicalFrag >= @maxfrag
	           AND INDEXPROPERTY(ObjectId, IndexName, 'IndexDepth') > 0
	    --ORDER BY CountPages DESC	--Maiores primeiro! o % fica mais linear sem a ordenação
	;
	
	-- Open the cursor.  
	OPEN indexes; 
	
	-- Loop through the indexes.  
	FETCH NEXT 
	FROM indexes 
	INTO @tablename, @objectid, @indexid, @frag;  
	
	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @Contador = @Contador+1;
	    PRINT CONVERT(varchar(5), ROUND((@Contador/@Qt_Comandos)*100,1) )+' % Executing DBCC INDEXDEFRAG (0, ' + RTRIM(@tablename) +
	    ', ' + RTRIM(@indexid) + ') - fragmentation currently ' 
	    + RTRIM(CONVERT(VARCHAR(15), @frag)) + '%';  
	    SELECT @execstr = 'DBCC INDEXDEFRAG (0, ' + RTRIM(@objectid) + ',  
       ' + RTRIM(@indexid) + ') WITH NO_INFOMSGS ';  
	    EXEC (@execstr); 
	    
	    FETCH NEXT 
	    FROM indexes 
	    INTO @tablename, @objectid, @indexid, @frag;
	END; 
	
	-- Close and deallocate the cursor.  
	CLOSE indexes; 
	DEALLOCATE indexes; 
	
	-- Delete the temporary table.  
	DROP TABLE #fraglist; 

	PRINT '##### TÉRMINO DESFRAGMENTAÇÃO '+CONVERT(varchar(23), GETDATE() , 21 );
	PRINT '##### TEMPO DESFRAGMENTAÇÃO '+Convert(varchar(30),GETDATE()-@StartDefrag,108);
	PRINT '##### TEMPO PARCIAL '+Convert(varchar(30),GETDATE()-@StartTime,108);
	PRINT '';
	
	-----------------------------------------------------------------------------------------------
    BEGIN
    	SET @StartVerif	= GETDATE()
    	
    	SET @mensagem = 'Verificando banco de dados';
	    PRINT @mensagem
	    SELECT @mensagem AS MENSAGEM

	    DBCC CHECKDB WITH NO_INFOMSGS;
	    
		PRINT '##### TÉRMINO VERIFICAÇÃO '+CONVERT(varchar(23), GETDATE() , 21 );
		PRINT '##### TEMPO VERIFICAÇÃO '+Convert(varchar(30),GETDATE()-@StartVerif,108);
		PRINT '##### TEMPO PARCIAL '+Convert(varchar(30),GETDATE()-@StartTime,108);
		PRINT '';
	END

	---------------------------------------------------------------------------------------------------
	SET @StartShrink	= GETDATE()
	IF (@Fl_Rodar_Shrink = 1)
	BEGIN
	    SET @Ds_Query = '
USE ' + DB_NAME() + '
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
			SET @mensagem ='Processamento da Database "' + DB_NAME() + '" concluído. Verifique o log se houve algum erro.'
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
(SUM(a.total_pages) / 128) AS Vl_Tamanho_Tabelas_Depois_DesCompactacao
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
