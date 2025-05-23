-- Create database
CREATE DATABASE MyDatabase;
GO

USE MyDatabase;
GO

-- Create input table
CREATE TABLE MyInputTable (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    Name NVARCHAR(100),
    Value INT,
    CreatedDate DATETIME DEFAULT GETDATE()
);
GO

-- Insert sample data
INSERT INTO MyInputTable (Name, Value)
VALUES 
    ('Item1', 100),
    ('Item2', 200),
    ('Item3', 300),
    ('Item4', 400),
    ('Item5', 500);
GO

-- Create output table
CREATE TABLE MyOutputTable (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    Name NVARCHAR(100),
    Value INT,
    ProcessedDate DATETIME DEFAULT GETDATE()
);
GO

-- Create stored procedure with multiple temporary table operations
CREATE PROCEDURE ProcessDataThroughTemporaryTables
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Initial copy to first temp table
    IF OBJECT_ID('tempdb..#TempTable1') IS NOT NULL
        DROP TABLE #TempTable1;
    SELECT * INTO #TempTable1 FROM MyInputTable;
    
    -- Explicit temporary table operations with drops
    IF OBJECT_ID('tempdb..#TempTable2') IS NOT NULL
        DROP TABLE #TempTable2;
    SELECT * INTO #TempTable2 FROM #TempTable1;
    
    IF OBJECT_ID('tempdb..#TempTable3') IS NOT NULL
        DROP TABLE #TempTable3;
    SELECT * INTO #TempTable3 FROM #TempTable2;
    
    IF OBJECT_ID('tempdb..#TempTable4') IS NOT NULL
        DROP TABLE #TempTable4;
    SELECT * INTO #TempTable4 FROM #TempTable3;
    
    IF OBJECT_ID('tempdb..#TempTable5') IS NOT NULL
        DROP TABLE #TempTable5;
    SELECT * INTO #TempTable5 FROM #TempTable4;
    
    IF OBJECT_ID('tempdb..#TempTable6') IS NOT NULL
        DROP TABLE #TempTable6;
    SELECT * INTO #TempTable6 FROM #TempTable5;
    
    IF OBJECT_ID('tempdb..#TempTable7') IS NOT NULL
        DROP TABLE #TempTable7;
    SELECT * INTO #TempTable7 FROM #TempTable6;
    
    IF OBJECT_ID('tempdb..#TempTable8') IS NOT NULL
        DROP TABLE #TempTable8;
    SELECT * INTO #TempTable8 FROM #TempTable7;
    
    IF OBJECT_ID('tempdb..#TempTable9') IS NOT NULL
        DROP TABLE #TempTable9;
    SELECT * INTO #TempTable9 FROM #TempTable8;
    
    IF OBJECT_ID('tempdb..#TempTable10') IS NOT NULL
        DROP TABLE #TempTable10;
    SELECT * INTO #TempTable10 FROM #TempTable9;
    
    IF OBJECT_ID('tempdb..#TempTable11') IS NOT NULL
        DROP TABLE #TempTable11;
    SELECT * INTO #TempTable11 FROM #TempTable10;
    
    IF OBJECT_ID('tempdb..#TempTable12') IS NOT NULL
        DROP TABLE #TempTable12;
    SELECT * INTO #TempTable12 FROM #TempTable11;
    
    IF OBJECT_ID('tempdb..#TempTable13') IS NOT NULL
        DROP TABLE #TempTable13;
    SELECT * INTO #TempTable13 FROM #TempTable12;
    
    IF OBJECT_ID('tempdb..#TempTable14') IS NOT NULL
        DROP TABLE #TempTable14;
    SELECT * INTO #TempTable14 FROM #TempTable13;
    
    IF OBJECT_ID('tempdb..#TempTable15') IS NOT NULL
        DROP TABLE #TempTable15;
    SELECT * INTO #TempTable15 FROM #TempTable14;
    
    IF OBJECT_ID('tempdb..#TempTable16') IS NOT NULL
        DROP TABLE #TempTable16;
    SELECT * INTO #TempTable16 FROM #TempTable15;
    
    IF OBJECT_ID('tempdb..#TempTable17') IS NOT NULL
        DROP TABLE #TempTable17;
    SELECT * INTO #TempTable17 FROM #TempTable16;
    
    IF OBJECT_ID('tempdb..#TempTable18') IS NOT NULL
        DROP TABLE #TempTable18;
    SELECT * INTO #TempTable18 FROM #TempTable17;
    
    IF OBJECT_ID('tempdb..#TempTable19') IS NOT NULL
        DROP TABLE #TempTable19;
    SELECT * INTO #TempTable19 FROM #TempTable18;
    
    IF OBJECT_ID('tempdb..#TempTable20') IS NOT NULL
        DROP TABLE #TempTable20;
    SELECT * INTO #TempTable20 FROM #TempTable19;
    
    IF OBJECT_ID('tempdb..#TempTable21') IS NOT NULL
        DROP TABLE #TempTable21;
    SELECT * INTO #TempTable21 FROM #TempTable20;
    
    IF OBJECT_ID('tempdb..#TempTable22') IS NOT NULL
        DROP TABLE #TempTable22;
    SELECT * INTO #TempTable22 FROM #TempTable21;
    
    IF OBJECT_ID('tempdb..#TempTable23') IS NOT NULL
        DROP TABLE #TempTable23;
    SELECT * INTO #TempTable23 FROM #TempTable22;
    
    IF OBJECT_ID('tempdb..#TempTable24') IS NOT NULL
        DROP TABLE #TempTable24;
    SELECT * INTO #TempTable24 FROM #TempTable23;
    
    IF OBJECT_ID('tempdb..#TempTable25') IS NOT NULL
        DROP TABLE #TempTable25;
    SELECT * INTO #TempTable25 FROM #TempTable24;
    
    IF OBJECT_ID('tempdb..#TempTable26') IS NOT NULL
        DROP TABLE #TempTable26;
    SELECT * INTO #TempTable26 FROM #TempTable25;
    
    IF OBJECT_ID('tempdb..#TempTable27') IS NOT NULL
        DROP TABLE #TempTable27;
    SELECT * INTO #TempTable27 FROM #TempTable26;
    
    IF OBJECT_ID('tempdb..#TempTable28') IS NOT NULL
        DROP TABLE #TempTable28;
    SELECT * INTO #TempTable28 FROM #TempTable27;
    
    IF OBJECT_ID('tempdb..#TempTable29') IS NOT NULL
        DROP TABLE #TempTable29;
    SELECT * INTO #TempTable29 FROM #TempTable28;
    
    IF OBJECT_ID('tempdb..#TempTable30') IS NOT NULL
        DROP TABLE #TempTable30;
    SELECT * INTO #TempTable30 FROM #TempTable29;
    
    IF OBJECT_ID('tempdb..#TempTable31') IS NOT NULL
        DROP TABLE #TempTable31;
    SELECT * INTO #TempTable31 FROM #TempTable30;
    
    IF OBJECT_ID('tempdb..#TempTable32') IS NOT NULL
        DROP TABLE #TempTable32;
    SELECT * INTO #TempTable32 FROM #TempTable31;
    
    IF OBJECT_ID('tempdb..#TempTable33') IS NOT NULL
        DROP TABLE #TempTable33;
    SELECT * INTO #TempTable33 FROM #TempTable32;
    
    IF OBJECT_ID('tempdb..#TempTable34') IS NOT NULL
        DROP TABLE #TempTable34;
    SELECT * INTO #TempTable34 FROM #TempTable33;
    
    IF OBJECT_ID('tempdb..#TempTable35') IS NOT NULL
        DROP TABLE #TempTable35;
    SELECT * INTO #TempTable35 FROM #TempTable34;
    
    IF OBJECT_ID('tempdb..#TempTable36') IS NOT NULL
        DROP TABLE #TempTable36;
    SELECT * INTO #TempTable36 FROM #TempTable35;
    
    IF OBJECT_ID('tempdb..#TempTable37') IS NOT NULL
        DROP TABLE #TempTable37;
    SELECT * INTO #TempTable37 FROM #TempTable36;
    
    IF OBJECT_ID('tempdb..#TempTable38') IS NOT NULL
        DROP TABLE #TempTable38;
    SELECT * INTO #TempTable38 FROM #TempTable37;
    
    IF OBJECT_ID('tempdb..#TempTable39') IS NOT NULL
        DROP TABLE #TempTable39;
    SELECT * INTO #TempTable39 FROM #TempTable38;
    
    IF OBJECT_ID('tempdb..#TempTable40') IS NOT NULL
        DROP TABLE #TempTable40;
    SELECT * INTO #TempTable40 FROM #TempTable39;
    
    IF OBJECT_ID('tempdb..#TempTable41') IS NOT NULL
        DROP TABLE #TempTable41;
    SELECT * INTO #TempTable41 FROM #TempTable40;
    
    IF OBJECT_ID('tempdb..#TempTable42') IS NOT NULL
        DROP TABLE #TempTable42;
    SELECT * INTO #TempTable42 FROM #TempTable41;
    
    IF OBJECT_ID('tempdb..#TempTable43') IS NOT NULL
        DROP TABLE #TempTable43;
    SELECT * INTO #TempTable43 FROM #TempTable42;
    
    IF OBJECT_ID('tempdb..#TempTable44') IS NOT NULL
        DROP TABLE #TempTable44;
    SELECT * INTO #TempTable44 FROM #TempTable43;
    
    IF OBJECT_ID('tempdb..#TempTable45') IS NOT NULL
        DROP TABLE #TempTable45;
    SELECT * INTO #TempTable45 FROM #TempTable44;
    
    IF OBJECT_ID('tempdb..#TempTable46') IS NOT NULL
        DROP TABLE #TempTable46;
    SELECT * INTO #TempTable46 FROM #TempTable45;
    
    IF OBJECT_ID('tempdb..#TempTable47') IS NOT NULL
        DROP TABLE #TempTable47;
    SELECT * INTO #TempTable47 FROM #TempTable46;
    
    IF OBJECT_ID('tempdb..#TempTable48') IS NOT NULL
        DROP TABLE #TempTable48;
    SELECT * INTO #TempTable48 FROM #TempTable47;
    
    IF OBJECT_ID('tempdb..#TempTable49') IS NOT NULL
        DROP TABLE #TempTable49;
    SELECT * INTO #TempTable49 FROM #TempTable48;
    
    IF OBJECT_ID('tempdb..#TempTable50') IS NOT NULL
        DROP TABLE #TempTable50;
    SELECT * INTO #TempTable50 FROM #TempTable49;
    
    IF OBJECT_ID('tempdb..#TempTable51') IS NOT NULL
        DROP TABLE #TempTable51;
    SELECT * INTO #TempTable51 FROM #TempTable50;
    
    IF OBJECT_ID('tempdb..#TempTable52') IS NOT NULL
        DROP TABLE #TempTable52;
    SELECT * INTO #TempTable52 FROM #TempTable51;
    
    IF OBJECT_ID('tempdb..#TempTable53') IS NOT NULL
        DROP TABLE #TempTable53;
    SELECT * INTO #TempTable53 FROM #TempTable52;
    
    IF OBJECT_ID('tempdb..#TempTable54') IS NOT NULL
        DROP TABLE #TempTable54;
    SELECT * INTO #TempTable54 FROM #TempTable53;
    
    IF OBJECT_ID('tempdb..#TempTable55') IS NOT NULL
        DROP TABLE #TempTable55;
    SELECT * INTO #TempTable55 FROM #TempTable54;
    
    IF OBJECT_ID('tempdb..#TempTable56') IS NOT NULL
        DROP TABLE #TempTable56;
    SELECT * INTO #TempTable56 FROM #TempTable55;
    
    IF OBJECT_ID('tempdb..#TempTable57') IS NOT NULL
        DROP TABLE #TempTable57;
    SELECT * INTO #TempTable57 FROM #TempTable56;
    
    IF OBJECT_ID('tempdb..#TempTable58') IS NOT NULL
        DROP TABLE #TempTable58;
    SELECT * INTO #TempTable58 FROM #TempTable57;
    
    IF OBJECT_ID('tempdb..#TempTable59') IS NOT NULL
        DROP TABLE #TempTable59;
    SELECT * INTO #TempTable59 FROM #TempTable58;
    
    IF OBJECT_ID('tempdb..#TempTable60') IS NOT NULL
        DROP TABLE #TempTable60;
    SELECT * INTO #TempTable60 FROM #TempTable59;
    
    IF OBJECT_ID('tempdb..#TempTable61') IS NOT NULL
        DROP TABLE #TempTable61;
    SELECT * INTO #TempTable61 FROM #TempTable60;
    
    IF OBJECT_ID('tempdb..#TempTable62') IS NOT NULL
        DROP TABLE #TempTable62;
    SELECT * INTO #TempTable62 FROM #TempTable61;
    
    IF OBJECT_ID('tempdb..#TempTable63') IS NOT NULL
        DROP TABLE #TempTable63;
    SELECT * INTO #TempTable63 FROM #TempTable62;
    
    IF OBJECT_ID('tempdb..#TempTable64') IS NOT NULL
        DROP TABLE #TempTable64;
    SELECT * INTO #TempTable64 FROM #TempTable63;
    
    IF OBJECT_ID('tempdb..#TempTable65') IS NOT NULL
        DROP TABLE #TempTable65;
    SELECT * INTO #TempTable65 FROM #TempTable64;
    
    IF OBJECT_ID('tempdb..#TempTable66') IS NOT NULL
        DROP TABLE #TempTable66;
    SELECT * INTO #TempTable66 FROM #TempTable65;
    
    IF OBJECT_ID('tempdb..#TempTable67') IS NOT NULL
        DROP TABLE #TempTable67;
    SELECT * INTO #TempTable67 FROM #TempTable66;
    
    IF OBJECT_ID('tempdb..#TempTable68') IS NOT NULL
        DROP TABLE #TempTable68;
    SELECT * INTO #TempTable68 FROM #TempTable67;
    
    IF OBJECT_ID('tempdb..#TempTable69') IS NOT NULL
        DROP TABLE #TempTable69;
    SELECT * INTO #TempTable69 FROM #TempTable68;
    
    IF OBJECT_ID('tempdb..#TempTable70') IS NOT NULL
        DROP TABLE #TempTable70;
    SELECT * INTO #TempTable70 FROM #TempTable69;
    
    IF OBJECT_ID('tempdb..#TempTable71') IS NOT NULL
        DROP TABLE #TempTable71;
    SELECT * INTO #TempTable71 FROM #TempTable70;
    
    IF OBJECT_ID('tempdb..#TempTable72') IS NOT NULL
        DROP TABLE #TempTable72;
    SELECT * INTO #TempTable72 FROM #TempTable71;
    
    IF OBJECT_ID('tempdb..#TempTable73') IS NOT NULL
        DROP TABLE #TempTable73;
    SELECT * INTO #TempTable73 FROM #TempTable72;
    
    IF OBJECT_ID('tempdb..#TempTable74') IS NOT NULL
        DROP TABLE #TempTable74;
    SELECT * INTO #TempTable74 FROM #TempTable73;
    
    IF OBJECT_ID('tempdb..#TempTable75') IS NOT NULL
        DROP TABLE #TempTable75;
    SELECT * INTO #TempTable75 FROM #TempTable74;
    
    IF OBJECT_ID('tempdb..#TempTable76') IS NOT NULL
        DROP TABLE #TempTable76;
    SELECT * INTO #TempTable76 FROM #TempTable75;
    
    IF OBJECT_ID('tempdb..#TempTable77') IS NOT NULL
        DROP TABLE #TempTable77;
    SELECT * INTO #TempTable77 FROM #TempTable76;
    
    IF OBJECT_ID('tempdb..#TempTable78') IS NOT NULL
        DROP TABLE #TempTable78;
    SELECT * INTO #TempTable78 FROM #TempTable77;
    
    IF OBJECT_ID('tempdb..#TempTable79') IS NOT NULL
        DROP TABLE #TempTable79;
    SELECT * INTO #TempTable79 FROM #TempTable78;
    
    IF OBJECT_ID('tempdb..#TempTable80') IS NOT NULL
        DROP TABLE #TempTable80;
    SELECT * INTO #TempTable80 FROM #TempTable79;
    
    IF OBJECT_ID('tempdb..#TempTable81') IS NOT NULL
        DROP TABLE #TempTable81;
    SELECT * INTO #TempTable81 FROM #TempTable80;
    
    IF OBJECT_ID('tempdb..#TempTable82') IS NOT NULL
        DROP TABLE #TempTable82;
    SELECT * INTO #TempTable82 FROM #TempTable81;
    
    IF OBJECT_ID('tempdb..#TempTable83') IS NOT NULL
        DROP TABLE #TempTable83;
    SELECT * INTO #TempTable83 FROM #TempTable82;
    
    IF OBJECT_ID('tempdb..#TempTable84') IS NOT NULL
        DROP TABLE #TempTable84;
    SELECT * INTO #TempTable84 FROM #TempTable83;
    
    IF OBJECT_ID('tempdb..#TempTable85') IS NOT NULL
        DROP TABLE #TempTable85;
    SELECT * INTO #TempTable85 FROM #TempTable84;
    
    IF OBJECT_ID('tempdb..#TempTable86') IS NOT NULL
        DROP TABLE #TempTable86;
    SELECT * INTO #TempTable86 FROM #TempTable85;
    
    IF OBJECT_ID('tempdb..#TempTable87') IS NOT NULL
        DROP TABLE #TempTable87;
    SELECT * INTO #TempTable87 FROM #TempTable86;
    
    IF OBJECT_ID('tempdb..#TempTable88') IS NOT NULL
        DROP TABLE #TempTable88;
    SELECT * INTO #TempTable88 FROM #TempTable87;
    
    IF OBJECT_ID('tempdb..#TempTable89') IS NOT NULL
        DROP TABLE #TempTable89;
    SELECT * INTO #TempTable89 FROM #TempTable88;
    
    IF OBJECT_ID('tempdb..#TempTable90') IS NOT NULL
        DROP TABLE #TempTable90;
    SELECT * INTO #TempTable90 FROM #TempTable89;
    
    IF OBJECT_ID('tempdb..#TempTable91') IS NOT NULL
        DROP TABLE #TempTable91;
    SELECT * INTO #TempTable91 FROM #TempTable90;
    
    IF OBJECT_ID('tempdb..#TempTable92') IS NOT NULL
        DROP TABLE #TempTable92;
    SELECT * INTO #TempTable92 FROM #TempTable91;
    
    IF OBJECT_ID('tempdb..#TempTable93') IS NOT NULL
        DROP TABLE #TempTable93;
    SELECT * INTO #TempTable93 FROM #TempTable92;
    
    IF OBJECT_ID('tempdb..#TempTable94') IS NOT NULL
        DROP TABLE #TempTable94;
    SELECT * INTO #TempTable94 FROM #TempTable93;
    
    IF OBJECT_ID('tempdb..#TempTable95') IS NOT NULL
        DROP TABLE #TempTable95;
    SELECT * INTO #TempTable95 FROM #TempTable94;
    
    IF OBJECT_ID('tempdb..#TempTable96') IS NOT NULL
        DROP TABLE #TempTable96;
    SELECT * INTO #TempTable96 FROM #TempTable95;
    
    IF OBJECT_ID('tempdb..#TempTable97') IS NOT NULL
        DROP TABLE #TempTable97;
    SELECT * INTO #TempTable97 FROM #TempTable96;
    
    IF OBJECT_ID('tempdb..#TempTable98') IS NOT NULL
        DROP TABLE #TempTable98;
    SELECT * INTO #TempTable98 FROM #TempTable97;
    
    IF OBJECT_ID('tempdb..#TempTable99') IS NOT NULL
        DROP TABLE #TempTable99;
    SELECT * INTO #TempTable99 FROM #TempTable98;
    
    IF OBJECT_ID('tempdb..#TempTable100') IS NOT NULL
        DROP TABLE #TempTable100;
    SELECT * INTO #TempTable100 FROM #TempTable99;
    
    IF OBJECT_ID('tempdb..#TempTable101') IS NOT NULL
        DROP TABLE #TempTable101;
    SELECT * INTO #TempTable101 FROM #TempTable100;
    
    -- Final copy to output table
    TRUNCATE TABLE MyOutputTable;
    INSERT INTO MyOutputTable (Name, Value)
    SELECT Name, Value FROM #TempTable101;
    
    -- Clean up temporary tables
    DROP TABLE #TempTable1;
    DROP TABLE #TempTable2;
    DROP TABLE #TempTable3;
    DROP TABLE #TempTable4;
    DROP TABLE #TempTable5;
    DROP TABLE #TempTable6;
    DROP TABLE #TempTable7;
    DROP TABLE #TempTable8;
    DROP TABLE #TempTable9;
    DROP TABLE #TempTable10;
    DROP TABLE #TempTable11;
    DROP TABLE #TempTable12;
    DROP TABLE #TempTable13;
    DROP TABLE #TempTable14;
    DROP TABLE #TempTable15;
    DROP TABLE #TempTable16;
    DROP TABLE #TempTable17;
    DROP TABLE #TempTable18;
    DROP TABLE #TempTable19;
    DROP TABLE #TempTable20;
    DROP TABLE #TempTable21;
    DROP TABLE #TempTable22;
    DROP TABLE #TempTable23;
    DROP TABLE #TempTable24;
    DROP TABLE #TempTable25;
    DROP TABLE #TempTable26;
    DROP TABLE #TempTable27;
    DROP TABLE #TempTable28;
    DROP TABLE #TempTable29;
    DROP TABLE #TempTable30;
    DROP TABLE #TempTable31;
    DROP TABLE #TempTable32;
    DROP TABLE #TempTable33;
    DROP TABLE #TempTable34;
    DROP TABLE #TempTable35;
    DROP TABLE #TempTable36;
    DROP TABLE #TempTable37;
    DROP TABLE #TempTable38;
    DROP TABLE #TempTable39;
    DROP TABLE #TempTable40;
    DROP TABLE #TempTable41;
    DROP TABLE #TempTable42;
    DROP TABLE #TempTable43;
    DROP TABLE #TempTable44;
    DROP TABLE #TempTable45;
    DROP TABLE #TempTable46;
    DROP TABLE #TempTable47;
    DROP TABLE #TempTable48;
    DROP TABLE #TempTable49;
    DROP TABLE #TempTable50;
    DROP TABLE #TempTable51;
    DROP TABLE #TempTable52;
    DROP TABLE #TempTable53;
    DROP TABLE #TempTable54;
    DROP TABLE #TempTable55;
    DROP TABLE #TempTable56;
    DROP TABLE #TempTable57;
    DROP TABLE #TempTable58;
    DROP TABLE #TempTable59;
    DROP TABLE #TempTable60;
    DROP TABLE #TempTable61;
    DROP TABLE #TempTable62;
    DROP TABLE #TempTable63;
    DROP TABLE #TempTable64;
    DROP TABLE #TempTable65;
    DROP TABLE #TempTable66;
    DROP TABLE #TempTable67;
    DROP TABLE #TempTable68;
    DROP TABLE #TempTable69;
    DROP TABLE #TempTable70;
    DROP TABLE #TempTable71;
    DROP TABLE #TempTable72;
    DROP TABLE #TempTable73;
    DROP TABLE #TempTable74;
    DROP TABLE #TempTable75;
    DROP TABLE #TempTable76;
    DROP TABLE #TempTable77;
    DROP TABLE #TempTable78;
    DROP TABLE #TempTable79;
    DROP TABLE #TempTable80;
    DROP TABLE #TempTable81;
    DROP TABLE #TempTable82;
    DROP TABLE #TempTable83;
    DROP TABLE #TempTable84;
    DROP TABLE #TempTable85;
    DROP TABLE #TempTable86;
    DROP TABLE #TempTable87;
    DROP TABLE #TempTable88;
    DROP TABLE #TempTable89;
    DROP TABLE #TempTable90;
    DROP TABLE #TempTable91;
    DROP TABLE #TempTable92;
    DROP TABLE #TempTable93;
    DROP TABLE #TempTable94;
    DROP TABLE #TempTable95;
    DROP TABLE #TempTable96;
    DROP TABLE #TempTable97;
    DROP TABLE #TempTable98;
    DROP TABLE #TempTable99;
    DROP TABLE #TempTable100;
    DROP TABLE #TempTable101;
END
GO

-- Execute the stored procedure
EXEC ProcessDataThroughTemporaryTables;
GO