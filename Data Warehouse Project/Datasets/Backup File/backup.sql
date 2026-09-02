/*
=============================================================
Backup DataWarehouse Database
=============================================================
*/

BACKUP DATABASE DataWarehouse
TO DISK = 'D:\projects\06 DataWarehouseProject\Datasets\Backup File\DataWarehouse.bak'
WITH
    FORMAT,
    INIT,
    COMPRESSION,
    STATS = 10;
GO


/*
=============================================================
Verify Backup
=============================================================
*/

RESTORE VERIFYONLY
FROM DISK = 'D:\02 projects\06 DataWarehouseProject\Datasets\Backup File\DataWarehouse.bak';
GO

