IF OBJECT_ID(N'dbo.FnbRestaurantProfile', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.FnbRestaurantProfile (
        Id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        Name NVARCHAR(128) NOT NULL,
        Location NVARCHAR(256) NOT NULL,
        SeatCapacityMinimum INT NOT NULL,
        SeatCapacityMaximum INT NOT NULL,
        CONSTRAINT CK_FnbRestaurantProfile_SeatCapacity
            CHECK (SeatCapacityMinimum > 0 AND SeatCapacityMaximum >= SeatCapacityMinimum)
    );
END;
GO

IF OBJECT_ID(N'dbo.FnbDiningTable', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.FnbDiningTable (
        Id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        Code NVARCHAR(16) NOT NULL,
        Capacity INT NOT NULL,
        Status NVARCHAR(32) NOT NULL,
        CONSTRAINT CK_FnbDiningTable_Capacity CHECK (Capacity > 0),
        CONSTRAINT CK_FnbDiningTable_Status
            CHECK (Status IN (N'Available', N'Occupied', N'Reserved'))
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'UX_FnbDiningTable_Code' AND object_id = OBJECT_ID(N'dbo.FnbDiningTable'))
BEGIN
    CREATE UNIQUE INDEX UX_FnbDiningTable_Code
        ON dbo.FnbDiningTable(Code);
END;
GO

IF OBJECT_ID(N'dbo.FnbMenuItem', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.FnbMenuItem (
        Id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        Name NVARCHAR(128) NOT NULL,
        Category NVARCHAR(64) NOT NULL,
        Price DECIMAL(18, 2) NOT NULL,
        IsAvailable BIT NOT NULL,
        CONSTRAINT CK_FnbMenuItem_Price CHECK (Price >= 0)
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_FnbMenuItem_Category_Name' AND object_id = OBJECT_ID(N'dbo.FnbMenuItem'))
BEGIN
    CREATE INDEX IX_FnbMenuItem_Category_Name
        ON dbo.FnbMenuItem(Category, Name);
END;
GO

IF OBJECT_ID(N'dbo.FnbServiceOrder', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.FnbServiceOrder (
        Id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        TableId UNIQUEIDENTIFIER NOT NULL,
        CreatedAtUtc DATETIMEOFFSET NOT NULL,
        Status NVARCHAR(32) NOT NULL,
        CONSTRAINT FK_FnbServiceOrder_DiningTable
            FOREIGN KEY (TableId) REFERENCES dbo.FnbDiningTable(Id),
        CONSTRAINT CK_FnbServiceOrder_Status
            CHECK (Status IN (N'Open', N'SentToKitchen', N'Completed', N'Cancelled'))
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_FnbServiceOrder_Status_CreatedAtUtc' AND object_id = OBJECT_ID(N'dbo.FnbServiceOrder'))
BEGIN
    CREATE INDEX IX_FnbServiceOrder_Status_CreatedAtUtc
        ON dbo.FnbServiceOrder(Status, CreatedAtUtc);
END;
GO

IF OBJECT_ID(N'dbo.FnbOrderLine', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.FnbOrderLine (
        OrderId UNIQUEIDENTIFIER NOT NULL,
        MenuItemId UNIQUEIDENTIFIER NOT NULL,
        Quantity INT NOT NULL,
        UnitPrice DECIMAL(18, 2) NOT NULL,
        CONSTRAINT PK_FnbOrderLine PRIMARY KEY (OrderId, MenuItemId),
        CONSTRAINT FK_FnbOrderLine_ServiceOrder
            FOREIGN KEY (OrderId) REFERENCES dbo.FnbServiceOrder(Id) ON DELETE CASCADE,
        CONSTRAINT FK_FnbOrderLine_MenuItem
            FOREIGN KEY (MenuItemId) REFERENCES dbo.FnbMenuItem(Id),
        CONSTRAINT CK_FnbOrderLine_Quantity CHECK (Quantity > 0),
        CONSTRAINT CK_FnbOrderLine_UnitPrice CHECK (UnitPrice >= 0)
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_FnbOrderLine_MenuItemId' AND object_id = OBJECT_ID(N'dbo.FnbOrderLine'))
BEGIN
    CREATE INDEX IX_FnbOrderLine_MenuItemId
        ON dbo.FnbOrderLine(MenuItemId);
END;
GO

IF OBJECT_ID(N'dbo.FnbPayment', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.FnbPayment (
        Id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        OrderId UNIQUEIDENTIFIER NOT NULL,
        Amount DECIMAL(18, 2) NOT NULL,
        Method NVARCHAR(32) NOT NULL,
        Status NVARCHAR(32) NOT NULL,
        PaidAtUtc DATETIMEOFFSET NOT NULL,
        CONSTRAINT FK_FnbPayment_ServiceOrder
            FOREIGN KEY (OrderId) REFERENCES dbo.FnbServiceOrder(Id),
        CONSTRAINT CK_FnbPayment_Amount CHECK (Amount > 0),
        CONSTRAINT CK_FnbPayment_Method
            CHECK (Method IN (N'Cash', N'Card', N'BankTransfer', N'EWallet')),
        CONSTRAINT CK_FnbPayment_Status
            CHECK (Status IN (N'Pending', N'Settled', N'Failed', N'Refunded'))
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_FnbPayment_OrderId' AND object_id = OBJECT_ID(N'dbo.FnbPayment'))
BEGIN
    CREATE INDEX IX_FnbPayment_OrderId
        ON dbo.FnbPayment(OrderId);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'UX_FnbPayment_Order_Settled' AND object_id = OBJECT_ID(N'dbo.FnbPayment'))
BEGIN
    CREATE UNIQUE INDEX UX_FnbPayment_Order_Settled
        ON dbo.FnbPayment(OrderId)
        WHERE Status = N'Settled';
END;
GO

IF OBJECT_ID(N'dbo.FnbReservation', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.FnbReservation (
        Id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        GuestName NVARCHAR(128) NOT NULL,
        PartySize INT NOT NULL,
        ReservedForUtc DATETIMEOFFSET NOT NULL,
        ContactPhone NVARCHAR(32) NOT NULL,
        Notes NVARCHAR(512) NULL,
        Status NVARCHAR(32) NOT NULL,
        CONSTRAINT CK_FnbReservation_PartySize CHECK (PartySize > 0),
        CONSTRAINT CK_FnbReservation_Status
            CHECK (Status IN (N'Pending', N'Confirmed', N'Seated', N'Cancelled'))
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_FnbReservation_ReservedForUtc_Status' AND object_id = OBJECT_ID(N'dbo.FnbReservation'))
BEGIN
    CREATE INDEX IX_FnbReservation_ReservedForUtc_Status
        ON dbo.FnbReservation(ReservedForUtc, Status);
END;
GO

IF OBJECT_ID(N'dbo.FnbInventoryItem', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.FnbInventoryItem (
        Id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        MenuItemId UNIQUEIDENTIFIER NOT NULL,
        Name NVARCHAR(128) NOT NULL,
        StockQuantity DECIMAL(18, 3) NOT NULL,
        Unit NVARCHAR(32) NOT NULL,
        CONSTRAINT FK_FnbInventoryItem_MenuItem
            FOREIGN KEY (MenuItemId) REFERENCES dbo.FnbMenuItem(Id),
        CONSTRAINT CK_FnbInventoryItem_StockQuantity CHECK (StockQuantity >= 0)
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'UX_FnbInventoryItem_MenuItemId' AND object_id = OBJECT_ID(N'dbo.FnbInventoryItem'))
BEGIN
    CREATE UNIQUE INDEX UX_FnbInventoryItem_MenuItemId
        ON dbo.FnbInventoryItem(MenuItemId);
END;
GO

IF OBJECT_ID(N'dbo.FnbStockMovement', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.FnbStockMovement (
        Id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        InventoryItemId UNIQUEIDENTIFIER NOT NULL,
        OrderId UNIQUEIDENTIFIER NOT NULL,
        QuantityChanged DECIMAL(18, 3) NOT NULL,
        OccurredAtUtc DATETIMEOFFSET NOT NULL,
        Reason NVARCHAR(128) NOT NULL,
        CONSTRAINT FK_FnbStockMovement_InventoryItem
            FOREIGN KEY (InventoryItemId) REFERENCES dbo.FnbInventoryItem(Id),
        CONSTRAINT FK_FnbStockMovement_ServiceOrder
            FOREIGN KEY (OrderId) REFERENCES dbo.FnbServiceOrder(Id),
        CONSTRAINT CK_FnbStockMovement_QuantityChanged CHECK (QuantityChanged <> 0)
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_FnbStockMovement_OrderId' AND object_id = OBJECT_ID(N'dbo.FnbStockMovement'))
BEGIN
    CREATE INDEX IX_FnbStockMovement_OrderId
        ON dbo.FnbStockMovement(OrderId);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'UX_FnbStockMovement_Order_Inventory_Reason' AND object_id = OBJECT_ID(N'dbo.FnbStockMovement'))
BEGIN
    CREATE UNIQUE INDEX UX_FnbStockMovement_Order_Inventory_Reason
        ON dbo.FnbStockMovement(OrderId, InventoryItemId, Reason);
END;
GO

MERGE dbo.FnbRestaurantProfile AS Target
USING (VALUES
    (CONVERT(uniqueidentifier, N'10000000-0000-0000-0000-000000000001'),
     N'Hemi Steak & Seafood Grill',
     N'Not specified',
     60,
     80)
) AS Source (Id, Name, Location, SeatCapacityMinimum, SeatCapacityMaximum)
ON Target.Id = Source.Id
WHEN MATCHED THEN
    UPDATE SET
        Name = Source.Name,
        Location = Source.Location,
        SeatCapacityMinimum = Source.SeatCapacityMinimum,
        SeatCapacityMaximum = Source.SeatCapacityMaximum
WHEN NOT MATCHED THEN
    INSERT (Id, Name, Location, SeatCapacityMinimum, SeatCapacityMaximum)
    VALUES (Source.Id, Source.Name, Source.Location, Source.SeatCapacityMinimum, Source.SeatCapacityMaximum);
GO

MERGE dbo.FnbDiningTable AS Target
USING (VALUES
    (CONVERT(uniqueidentifier, N'20000000-0000-0000-0000-000000000001'), N'T01', 4, N'Available'),
    (CONVERT(uniqueidentifier, N'20000000-0000-0000-0000-000000000002'), N'T02', 4, N'Available'),
    (CONVERT(uniqueidentifier, N'20000000-0000-0000-0000-000000000003'), N'T03', 4, N'Reserved'),
    (CONVERT(uniqueidentifier, N'20000000-0000-0000-0000-000000000004'), N'T04', 4, N'Occupied'),
    (CONVERT(uniqueidentifier, N'20000000-0000-0000-0000-000000000005'), N'T05', 6, N'Available'),
    (CONVERT(uniqueidentifier, N'20000000-0000-0000-0000-000000000006'), N'T06', 6, N'Available'),
    (CONVERT(uniqueidentifier, N'20000000-0000-0000-0000-000000000007'), N'T07', 6, N'Reserved'),
    (CONVERT(uniqueidentifier, N'20000000-0000-0000-0000-000000000008'), N'T08', 8, N'Available'),
    (CONVERT(uniqueidentifier, N'20000000-0000-0000-0000-000000000009'), N'T09', 8, N'Occupied'),
    (CONVERT(uniqueidentifier, N'20000000-0000-0000-0000-000000000010'), N'T10', 8, N'Available'),
    (CONVERT(uniqueidentifier, N'20000000-0000-0000-0000-000000000011'), N'P01', 10, N'Available'),
    (CONVERT(uniqueidentifier, N'20000000-0000-0000-0000-000000000012'), N'P02', 10, N'Reserved')
) AS Source (Id, Code, Capacity, Status)
ON Target.Id = Source.Id
WHEN MATCHED THEN
    UPDATE SET
        Code = Source.Code,
        Capacity = Source.Capacity
WHEN NOT MATCHED THEN
    INSERT (Id, Code, Capacity, Status)
    VALUES (Source.Id, Source.Code, Source.Capacity, Source.Status);
GO

MERGE dbo.FnbMenuItem AS Target
USING (VALUES
    (CONVERT(uniqueidentifier, N'30000000-0000-0000-0000-000000000001'), N'USDA Prime Ribeye', N'Steak', 1350000.00, CONVERT(bit, 1)),
    (CONVERT(uniqueidentifier, N'30000000-0000-0000-0000-000000000002'), N'Grilled Lobster Tail', N'Seafood', 1490000.00, CONVERT(bit, 1)),
    (CONVERT(uniqueidentifier, N'30000000-0000-0000-0000-000000000003'), N'Pan-Seared Salmon', N'Seafood', 690000.00, CONVERT(bit, 1)),
    (CONVERT(uniqueidentifier, N'30000000-0000-0000-0000-000000000004'), N'Wagyu Beef Carpaccio', N'Starter', 480000.00, CONVERT(bit, 1)),
    (CONVERT(uniqueidentifier, N'30000000-0000-0000-0000-000000000005'), N'Caesar Salad', N'Starter', 260000.00, CONVERT(bit, 1)),
    (CONVERT(uniqueidentifier, N'30000000-0000-0000-0000-000000000006'), N'Chocolate Lava Cake', N'Dessert', 240000.00, CONVERT(bit, 1)),
    (CONVERT(uniqueidentifier, N'30000000-0000-0000-0000-000000000007'), N'Seasonal Oyster Platter', N'Seafood', 820000.00, CONVERT(bit, 0))
) AS Source (Id, Name, Category, Price, IsAvailable)
ON Target.Id = Source.Id
WHEN MATCHED THEN
    UPDATE SET
        Name = Source.Name,
        Category = Source.Category,
        Price = Source.Price,
        IsAvailable = Source.IsAvailable
WHEN NOT MATCHED THEN
    INSERT (Id, Name, Category, Price, IsAvailable)
    VALUES (Source.Id, Source.Name, Source.Category, Source.Price, Source.IsAvailable);
GO

MERGE dbo.FnbInventoryItem AS Target
USING (VALUES
    (CONVERT(uniqueidentifier, N'40000000-0000-0000-0000-000000000001'), CONVERT(uniqueidentifier, N'30000000-0000-0000-0000-000000000001'), N'USDA Prime Ribeye', 100.000, N'portion'),
    (CONVERT(uniqueidentifier, N'40000000-0000-0000-0000-000000000002'), CONVERT(uniqueidentifier, N'30000000-0000-0000-0000-000000000002'), N'Grilled Lobster Tail', 100.000, N'portion'),
    (CONVERT(uniqueidentifier, N'40000000-0000-0000-0000-000000000003'), CONVERT(uniqueidentifier, N'30000000-0000-0000-0000-000000000003'), N'Pan-Seared Salmon', 100.000, N'portion'),
    (CONVERT(uniqueidentifier, N'40000000-0000-0000-0000-000000000004'), CONVERT(uniqueidentifier, N'30000000-0000-0000-0000-000000000004'), N'Wagyu Beef Carpaccio', 100.000, N'portion'),
    (CONVERT(uniqueidentifier, N'40000000-0000-0000-0000-000000000005'), CONVERT(uniqueidentifier, N'30000000-0000-0000-0000-000000000005'), N'Caesar Salad', 100.000, N'portion'),
    (CONVERT(uniqueidentifier, N'40000000-0000-0000-0000-000000000006'), CONVERT(uniqueidentifier, N'30000000-0000-0000-0000-000000000006'), N'Chocolate Lava Cake', 100.000, N'portion'),
    (CONVERT(uniqueidentifier, N'40000000-0000-0000-0000-000000000007'), CONVERT(uniqueidentifier, N'30000000-0000-0000-0000-000000000007'), N'Seasonal Oyster Platter', 100.000, N'portion')
) AS Source (Id, MenuItemId, Name, StockQuantity, Unit)
ON Target.Id = Source.Id
WHEN MATCHED THEN
    UPDATE SET
        MenuItemId = Source.MenuItemId,
        Name = Source.Name,
        Unit = Source.Unit
WHEN NOT MATCHED THEN
    INSERT (Id, MenuItemId, Name, StockQuantity, Unit)
    VALUES (Source.Id, Source.MenuItemId, Source.Name, Source.StockQuantity, Source.Unit);
GO

MERGE dbo.FnbReservation AS Target
USING (VALUES
    (CONVERT(uniqueidentifier, N'50000000-0000-0000-0000-000000000001'), N'Nguyen Minh Anh', 4, CONVERT(datetimeoffset, N'2030-01-15T19:00:00+00:00'), N'0901234567', N'Window-side table', N'Confirmed'),
    (CONVERT(uniqueidentifier, N'50000000-0000-0000-0000-000000000002'), N'Tran Hoang Long', 6, CONVERT(datetimeoffset, N'2030-01-15T21:00:00+00:00'), N'0912345678', NULL, N'Pending')
) AS Source (Id, GuestName, PartySize, ReservedForUtc, ContactPhone, Notes, Status)
ON Target.Id = Source.Id
WHEN MATCHED THEN
    UPDATE SET
        GuestName = Source.GuestName,
        PartySize = Source.PartySize,
        ReservedForUtc = Source.ReservedForUtc,
        ContactPhone = Source.ContactPhone,
        Notes = Source.Notes,
        Status = Source.Status
WHEN NOT MATCHED THEN
    INSERT (Id, GuestName, PartySize, ReservedForUtc, ContactPhone, Notes, Status)
    VALUES (Source.Id, Source.GuestName, Source.PartySize, Source.ReservedForUtc, Source.ContactPhone, Source.Notes, Source.Status);
GO
