IF OBJECT_ID(N'dbo.SagaInstance', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.SagaInstance (
        Id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        SagaType NVARCHAR(128) NOT NULL,
        CorrelationId UNIQUEIDENTIFIER NOT NULL,
        CurrentStep NVARCHAR(128) NOT NULL,
        Status NVARCHAR(32) NOT NULL, -- Started, Waiting, Completed, Failed, Compensating, Compensated
        PayloadJson NVARCHAR(MAX) NOT NULL,
        Version INT NOT NULL,
        CreatedAt DATETIMEOFFSET NOT NULL,
        UpdatedAt DATETIMEOFFSET NOT NULL
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_SagaInstance_CorrelationId' AND object_id = OBJECT_ID(N'dbo.SagaInstance'))
BEGIN
    CREATE UNIQUE INDEX IX_SagaInstance_CorrelationId ON dbo.SagaInstance(CorrelationId);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_SagaInstance_Status' AND object_id = OBJECT_ID(N'dbo.SagaInstance'))
BEGIN
    CREATE INDEX IX_SagaInstance_Status ON dbo.SagaInstance(Status);
END;
GO

IF OBJECT_ID(N'dbo.SagaStep', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.SagaStep (
        Id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        SagaInstanceId UNIQUEIDENTIFIER NOT NULL,
        StepName NVARCHAR(128) NOT NULL,
        StepOrder INT NOT NULL,
        Status NVARCHAR(32) NOT NULL, -- Pending, Sent, Succeeded, Failed, Compensated
        CommandId UNIQUEIDENTIFIER NULL,
        ReplyMessageId UNIQUEIDENTIFIER NULL,
        ErrorMessage NVARCHAR(1024) NULL,
        StartedAt DATETIMEOFFSET NULL,
        CompletedAt DATETIMEOFFSET NULL,
        CONSTRAINT FK_SagaStep_SagaInstance FOREIGN KEY (SagaInstanceId) REFERENCES dbo.SagaInstance(Id)
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_SagaStep_Instance_Order' AND object_id = OBJECT_ID(N'dbo.SagaStep'))
BEGIN
    CREATE UNIQUE INDEX IX_SagaStep_Instance_Order ON dbo.SagaStep(SagaInstanceId, StepOrder);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_SagaStep_Instance_Status' AND object_id = OBJECT_ID(N'dbo.SagaStep'))
BEGIN
    CREATE INDEX IX_SagaStep_Instance_Status ON dbo.SagaStep(SagaInstanceId, Status);
END;
GO

IF OBJECT_ID(N'dbo.OutboxMessage', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.OutboxMessage (
        Id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        SagaInstanceId UNIQUEIDENTIFIER NOT NULL,
        MessageType NVARCHAR(128) NOT NULL,
        Destination NVARCHAR(256) NOT NULL,
        PayloadJson NVARCHAR(MAX) NOT NULL,
        Status NVARCHAR(32) NOT NULL, -- Pending, Published, Failed
        RetryCount INT NOT NULL,
        CreatedAt DATETIMEOFFSET NOT NULL,
        PublishedAt DATETIMEOFFSET NULL,
        CONSTRAINT FK_OutboxMessage_SagaInstance FOREIGN KEY (SagaInstanceId) REFERENCES dbo.SagaInstance(Id)
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_OutboxMessage_Status_CreatedAt' AND object_id = OBJECT_ID(N'dbo.OutboxMessage'))
BEGIN
    CREATE INDEX IX_OutboxMessage_Status_CreatedAt ON dbo.OutboxMessage(Status, CreatedAt);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_OutboxMessage_SagaInstanceId' AND object_id = OBJECT_ID(N'dbo.OutboxMessage'))
BEGIN
    CREATE INDEX IX_OutboxMessage_SagaInstanceId ON dbo.OutboxMessage(SagaInstanceId);
END;
GO

IF OBJECT_ID(N'dbo.InboxMessage', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.InboxMessage (
        MessageId UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        Source NVARCHAR(256) NOT NULL,
        ReceivedAt DATETIMEOFFSET NOT NULL,
        ProcessedAt DATETIMEOFFSET NULL
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_InboxMessage_Source_ReceivedAt' AND object_id = OBJECT_ID(N'dbo.InboxMessage'))
BEGIN
    CREATE INDEX IX_InboxMessage_Source_ReceivedAt ON dbo.InboxMessage(Source, ReceivedAt);
END;
GO
