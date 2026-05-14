IF OBJECT_ID(N'dbo.WorkflowInstance', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.WorkflowInstance (
        Id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        CommandId UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_WorkflowInstance_CommandId DEFAULT NEWID(),
        WorkflowId NVARCHAR(128) NOT NULL,
        WorkflowName NVARCHAR(128) NOT NULL,
        CorrelationId NVARCHAR(128) NOT NULL,
        State NVARCHAR(32) NOT NULL,
        PayloadJson NVARCHAR(MAX) NOT NULL,
        LastError NVARCHAR(1024) NULL,
        Version INT NOT NULL,
        CreatedAtUtc DATETIMEOFFSET NOT NULL,
        UpdatedAtUtc DATETIMEOFFSET NOT NULL,
        CompletedAtUtc DATETIMEOFFSET NULL,
        IdempotencyKey NVARCHAR(256) NULL,
        RequestHash CHAR(64) NULL,
        RequestedBy NVARCHAR(128) NULL,
        Attempt INT NOT NULL CONSTRAINT DF_WorkflowInstance_Attempt DEFAULT 0,
        NextAttemptAtUtc DATETIMEOFFSET NULL,
        LeaseOwner NVARCHAR(128) NULL,
        LeaseUntilUtc DATETIMEOFFSET NULL
    );
END;
GO

IF COL_LENGTH(N'dbo.WorkflowInstance', N'CommandId') IS NULL
BEGIN
    ALTER TABLE dbo.WorkflowInstance
        ADD CommandId UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT DF_WorkflowInstance_CommandId DEFAULT NEWID() WITH VALUES;
END;
GO

IF COL_LENGTH(N'dbo.WorkflowInstance', N'IdempotencyKey') IS NULL
BEGIN
    ALTER TABLE dbo.WorkflowInstance
        ADD IdempotencyKey NVARCHAR(256) NULL;
END;
GO

IF COL_LENGTH(N'dbo.WorkflowInstance', N'RequestHash') IS NULL
BEGIN
    ALTER TABLE dbo.WorkflowInstance
        ADD RequestHash CHAR(64) NULL;
END;
GO

IF COL_LENGTH(N'dbo.WorkflowInstance', N'RequestedBy') IS NULL
BEGIN
    ALTER TABLE dbo.WorkflowInstance
        ADD RequestedBy NVARCHAR(128) NULL;
END;
GO

IF COL_LENGTH(N'dbo.WorkflowInstance', N'Attempt') IS NULL
BEGIN
    ALTER TABLE dbo.WorkflowInstance
        ADD Attempt INT NOT NULL
            CONSTRAINT DF_WorkflowInstance_Attempt DEFAULT 0 WITH VALUES;
END;
GO

IF COL_LENGTH(N'dbo.WorkflowInstance', N'NextAttemptAtUtc') IS NULL
BEGIN
    ALTER TABLE dbo.WorkflowInstance
        ADD NextAttemptAtUtc DATETIMEOFFSET NULL;
END;
GO

IF COL_LENGTH(N'dbo.WorkflowInstance', N'LeaseOwner') IS NULL
BEGIN
    ALTER TABLE dbo.WorkflowInstance
        ADD LeaseOwner NVARCHAR(128) NULL;
END;
GO

IF COL_LENGTH(N'dbo.WorkflowInstance', N'LeaseUntilUtc') IS NULL
BEGIN
    ALTER TABLE dbo.WorkflowInstance
        ADD LeaseUntilUtc DATETIMEOFFSET NULL;
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'UX_WorkflowInstance_Workflow_Correlation' AND object_id = OBJECT_ID(N'dbo.WorkflowInstance'))
BEGIN
    CREATE UNIQUE INDEX UX_WorkflowInstance_Workflow_Correlation
        ON dbo.WorkflowInstance(WorkflowId, CorrelationId);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'UX_WorkflowInstance_IdempotencyKey' AND object_id = OBJECT_ID(N'dbo.WorkflowInstance'))
BEGIN
    CREATE UNIQUE INDEX UX_WorkflowInstance_IdempotencyKey
        ON dbo.WorkflowInstance(IdempotencyKey)
        WHERE IdempotencyKey IS NOT NULL;
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_WorkflowInstance_State_UpdatedAtUtc' AND object_id = OBJECT_ID(N'dbo.WorkflowInstance'))
BEGIN
    CREATE INDEX IX_WorkflowInstance_State_UpdatedAtUtc
        ON dbo.WorkflowInstance(State, UpdatedAtUtc);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_WorkflowInstance_State_NextAttempt_Lease' AND object_id = OBJECT_ID(N'dbo.WorkflowInstance'))
BEGIN
    CREATE INDEX IX_WorkflowInstance_State_NextAttempt_Lease
        ON dbo.WorkflowInstance(State, NextAttemptAtUtc, LeaseUntilUtc);
END;
GO

IF OBJECT_ID(N'dbo.WorkflowStepExecution', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.WorkflowStepExecution (
        Id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        WorkflowInstanceId UNIQUEIDENTIFIER NOT NULL,
        StepName NVARCHAR(128) NOT NULL,
        StepOrder INT NOT NULL,
        Status NVARCHAR(32) NOT NULL,
        Attempt INT NOT NULL,
        CommandId UNIQUEIDENTIFIER NULL,
        ErrorMessage NVARCHAR(1024) NULL,
        StartedAtUtc DATETIMEOFFSET NULL,
        CompletedAtUtc DATETIMEOFFSET NULL,
        CompensatedAtUtc DATETIMEOFFSET NULL,
        CONSTRAINT FK_WorkflowStepExecution_WorkflowInstance
            FOREIGN KEY (WorkflowInstanceId) REFERENCES dbo.WorkflowInstance(Id)
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'UX_WorkflowStepExecution_Instance_Order_Attempt' AND object_id = OBJECT_ID(N'dbo.WorkflowStepExecution'))
BEGIN
    CREATE UNIQUE INDEX UX_WorkflowStepExecution_Instance_Order_Attempt
        ON dbo.WorkflowStepExecution(WorkflowInstanceId, StepOrder, Attempt);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_WorkflowStepExecution_Instance_Status' AND object_id = OBJECT_ID(N'dbo.WorkflowStepExecution'))
BEGIN
    CREATE INDEX IX_WorkflowStepExecution_Instance_Status
        ON dbo.WorkflowStepExecution(WorkflowInstanceId, Status);
END;
GO

IF OBJECT_ID(N'dbo.WorkflowOutboxMessage', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.WorkflowOutboxMessage (
        Id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        WorkflowInstanceId UNIQUEIDENTIFIER NOT NULL,
        MessageType NVARCHAR(128) NOT NULL,
        Destination NVARCHAR(256) NOT NULL,
        PayloadJson NVARCHAR(MAX) NOT NULL,
        HeadersJson NVARCHAR(MAX) NOT NULL CONSTRAINT DF_WorkflowOutboxMessage_HeadersJson DEFAULT N'{}',
        Status NVARCHAR(32) NOT NULL,
        RetryCount INT NOT NULL,
        ErrorMessage NVARCHAR(1024) NULL,
        CreatedAtUtc DATETIMEOFFSET NOT NULL,
        LastAttemptAtUtc DATETIMEOFFSET NULL,
        NextAttemptAtUtc DATETIMEOFFSET NULL,
        PublishedAtUtc DATETIMEOFFSET NULL,
        LeaseOwner NVARCHAR(128) NULL,
        LeaseUntilUtc DATETIMEOFFSET NULL,
        CONSTRAINT FK_WorkflowOutboxMessage_WorkflowInstance
            FOREIGN KEY (WorkflowInstanceId) REFERENCES dbo.WorkflowInstance(Id)
    );
END;
GO

IF COL_LENGTH(N'dbo.WorkflowOutboxMessage', N'HeadersJson') IS NULL
BEGIN
    ALTER TABLE dbo.WorkflowOutboxMessage
        ADD HeadersJson NVARCHAR(MAX) NOT NULL
            CONSTRAINT DF_WorkflowOutboxMessage_HeadersJson DEFAULT N'{}' WITH VALUES;
END;
GO

IF COL_LENGTH(N'dbo.WorkflowOutboxMessage', N'LeaseOwner') IS NULL
BEGIN
    ALTER TABLE dbo.WorkflowOutboxMessage
        ADD LeaseOwner NVARCHAR(128) NULL;
END;
GO

IF COL_LENGTH(N'dbo.WorkflowOutboxMessage', N'LeaseUntilUtc') IS NULL
BEGIN
    ALTER TABLE dbo.WorkflowOutboxMessage
        ADD LeaseUntilUtc DATETIMEOFFSET NULL;
END;
GO

IF EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_WorkflowOutboxMessage_Status_NextAttempt_Lease'
      AND object_id = OBJECT_ID(N'dbo.WorkflowOutboxMessage')
)
AND (
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes i
        INNER JOIN sys.index_columns ic
            ON ic.object_id = i.object_id
           AND ic.index_id = i.index_id
        INNER JOIN sys.columns c
            ON c.object_id = ic.object_id
           AND c.column_id = ic.column_id
        WHERE i.name = N'IX_WorkflowOutboxMessage_Status_NextAttempt_Lease'
          AND i.object_id = OBJECT_ID(N'dbo.WorkflowOutboxMessage')
          AND ic.is_included_column = 0
          AND ic.key_ordinal = 1
          AND c.name = N'Status'
    )
    OR NOT EXISTS (
        SELECT 1
        FROM sys.indexes i
        INNER JOIN sys.index_columns ic
            ON ic.object_id = i.object_id
           AND ic.index_id = i.index_id
        INNER JOIN sys.columns c
            ON c.object_id = ic.object_id
           AND c.column_id = ic.column_id
        WHERE i.name = N'IX_WorkflowOutboxMessage_Status_NextAttempt_Lease'
          AND i.object_id = OBJECT_ID(N'dbo.WorkflowOutboxMessage')
          AND ic.is_included_column = 0
          AND ic.key_ordinal = 2
          AND c.name = N'NextAttemptAtUtc'
    )
    OR NOT EXISTS (
        SELECT 1
        FROM sys.indexes i
        INNER JOIN sys.index_columns ic
            ON ic.object_id = i.object_id
           AND ic.index_id = i.index_id
        INNER JOIN sys.columns c
            ON c.object_id = ic.object_id
           AND c.column_id = ic.column_id
        WHERE i.name = N'IX_WorkflowOutboxMessage_Status_NextAttempt_Lease'
          AND i.object_id = OBJECT_ID(N'dbo.WorkflowOutboxMessage')
          AND ic.is_included_column = 0
          AND ic.key_ordinal = 3
          AND c.name = N'LeaseUntilUtc'
    )
    OR NOT EXISTS (
        SELECT 1
        FROM sys.indexes i
        INNER JOIN sys.index_columns ic
            ON ic.object_id = i.object_id
           AND ic.index_id = i.index_id
        INNER JOIN sys.columns c
            ON c.object_id = ic.object_id
           AND c.column_id = ic.column_id
        WHERE i.name = N'IX_WorkflowOutboxMessage_Status_NextAttempt_Lease'
          AND i.object_id = OBJECT_ID(N'dbo.WorkflowOutboxMessage')
          AND ic.is_included_column = 0
          AND ic.key_ordinal = 4
          AND c.name = N'CreatedAtUtc'
    )
    OR EXISTS (
        SELECT 1
        FROM sys.indexes i
        INNER JOIN sys.index_columns ic
            ON ic.object_id = i.object_id
           AND ic.index_id = i.index_id
        WHERE i.name = N'IX_WorkflowOutboxMessage_Status_NextAttempt_Lease'
          AND i.object_id = OBJECT_ID(N'dbo.WorkflowOutboxMessage')
          AND ic.is_included_column = 0
          AND ic.key_ordinal > 4
    )
)
BEGIN
    DROP INDEX IX_WorkflowOutboxMessage_Status_NextAttempt_Lease
        ON dbo.WorkflowOutboxMessage;
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_WorkflowOutboxMessage_Status_NextAttempt_Lease' AND object_id = OBJECT_ID(N'dbo.WorkflowOutboxMessage'))
BEGIN
    CREATE INDEX IX_WorkflowOutboxMessage_Status_NextAttempt_Lease
        ON dbo.WorkflowOutboxMessage(Status, NextAttemptAtUtc, LeaseUntilUtc, CreatedAtUtc);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_WorkflowOutboxMessage_WorkflowInstanceId' AND object_id = OBJECT_ID(N'dbo.WorkflowOutboxMessage'))
BEGIN
    CREATE INDEX IX_WorkflowOutboxMessage_WorkflowInstanceId
        ON dbo.WorkflowOutboxMessage(WorkflowInstanceId);
END;
GO
