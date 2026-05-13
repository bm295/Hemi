IF OBJECT_ID(N'dbo.WorkflowInstance', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.WorkflowInstance (
        Id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        WorkflowId NVARCHAR(128) NOT NULL,
        WorkflowName NVARCHAR(128) NOT NULL,
        CorrelationId NVARCHAR(128) NOT NULL,
        State NVARCHAR(32) NOT NULL,
        PayloadJson NVARCHAR(MAX) NOT NULL,
        LastError NVARCHAR(1024) NULL,
        Version INT NOT NULL,
        CreatedAtUtc DATETIMEOFFSET NOT NULL,
        UpdatedAtUtc DATETIMEOFFSET NOT NULL,
        CompletedAtUtc DATETIMEOFFSET NULL
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'UX_WorkflowInstance_Workflow_Correlation' AND object_id = OBJECT_ID(N'dbo.WorkflowInstance'))
BEGIN
    CREATE UNIQUE INDEX UX_WorkflowInstance_Workflow_Correlation
        ON dbo.WorkflowInstance(WorkflowId, CorrelationId);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_WorkflowInstance_State_UpdatedAtUtc' AND object_id = OBJECT_ID(N'dbo.WorkflowInstance'))
BEGIN
    CREATE INDEX IX_WorkflowInstance_State_UpdatedAtUtc
        ON dbo.WorkflowInstance(State, UpdatedAtUtc);
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
        Status NVARCHAR(32) NOT NULL,
        RetryCount INT NOT NULL,
        ErrorMessage NVARCHAR(1024) NULL,
        CreatedAtUtc DATETIMEOFFSET NOT NULL,
        LastAttemptAtUtc DATETIMEOFFSET NULL,
        NextAttemptAtUtc DATETIMEOFFSET NULL,
        PublishedAtUtc DATETIMEOFFSET NULL,
        CONSTRAINT FK_WorkflowOutboxMessage_WorkflowInstance
            FOREIGN KEY (WorkflowInstanceId) REFERENCES dbo.WorkflowInstance(Id)
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_WorkflowOutboxMessage_Status_NextAttempt_CreatedAt' AND object_id = OBJECT_ID(N'dbo.WorkflowOutboxMessage'))
BEGIN
    CREATE INDEX IX_WorkflowOutboxMessage_Status_NextAttempt_CreatedAt
        ON dbo.WorkflowOutboxMessage(Status, NextAttemptAtUtc, CreatedAtUtc);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_WorkflowOutboxMessage_WorkflowInstanceId' AND object_id = OBJECT_ID(N'dbo.WorkflowOutboxMessage'))
BEGIN
    CREATE INDEX IX_WorkflowOutboxMessage_WorkflowInstanceId
        ON dbo.WorkflowOutboxMessage(WorkflowInstanceId);
END;
GO
